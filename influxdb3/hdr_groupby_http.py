# Dynamic group-by HDR aggregator over latency_5m
# - Merges 5m histograms (histo_b64) across ANY requested grouping and order
# - Returns one row per unique group with requested percentiles + min/max/count
#
# Request JSON:
# {
#   "start": "2025-10-16T00:00:00Z",
#   "end":   "2025-10-16T01:00:00Z",
#   "percentiles": "50@95@99@99.9",          # optional (default set used if omitted)
#   "group_by": "channel@source",            # optional; empty or missing = single merged result
#   "filters": { "component": ["ingest"] },  # optional tag filters
#   "min_count": 0,                          # optional; filter out tiny groups
#   "order_by": "p99",                       # optional; sort groups by a field
#   "order_dir": "desc",                     # optional; asc|desc
#   "limit": 100                             # optional; limit number of groups returned
# }

from typing import Dict, Any, List, Tuple, DefaultDict
from collections import defaultdict
import base64

# Try common HDR bindings
_hdr_cls = None
_err = None
try:
    from hdrhistogram import HdrHistogram as _Hdr
    _hdr_cls = _Hdr
except Exception as e1:
    _err = e1
    try:
        from hdrh.histogram import HdrHistogram as _Hdr
        _hdr_cls = _Hdr
    except Exception as e2:
        _err = (e1, e2)

# Must MATCH your downsampler settings
SIGFIGS = 3
LOWEST  = 1
HIGHEST = 30_000_000_000   # 30s in ns
UNIT    = "ns"

DEFAULT_PCTS = [50.0, 90.0, 95.0, 99.0, 99.9]

def _require_hdr():
    if _hdr_cls is None:
        raise RuntimeError(
            "HDRHistogram not found in engine venv. Install:\n"
            "  influxdb3 install package hdrhistogram\n"
            f"Import errors: {_err}"
        )

def _decode_hdr(b64: str):
    raw = base64.b64decode(b64)
    if hasattr(_hdr_cls, "decode"):
        return _hdr_cls.decode(raw)
    if hasattr(_hdr_cls, "from_byte_array"):
        return _hdr_cls.from_byte_array(raw)
    raise RuntimeError("HDR binding lacks decode().")

def _new_hdr():
    return _hdr_cls(LOWEST, HIGHEST, SIGFIGS)

def _merge_into(target, src):
    if hasattr(target, "add"):
        target.add(src)
    else:
        it = getattr(src, "recorded_values", None)
        if it is None:
            raise RuntimeError("HDR binding lacks add() and recorded_values().")
        for v, c in it():
            for _ in range(int(c)):
                target.record_value(int(v))

def _parse_pcts(spec: str) -> List[float]:
    if not spec or not spec.strip():
        return DEFAULT_PCTS
    out: List[float] = []
    seen = set()
    for tok in spec.split("@"):
        tok = tok.strip()
        if not tok:
            continue
        try:
            p = float(tok)
            if 0.0 < p < 100.0 and p not in seen:
                out.append(p); seen.add(p)
        except ValueError:
            pass
    return out or DEFAULT_PCTS

def process_http_call(influxdb3_local, request):
    _require_hdr()

    body = request.json()
    start = body.get("start")
    end   = body.get("end")
    if not start or not end:
        return {"error": "start and end are required RFC3339 timestamps"}

    pct_list = _parse_pcts(body.get("percentiles", ""))
    group_by_str = body.get("group_by", "") or ""
    group_tags: List[str] = [t.strip() for t in group_by_str.split("@") if t.strip()]
    filters: Dict[str, List[str]] = body.get("filters") or {}
    min_count = int(body.get("min_count", 0) or 0)
    order_by  = body.get("order_by") or None   # e.g., "p99" or "count"
    order_dir = (body.get("order_dir") or "desc").lower()
    limit     = int(body.get("limit", 0) or 0)

    # Build WHERE clause from time + filters
    where = [f"\"time\" >= TIMESTAMP '{start}'", f"\"time\" < TIMESTAMP '{end}'"]
    for tag, vals in filters.items():
        if not isinstance(vals, list) or not vals:
            continue
        quoted = ",".join(f"'{str(v)}'" for v in vals)
        where.append(f"\"{tag}\" IN ({quoted})")
    where_sql = " AND ".join(where)

    # Pull only what we need from latency_5m
    # We need tags that might be used in group_by, plus min/max/count and histo_b64
    select_cols = ["\"histo_b64\"", "\"min\"", "\"max\"", "\"count\""]
    # Include all potential tag columns we could group by (safe to include extra)
    # Add the tags you use in latency_5m:
    possible_tags = ["component", "session", "channel", "source", "env", "region"]
    for t in possible_tags:
        select_cols.append(f"\"{t}\"")
    q = f"""
      SELECT {", ".join(select_cols)}
      FROM latency_5m
      WHERE {where_sql}
    """

    rows = influxdb3_local.query(q, {}) or []
    if not rows:
        return {"groups": [], "total_groups": 0, "window": {"start": start, "end": end}}

    # Bucket key is ORDERED by requested group_tags, so ("channel","source") != ("source","channel") in output
    buckets: DefaultDict[Tuple[Tuple[str,str], ...], Dict[str, Any]] = defaultdict(lambda: {
        "hdr": _new_hdr(),
        "min": None, "max": None, "count": 0,
        "tags": {}  # for echoing the group tags in requested order
    })

    # Merge rows into buckets
    for r in rows:
        hb64 = r.get("histo_b64")
        if not hb64:
            continue

        # Build the group key in the ORDER the user requested
        key_pairs: List[Tuple[str,str]] = []
        for t in group_tags:
            v = r.get(t)
            key_pairs.append((t, "" if v is None else str(v)))
        key = tuple(key_pairs)

        b = buckets[key]
        try:
            h = _decode_hdr(hb64)
            _merge_into(b["hdr"], h)
        except Exception:
            # skip un-decodable rows
            continue

        # Aggregate min/max/count
        vmin, vmax, cnt = r.get("min"), r.get("max"), r.get("count")
        if vmin is not None: b["min"] = vmin if b["min"] is None else min(b["min"], vmin)
        if vmax is not None: b["max"] = vmax if b["max"] is None else max(b["max"], vmax)
        if cnt  is not None: b["count"] = b["count"] + int(cnt)

        # Save tag values for output
        for t, val in key_pairs:
            b["tags"][t] = val

    # Build response objects
    out_rows: List[Dict[str, Any]] = []
    for key, b in buckets.items():
        if b["hdr"].total_count == 0:
            continue
        if b["count"] < min_count:
            continue

        row = {"unit": UNIT, "count": b["count"], "min": b["min"], "max": b["max"], "tags": {}}
        # tags in requested order
        for t, v in key:
            row["tags"][t] = v

        for p in pct_list:
            fname = "p" + str(p).replace(".", "_")
            row[fname] = b["hdr"].get_value_at_percentile(p)

        out_rows.append(row)

    # Ordering and limiting
    if order_by:
        rev = (order_dir != "asc")
        out_rows.sort(key=lambda r: r.get(order_by, 0), reverse=rev)
    if limit and limit > 0:
        out_rows = out_rows[:limit]

    return {
        "groups": out_rows,
        "total_groups": len(out_rows),
        "window": {"start": start, "end": end}
    }