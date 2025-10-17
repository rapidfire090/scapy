# Python 3.8+
# Merge last COMPLETE hour of latency_5m -> latency_1h
# Groups by (component, session); writes p50,p90,p95,p99,p99_9,min,max,count,unit,histo_b64
# Depends on the same HDR binding as your downsampler (hdrhistogram or hdrh).

from typing import Dict, Any, List, Tuple, DefaultDict
from collections import defaultdict
from time import time_ns
from datetime import datetime, timezone, timedelta
import base64

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

SIGFIGS = 4
LOWEST  = 1
HIGHEST = 1_000_000_000  # 1s in ns
UNIT = "ns"
PCTS = (50.0, 90.0, 95.0, 99.0, 99.9)

def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _decode_hdr(b64: str):
    raw = base64.b64decode(b64)
    if hasattr(_hdr_cls, "decode"):
        return _hdr_cls.decode(raw)
    if hasattr(_hdr_cls, "from_byte_array"):
        return _hdr_cls.from_byte_array(raw)
    raise RuntimeError("HDR binding lacks decode().")

def _encode_hdr(h) -> str:
    if hasattr(h, "encode"):
        return base64.b64encode(h.encode()).decode("ascii")
    if hasattr(h, "to_byte_array"):
        return base64.b64encode(h.to_byte_array()).decode("ascii")
    raise RuntimeError("HDR binding lacks encode().")

def _merge_into(target, src):
    if hasattr(target, "add"):
        target.add(src)
    else:
        # very rare fallback
        it = getattr(src, "recorded_values", None)
        if it is None:
            raise RuntimeError("HDR binding lacks add() and recorded_values().")
        for v, c in it():
            for _ in range(int(c)):
                target.record_value(int(v))

def process_scheduled_call(influxdb3_local, call_time: str, args):
    if _hdr_cls is None:
        raise RuntimeError(
            "Install HDR into engine venv: influxdb3 install package hdrhistogram"
        )

    # Last COMPLETE hour: [now-1h floored .. now floored)
    now = datetime.now(timezone.utc)
    end  = now.replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(hours=1)
    start_iso, end_iso = _iso_utc(start), _iso_utc(end)

    q = f"""
      SELECT "component","session","histo_b64","min","max","count"
      FROM latency_5m
      WHERE "time" >= TIMESTAMP '{start_iso}'
        AND "time"  < TIMESTAMP '{end_iso}'
    """
    rows = influxdb3_local.query(q, {}) or []
    if not rows:
        influxdb3_local.info("hdr_rollup_1h: no rows to roll up", {"window": f"{start_iso}..{end_iso}"})
        return

    buckets: DefaultDict[Tuple[str,str], List[Dict[str,Any]]] = defaultdict(list)
    for r in rows:
        comp = "" if r.get("component") is None else str(r["component"])
        sess = "" if r.get("session")   is None else str(r["session"])
        buckets[(comp, sess)].append(r)

    wrote = 0
    for (comp, sess), rs in buckets.items():
        merged = _hdr_cls(LOWEST, HIGHEST, SIGFIGS)
        gmin, gmax, gcount = None, None, 0

        for r in rs:
            hb64 = r.get("histo_b64")
            if hb64:
                _merge_into(merged, _decode_hdr(hb64))
            vmin, vmax, cnt = r.get("min"), r.get("max"), r.get("count")
            if vmin is not None: gmin = vmin if gmin is None else min(gmin, vmin)
            if vmax is not None: gmax = vmax if gmax is None else max(gmax, vmax)
            if cnt  is not None: gcount += int(cnt)

        if merged.total_count == 0:
            continue

        lb = LineBuilder("latency_1h")
        lb.tag("component", comp)
        lb.tag("session",   sess)
        for p in PCTS:
            lb.float64_field("p" + str(p).replace(".","_"), merged.get_value_at_percentile(p))
        lb.float64_field("min",  gmin if gmin is not None else merged.get_min_value())
        lb.float64_field("max",  gmax if gmax is not None else merged.get_max_value())
        lb.uint64_field("count", gcount)
        lb.string_field("unit",  UNIT)
        lb.string_field("histo_b64", _encode_hdr(merged))
        # Timestamp at end of hour window
        lb.time_ns(int(end.timestamp() * 1e9))
        influxdb3_local.write(lb)
        wrote += 1

    influxdb3_local.info("hdr_rollup_1h: wrote buckets", {"count": wrote, "window": f"{start_iso}..{end_iso}"})
