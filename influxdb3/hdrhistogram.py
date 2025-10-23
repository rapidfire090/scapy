# ~/.plugins/hdr_downsample_5m.py
# Python 3.8+
# Downsample streaming2(latency ns) → latency_5m
# Buckets align on :00/:05/:10/... (every 5 min) and run with a +2m ingest delay.

from typing import Dict, Any, List, Tuple, DefaultDict
from collections import defaultdict
from time import time_ns
from datetime import datetime, timezone, timedelta
import base64

# ---- HDR binding detection ----
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

# ---- Defaults (overridable via trigger-arguments) ----
SIGFIGS_DEFAULT = 3
LOWEST_DEFAULT = 1
HIGHEST_DEFAULT = 30_000_000_000  # 30s in ns
UNIT = "ns"
PCTS = (50.0, 90.0, 95.0, 99.0, 99.9)
INGEST_DELAY_MIN_DEFAULT = 2      # evaluate window end as (now - delay)

# ---- Helpers ----
def _args_or_empty(a):
    return a if isinstance(a, dict) else {}

def _require_hdr():
    if _hdr_cls is None:
        raise RuntimeError(
            "HDRHistogram not found. Install in engine venv:\n"
            "  influxdb3 install package hdrhistogram\n"
            f"Import errors: {_err}"
        )

def _encode_hist(h) -> str:
    if hasattr(h, "encode"):
        b = h.encode()
        return base64.b64encode(b).decode("ascii")
    if hasattr(h, "to_byte_array"):
        b = h.to_byte_array()
        return base64.b64encode(b).decode("ascii")
    raise RuntimeError("HDR binding lacks encode(); cannot serialize histogram.")

def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _align_to_5m_boundary(dt: datetime) -> datetime:
    """Floor dt to the latest :00/:05/:10/... boundary."""
    # Remove seconds/micros, then subtract minute remainder mod 5
    dt = dt.replace(second=0, microsecond=0)
    rem = dt.minute % 5
    return dt - timedelta(minutes=rem)

# ---- Entry point ----
def process_scheduled_call(influxdb3_local, call_time: str, args):
    """
    Optional trigger-arguments (CSV k=v):
      lowest=1
      highest=30000000000
      sigfigs=3
      ingest_delay_min=2
      extra_tags=k=v,k2=v2
    """
    _require_hdr()
    args = _args_or_empty(args)

    # Config
    try:
        lowest  = int(args.get("lowest",  str(LOWEST_DEFAULT)))
        highest = int(args.get("highest", str(HIGHEST_DEFAULT)))
        sigfigs = int(args.get("sigfigs", str(SIGFIGS_DEFAULT)))
    except Exception:
        lowest, highest, sigfigs = LOWEST_DEFAULT, HIGHEST_DEFAULT, SIGFIGS_DEFAULT

    try:
        ingest_delay_min = int(args.get("ingest_delay_min", str(INGEST_DELAY_MIN_DEFAULT)))
        if ingest_delay_min < 0:
            ingest_delay_min = INGEST_DELAY_MIN_DEFAULT
    except Exception:
        ingest_delay_min = INGEST_DELAY_MIN_DEFAULT

    extra = args.get("extra_tags", "") or ""

    # --- Compute aligned window ---
    # 1) shift now by ingest delay
    now_utc = datetime.now(timezone.utc)
    shifted = now_utc - timedelta(minutes=ingest_delay_min)
    # 2) floor to :00/:05/:10/... boundary
    end_dt = _align_to_5m_boundary(shifted)
    # 3) 5-minute window ending at that boundary
    start_dt = end_dt - timedelta(minutes=5)

    start_iso = _iso_utc(start_dt)
    end_iso   = _iso_utc(end_dt)

    # Build query (explicit TIMESTAMP literals and quoted identifiers)
    q = f"""
      SELECT "time","component","session","latency"
      FROM streaming2
      WHERE "time" >= TIMESTAMP '{start_iso}'
        AND "time"  < TIMESTAMP '{end_iso}'
      ORDER BY "time" ASC
    """
    rows = influxdb3_local.query(q, {}) or []
    if not rows:
        influxdb3_local.info("hdr_downsample_5m: no rows in window",
                             {"window": f"{start_iso}..{end_iso}", "delay_min": ingest_delay_min})
        return

    # Group by (component, session)
    buckets: DefaultDict[Tuple[str, str], List[int]] = defaultdict(list)
    for r in rows:
        comp = "" if r.get("component") is None else str(r["component"])
        sess = "" if r.get("session")   is None else str(r["session"])
        v = r.get("latency")
        if v is None:
            continue
        try:
            x = int(v)
        except Exception:
            continue
        if x < 0:
            continue
        buckets[(comp, sess)].append(x)

    if not buckets:
        influxdb3_local.info("hdr_downsample_5m: no valid samples after filtering",
                             {"window": f"{start_iso}..{end_iso}"})
        return

    # Parse extra tags once
    extra_tag_pairs = []
    if extra:
        for kv in extra.split(","):
            kv = kv.strip()
            if kv and "=" in kv:
                k, v = kv.split("=", 1)
                extra_tag_pairs.append((k.strip(), v.strip()))

    wrote = 0
    for (comp, sess), samples in buckets.items():
        if not samples:
            continue

        h = _hdr_cls(lowest, highest, sigfigs)
        for x in samples:
            try:
                h.record_value(x)
            except Exception:
                pass

        if getattr(h, "total_count", 0) == 0:
            continue

        lb = LineBuilder("latency_5m")
        lb.tag("component", comp)
        lb.tag("session",   sess)
        for k, v in extra_tag_pairs:
            lb.tag(k, v)

        # Percentiles
        for p in PCTS:
            fname = "p" + str(p).replace(".", "_")
            lb.float64_field(fname, h.get_value_at_percentile(p))

        lb.float64_field("min",  h.get_min_value())
        lb.float64_field("max",  h.get_max_value())
        lb.uint64_field("count", int(h.total_count))
        lb.string_field("unit",  UNIT)

        # Serialized HDR for future merging
        try:
            lb.string_field("histo_b64", _encode_hist(h))
        except Exception as e:
            influxdb3_local.warn("hdr_downsample_5m: serialization failed; writing without histo_b64",
                                 {"error": str(e)})

        # Use the ALIGNED boundary (end_dt) as the point timestamp
        lb.time_ns(int(end_dt.timestamp() * 1e9))
        influxdb3_local.write(lb)
        wrote += 1

    influxdb3_local.info("hdr_downsample_5m: wrote buckets",
                         {"count": wrote, "window": f"{start_iso}..{end_iso}",
                          "delay_min": ingest_delay_min, "sigfigs": sigfigs, "highest_ns": highest})