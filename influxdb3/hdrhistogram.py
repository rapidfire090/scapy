# ~/.plugins/hdr_downsample_5m.py
# Python 3.8+
# InfluxDB 3 Processing Engine: downsample streaming2(latency ns) → latency_5m
# - Groups by tags: component, session
# - Window: last 5 minutes
# - Fields written: p50, p90, p95, p99, p99_9, min, max, count, unit="ns", histo_b64 (serialized HDR)
#
# Required dep (install into engine venv):
#   influxdb3 install package hdrhistogram
#
# Notes:
# - Uses significant_figures=4 (higher precision)
# - Tolerates args=None from the scheduler
# - Tries both common HDRHistogram bindings (hdrhistogram, hdrh)

from typing import Dict, Any, List, Tuple, DefaultDict
from collections import defaultdict
from time import time_ns
import base64
from datetime import datetime, timezone, timedelta

# ---------- HDR binding detection ----------
_hdr_cls = None
_err = None
try:
    from hdrhistogram import HdrHistogram as _Hdr  # PyPI: hdrhistogram
    _hdr_cls = _Hdr
except Exception as e1:
    _err = e1
    try:
        from hdrh.histogram import HdrHistogram as _Hdr  # PyPI: hdrh
        _hdr_cls = _Hdr
    except Exception as e2:
        _err = (e1, e2)

# ---------- Defaults (you can override via trigger-arguments) ----------
SIGFIGS_DEFAULT = 4
LOWEST_DEFAULT = 1
HIGHEST_DEFAULT = 1_000_000_000  # 1 second in nanoseconds
UNIT = "ns"
PCTS = (50.0, 90.0, 95.0, 99.0, 99.9)

# ---------- Helpers ----------
def _args_or_empty(a):
    return a if isinstance(a, dict) else {}

def _require_hdr():
    if _hdr_cls is None:
        raise RuntimeError(
            "HDRHistogram module not found. Install into Processing Engine venv:\n"
            "  influxdb3 install package hdrhistogram\n"
            f"Import errors: {_err}"
        )

def _encode_hist(h) -> str:
    # Prefer native compressed encoding if available
    if hasattr(h, "encode"):
        b = h.encode()
        return base64.b64encode(b).decode("ascii")
    if hasattr(h, "to_byte_array"):
        b = h.to_byte_array()
        return base64.b64encode(b).decode("ascii")
    raise RuntimeError("HDRHistogram binding lacks encode(); cannot serialize histogram.")

# ---------- Entry point (scheduled) ----------
def process_scheduled_call(influxdb3_local, call_time: str, args):
    """
    Scheduled trigger: every 5 minutes

    Optional trigger-arguments (CSV k=v list):
      lowest       -> override histogram lowest trackable value (default 1)
      highest      -> override histogram highest trackable value (default 1_000_000_000)
      sigfigs      -> override significant figures (default 4)
      extra_tags   -> CSV "k=v,k2=v2" added to every output point
    """
    _require_hdr()
    args = _args_or_empty(args)

    # Histogram config (allow overrides)
    try:
        lowest  = int(args.get("lowest",  str(LOWEST_DEFAULT)))
        highest = int(args.get("highest", str(HIGHEST_DEFAULT)))
        sigfigs = int(args.get("sigfigs", str(SIGFIGS_DEFAULT)))
    except Exception:
        lowest, highest, sigfigs = LOWEST_DEFAULT, HIGHEST_DEFAULT, SIGFIGS_DEFAULT

    extra = args.get("extra_tags", "") or ""

    end  = datetime.now(timezone.utc)
    start = end - timedelta(minutes=5)
    start_iso = start.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso   = end.strftime("%Y-%m-%dT%H:%M:%SZ")
    

    # Query only needed columns for last 5 minutes
    q = f"""
    SELECT "time","component","session","latency"
    FROM streaming2
    WHERE "time" >= TIMESTAMP '{start_iso}'
      AND "time"  <  TIMESTAMP '{end_iso}'
    ORDER BY "time" ASC
    """
    rows = influxdb3_local.query(q, {}) or []

    if not rows:
        influxdb3_local.info("hdr_downsample_5m: no rows in window")
        return

    # Partition by (component, session)
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
        influxdb3_local.info("hdr_downsample_5m: no valid samples after filtering")
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
                # value outside trackable range or invalid; skip
                pass

        if getattr(h, "total_count", 0) == 0:
            continue

        lb = LineBuilder("latency_5m")
        lb.tag("component", comp)
        lb.tag("session",   sess)
        for k, v in extra_tag_pairs:
            lb.tag(k, v)

        # convenience percentiles
        for p in PCTS:
            fname = "p" + str(p).replace(".", "_")
            lb.float64_field(fname, h.get_value_at_percentile(p))

        lb.float64_field("min",  h.get_min_value())
        lb.float64_field("max",  h.get_max_value())
        lb.uint64_field("count", int(h.total_count))
        lb.string_field("unit",  UNIT)

        # serialized histogram for future merging
        try:
            lb.string_field("histo_b64", _encode_hist(h))
        except Exception as e:
            influxdb3_local.warn("hdr_downsample_5m: serialization failed; writing without histo_b64", {"error": str(e)})

        # timestamp at window end
        lb.time_ns(time_ns())
        influxdb3_local.write(lb)
        wrote += 1

    influxdb3_local.info("hdr_downsample_5m: wrote buckets", {"count": wrote})
