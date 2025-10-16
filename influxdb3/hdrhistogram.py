# Python 3.8+
# InfluxDB 3 Processing Engine: downsample streaming2(latency ns) → latency_5m
# - Groups by tags: component, session
# - Window: last 5 minutes
# - Fields written: p50, p90, p95, p99, p99_9, min, max, count, unit="ns", histo_b64 (serialized HDR)
#
# Required dependency in engine venv: hdrhistogram (or hdrh)
#   influxdb3 install package hdrhistogram
#
# Notes:
# - Uses significant_figures=4 for higher precision
# - Uses encode()/decode() if available; raises a clear error if missing

from typing import Dict, Any, List, Tuple, DefaultDict
from collections import defaultdict
from time import time_ns
import base64

# Try common HDRHistogram bindings
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

SIGFIGS = 4
LOWEST = 1
HIGHEST = 1_000_000_000  # 1 second in ns; adjust if you need wider range
UNIT = "ns"

PCTS = (50.0, 90.0, 95.0, 99.0, 99.9)

def _require_hdr():
    if _hdr_cls is None:
        raise RuntimeError(
            "HDRHistogram module not found. Install into Processing Engine venv:\n"
            "  influxdb3 install package hdrhistogram\n"
            f"Import errors: {_err}"
        )

def _new_hist():
    return _hdr_cls(LOWEST, HIGHEST, SIGFIGS)

def _encode_hist(h) -> str:
    # Prefer native compressed encoding if available
    if hasattr(h, "encode"):
        b = h.encode()
        return base64.b64encode(b).decode("ascii")
    # Some bindings expose to_byte_array()
    if hasattr(h, "to_byte_array"):
        b = h.to_byte_array()
        return base64.b64encode(b).decode("ascii")
    raise RuntimeError("HDRHistogram binding lacks encode(); cannot serialize histogram.")

def process_scheduled_call(influxdb3_local, call_time: str, args: Dict[str, Any]):
    """
    Scheduled trigger: every 5 minutes
    Arguments (optional):
      lowest, highest, sigfigs    -> override bounds/precision if needed
      extra_tags                  -> CSV "k=v,k2=v2" applied to all outputs
    """
    _require_hdr()

    lowest  = int(args.get("lowest", str(LOWEST)))
    highest = int(args.get("highest", str(HIGHEST)))
    sigfigs = int(args.get("sigfigs", str(SIGFIGS)))
    extra   = args.get("extra_tags", "")

    # Query only needed columns
    where = "time >= now() - INTERVAL '5 minutes' AND time < now()"
    q = f"""
      SELECT time, component, session, latency
      FROM streaming2
      WHERE {where}
    """
    rows = influxdb3_local.query(q, {})

    if not rows:
        influxdb3_local.info("hdr_downsample_5m: no rows in window")
        return

    # Partition by (component, session)
    buckets: DefaultDict[Tuple[str, str], List[int]] = defaultdict(list)

    for r in rows:
        comp = "" if r.get("component") is None else str(r["component"])
        sess = "" if r.get("session") is None else str(r["session"])
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

    # Pre-parse extra tags
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
                # out-of-range or invalid
                pass

        if h.total_count == 0:
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
        lb.string_field("histo_b64", _encode_hist(h))

        # timestamp at window end
        lb.time_ns(time_ns())
        influxdb3_local.write(lb)
        wrote += 1

    influxdb3_local.info("hdr_downsample_5m: wrote buckets", {"count": wrote})
