# ~/.plugins/hdr_latency.py
# Scheduled Processing Engine plugin:
#   - Queries the last 5 minutes of latency samples
#   - Builds an HDRHistogram
#   - Writes: p50,p90,p95,p99,p999,min,max,count (+ unit tag/field)
#
# Args (via trigger --trigger-arguments):
#   source_measurement   e.g. "latency"
#   value_field          e.g. "latency_us"
#   target_measurement   e.g. "latency_summary"
#   unit                 "us"|"ms"|"ns" (for metadata only)
#   lowest               histogram lower bound (raw units)
#   highest              histogram upper bound (raw units)
#   sigfigs              histogram significant figures
#   extra_tags           CSV "k=v,k2=v2"
#   filter_tag           optional tag name to filter on
#   filter_values        optional values list like "api@gateway"

from typing import Dict, Any, List
from hdrhistogram import HdrHistogram
from time import time_ns

def _now_ns() -> int:
    return time_ns()

def _to_number(v):
    try:
        return float(v)
    except Exception:
        return None

def process_scheduled_call(influxdb3_local, call_time: str, args: Dict[str, Any]):
    # --- Config ---
    src = args.get("source_measurement", "latency")
    fld = args.get("value_field", "latency_us")
    dst = args.get("target_measurement", "latency_summary")
    unit = args.get("unit", "us")                 # informational
    lowest = int(args.get("lowest", "1"))         # histogram bounds in *raw* units
    highest = int(args.get("highest", "10000000"))
    sigfigs = int(args.get("sigfigs", "3"))
    filt_tag = args.get("filter_tag")
    filt_vals = args.get("filter_values")         # "a@b@c"
    extra_tags = args.get("extra_tags", "")       # "k1=v1,k2=v2"

    # --- Build WHERE for last 5 minutes (+ optional tag filter) ---
    where = "time >= now() - INTERVAL '5 minutes' AND time < now()"
    if filt_tag and filt_vals:
        vals = [v for v in filt_vals.split("@") if v]
        if vals:
            quoted = ",".join([f"'{v}'" for v in vals])
            where += f" AND {filt_tag} IN ({quoted})"

    q = f"SELECT time, {fld} FROM {src} WHERE {where}"
    rows: List[Dict[str, Any]] = influxdb3_local.query(q, {})

    samples: List[float] = []
    for r in rows:
        v = _to_number(r.get(fld))
        if v is not None and v >= 0:
            samples.append(v)

    if not samples:
        influxdb3_local.info("hdr_latency: no data in window")
        return

    # --- HDRHistogram (integers expected) ---
    hist = HdrHistogram(lowest_nonzero_value=lowest,
                        highest_trackable_value=highest,
                        significant_figures=sigfigs)
    for v in samples:
        try:
            hist.record_value(int(v))
        except Exception:
            # skip out-of-range or invalid
            pass

    if hist.total_count == 0:
        influxdb3_local.info("hdr_latency: no valid samples")
        return

    # --- Stats (percentiles + min/max + count) ---
    p50  = hist.get_value_at_percentile(50.0)
    p90  = hist.get_value_at_percentile(90.0)
    p95  = hist.get_value_at_percentile(95.0)
    p99  = hist.get_value_at_percentile(99.0)
    p999 = hist.get_value_at_percentile(99.9)
    vmin = hist.get_min_value()
    vmax = hist.get_max_value()
    cnt  = int(hist.total_count)

    # --- Emit point at "now" (window end) ---
    lb = LineBuilder(dst)

    # optional tags
    if extra_tags:
        for kv in extra_tags.split(","):
            kv = kv.strip()
            if not kv or "=" not in kv:
                continue
            k, v = kv.split("=", 1)
            lb.tag(k.strip(), v.strip())

    # fields
    lb.float64_field("p50",  p50)
    lb.float64_field("p90",  p90)
    lb.float64_field("p95",  p95)
    lb.float64_field("p99",  p99)
    lb.float64_field("p999", p999)
    lb.float64_field("min",  vmin)
    lb.float64_field("max",  vmax)
    lb.uint64_field("count", cnt)
    lb.string_field("unit", unit)

    lb.time_ns(_now_ns())

    influxdb3_local.write(lb)
    influxdb3_local.info("hdr_latency: wrote summary", {"count": cnt, "p99": p99, "dst": dst})
