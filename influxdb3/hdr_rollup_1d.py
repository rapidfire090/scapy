# Python 3.8+
# Merge latency_5m -> latency_1d for a CUSTOM daily window defined by hours and timezone.
# Example: window_hours="09:00-17:00", timezone="America/New_York"
# Groups by (component, session); writes p50,p90,p95,p99,p99_9,min,max,count,unit,histo_b64
#
# Trigger-arguments (all optional; sensible defaults provided):
#   window_hours   e.g. "09:00-17:00"  (24h, inclusive start, exclusive end)
#   timezone       e.g. "America/New_York" (IANA name)
#   days_back      integer; 0=today's window (if already finished), 1=yesterday, etc.
#   offset_minutes integer; run a few minutes after window end (default 5)
#
# Requires Python tzinfo support. If zoneinfo not available in your runtime,
# you can fall back to pytz (install in engine venv).

from typing import Dict, Any, List, Tuple, DefaultDict
from collections import defaultdict
from time import time_ns
from datetime import datetime, date, time as dtime, timedelta, timezone
import base64
import os

# tz support: prefer zoneinfo (Py3.9+), else pytz
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None
try:
    import pytz
except Exception:
    pytz = None

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
HIGHEST = 1_000_000_000
UNIT = "ns"
PCTS = (50.0, 90.0, 95.0, 99.0, 99.9)

def _tz(tzname: str):
    if ZoneInfo is not None:
        return ZoneInfo(tzname)
    if pytz is not None:
        return pytz.timezone(tzname)
    raise RuntimeError("No timezone support. Install pytz: influxdb3 install package pytz")

def _parse_hhmm(s: str) -> dtime:
    hh, mm = s.split(":")
    return dtime(hour=int(hh), minute=int(mm))

def _window_bounds_local(tzname: str, days_back: int, window_hours: str, now_utc: datetime):
    # Determine the *local* date whose window we want.
    tz = _tz(tzname)
    now_local = now_utc.astimezone(tz)

    # If the current local time is before today's window end, you might want yesterday's window (days_back>=1).
    # We honor explicit days_back; caller chooses 0 or 1, etc.
    base_local_date: date = (now_local - timedelta(days=days_back)).date()

    start_s, end_s = window_hours.split("-")
    start_t = _parse_hhmm(start_s)
    end_t   = _parse_hhmm(end_s)

    start_local = datetime.combine(base_local_date, start_t, tzinfo=tz)
    end_local   = datetime.combine(base_local_date, end_t,   tzinfo=tz)

    # If the window crosses midnight (end <= start), push end to next day
    if end_local <= start_local:
        end_local = end_local + timedelta(days=1)

    return start_local, end_local  # both timezone-aware

def _to_utc_iso(dt_local: datetime) -> str:
    return dt_local.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

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
        it = getattr(src, "recorded_values", None)
        if it is None:
            raise RuntimeError("HDR binding lacks add() and recorded_values().")
        for v, c in it():
            for _ in range(int(c)):
                target.record_value(int(v))

def _args_or_empty(a):
    return a if isinstance(a, dict) else {}

def process_scheduled_call(influxdb3_local, call_time: str, args):
    if _hdr_cls is None:
        raise RuntimeError(
            "Install HDR into engine venv: influxdb3 install package hdrhistogram"
        )
    args = _args_or_empty(args)

    tzname  = args.get("timezone", "America/New_York")
    hours   = args.get("window_hours", "09:00-17:00")
    back    = int(args.get("days_back", "1"))   # default: yesterday's business window
    offsetm = int(args.get("offset_minutes", "5"))

    now_utc = datetime.now(timezone.utc)
    start_local, end_local = _window_bounds_local(tzname, back, hours, now_utc)

    # Optional safety: ensure we're running *after* the window end + offset
    if now_utc < (end_local.astimezone(timezone.utc) + timedelta(minutes=offsetm)):
        influxdb3_local.info("hdr_rollup_1d: not time yet; skipping", {
            "window_local": f"{start_local}..{end_local}",
            "now_utc": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        })
        return

    start_iso = _to_utc_iso(start_local)
    end_iso   = _to_utc_iso(end_local)

    # Pull the entire UTC span; we DON'T need additional EXTRACT(HOUR) filters
    # because the span is already constrained to your business hours in local tz converted to UTC.
    q = f"""
      SELECT "component","session","histo_b64","min","max","count"
      FROM latency_5m
      WHERE "time" >= TIMESTAMP '{start_iso}'
        AND "time"  < TIMESTAMP '{end_iso}'
    """
    rows = influxdb3_local.query(q, {}) or []
    if not rows:
        influxdb3_local.info("hdr_rollup_1d: no rows to roll up", {
            "window_local": f"{start_local}..{end_local}",
            "window_utc":   f"{start_iso}..{end_iso}"
        })
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

        lb = LineBuilder("latency_1d")
        lb.tag("component", comp)
        lb.tag("session",   sess)
        lb.tag("tz",        tzname)
        lb.tag("window",    hours)

        for p in PCTS:
            lb.float64_field("p" + str(p).replace(".","_"), merged.get_value_at_percentile(p))
        lb.float64_field("min",  gmin if gmin is not None else merged.get_min_value())
        lb.float64_field("max",  gmax if gmax is not None else merged.get_max_value())
        lb.uint64_field("count", gcount)
        lb.string_field("unit",  UNIT)
        lb.string_field("histo_b64", _encode_hdr(merged))

        # Timestamp at end of the business window (UTC)
        lb.time_ns(int(end_local.astimezone(timezone.utc).timestamp() * 1e9))
        influxdb3_local.write(lb)
        wrote += 1

    influxdb3_local.info("hdr_rollup_1d: wrote buckets", {
        "count": wrote,
        "window_local": f"{start_local}..{end_local}",
        "window_utc":   f"{start_iso}..{end_iso}"
    })
