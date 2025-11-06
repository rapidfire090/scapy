import base64
from hdrhistogram import HdrHistogram

def dump_histo_b64(b64_str):
    # Decode Base64 → binary → HDRHistogram
    raw = base64.b64decode(b64_str.strip())
    h = HdrHistogram.decode(raw)

    print("\n=== HDRHistogram Metadata ===")
    print(f"Lowest Trackable Value : {h.lowest_trackable_value}")
    print(f"Highest Trackable Value: {h.highest_trackable_value}")
    print(f"Significant Figures    : {h.significant_figures}")
    print(f"Total Count            : {h.get_total_count()}")
    print(f"Mean                   : {h.get_mean():.2f}")
    print(f"StdDev                 : {h.get_stddev():.2f}")
    print(f"Min Value              : {h.get_min_value()}")
    print(f"Max Value              : {h.get_max_value()}")

    print("\n=== Percentiles ===")
    for p in range(0, 101):
        if p % 1 == 0:
            print(f"p{p:>3}: {h.get_value_at_percentile(p)}")

    print("\n=== Recorded Values ===")
    print(f"{'ValueFrom':>12} {'ValueTo':>12} {'Count':>10} {'% of Total':>12}")
    total = h.get_total_count()
    for v in h.get_recorded_iterator():
        pct = (v.count_added_in_this_iteration_step / total) * 100 if total > 0 else 0
        print(f"{v.value_iterated_from:12} {v.value_iterated_to:12} "
              f"{v.count_added_in_this_iteration_step:10} {pct:11.4f}%")

    print("\n=== Raw Bucket Dump Complete ===")


# Example usage:
# dump_histo_b64("<paste your b64-encoded HDR histogram here>")
