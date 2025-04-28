from prometheus_client import start_http_server, Gauge
import subprocess
import time
import re
import threading
import argparse

# Dynamic Prometheus metrics storage
metrics = {}

# Regex to find important blocks and key-value pairs
CI_NETIF_DUMP_HEADER_RE = re.compile(r'ci_netif_dump_to_logger', re.IGNORECASE)
STACK_INFO_RE = re.compile(r'stack=(\d+)', re.IGNORECASE)
PID_INFO_RE = re.compile(r'Onload\s+([\d\.]+).*?pid=(\d+)', re.IGNORECASE)
CI_NETIF_STATS_HEADER_RE = re.compile(r'ci_netif_stats:\s*(\d+)', re.IGNORECASE)
CI_NETIF_STATS_LINE_RE = re.compile(r'\s*(\w+)\s*:\s*(\d+)', re.IGNORECASE)


def scrape_onload_stats(scrape_interval):
    """
    Periodically scrape Onload ci_netif_stats by parsing global onload_stackdump lots output.
    """
    while True:
        try:
            output = subprocess.check_output(['onload_stackdump', 'lots'], universal_newlines=True)
            inside_dump = False
            current_stack_id = None
            current_onload_version = None
            current_pid = None
            ready_for_stats = False
            seen_labels = set()

            for line in output.splitlines():
                if CI_NETIF_DUMP_HEADER_RE.search(line):
                    inside_dump = True
                    continue

                if inside_dump:
                    stack_info = STACK_INFO_RE.search(line)
                    if stack_info:
                        current_stack_id = stack_info.group(1)
                        continue

                    pid_info = PID_INFO_RE.search(line)
                    if pid_info:
                        current_onload_version, current_pid = pid_info.groups()
                        continue

                    stats_header = CI_NETIF_STATS_HEADER_RE.search(line)
                    if stats_header:
                        stats_stack_id = stats_header.group(1)
                        if stats_stack_id == current_stack_id:
                            ready_for_stats = True
                        else:
                            ready_for_stats = False
                        continue

                    if ready_for_stats:
                        stat_match = CI_NETIF_STATS_LINE_RE.search(line)
                        if stat_match:
                            key, value = stat_match.groups()
                            metric_name = f"onload_netif_{key.lower()}"

                            if metric_name not in metrics:
                                metrics[metric_name] = Gauge(
                                    metric_name,
                                    f"Onload netif stat {key}",
                                    ['stack_id', 'onload_version', 'pid']
                                )

                            labels = {
                                'stack_id': current_stack_id,
                                'onload_version': current_onload_version,
                                'pid': current_pid
                            }
                            seen_labels.add((metric_name, tuple(labels.items())))
                            metrics[metric_name].labels(**labels).set(int(value))

            # Cleanup stale metrics
            for metric_name, metric_obj in metrics.items():
                to_remove = []
                for labelset in list(metric_obj._metrics.keys()):
                    label_dict = tuple(sorted(labelset.items()))
                    if (metric_name, label_dict) not in seen_labels:
                        to_remove.append(labelset)
                for labelset in to_remove:
                    metric_obj.remove(**labelset)

        except Exception as e:
            print(f"Error scraping onload_stackdump lots: {e}")

        time.sleep(scrape_interval)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Onload ci_netif_stats Exporter')
    parser.add_argument(
        '--scrape-interval', type=int, default=5,
        help='Seconds between Onload scrapes'
    )
    parser.add_argument(
        '--port', type=int, default=9100,
        help='HTTP port for Prometheus metrics'
    )
    args = parser.parse_args()

    # Start HTTP server for Prometheus
    start_http_server(args.port)

    # Launch background thread for scraping
    t = threading.Thread(target=scrape_onload_stats, args=(args.scrape_interval,))
    t.daemon = True
    t.start()

    # Keep main thread alive indefinitely
    while True:
        time.sleep(60)
