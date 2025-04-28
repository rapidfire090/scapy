from prometheus_client import start_http_server, Gauge
import subprocess
import time
import re
import threading
import argparse

# Dynamic Prometheus metrics storage
enmetrics = {}

# Regex to find important blocks and key-value pairs
CI_NETIF_DUMP_HEADER_RE = re.compile(r'ci_netif_dump_to_logger', re.IGNORECASE)
STACK_INFO_RE = re.compile(r'stack=(\d+)', re.IGNORECASE)
PID_INFO_RE = re.compile(r'Onload\s+([\d\.]+).*?pid=(\d+)', re.IGNORECASE)
CI_NETIF_STATS_HEADER_RE = re.compile(r'ci_netif_stats:\s*(\d+)', re.IGNORECASE)
CI_NETIF_STATS_LINE_RE = re.compile(r'\s*(\w+)\s*:\s*(\d+)', re.IGNORECASE)

# The metrics dict holds Gauge objects by metric name
enmetrics = {}


def scrape_onload_stats(scrape_interval):
    """
    Periodically scrape Onload ci_netif_stats by parsing global onload_stackdump lots output.
    """
    while True:
        try:
            output = subprocess.check_output(
                ['onload_stackdump', 'lots'], universal_newlines=True
            )
            if not output.strip():
                time.sleep(scrape_interval)
                continue

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
                    m_stack = STACK_INFO_RE.search(line)
                    if m_stack:
                        current_stack_id = m_stack.group(1)
                        continue

                    m_pid = PID_INFO_RE.search(line)
                    if m_pid:
                        current_onload_version, current_pid = m_pid.groups()
                        continue

                    m_stats_header = CI_NETIF_STATS_HEADER_RE.search(line)
                    if m_stats_header:
                        stats_stack_id = m_stats_header.group(1)
                        ready_for_stats = (stats_stack_id == current_stack_id)
                        continue

                    if ready_for_stats:
                        m_line = CI_NETIF_STATS_LINE_RE.search(line)
                        if m_line:
                            key, val = m_line.groups()
                            metric_name = f"onload_netif_{key.lower()}"
                            # Initialize Gauge if needed
                            if metric_name not in enmetrics:
                                enmetrics[metric_name] = Gauge(
                                    metric_name,
                                    f"Onload netif stat {key}",
                                    ['stack_id', 'onload_version', 'pid']
                                )
                            # Prepare labels
                            labels = {
                                'stack_id': current_stack_id,
                                'onload_version': current_onload_version,
                                'pid': current_pid
                            }
                            # Record seen labelset for cleanup
                            labels_as_set = frozenset(labels.items())
                            seen_labels.add((metric_name, labels_as_set))
                            # Set metric value
                            enmetrics[metric_name].labels(**labels).set(int(val))

            # Cleanup stale metrics
            for metric_name, metric_obj in enmetrics.items():
                to_remove = []
                for label_tuple in list(metric_obj._metrics.keys()):
                    # Reconstruct dict for this label tuple
                    label_dict = dict(zip(metric_obj._labelnames, label_tuple))
                    labels_as_set = frozenset(label_dict.items())
                    if (metric_name, labels_as_set) not in seen_labels:
                        to_remove.append(label_tuple)
                for label_tuple in to_remove:
                    metric_obj.remove(*label_tuple)

        except Exception as e:
            print(f"Error scraping onload_stackdump lots: {e}")

        time.sleep(scrape_interval)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Onload ci_netif_stats Exporter'
    )
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
    thread = threading.Thread(
        target=scrape_onload_stats,
        args=(args.scrape_interval,)
    )
    thread.daemon = True
    thread.start()

    # Keep main thread alive indefinitely
    while True:
        time.sleep(60)
