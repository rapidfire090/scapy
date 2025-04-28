from prometheus_client import start_http_server, Gauge
import subprocess
import time
import re
import threading
import argparse

# Dynamic Prometheus metrics storage
metrics = {}

# Regex to find ci_netif_stats block and key-value pairs
CI_NETIF_STATS_HEADER_RE = re.compile(r'ci_netif_stats:\s*(\d+)', re.IGNORECASE)
CI_NETIF_STATS_LINE_RE = re.compile(r'\s*(\w+)\s*:\s*(\d+)', re.IGNORECASE)


def scrape_onload_stats(scrape_interval):
    """
    Periodically scrape Onload ci_netif_stats by parsing global onload_stackdump lots output.
    """
    while True:
        try:
            output = subprocess.check_output(['onload_stackdump', 'lots'], text=True)
            current_stack_id = None

            for line in output.splitlines():
                header_match = CI_NETIF_STATS_HEADER_RE.search(line)
                if header_match:
                    current_stack_id = header_match.group(1)
                    continue

                if current_stack_id:
                    stat_match = CI_NETIF_STATS_LINE_RE.search(line)
                    if stat_match:
                        key, value = stat_match.groups()
                        metric_name = f"onload_netif_{key}"

                        if metric_name not in metrics:
                            metrics[metric_name] = Gauge(metric_name, f"Onload netif stat {key}", ['stack_id'])

                        metrics[metric_name].labels(stack_id=current_stack_id).set(int(value))

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
