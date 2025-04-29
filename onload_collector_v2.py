from prometheus_client import start_http_server, Gauge
import subprocess
import time
import re
import threading
import argparse

# Store dynamic metrics by name
metrics = {}

# Regex patterns
DUMP_HEADER_RE = re.compile(r'^\s*ci_netif_dump_to_logger:\s*stack=(?P<stack_id>\d+)', re.IGNORECASE)
ONLOAD_PID_RE = re.compile(r'Onload\s+(?P<version>[\d\.]+).*?pid=(?P<pid>\d+)', re.IGNORECASE)
SECTION_HEADER_RE = re.compile(r'^-+\s*(?P<header>\w+):\s*(?P<stack_id>\d+)\s*-+', re.IGNORECASE)
METRIC_RE = re.compile(r'^\s*(?P<key>\w+)\s*:\s*(?P<value>\d+)', re.IGNORECASE)


def scrape_onload_stats(interval):
    """
    Scrape only ci_netif_stats sections, capturing onload version and pid per stack.
    """
    while True:
        try:
            output = subprocess.check_output(['onload_stackdump', 'lots'], universal_newlines=True)
            current_stack = None
            current_version = None
            current_pid = None
            in_section = False
            seen = set()

            for line in output.splitlines():
                # Detect dump start for stack
                m_dump = DUMP_HEADER_RE.match(line)
                if m_dump:
                    current_stack = m_dump.group('stack_id')
                    current_version = None
                    current_pid = None
                    in_section = False
                    continue

                # After dump header, find Onload version and pid
                if current_stack and current_version is None:
                    m_opid = ONLOAD_PID_RE.search(line)
                    if m_opid:
                        current_version = m_opid.group('version')
                        current_pid = m_opid.group('pid')
                        continue

                # Detect section header like --- ci_netif_stats: stack_id ---
                m_sec = SECTION_HEADER_RE.match(line)
                if m_sec:
                    hdr = m_sec.group('header').lower()
                    sid = m_sec.group('stack_id')
                    # only ci_netif_stats and matching stack
                    in_section = (hdr == 'ci_netif_stats' and sid == current_stack)
                    continue

                # Parse metrics within ci_netif_stats
                if in_section and current_stack and current_version and current_pid:
                    m = METRIC_RE.match(line)
                    if m:
                        key = m.group('key').lower()
                        val = int(m.group('value'))
                        metric_name = f"onload_ci_netif_stats_{key}"
                        # initialize Gauge
                        if metric_name not in metrics:
                            metrics[metric_name] = Gauge(
                                metric_name,
                                f"Onload ci_netif_stats metric {key}",
                                ['stack_id','onload_version','pid']
                            )
                        labels = {
                            'stack_id': current_stack,
                            'onload_version': current_version,
                            'pid': current_pid
                        }
                        metrics[metric_name].labels(**labels).set(val)
                        seen.add((metric_name, frozenset(labels.items())))

            # cleanup stale
            for name, gauge in metrics.items():
                for label_tuple in list(gauge._metrics.keys()):
                    label_dict = dict(zip(gauge._labelnames, label_tuple))
                    if (name, frozenset(label_dict.items())) not in seen:
                        gauge.remove(*label_tuple)

        except Exception as e:
            print(f"Error scraping onload_stackdump lots: {e}")
        time.sleep(interval)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Onload ci_netif_stats exporter')
    parser.add_argument('--scrape-interval', type=int, default=5,
                        help='Seconds between scrapes')
    parser.add_argument('--port', type=int, default=9100,
                        help='HTTP port for metrics')
    args = parser.parse_args()

    # Start Prometheus HTTP server
    start_http_server(args.port)
    # Launch scrape thread
    t = threading.Thread(target=scrape_onload_stats,
                         args=(args.scrape_interval,))
    t.daemon = True
    t.start()

    # Keep alive
    while True:
        time.sleep(60)
