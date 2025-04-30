from prometheus_client import start_http_server
from prometheus_client.core import CounterMetricFamily, REGISTRY
import subprocess
import re
import time
import argparse

# Regex patterns
DUMP_HEADER_RE = re.compile(r'^\s*ci_netif_dump_to_logger:\s*stack=(?P<stack_id>\d+)(?:\s+name=(?P<stack_name>\S*))?', re.IGNORECASE)
ONLOAD_PID_RE = re.compile(r'Onload\s+(?P<version>[\d\.]+).*?pid=(?P<pid>\d+)', re.IGNORECASE)
SECTION_RE = re.compile(r'^-+\s*(?P<header>\w+):\s*(?P<stack_id>\d+)\s*-+', re.IGNORECASE)
METRIC_RE = re.compile(r'^\s*(?P<key>\w+)\s*:\s*(\d+)', re.IGNORECASE)
TARGET_HEADER = 'ci_netif_stats'

class OnloadCollector:
    def __init__(self, timeout):
        self.timeout = timeout

    def collect(self):
        try:
            output = subprocess.check_output(
                ['onload_stackdump', 'lots'], universal_newlines=True, timeout=self.timeout
            )
        except subprocess.TimeoutExpired:
            print('Timeout: onload_stackdump hung')
            return
        except subprocess.CalledProcessError:
            print('onload_stackdump failed with non-zero exit')
            return
        except Exception as e:
            print(f"General error calling onload_stackdump: {e}")
            return

        metrics_data = {}
        current_stack = None
        current_version = None
        current_pid = None
        current_stack_name = ''
        in_section = False

        for line in output.splitlines():
            m = DUMP_HEADER_RE.match(line)
            if m:
                current_stack = m.group('stack_id')
                current_stack_name = m.group('stack_name') or ''
                current_version = None
                current_pid = None
                in_section = False
                continue

            if current_stack and current_version is None:
                m2 = ONLOAD_PID_RE.search(line)
                if m2:
                    current_version = m2.group('version')
                    current_pid = m2.group('pid')
                    continue

            m3 = SECTION_RE.match(line)
            if m3:
                hdr = m3.group('header').lower()
                sid = m3.group('stack_id')
                in_section = (hdr == TARGET_HEADER and sid == current_stack)
                continue

            if in_section and current_stack and current_version and current_pid:
                m4 = METRIC_RE.match(line)
                if m4:
                    key = m4.group(1).lower()
                    val = int(m4.group(2))
                    metric_name = f"onload_{TARGET_HEADER}_{key}"
                    labels = [current_stack, current_pid, current_version, current_stack_name]
                    metrics_data.setdefault(metric_name, []).append((labels, val))

        for metric_name, samples in metrics_data.items():
            c = CounterMetricFamily(
                metric_name,
                f"Onload {TARGET_HEADER} metric",
                labels=['stack_id', 'pid', 'onload_version', 'stack_name']
            )
            for labels, val in samples:
                c.add_metric(labels, val)
            yield c

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Onload ci_netif_stats exporter')
    parser.add_argument('--port', type=int, default=9100, help='HTTP port for Prometheus metrics')
    parser.add_argument('--timeout', type=float, default=1.0, help='Timeout in seconds for onload_stackdump command')
    args = parser.parse_args()

    REGISTRY.register(OnloadCollector(timeout=args.timeout))
    start_http_server(args.port)

    while True:
        time.sleep(60)
