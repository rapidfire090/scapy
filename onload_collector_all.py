from prometheus_client import start_http_server
from prometheus_client.core import CounterMetricFamily, REGISTRY
import subprocess
import re
import time
import argparse
import os

# Regex definitions
HEADER_SPLITTER_RE = re.compile(r'^=+\s*$')
DUMP_HEADER_RE = re.compile(r'^ci_netif_dump_to_logger:\s*stack=(?P<stack_id>\d+)(?:\s+name=(?P<stack_name>\S+))?', re.IGNORECASE)
ONLOAD_PID_RE = re.compile(r'Onload\s+(?P<version>[\d\.]+).*?pid=(?P<pid>\d+)', re.IGNORECASE)
SECTION_RE = re.compile(r'^-+\s*(?P<header>\w+):\s*(?P<stack_id>\d+)\s*-+', re.IGNORECASE)
METRIC_RE = re.compile(r'^\s*(?P<key>[\w\.]+)\s*:\s*(\d+)', re.IGNORECASE)
VI_HEADER_RE = re.compile(r'^ci_netif_dump_vi:\s*stack=(?P<stack_id>\d+)\s+intf=(?P<intf>\S+)\s+dev=(?P<dev>\S+)\s+hw=(?P<hw>\S+)', re.IGNORECASE)
SOCKET_DELIM_RE = re.compile(r'^-+\s*sockets\s*-+', re.IGNORECASE)
TCP_SOCKET_RE = re.compile(r'^\s*TCP\s+stack_id:(?P<stack_id>\d+)\s+lcl=(?P<lcl>\S+)\s+rmt=(?P<rmt>\S+)', re.IGNORECASE)


class OnloadCollector:
    def __init__(self, timeout, enabled_sections, test_file=None):
        self.timeout = timeout
        self.enabled_sections = enabled_sections
        self.test_file = test_file

    def collect(self):
        # Load data
        if self.test_file:
            if not os.path.exists(self.test_file):
                print(f"Test file {self.test_file} not found.")
                return
            with open(self.test_file) as f:
                lines = f.readlines()
        else:
            try:
                output = subprocess.check_output(
                    ['onload_stackdump', 'lots'], universal_newlines=True, timeout=self.timeout
                )
                lines = output.splitlines()
            except Exception as e:
                print(f"Failed to run onload_stackdump: {e}")
                return

        for metric in self._parse_lines(lines):
            yield metric

    def _parse_lines(self, lines):
        metrics_data = {}
        stack_blocks = self._split_blocks(lines)
        for block in stack_blocks:
            self._parse_block(block, metrics_data)
        for metric_name, samples in metrics_data.items():
            if metric_name.startswith("onload_ci_netif_stats_"):
                fam = CounterMetricFamily(metric_name, "ci_netif_stats", labels=['stack_id', 'pid', 'onload_version', 'stack_name'])
            elif metric_name.startswith("onload_vi_stats_"):
                fam = CounterMetricFamily(metric_name, "vi interface stats", labels=['stack_id', 'interface_id', 'device_id', 'hw_addr'])
            elif metric_name.startswith("onload_socket_stats_"):
                fam = CounterMetricFamily(metric_name, "socket stats", labels=['stack_id', 'local', 'remote'])
            else:
                continue
            for labels, val in samples:
                fam.add_metric(labels, val)
            yield fam

    def _split_blocks(self, lines):
        blocks = []
        current = []
        for line in lines:
            if HEADER_SPLITTER_RE.match(line):
                if current:
                    blocks.append(current)
                    current = []
            current.append(line)
        if current:
            blocks.append(current)
        return blocks

    def _parse_block(self, block, metrics):
        current_stack = current_stack_name = current_version = current_pid = None
        in_netif = in_vi = in_sockets = False
        vi_labels = socket_labels = None

        i = 0
        while i < len(block):
            line = block[i]

            if DUMP_HEADER_RE.match(line):
                m = DUMP_HEADER_RE.match(line)
                current_stack = m.group('stack_id')
                current_stack_name = m.group('stack_name') or ''
                in_netif = in_vi = in_sockets = False
                current_version = current_pid = None

            elif current_stack and not current_version:
                m = ONLOAD_PID_RE.search(line)
                if m:
                    current_version = m.group('version')
                    current_pid = m.group('pid')

            elif SECTION_RE.match(line):
                m = SECTION_RE.match(line)
                header = m.group('header').lower()
                sid = m.group('stack_id')
                in_netif = ('ci_netif_stats' in self.enabled_sections and header == 'ci_netif_stats' and sid == current_stack)
                in_vi = False
                in_sockets = False

            elif 'vi' in self.enabled_sections:
                m = VI_HEADER_RE.match(line)
                if m and m.group('stack_id') == current_stack:
                    in_vi = True
                    vi_labels = [current_stack, m.group('intf'), m.group('dev'), m.group('hw')]
                    in_netif = in_sockets = False

            elif 'sockets' in self.enabled_sections and SOCKET_DELIM_RE.match(line):
                in_sockets = True
                in_netif = in_vi = False

            elif in_netif and all([current_stack, current_version, current_pid]):
                m = METRIC_RE.match(line)
                if m:
                    k, v = m.group('key').lower(), int(m.group(2))
                    name = f'onload_ci_netif_stats_{k}'
                    labels = [current_stack, current_pid, current_version, current_stack_name]
                    metrics.setdefault(name, []).append((labels, v))

            elif in_vi and vi_labels:
                m = METRIC_RE.match(line)
                if m:
                    k, v = m.group('key').lower(), int(m.group(2))
                    name = f'onload_vi_stats_{k}'
                    metrics.setdefault(name, []).append((vi_labels, v))

            elif in_sockets:
                m = TCP_SOCKET_RE.match(line)
                if m:
                    socket_labels = [m.group('stack_id'), m.group('lcl'), m.group('rmt')]
                else:
                    m = METRIC_RE.match(line)
                    if m and socket_labels:
                        k, v = m.group('key').lower(), int(m.group(2))
                        name = f'onload_socket_stats_{k}'
                        metrics.setdefault(name, []).append((socket_labels, v))
            i += 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Onload Prometheus Exporter')
    parser.add_argument('--port', type=int, default=9100, help='Prometheus scrape port')
    parser.add_argument('--timeout', type=float, default=1.0, help='onload_stackdump timeout')
    parser.add_argument('--test-file', help='Optional path to onload_stackdump output for testing')
    parser.add_argument('--sections', default='ci_netif_stats', help='Comma-separated sections to parse (e.g., ci_netif_stats,vi,sockets)')
    args = parser.parse_args()

    enabled_sections = set(s.strip().lower() for s in args.sections.split(',') if s.strip())
    REGISTRY.register(OnloadCollector(timeout=args.timeout, enabled_sections=enabled_sections, test_file=args.test_file))
    start_http_server(args.port)

    while True:
        time.sleep(60)
