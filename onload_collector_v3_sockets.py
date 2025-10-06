from prometheus_client import start_http_server
from prometheus_client.core import CounterMetricFamily, REGISTRY
import subprocess
import re
import time
import argparse

DUMP_HEADER_RE = re.compile(
    r'^\s*ci_netif_dump_to_logger:\s*stack=(?P<stack_id>\d+)(?:\s+name=(?P<stack_name>\S*))?',
    re.IGNORECASE
)
ONLOAD_PID_RE = re.compile(r'Onload\s+(?P<version>[\d\.]+).*?pid=(?P<pid>\d+)', re.IGNORECASE)
SECTION_RE = re.compile(r'^-+\s*(?P<header>\w+):\s*(?P<stack_id>\d+)\s*-+', re.IGNORECASE)
METRIC_RE = re.compile(r'^\s*(?P<key>\w+)\s*:\s*(\d+)', re.IGNORECASE)
TARGET_HEADER = 'ci_netif_stats'

# New: sockets-section helpers
SOCKETS_OPEN_RE = re.compile(r'^\-+\s*sockets\s*\-+\s*$', re.IGNORECASE)
SECTION_TITLE_RE = re.compile(r'^\-+\s*[A-Za-z_ ].*?\-+\s*$')
DASH_ONLY_RE = re.compile(r'^\-+\s*$')
SOCK_HDR_RE = re.compile(
    r'^(TCP|UDP)\s+(\d+):(\d+)\s+lcl=([^\s]+)\s+rmt=([^\s]+)\s+(\S+)\s*$'
)


def _iter_socket_kv(line: str):
    """
    Yield (key, value_str) pairs from a metrics line inside a socket block.
    If the line starts with a label like 'listenq:' or 'TX timestamping queue:',
    prefix keys, e.g. 'TX_timestamping_queue.q_pkts'.
    """
    first_colon = line.find(':')
    first_equal = line.find('=')
    if first_colon != -1 and (first_equal == -1 or first_colon < first_equal):
        prefix = line[:first_colon].strip().replace(" ", "_")  # normalize spaces to underscores
        body = line[first_colon + 1 :]
    else:
        prefix = None
        body = line

    for m in re.finditer(r'([A-Za-z0-9_.\-]+)=([^\s]+)', body):
        k, v = m.group(1), m.group(2)
        yield (f"{prefix}.{k}" if prefix else k, v)


def _to_int_or_none(val: str):
    """Convert numeric-like strings to int, including hex."""
    try:
        return int(val, 0)
    except Exception:
        cleaned = val.rstrip(',);')
        try:
            return int(cleaned, 0)
        except Exception:
            return None


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

        ci_metrics_data = {}
        sockets_metrics_data = {}

        current_stack = None
        current_version = None
        current_pid = None
        current_stack_name = ''
        in_ci_section = False

        in_sockets_section = False
        cur_sock = None

        lines = output.splitlines()
        i = 0
        n = len(lines)

        while i < n:
            line = lines[i].rstrip('\n')
            i += 1

            m = DUMP_HEADER_RE.match(line)
            if m:
                current_stack = m.group('stack_id')
                current_stack_name = m.group('stack_name') or ''
                current_version = None
                current_pid = None
                in_ci_section = False
                in_sockets_section = False
                cur_sock = None
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
                in_ci_section = (hdr == TARGET_HEADER and sid == current_stack)
                in_sockets_section = False
                cur_sock = None
                continue

            if SOCKETS_OPEN_RE.match(line):
                in_sockets_section = True
                in_ci_section = False
                cur_sock = None
                continue

            if not in_ci_section and not in_sockets_section:
                continue

            # ci_netif_stats
            if in_ci_section and current_stack and current_version and current_pid:
                m4 = METRIC_RE.match(line)
                if m4:
                    key = m4.group(1).lower()
                    val = int(m4.group(2))
                    metric_name = f"onload_{TARGET_HEADER}_{key}"
                    labels = [current_stack, current_pid, current_version, current_stack_name]
                    ci_metrics_data.setdefault(metric_name, []).append((labels, val))
                continue

            # sockets section
            if in_sockets_section and current_stack and current_version and current_pid:
                if SECTION_TITLE_RE.match(line) and not DASH_ONLY_RE.match(line):
                    cur_sock = None
                    in_sockets_section = False
                    continue

                if DASH_ONLY_RE.match(line):
                    cur_sock = None
                    continue

                mh = SOCK_HDR_RE.match(line)
                if mh:
                    proto, stack_num, sock_index, lcl, rmt, state = mh.groups()
                    cur_sock = {
                        "proto": proto,
                        "sock_index": sock_index,
                        "lcl": lcl,
                        "rmt": rmt,
                        "state": state,
                    }
                    continue

                if cur_sock is not None:
                    for k, v_str in _iter_socket_kv(line):
                        ival = _to_int_or_none(v_str)
                        if ival is None:
                            continue
                        metric_name = f"onload_sockets_{k.lower()}"
                        labels = [
                            str(current_stack),
                            str(current_pid),
                            str(current_version),
                            str(current_stack_name),
                            cur_sock.get("proto", ""),
                            str(cur_sock.get("sock_index", "")),
                            cur_sock.get("lcl", ""),
                            cur_sock.get("rmt", ""),
                            cur_sock.get("state", ""),
                        ]
                        sockets_metrics_data.setdefault(metric_name, []).append((labels, ival))

        # Emit ci_netif_stats metrics
        for metric_name, samples in ci_metrics_data.items():
            c = CounterMetricFamily(
                metric_name,
                f"Onload {TARGET_HEADER} metric",
                labels=['stack_id', 'pid', 'onload_version', 'stack_name']
            )
            for labels, val in samples:
                c.add_metric(labels, val)
            yield c

        # Emit sockets metrics
        sockets_label_names = [
            'stack_id', 'pid', 'onload_version', 'stack_name',
            'proto', 'sock_index', 'lcl', 'rmt', 'state'
        ]
        for metric_name, samples in sockets_metrics_data.items():
            c = CounterMetricFamily(
                metric_name,
                "Onload sockets metrics parsed from --- sockets --- section",
                labels=sockets_label_names
            )
            for labels, val in samples:
                c.add_metric(labels, val)
            yield c


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Onload ci_netif_stats + sockets exporter')
    parser.add_argument('--port', type=int, default=9100, help='HTTP port for Prometheus metrics')
    parser.add_argument('--timeout', type=float, default=1.0, help='Timeout in seconds for onload_stackdump command')
    args = parser.parse_args()

    REGISTRY.register(OnloadCollector(timeout=args.timeout))
    start_http_server(args.port)

    while True:
        time.sleep(60)
