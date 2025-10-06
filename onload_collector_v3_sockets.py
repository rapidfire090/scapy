#!/usr/bin/env python3
import sys
import argparse
import json
import re
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional

SECTION_RE = re.compile(r'^-+\s*([A-Za-z0-9_ ]+?)\s*-+\s*$')

# ===== Sockets section support =====
# Matches the opening title line for sockets
_SOCKETS_OPEN_RE = re.compile(r'^\-+\s*sockets\s*\-+\s*$', re.IGNORECASE)
# Matches any titled section header (e.g., '--- ci_netif_stats ---') to know when sockets section ends
_SECTION_TITLE_RE = re.compile(r'^\-+\s*[A-Za-z_ ].*?\-+\s*$')
# A line of only dashes used as socket block separators (e.g., '--------')
_DASH_ONLY_RE = re.compile(r'^\-+\s*$')
# Socket header: e.g., "TCP 0:3 lcl=10.80.20.212:22605 rmt=0.0.0.0:0 LISTEN"
_SOCK_HDR_RE = re.compile(r'^(TCP|UDP)\s+(\d+):(\d+)\s+lcl=([^\s]+)\s+rmt=([^\s]+)\s+(\S+)\s*$')

def _iter_socket_kv(line: str):
    """
    Yield (key, value) pairs from a metrics line inside a socket block.
    If the line has a leading label like 'listenq:' / 'rcv:' / 'snd:', prefix keys:
        'listenq.max=2048' -> ('listenq.max', '2048')
    IMPORTANT: treat a prefix only if the first ':' occurs before the first '='
              so '10.0.0.1:80' is not misinterpreted as a label.
    """
    first_colon = line.find(':')
    first_equal = line.find('=')
    if first_colon != -1 and (first_equal == -1 or first_colon < first_equal):
        prefix = line[:first_colon].strip()
        body = line[first_colon + 1 :]
    else:
        prefix = None
        body = line

    for m in re.finditer(r'([A-Za-z0-9_.\-]+)=([^\s]+)', body):
        k, v = m.group(1), m.group(2)
        yield (f"{prefix}.{k}" if prefix else k, v)

def parse_sockets_section(lines: List[str], start_idx: int) -> Tuple[List[Dict[str, Any]], int]:
    """
    Parse a '--- sockets ---' section starting at start_idx (the header line index).
    Each socket block:
      - header: 'TCP|UDP {stack}:{index} lcl=IP:port rmt=IP:port STATE'
      - followed by any number of metrics lines with field=value pairs
      - terminated by a dashed line '--------' (any number of dashes)
    The sockets section ends when the next titled section header is encountered.
    Returns (sockets_list, next_index_after_section).
    """
    sockets: List[Dict[str, Any]] = []
    cur: Optional[Dict[str, Any]] = None
    i = start_idx + 1
    n = len(lines)

    while i < n:
        raw = lines[i]
        line = raw.rstrip()

        # If we see the next titled section header (not just a separator), sockets section ends.
        if _SECTION_TITLE_RE.match(line) and not _DASH_ONLY_RE.match(line):
            if cur is not None:
                sockets.append(cur)
                cur = None
            break

        # Socket boundary separator
        if _DASH_ONLY_RE.match(line):
            if cur is not None:
                sockets.append(cur)
                cur = None
            i += 1
            continue

        # New socket header?
        m = _SOCK_HDR_RE.match(line)
        if m:
            if cur is not None:
                sockets.append(cur)
            proto, stack, idx, lcl, rmt, state = m.groups()
            cur = {
                "proto": proto,
                "stack": int(stack),
                "index": int(idx),
                "lcl": lcl,
                "rmt": rmt,
                "state": state,
            }
            i += 1
            continue

        # Accumulate metrics (field=value pairs), possibly prefixed by a label
        if cur is not None:
            for k, v in _iter_socket_kv(line):
                cur[k] = v

        i += 1

    # If we ended the file while in a socket block, push it.
    if cur is not None:
        sockets.append(cur)

    return sockets, i

# ===== Generic section collector =====
def parse_generic_block(lines: List[str], start_idx: int) -> Tuple[List[str], int]:
    """
    Collect raw lines of a section until the next titled header.
    Useful for preserving unknown sections; caller can parse later if desired.
    Returns (list_of_lines, next_index_after_section).
    """
    i = start_idx + 1
    n = len(lines)
    buf: List[str] = []
    while i < n:
        line = lines[i].rstrip('\n')
        if SECTION_RE.match(line):
            break
        buf.append(line)
        i += 1
    return buf, i

def parse_stackdump_text(text: str) -> Dict[str, Any]:
    lines = text.splitlines()
    out: Dict[str, Any] = {}
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i].rstrip('\n')
        m = SECTION_RE.match(line)
        if not m:
            i += 1
            continue

        section_name = m.group(1).strip().lower().replace(' ', '_')

        # sockets
        if section_name == 'sockets':
            sockets, i = parse_sockets_section(lines, i)
            out['sockets'] = sockets
            continue

        # generic fallback for other sections: collect raw lines
        raw_block, i = parse_generic_block(lines, i)
        out[section_name] = raw_block

    return out

def main():
    p = argparse.ArgumentParser(description="Parse onload stackdump, including --- sockets --- section.")
    p.add_argument("files", nargs="+", help="Input stackdump text files")
    p.add_argument("--json-out", help="Write combined JSON to this path")
    p.add_argument("--sockets-csv", help="Optional CSV of parsed sockets across all inputs")
    args = p.parse_args()

    combined: Dict[str, Any] = {"inputs": []}
    all_sockets: List[Dict[str, Any]] = []

    for f in args.files:
        path = Path(f)
        txt = path.read_text(errors="ignore")
        parsed = parse_stackdump_text(txt)
        combined["inputs"].append({"file": str(path), "data": parsed})
        if "sockets" in parsed:
            # tag each row with filename
            for row in parsed["sockets"]:
                row_with_src = dict(row)
                row_with_src["_source_file"] = str(path.name)
                all_sockets.append(row_with_src)

    if args.json_out:
        Path(args.json_out).write_text(json.dumps(combined, indent=2))
        print(f"Wrote JSON: {args.json_out}")

    if args.sockets_csv and all_sockets:
        # Derive header set
        keys: List[str] = []
        seen = set()
        for r in all_sockets:
            for k in r.keys():
                if k not in seen:
                    seen.add(k)
                    keys.append(k)
        # write CSV
        import csv
        with open(args.sockets_csv, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=keys)
            w.writeheader()
            for r in all_sockets:
                w.writerow(r)
        print(f"Wrote sockets CSV: {args.sockets_csv}")

    if not args.json_out:
        # default: print combined JSON to stdout
        print(json.dumps(combined, indent=2))

if __name__ == "__main__":
    main()
