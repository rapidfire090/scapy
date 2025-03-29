import dpkt
import socket
import struct

def rewrite_soupbin_dpkt(
    input_pcap,
    output_pcap,
    custom_dst_ip,
    custom_dst_port,
    custom_dst_mac,
    custom_hex_header
):
    custom_header = bytes.fromhex(custom_hex_header)
    if len(custom_header) != 16:
        raise ValueError("Custom hex header must be exactly 16 bytes")

    with open(input_pcap, 'rb') as f:
        pcap = dpkt.pcap.Reader(f)
        packets = []

        for ts, buf in pcap:
            try:
                eth = dpkt.ethernet.Ethernet(buf)

                if not isinstance(eth.data, dpkt.ip.IP):
                    continue
                ip = eth.data

                if not isinstance(ip.data, dpkt.tcp.TCP):
                    continue
                tcp = ip.data

                if not tcp.data:
                    continue

                raw_data = tcp.data
                payload_parts = []
                while len(raw_data) >= 3:
                    msg_len = int.from_bytes(raw_data[:2], byteorder='little')
                    if msg_len < 1 or len(raw_data) < msg_len + 2:
                        break
                    payload_parts.append(custom_header + raw_data[3:msg_len + 2])
                    raw_data = raw_data[msg_len + 2:]

                if not payload_parts:
                    continue

                payload = b''.join(payload_parts)

                # Build new IP/UDP packet
                udp = dpkt.udp.UDP(
                    sport=tcp.sport,
                    dport=custom_dst_port,
                    data=payload
                )
                udp.ulen = len(udp)

                new_ip = dpkt.ip.IP(
                    src=ip.src,
                    dst=socket.inet_aton(custom_dst_ip),
                    p=dpkt.ip.IP_PROTO_UDP,
                    ttl=ip.ttl,
                    id=ip.id,
                    off=ip.off,
                    data=udp
                )
                new_ip.len = len(new_ip)

                eth.dst = bytes.fromhex(custom_dst_mac.replace(':', ''))
                eth.data = new_ip

                packets.append((ts, bytes(eth)))

            except Exception as e:
                print(f"[!] Skipped a packet: {e}")
                continue

    # Sort packets by timestamp
    packets.sort(key=lambda p: p[0])

    with open(output_pcap, 'wb') as f:
        writer = dpkt.pcap.Writer(f)
        for ts, pkt in packets:
            writer.writepkt(pkt, ts=ts)

    print(f"\n✅ Wrote {len(packets)} packets to '{output_pcap}' (dpkt, fast mode)")


# Example usage
if __name__ == '__main__':
    rewrite_soupbin_dpkt(
        input_pcap='soupbin_tcp_data.pcap',
        output_pcap='modified_soupbin_udp_dpkt.pcap',
        custom_dst_ip='192.168.1.100',
        custom_dst_port=5000,
        custom_dst_mac='aa:bb:cc:dd:ee:ff',
        custom_hex_header='0102030405060708090A0B0C0D0E0F10'
    )
