from scapy.all import RawPcapReader, PcapWriter, Ether, IP, TCP, UDP, Raw
from scapy.utils import PcapReader
from datetime import datetime, timezone

def rewrite_soupbin_to_udp_minimal(
    input_pcap, output_pcap,
    custom_dst_ip, custom_dst_port, custom_dst_mac,
    custom_hex_header
):
    custom_header_bytes = bytes.fromhex(custom_hex_header)
    if len(custom_header_bytes) != 16:
        raise ValueError("Custom hex header must be exactly 16 bytes (32 hex characters).")

    # Detect link-layer type
    try:
        with PcapReader(input_pcap) as reader:
            ll_type = reader.linktype
    except Exception as e:
        print(f"[!] Failed to determine link-layer type: {e}")
        ll_type = 1  # default to Ethernet

    pcap_writer = PcapWriter(output_pcap, append=False, sync=False, linktype=ll_type)
    processed_packets = []

    for pkt_data, pkt_metadata in RawPcapReader(input_pcap):
        try:
            if ll_type != 1:
                continue  # Only handle Ethernet

            ether_pkt = Ether(pkt_data)

            if not ether_pkt.haslayer(IP) or not ether_pkt.haslayer(TCP) or not ether_pkt.haslayer(Raw):
                continue

            original_ip = ether_pkt[IP]
            original_raw = ether_pkt[Raw].load
            original_udp_sport = ether_pkt[TCP].sport

            # Strip SoupBinTCP headers and prepend custom header
            payload = b""
            while original_raw:
                if len(original_raw) < 3:
                    break
                msg_len = int.from_bytes(original_raw[:2], byteorder='little')  # adjust if big-endian
                if msg_len < 1 or len(original_raw) < msg_len + 2:
                    break
                payload += custom_header_bytes + original_raw[3:msg_len + 2]
                original_raw = original_raw[msg_len + 2:]

            if not payload:
                continue

            # Replace only IP layer, remove TCP
            ether_pkt.dst = custom_dst_mac
            new_ip = IP(
                src=original_ip.src,
                dst=custom_dst_ip,
                ttl=original_ip.ttl,
                id=original_ip.id,
                flags=original_ip.flags
            )
            new_udp = UDP(sport=original_udp_sport, dport=custom_dst_port)
            ether_pkt.remove_payload()
            ether_pkt /= new_ip / new_udp / Raw(payload)

            # Apply nanosecond timestamp
            ether_pkt.time = pkt_metadata.sec + pkt_metadata.usec / 1_000_000_000

            processed_packets.append(ether_pkt)

        except Exception as e:
            print(f"[!] Skipping packet due to error: {e}")
            continue

    # Sort packets by time for clean output
    processed_packets.sort(key=lambda p: p.time)
    for pkt in processed_packets:
        pcap_writer.write(pkt)

    pcap_writer.close()
    print(f"\n✅ Wrote {len(processed_packets)} packets to '{output_pcap}' with nanosecond timestamps.")


# Example usage
if __name__ == "__main__":
    input_pcap_file = "soupbin_tcp_data.pcap"
    output_pcap_file = "modified_soupbin_udp.pcap"
    custom_dst_ip = "192.168.1.100"
    custom_dst_port = 5000
    custom_dst_mac = "AA:BB:CC:DD:EE:FF"
    custom_hex_header = "0102030405060708090A0B0C0D0E0F10"

    rewrite_soupbin_to_udp_minimal(
        input_pcap=input_pcap_file,
        output_pcap=output_pcap_file,
        custom_dst_ip=custom_dst_ip,
        custom_dst_port=custom_dst_port,
        custom_dst_mac=custom_dst_mac,
        custom_hex_header=custom_hex_header
    )
