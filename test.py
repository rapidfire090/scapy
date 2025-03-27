from scapy.all import RawPcapReader, PcapWriter, Ether, IP, TCP, UDP, Raw
from datetime import datetime, timezone

def rewrite_soupbin_to_udp_with_nanos(input_pcap, output_pcap, custom_dst_ip, custom_dst_port, custom_dst_mac, custom_hex_header):
    """
    Reads a PCAP file using RawPcapReader to preserve nanosecond timestamps,
    removes SoupBinTCP headers, replaces them with a custom 16-byte header,
    converts TCP to UDP, and writes to a new PCAP file.
    """
    custom_header_bytes = bytes.fromhex(custom_hex_header)
    if len(custom_header_bytes) != 16:
        raise ValueError("Custom hex header must be exactly 16 bytes (32 hex characters).")

    pcap_writer = PcapWriter(output_pcap, append=False, sync=True)

    for pkt_data, pkt_metadata in RawPcapReader(input_pcap):
        try:
            original_packet = Ether(pkt_data)

            if not original_packet.haslayer(TCP) or not original_packet.haslayer(Raw):
                continue  # Skip non-TCP or non-payload packets

            raw_data = bytes(original_packet[Raw].load)
            new_payload = b""

            while raw_data:
                if len(raw_data) < 3:
                    break

                length_bytes = raw_data[:2]
                message_length = int.from_bytes(length_bytes, byteorder='big')

                if message_length < 1 or len(raw_data) < message_length + 2:
                    break

                payload = custom_header_bytes + raw_data[3:message_length + 2]
                new_payload += payload
                raw_data = raw_data[message_length + 2:]

            if new_payload:
                # Preserve source MAC or assign default
                src_mac = original_packet[Ether].src if original_packet.haslayer(Ether) else "00:00:00:00:00:01"

                new_packet = Ether(src=src_mac, dst=custom_dst_mac) / \
                             IP(src=original_packet[IP].src, dst=custom_dst_ip) / \
                             UDP(sport=original_packet[TCP].sport, dport=custom_dst_port) / \
                             Raw(load=new_payload)

                # Convert timestamp to nanoseconds
                ts_sec = pkt_metadata.sec
                ts_usec = pkt_metadata.usec
                ts_nsec = ts_usec * 1000  # µs to ns

                # Apply timestamp to packet before writing
                new_packet.time = ts_sec + ts_nsec / 1_000_000_000

                # Optionally print human-readable nanosecond timestamp
                dt = datetime.fromtimestamp(new_packet.time, tz=timezone.utc)
                print(f"[+] {dt.strftime('%Y-%m-%d %H:%M:%S.')}{ts_nsec % 1_000_000_000:09d}")

                pcap_writer.write(new_packet)

        except Exception as e:
            print(f"[!] Failed to process a packet: {e}")
            continue

    pcap_writer.close()
    print(f"\n✅ Output written to '{output_pcap}' with nanosecond timestamps preserved.")

# Example usage
if __name__ == "__main__":
    input_pcap_file = "soupbin_tcp_data.pcap"
    output_pcap_file = "modified_soupbin_udp.pcap"
    custom_dst_ip = "192.168.1.100"
    custom_dst_port = 5000
    custom_dst_mac = "AA:BB:CC:DD:EE:FF"
    custom_hex_header = "0102030405060708090A0B0C0D0E0F10"  # 16 bytes

    rewrite_soupbin_to_udp_with_nanos(
        input_pcap=input_pcap_file,
        output_pcap=output_pcap_file,
        custom_dst_ip=custom_dst_ip,
        custom_dst_port=custom_dst_port,
        custom_dst_mac=custom_dst_mac,
        custom_hex_header=custom_hex_header
    )
