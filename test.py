from scapy.all import RawPcapReader, PcapWriter, Ether, IP, TCP, UDP, Raw
from scapy.utils import PcapReader
from datetime import datetime, timezone

def rewrite_soupbin_to_udp_with_nanos(
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
        ll_type = 1  # Default to Ethernet

    pcap_writer = PcapWriter(output_pcap, append=False, sync=True, linktype=ll_type)

    total_packets = 0
    converted_packets = 0
    skipped_due_to_format = 0
    skipped_due_to_no_payload = 0

    for pkt_data, pkt_metadata in RawPcapReader(input_pcap):
        total_packets += 1
        try:
            if ll_type != 1:
                print(f"[!] Skipping unsupported link-layer type {ll_type}")
                continue

            pkt = Ether(pkt_data)
            if not pkt.haslayer(TCP):
                skipped_due_to_format += 1
                continue
            if not pkt.haslayer(Raw):
                skipped_due_to_no_payload += 1
                continue

            raw_data = bytes(pkt[Raw].load)
            new_payload = b""

            while raw_data:
                if len(raw_data) < 3:
                    print("[!] Not enough data for header, skipping remaining payload")
                    break

                length_bytes = raw_data[:2]
                message_length = int.from_bytes(length_bytes, byteorder='big')

                if message_length < 1 or len(raw_data) < message_length + 2:
                    print(f"[!] Malformed message or too short: len={message_length}")
                    break

                payload = custom_header_bytes + raw_data[3:message_length + 2]
                new_payload += payload
                raw_data = raw_data[message_length + 2:]

            if not new_payload:
                print("[!] Packet contained no valid SoupBinTCP messages")
                continue

            src_mac = pkt[Ether].src
            new_packet = Ether(src=src_mac, dst=custom_dst_mac) / \
                         IP(src=pkt[IP].src, dst=custom_dst_ip) / \
                         UDP(sport=pkt[TCP].sport, dport=custom_dst_port) / \
                         Raw(load=new_payload)

            # Set timestamp
            ts_sec = pkt_metadata.sec
            ts_usec = pkt_metadata.usec
            ts_nsec = ts_usec * 1000
            new_packet.time = ts_sec + ts_nsec / 1_000_000_000

            # Log the timestamp and payload info
            dt = datetime.fromtimestamp(new_packet.time, tz=timezone.utc)
            print(f"[✓] Written packet at {dt.strftime('%Y-%m-%d %H:%M:%S.')}{ts_nsec % 1_000_000_000:09d} with {len(new_payload)} bytes")

            pcap_writer.write(new_packet)
            converted_packets += 1

        except Exception as e:
            print(f"[!] Error parsing packet: {e}")
            continue

    pcap_writer.close()

    print("\n🔍 Summary")
    print(f"  Total packets read      : {total_packets}")
    print(f"  Converted successfully  : {converted_packets}")
    print(f"  Skipped (no TCP)        : {skipped_due_to_format}")
    print(f"  Skipped (no Raw payload): {skipped_due_to_no_payload}")
    print(f"  Output written to       : {output_pcap}")

# Example usage
if __name__ == "__main__":
    input_pcap_file = "soupbin_tcp_data.pcap"
    output_pcap_file = "modified_soupbin_udp.pcap"
    custom_dst_ip = "192.168.1.100"
    custom_dst_port = 5000
    custom_dst_mac = "AA:BB:CC:DD:EE:FF"
    custom_hex_header = "0102030405060708090A0B0C0D0E0F10"

    rewrite_soupbin_to_udp_with_nanos(
        input_pcap=input_pcap_file,
        output_pcap=output_pcap_file,
        custom_dst_ip=custom_dst_ip,
        custom_dst_port=custom_dst_port,
        custom_dst_mac=custom_dst_mac,
        custom_hex_header=custom_hex_header
    )
