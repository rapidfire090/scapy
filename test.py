from scapy.all import RawPcapReader, PcapWriter, conf, Ether, IP, TCP, UDP, Raw
from scapy.utils import PcapReader
from scapy.layers.l2 import L2Types
from datetime import datetime, timezone

def rewrite_soupbin_to_udp_with_nanos(input_pcap, output_pcap, custom_dst_ip, custom_dst_port, custom_dst_mac, custom_hex_header):
    custom_header_bytes = bytes.fromhex(custom_hex_header)
    if len(custom_header_bytes) != 16:
        raise ValueError("Custom hex header must be exactly 16 bytes (32 hex characters).")

    # 🔍 Detect original link-layer type
    try:
        with PcapReader(input_pcap) as reader:
            ll_type = reader.linktype
    except Exception as e:
        print(f"[!] Failed to determine link-layer type: {e}")
        ll_type = 1  # Default to Ethernet if unknown

    # 🧠 Use PcapWriter with the correct link-layer type
    pcap_writer = PcapWriter(output_pcap, append=False, sync=True, linktype=ll_type)

    for pkt_data, pkt_metadata in RawPcapReader(input_pcap):
        try:
            # Use appropriate decoder — assuming Ethernet (DLT 1)
            if ll_type == 1:
                pkt = Ether(pkt_data)
            else:
                print(f"[!] Unsupported link-layer type {ll_type}, skipping packet.")
                continue

            if not pkt.haslayer(TCP) or not pkt.haslayer(Raw):
                continue

            raw_data = bytes(pkt[Raw].load)
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
                src_mac = pkt[Ether].src if pkt.haslayer(Ether) else "00:00:00:00:00:01"

                new_packet = Ether(src=src_mac, dst=custom_dst_mac) / \
                             IP(src=pkt[IP].src, dst=custom_dst_ip) / \
                             UDP(sport=pkt[TCP].sport, dport=custom_dst_port) / \
                             Raw(load=new_payload)

                # Preserve nanosecond timestamp
                ts_sec = pkt_metadata.sec
                ts_usec = pkt_metadata.usec
                ts_nsec = ts_usec * 1000
                new_packet.time = ts_sec + ts_nsec / 1_000_000_000

                dt = datetime.fromtimestamp(new_packet.time, tz=timezone.utc)
                print(f"[+] {dt.strftime('%Y-%m-%d %H:%M:%S.')}{ts_nsec % 1_000_000_000:09d}")

                pcap_writer.write(new_packet)

        except Exception as e:
            print(f"[!] Skipping packet due to error: {e}")
            continue

    pcap_writer.close()
    print(f"\n✅ Output written to '{output_pcap}' with timestamps preserved and proper link-layer type.")

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
