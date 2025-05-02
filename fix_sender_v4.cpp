#include <iostream>
#include <sstream>
#include <string>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>
#include <algorithm>
#include <iomanip>

// Calculate FIX Checksum (tag 10)
std::string calculate_checksum(const std::string& msg) {
    int sum = 0;
    for (char c : msg) sum += static_cast<unsigned char>(c);
    int checksum = sum % 256;
    std::ostringstream ss;
    ss << std::setw(3) << std::setfill('0') << checksum;
    return ss.str();
}

// Generate FIX-compliant message with proper BodyLength and Checksum
std::string generate_fix_message(int seq_num) {
    std::ostringstream fix_body;
    std::string cl_ord_id = "ORD" + std::to_string(seq_num);

    fix_body
        << "35=D\x01"
        << "34=" << seq_num << "\x01"
        << "49=SENDER\x01"
        << "56=TARGET\x01"
        << "11=" << cl_ord_id << "\x01"
        << "21=1\x01"
        << "40=1\x01"
        << "54=1\x01"
        << "38=100\x01"
        << "55=TEST\x01";

    std::string body = fix_body.str();

    std::ostringstream full_msg;
    full_msg << "8=FIX.4.2\x01"
             << "9=" << body.length() << "\x01"
             << body;

    std::string msg = full_msg.str();
    std::string checksum = calculate_checksum(msg);
    msg += "10=" + checksum + "\x01";

    return msg;
}

int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cerr << "Usage: " << argv[0] << " <server_ip> <server_port> <sleep_ms>\n";
        return 1;
    }

    const char* server_ip = argv[1];
    int server_port = std::stoi(argv[2]);
    int sleep_ms = std::stoi(argv[3]);

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return 1;
    }

    sockaddr_in server_addr {};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port);
    if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) <= 0) {
        std::cerr << "Invalid IP address: " << server_ip << "\n";
        return 1;
    }

    if (connect(sock, (sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("connect");
        return 1;
    }

    for (int i = 1; ; ++i) {
        std::string fix = generate_fix_message(i);
        ssize_t sent = send(sock, fix.c_str(), fix.size(), 0);
        if (sent <= 0) {
            std::cerr << "Send error or connection closed\n";
            break;
        }

        std::cout << "Sent FIX msg with ClOrdID: ORD" << i << std::endl;
        usleep(sleep_ms * 1000); // sleep in microseconds
    }

    close(sock);
    return 0;
}
