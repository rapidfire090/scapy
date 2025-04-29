#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>

// Generate correct FIX NewOrderSingle with real BodyLength and Checksum
std::string generate_fix_message(int seq_num) {
    const std::string soh = "\x01";

    std::ostringstream body;
    body << "35=D" << soh
         << "34=" << seq_num << soh
         << "49=SENDER" << soh
         << "56=TARGET" << soh
         << "11=ORD" << seq_num << soh
         << "21=1" << soh
         << "40=1" << soh
         << "54=1" << soh
         << "38=100" << soh
         << "55=TESTSYM" << soh;

    std::string body_str = body.str();

    int body_length = body_str.size(); // BodyLength is size after 9=XXX| up to before 10=
    
    std::ostringstream fix;
    fix << "8=FIX.4.2" << soh
        << "9=" << body_length << soh
        << body_str;

    std::string fix_message = fix.str();

    // Calculate checksum
    int checksum = 0;
    for (char c : fix_message) {
        checksum += static_cast<unsigned char>(c);
    }
    checksum %= 256;

    std::ostringstream full_fix;
    full_fix << fix_message << "10=" << std::setfill('0') << std::setw(3) << checksum << soh;

    return full_fix.str();
}

int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cerr << "Usage: " << argv[0] << " <relay_ip> <relay_port> <send_interval_usec>\n";
        return 1;
    }

    const char* server_ip = argv[1];
    int server_port = std::stoi(argv[2]);
    int send_interval_usec = std::stoi(argv[3]);

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return 1;
    }

    sockaddr_in server_addr {};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port);
    if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) <= 0) {
        perror("inet_pton");
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
        std::cout << "Sent FIX message " << i << std::endl;
        usleep(send_interval_usec);
    }

    close(sock);
    return 0;
}
