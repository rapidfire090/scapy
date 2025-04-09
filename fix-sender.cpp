#include <iostream>
#include <sstream>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>
#include <algorithm>

std::string generate_fix_message(int seq_num) {
    std::ostringstream fix;
    std::string cl_ord_id = "ORD" + std::to_string(seq_num);

    fix << "8=FIX.4.2|9=65|35=D|34=" << seq_num
        << "|49=SENDER|56=TARGET|11=" << cl_ord_id
        << "|21=1|40=1|54=1|38=100|55=TEST|10=000|";

    std::string msg = fix.str();
    std::replace(msg.begin(), msg.end(), '|', '\x01'); // FIX delimiter
    return msg;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <server_ip> <server_port>\n";
        return 1;
    }

    const char* server_ip = argv[1];
    int server_port = std::stoi(argv[2]);

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
        send(sock, fix.c_str(), fix.size(), 0);
        std::cout << "Sent FIX msg with ClOrdID: ORD" << i << std::endl;
        usleep(10000); // 10 ms
    }

    close(sock);
    return 0;
}