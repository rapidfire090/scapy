#include <iostream>
#include <sstream>
#include <string>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>

std::string generate_fix_message(int seq_num) {
    std::ostringstream fix;
    fix << "8=FIX.4.2\x01"
        << "9=100\x01"
        << "35=D\x01"
        << "34=" << seq_num << "\x01"
        << "49=SENDER\x01"
        << "56=TARGET\x01"
        << "11=ORD" << seq_num << "\x01"
        << "21=1\x01"
        << "40=1\x01"
        << "54=1\x01"
        << "38=100\x01"
        << "55=TESTSYM\x01"
        << "10=000\x01";
    return fix.str();
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <relay_ip> <relay_port>\n";
        return 1;
    }

    const char* server_ip = argv[1];
    int server_port = std::stoi(argv[2]);

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in server_addr {};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port);
    inet_pton(AF_INET, server_ip, &server_addr.sin_addr);

    if (connect(sock, (sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("connect");
        return 1;
    }

    for (int i = 1; ; ++i) {
        std::string fix = generate_fix_message(i);
        send(sock, fix.c_str(), fix.size(), 0);
        std::cout << "Sent FIX message " << i << std::endl;
        usleep(10000); // 10ms between messages
    }

    close(sock);
    return 0;
}
