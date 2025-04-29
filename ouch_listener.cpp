#include <iostream>
#include <cstring>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#pragma pack(push, 1)
struct OuchLoginRequest {
    char message_type;
    char username[6];
    char password[20];
    char requested_session[4];
    char requested_sequence[20];
};

struct OuchAccepted {
    char message_type;
    char session_id[6];
};
#pragma pack(pop)

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <listen_ip> <listen_port>\n";
        return 1;
    }

    const char* listen_ip = argv[1];
    int listen_port = std::stoi(argv[2]);

    int server_sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(listen_port);
    inet_pton(AF_INET, listen_ip, &addr.sin_addr);

    bind(server_sock, (sockaddr*)&addr, sizeof(addr));
    listen(server_sock, 5);

    sockaddr_in client_addr {};
    socklen_t client_len = sizeof(client_addr);
    int client_sock = accept(server_sock, (sockaddr*)&client_addr, &client_len);

    // Receive login
    OuchLoginRequest login {};
    recv(client_sock, &login, sizeof(login), MSG_WAITALL);

    if (login.message_type == 'U') {
        OuchAccepted ack {};
        ack.message_type = 'A';
        memcpy(ack.session_id, "ABC123", 6);
        send(client_sock, &ack, sizeof(ack), 0);
        std::cout << "Sent OUCH 5.0 ACK" << std::endl;
    } else {
        std::cerr << "Unexpected login message type!" << std::endl;
        close(client_sock);
        return 1;
    }

    char buffer[512];
    while (recv(client_sock, buffer, sizeof(buffer), 0) > 0) {
        std::cout << "Received OUCH message." << std::endl;
    }

    close(client_sock);
    close(server_sock);
    return 0;
}
