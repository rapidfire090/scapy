#include <iostream>
#include <thread>
#include <cstring>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

void relay(int client_sock, const char* out_ip, int out_port) {
    int forward_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (forward_sock < 0) {
        perror("socket (forward)");
        close(client_sock);
        return;
    }

    sockaddr_in forward_addr {};
    forward_addr.sin_family = AF_INET;
    forward_addr.sin_port = htons(out_port);
    inet_pton(AF_INET, out_ip, &forward_addr.sin_addr);

    if (connect(forward_sock, (sockaddr*)&forward_addr, sizeof(forward_addr)) < 0) {
        perror("connect (forward)");
        close(client_sock);
        close(forward_sock);
        return;
    }

    char buffer[2048];
    ssize_t len;
    while ((len = recv(client_sock, buffer, sizeof(buffer), 0)) > 0) {
        send(forward_sock, buffer, len, 0);
    }

    close(client_sock);
    close(forward_sock);
}

int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cerr << "Usage: " << argv[0] << " <listen_port> <forward_ip> <forward_port>\n";
        return 1;
    }

    int listen_port = std::stoi(argv[1]);
    const char* forward_ip = argv[2];
    int forward_port = std::stoi(argv[3]);

    int listen_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_sock < 0) {
        perror("socket (listen)");
        return 1;
    }

    int opt = 1;
    setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in listen_addr {};
    listen_addr.sin_family = AF_INET;
    listen_addr.sin_port = htons(listen_port);
    listen_addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(listen_sock, (sockaddr*)&listen_addr, sizeof(listen_addr)) < 0) {
        perror("bind");
        return 1;
    }

    if (listen(listen_sock, 10) < 0) {
        perror("listen");
        return 1;
    }

    std::cout << "Listening on port " << listen_port << " and forwarding to "
              << forward_ip << ":" << forward_port << std::endl;

    while (true) {
        sockaddr_in client_addr {};
        socklen_t addrlen = sizeof(client_addr);
        int client_sock = accept(listen_sock, (sockaddr*)&client_addr, &addrlen);
        if (client_sock >= 0) {
            std::thread(relay, client_sock, forward_ip, forward_port).detach();
        }
    }

    close(listen_sock);
    return 0;
}