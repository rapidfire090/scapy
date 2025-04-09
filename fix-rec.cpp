// fix_relay_tcp.cpp
#include <iostream>
#include <thread>
#include <cstring>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

void relay(int client_sock, const char* out_ip, int out_port) {
    int forward_sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in forward_addr {};
    forward_addr.sin_family = AF_INET;
    forward_addr.sin_port = htons(out_port);
    inet_pton(AF_INET, out_ip, &forward_addr.sin_addr);

    if (connect(forward_sock, (sockaddr*)&forward_addr, sizeof(forward_addr)) < 0) {
        perror("connect (outgoing)");
        close(client_sock);
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

int main() {
    const int listen_port = 4000;
    const char* forward_ip = "192.168.2.2"; // Next-hop IP
    const int forward_port = 4001;

    int listen_sock = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in listen_addr {};
    listen_addr.sin_family = AF_INET;
    listen_addr.sin_port = htons(listen_port);
    listen_addr.sin_addr.s_addr = INADDR_ANY;

    bind(listen_sock, (sockaddr*)&listen_addr, sizeof(listen_addr));
    listen(listen_sock, 10);

    while (true) {
        sockaddr_in client_addr;
        socklen_t addrlen = sizeof(client_addr);
        int client_sock = accept(listen_sock, (sockaddr*)&client_addr, &addrlen);
        std::thread(relay, client_sock, forward_ip, forward_port).detach();
    }

    close(listen_sock);
    return 0;
}