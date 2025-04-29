#include <iostream>
#include <cstring>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <listen_ip> <listen_port>\n";
        return 1;
    }

    const char* listen_ip = argv[1];
    int listen_port = std::stoi(argv[2]);

    int server_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (server_sock < 0) {
        perror("socket");
        return 1;
    }

    sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(listen_port);
    if (inet_pton(AF_INET, listen_ip, &addr.sin_addr) <= 0) {
        std::cerr << "Invalid listen IP: " << listen_ip << "\n";
        return 1;
    }

    if (bind(server_sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind");
        return 1;
    }

    if (listen(server_sock, 5) < 0) {
        perror("listen");
        return 1;
    }

    std::cout << "Forward server listening on " << listen_ip << ":" << listen_port << std::endl;

    sockaddr_in client_addr {};
    socklen_t client_len = sizeof(client_addr);
    int client_sock = accept(server_sock, (sockaddr*)&client_addr, &client_len);

    // Print connected relay IP
    char client_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &(client_addr.sin_addr), client_ip, INET_ADDRSTRLEN);
    std::cout << "Accepted connection from " << client_ip << std::endl;

    // Receive login message
    char login[17];
    ssize_t login_len = recv(client_sock, login, sizeof(login), MSG_WAITALL);
    if (login_len != sizeof(login)) {
        std::cerr << "Incomplete login message\n";
        close(client_sock);
        return 1;
    }

    if (login[0] != 'L') {
        std::cerr << "Did not receive login message\n";
        close(client_sock);
        return 1;
    }

    std::string username(login + 1, 6);
    std::string password(login + 7, 10);

    std::cout << "Received login: Username=[" << username << "], Password=[" << password << "]\n";

    if (username.substr(0, 4) == "TEST" && password.substr(0, 8) == "12345678") {
        char response[7];
        response[0] = 'A';
        memcpy(response + 1, "ABC123", 6);
        send(client_sock, response, sizeof(response), 0);
        std::cout << "Login accepted. Session ID: ABC123\n";
    } else {
        send(client_sock, "N", 1, 0);
        std::cout << "Login rejected.\n";
        close(client_sock);
        return 1;
    }

    char buffer[1024];
    while (recv(client_sock, buffer, sizeof(buffer), 0) > 0) {
        std::cout << "Received binary order.\n";
    }

    close(client_sock);
    close(server_sock);
    return 0;
}
