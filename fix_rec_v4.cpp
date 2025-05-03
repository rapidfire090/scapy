#include <iostream>
#include <thread>
#include <array>
#include <cstring>
#include <atomic>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <boost/lockfree/spsc_queue.hpp>

struct Message {
    std::array<char, 1024> data;
    size_t length;
};

boost::lockfree::spsc_queue<Message, boost::lockfree::capacity<256>> queue;

void recv_thread(int client_sock) {
    std::cout << "[recv] Thread started for client socket " << client_sock << std::endl;
    while (true) {
        Message msg;
        ssize_t len = recv(client_sock, msg.data.data(), msg.data.size(), 0);
        if (len <= 0) {
            if (len == 0)
                std::cout << "[recv] Client closed connection" << std::endl;
            else
                perror("[recv] Error reading");
            break;
        }

        msg.length = static_cast<size_t>(len);
        while (!queue.push(msg)) {
            std::this_thread::yield();
        }
    }
    close(client_sock);
    std::cout << "[recv] Closed client socket " << client_sock << std::endl;
}

void send_thread(const char* forward_ip, int forward_port) {
    std::cout << "[send] Thread connecting to forward target " << forward_ip << ":" << forward_port << std::endl;

    int forward_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (forward_sock < 0) {
        perror("[send] socket");
        return;
    }

    sockaddr_in forward_addr{};
    forward_addr.sin_family = AF_INET;
    forward_addr.sin_port = htons(forward_port);
    inet_pton(AF_INET, forward_ip, &forward_addr.sin_addr);

    if (connect(forward_sock, (sockaddr*)&forward_addr, sizeof(forward_addr)) < 0) {
        perror("[send] connect");
        close(forward_sock);
        return;
    }

    std::cout << "[send] Connected to forward target" << std::endl;

    Message msg;
    while (true) {
        if (queue.pop(msg)) {
            ssize_t sent = send(forward_sock, msg.data.data(), msg.length, 0);
            if (sent < 0) {
                perror("[send] Error sending data");
                break;
            }
        } else {
            std::this_thread::yield();
        }
    }

    close(forward_sock);
    std::cout << "[send] Forward socket closed" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc < 5) {
        std::cerr << "Usage: " << argv[0] << " <listen_ip> <listen_port> <forward_ip> <forward_port>" << std::endl;
        return 1;
    }

    const char* listen_ip = argv[1];
    int listen_port = std::stoi(argv[2]);
    const char* forward_ip = argv[3];
    int forward_port = std::stoi(argv[4]);

    int listen_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_sock < 0) {
        perror("[main] socket");
        return 1;
    }

    int opt = 1;
    setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in listen_addr{};
    listen_addr.sin_family = AF_INET;
    listen_addr.sin_port = htons(listen_port);
    inet_pton(AF_INET, listen_ip, &listen_addr.sin_addr);

    if (bind(listen_sock, (sockaddr*)&listen_addr, sizeof(listen_addr)) < 0) {
        perror("[main] bind");
        return 1;
    }

    if (listen(listen_sock, 10) < 0) {
        perror("[main] listen");
        return 1;
    }

    std::cout << "[main] Listening on " << listen_ip << ":" << listen_port
              << ", forwarding to " << forward_ip << ":" << forward_port << std::endl;

    while (true) {
        sockaddr_in client_addr{};
        socklen_t addrlen = sizeof(client_addr);
        int client_sock = accept(listen_sock, (sockaddr*)&client_addr, &addrlen);
        if (client_sock >= 0) {
            std::cout << "[main] Accepted client connection: socket " << client_sock << std::endl;
            std::thread(recv_thread, client_sock).detach();
            std::thread(send_thread, forward_ip, forward_port).detach();
        } else {
            perror("[main] accept");
        }
    }

    close(listen_sock);
    std::cout << "[main] Closed listening socket" << std::endl;
    return 0;
}
