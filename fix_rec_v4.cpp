#include <iostream>
#include <thread>
#include <array>
#include <atomic>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <boost/lockfree/spsc_queue.hpp>

// Fixed-size message struct with length tracking
struct Message {
    std::array<char, 1024> data;
    size_t length;
};

// Preallocated lock-free queue
boost::lockfree::spsc_queue<Message, boost::lockfree::capacity<256>> queue;

// Receiving thread: from client socket into queue
void recv_thread(int client_sock) {
    while (true) {
        Message msg;
        ssize_t len = recv(client_sock, msg.data.data(), msg.data.size(), 0);
        if (len <= 0) break;

        msg.length = static_cast<size_t>(len);
        while (!queue.push(msg)) {
            std::this_thread::yield(); // wait until space is available
        }
    }
    close(client_sock);
}

// Sending thread: persistent loop; keeps socket open indefinitely
void send_thread(const char* forward_ip, int forward_port) {
    int forward_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (forward_sock < 0) {
        perror("socket (forward)");
        return;
    }

    sockaddr_in forward_addr{};
    forward_addr.sin_family = AF_INET;
    forward_addr.sin_port = htons(forward_port);
    inet_pton(AF_INET, forward_ip, &forward_addr.sin_addr);

    if (connect(forward_sock, (sockaddr*)&forward_addr, sizeof(forward_addr)) < 0) {
        perror("connect (forward)");
        close(forward_sock);
        return;
    }

    Message msg;
    while (true) {
        if (queue.pop(msg)) {
            send(forward_sock, msg.data.data(), msg.length, 0);
        } else {
            std::this_thread::yield();
        }
    }

    // never closes (persistent)
}

int main(int argc, char* argv[]) {
    if (argc < 5) {
        std::cerr << "Usage: " << argv[0] << " <listen_ip> <listen_port> <forward_ip> <forward_port>\n";
        return 1;
    }

    const char* listen_ip = argv[1];
    int listen_port = std::stoi(argv[2]);
    const char* forward_ip = argv[3];
    int forward_port = std::stoi(argv[4]);

    int listen_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_sock < 0) {
        perror("socket (listen)");
        return 1;
    }

    int opt = 1;
    setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in listen_addr{};
    listen_addr.sin_family = AF_INET;
    listen_addr.sin_port = htons(listen_port);
    inet_pton(AF_INET, listen_ip, &listen_addr.sin_addr);

    if (bind(listen_sock, (sockaddr*)&listen_addr, sizeof(listen_addr)) < 0) {
        perror("bind");
        return 1;
    }

    if (listen(listen_sock, 10) < 0) {
        perror("listen");
        return 1;
    }

    std::cout << "Listening on " << listen_ip << ":" << listen_port
              << ", forwarding to " << forward_ip << ":" << forward_port << std::endl;

    while (true) {
        sockaddr_in client_addr{};
        socklen_t addrlen = sizeof(client_addr);
        int client_sock = accept(listen_sock, (sockaddr*)&client_addr, &addrlen);
        if (client_sock >= 0) {
            std::thread(recv_thread, client_sock).detach();
            std::thread(send_thread, forward_ip, forward_port).detach();
        }
    }

    close(listen_sock);
    return 0;
}
