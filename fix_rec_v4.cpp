#include <iostream>
#include <thread>
#include <vector>
#include <array>
#include <cstring>
#include <atomic>
#include <csignal>
#include <optional>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sched.h>
#include <pthread.h>
#include <boost/lockfree/spsc_queue.hpp>

// Fixed-size message struct with length tracking
struct Message {
    std::array<char, 1024> data;
    size_t length;
};

// Preallocated lock-free queue
boost::lockfree::spsc_queue<Message, boost::lockfree::capacity<256>> queue;

// Thread pinning
void pin_thread_to_core(int core_id) {
    if (core_id < 0) return;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}

// Receiving thread: from client socket into queue
void recv_thread(int client_sock, int rx_cpu) {
    pin_thread_to_core(rx_cpu);
    while (true) {
        Message msg;
        ssize_t len = recv(client_sock, msg.data.data(), msg.data.size(), 0);
        if (len <= 0) break;

        msg.length = static_cast<size_t>(len);
        if (!queue.push(msg)) {
            std::cerr << "Queue overflow. Dropping message.\n";
        }
    }
    close(client_sock);
}

// Sending thread: from queue to forward socket
void send_thread(const char* forward_ip, int forward_port, int tx_cpu) {
    pin_thread_to_core(tx_cpu);
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
    while (queue.pop(msg)) {
        send(forward_sock, msg.data.data(), msg.length, 0);
    }

    close(forward_sock);
}

int main(int argc, char* argv[]) {
    if (argc < 5) {
        std::cerr << "Usage: " << argv[0] << " <listen_ip> <listen_port> <forward_ip> <forward_port> [--rx-cpu N] [--tx-cpu M]\n";
        return 1;
    }

    const char* listen_ip = argv[1];
    int listen_port = std::stoi(argv[2]);
    const char* forward_ip = argv[3];
    int forward_port = std::stoi(argv[4]);

    int rx_cpu = -1, tx_cpu = -1;
    for (int i = 5; i + 1 < argc; i += 2) {
        if (std::strcmp(argv[i], "--rx-cpu") == 0) rx_cpu = std::stoi(argv[i + 1]);
        else if (std::strcmp(argv[i], "--tx-cpu") == 0) tx_cpu = std::stoi(argv[i + 1]);
    }

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
    if (inet_pton(AF_INET, listen_ip, &listen_addr.sin_addr) <= 0) {
        std::cerr << "Invalid listen IP: " << listen_ip << "\n";
        return 1;
    }

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
            std::thread(recv_thread, client_sock, rx_cpu).detach();
            std::thread(send_thread, forward_ip, forward_port, tx_cpu).detach();
        }
    }

    close(listen_sock);
    return 0;
}
