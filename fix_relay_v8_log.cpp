#include <iostream>
#include <thread>
#include <array>
#include <vector>
#include <cstring>
#include <atomic>
#include <chrono>
#include <fstream>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <pthread.h>
#include <sched.h>
#include <sys/resource.h>

// ---------------------- Message & LogEntry ----------------------

struct Message {
    std::array<char, 1024> data;
    size_t length;
    std::chrono::high_resolution_clock::time_point timestamp;
};

struct LogEntry {
    uint64_t now_ns;
    uint64_t latency_ns;
    std::array<char, 32> clordid;
};

// ---------------------- Lock-Free SPSC Queue ----------------------

template <typename T, size_t Capacity>
class SPSCQueue {
private:
    std::array<T, Capacity> buffer;
    std::atomic<size_t> head{0};
    std::atomic<size_t> tail{0};

public:
    bool push(const T& item) {
        size_t h = head.load(std::memory_order_relaxed);
        size_t next = (h + 1) % Capacity;
        if (next == tail.load(std::memory_order_acquire)) return false; // full
        buffer[h] = item;
        head.store(next, std::memory_order_release);
        return true;
    }

    bool pop(T& item) {
        size_t t = tail.load(std::memory_order_relaxed);
        if (t == head.load(std::memory_order_acquire)) return false; // empty
        item = buffer[t];
        tail.store((t + 1) % Capacity, std::memory_order_release);
        return true;
    }
};

// ---------------------- Globals ----------------------

SPSCQueue<Message, 256> queue;
SPSCQueue<LogEntry, 4096> log_queue;

std::atomic<bool> enable_latency{false};
std::string log_file_path;
int log_flush_interval_ms = 50;

// ---------------------- Helpers ----------------------

std::string extract_fix_tag11(const char* data, size_t len) {
    std::string msg(data, len);
    size_t pos = msg.find("11=");
    if (pos == std::string::npos) return "";
    size_t end = msg.find('\x01', pos);
    if (end == std::string::npos) return msg.substr(pos + 3);
    return msg.substr(pos + 3, end - pos - 3);
}

// ---------------------- Logging Thread ----------------------

void log_writer_thread(const std::string& file_path, int flush_interval_ms) {
    // Demote to normal scheduling
    sched_param param{};
    param.sched_priority = 0;
    pthread_setschedparam(pthread_self(), SCHED_OTHER, &param);

    std::ofstream out(file_path, std::ios::out | std::ios::app);
    if (!out) {
        std::cerr << "Failed to open log file: " << file_path << std::endl;
        return;
    }

    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(flush_interval_ms));
        LogEntry entry;
        while (log_queue.pop(entry)) {
            out << entry.now_ns << "," << entry.latency_ns << "," << entry.clordid.data() << "\n";
        }
        out.flush();
    }
}

// ---------------------- Threads ----------------------

void recv_thread(int client_sock) {
    std::cout << "[recv] Thread started" << std::endl;
    while (true) {
        Message msg;
        ssize_t len = recv(client_sock, msg.data.data(), msg.data.size(), 0);
        if (len <= 0) {
            if (len == 0)
                std::cout << "[recv] Client closed connection" << std::endl;
            else
                perror("[recv] recv error");
            break;
        }

        msg.length = static_cast<size_t>(len);
        if (enable_latency) {
            msg.timestamp = std::chrono::high_resolution_clock::now();
        }

        int spin = 0;
        while (!queue.push(msg)) {
            if (++spin > 1000) spin = 0;
        }
    }
    close(client_sock);
    std::cout << "[recv] Closed client socket" << std::endl;
}

void send_thread(const char* forward_ip, int forward_port) {
    std::cout << "[send] Connecting to " << forward_ip << ":" << forward_port << std::endl;

    int forward_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (forward_sock < 0) {
        perror("[send] socket");
        return;
    }

    int flag = 1;
    setsockopt(forward_sock, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    sockaddr_in forward_addr{};
    forward_addr.sin_family = AF_INET;
    forward_addr.sin_port = htons(forward_port);
    inet_pton(AF_INET, forward_ip, &forward_addr.sin_addr);

    if (connect(forward_sock, (sockaddr*)&forward_addr, sizeof(forward_addr)) < 0) {
        perror("[send] connect");
        close(forward_sock);
        return;
    }

    std::cout << "[send] Connected" << std::endl;

    Message msg;
    while (true) {
        int spin = 0;
        while (!queue.pop(msg)) {
            if (++spin > 1000) spin = 0;
        }

        if (enable_latency) {
            auto now = std::chrono::high_resolution_clock::now();
            auto now_ns = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count()
            );
            auto latency_ns = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(now - msg.timestamp).count()
            );
            std::string clordid = extract_fix_tag11(msg.data.data(), msg.length);

            LogEntry entry{now_ns, latency_ns};
            std::strncpy(entry.clordid.data(), clordid.c_str(), entry.clordid.size() - 1);
            log_queue.push(entry);
        }

        ssize_t sent = send(forward_sock, msg.data.data(), msg.length, 0);
        if (sent < 0) {
            perror("[send] send");
            break;
        }
    }

    close(forward_sock);
    std::cout << "[send] Forward socket closed" << std::endl;
}

// ---------------------- Main ----------------------

int main(int argc, char* argv[]) {
    if (argc < 7) {
        std::cerr << "Usage: " << argv[0]
                  << " <listen_ip> <listen_port> <forward_ip> <forward_port> <rx_cpu> <tx_cpu> "
                  << "[--measure-latency <log_file> <flush_interval_ms>]" << std::endl;
        return 1;
    }

    const char* listen_ip = argv[1];
    int listen_port = std::stoi(argv[2]);
    const char* forward_ip = argv[3];
    int forward_port = std::stoi(argv[4]);
    int rx_cpu = std::stoi(argv[5]);
    int tx_cpu = std::stoi(argv[6]);

    if (argc >= 10 && std::string(argv[7]) == "--measure-latency") {
        enable_latency = true;
        log_file_path = argv[8];
        log_flush_interval_ms = std::stoi(argv[9]);

        std::thread logger(log_writer_thread, log_file_path, log_flush_interval_ms);
        logger.detach();
    } else if (argc >= 8 && std::string(argv[7]) == "--measure-latency") {
        std::cerr << "Missing log file path and flush interval after --measure-latency" << std::endl;
        return 1;
    }

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

    std::cout << "[main] Listening on " << listen_ip << ":" << listen_port << std::endl;

    while (true) {
        sockaddr_in client_addr{};
        socklen_t addrlen = sizeof(client_addr);
        int client_sock = accept(listen_sock, (sockaddr*)&client_addr, &addrlen);
        if (client_sock >= 0) {
            std::cout << "[main] Accepted connection" << std::endl;

            std::thread rx(recv_thread, client_sock);
            cpu_set_t set_rx;
            CPU_ZERO(&set_rx);
            CPU_SET(rx_cpu, &set_rx);
            pthread_setaffinity_np(rx.native_handle(), sizeof(cpu_set_t), &set_rx);
            rx.detach();

            std::thread tx(send_thread, forward_ip, forward_port);
            cpu_set_t set_tx;
            CPU_ZERO(&set_tx);
            CPU_SET(tx_cpu, &set_tx);
            pthread_setaffinity_np(tx.native_handle(), sizeof(cpu_set_t), &set_tx);
            tx.detach();
        }
    }

    close(listen_sock);
    return 0;
}
