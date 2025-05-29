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

struct Message {
    std::array<char, 1024> data;
    size_t length;
    uint64_t recv_start_ns;
    uint64_t recv_end_ns;
};

struct LogEntry {
    uint64_t recv_start_ns;
    uint64_t recv_delta_ns;
    uint64_t latency_ns;
    uint64_t send_delta_ns;
    uint64_t total_latency_ns;
    std::array<char, 32> clordid;
};

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
        if (next == tail.load(std::memory_order_acquire)) return false;
        buffer[h] = item;
        head.store(next, std::memory_order_release);
        return true;
    }

    bool pop(T& item) {
        size_t t = tail.load(std::memory_order_relaxed);
        if (t == head.load(std::memory_order_acquire)) return false;
        item = buffer[t];
        tail.store((t + 1) % Capacity, std::memory_order_release);
        return true;
    }
};

SPSCQueue<Message, 256> queue;
SPSCQueue<LogEntry, 4096> log_queue;

std::atomic<bool> enable_latency{false};
std::atomic<int> debug_level{0};
std::string log_file_path;
int log_flush_interval_ms = 50;

std::string extract_fix_tag11(const char* data, size_t len) {
    std::string msg(data, len);
    size_t pos = msg.find("11=");
    if (pos == std::string::npos) return "";
    size_t end = msg.find('\x01', pos);
    if (end == std::string::npos) return msg.substr(pos + 3);
    return msg.substr(pos + 3, end - pos - 3);
}

void log_writer_thread(const std::string& file_path, int flush_interval_ms) {
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
            out << entry.recv_start_ns << ","
                << entry.recv_delta_ns << ","
                << entry.latency_ns << ","
                << entry.send_delta_ns << ","
                << entry.total_latency_ns << ","
                << entry.clordid.data() << "\n";
        }
        out.flush();
    }
}

void recv_thread(int client_sock) {
    std::cout << "[recv] Thread started" << std::endl;
    while (true) {
        Message msg;
        msg.recv_start_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch()).count();

        ssize_t len = recv(client_sock, msg.data.data(), msg.data.size(), 0);

        msg.recv_end_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch()).count();

        if (len <= 0) {
            if (len == 0)
                std::cout << "[recv] Client closed connection" << std::endl;
            else
                perror("[recv] recv error");
            break;
        }

        msg.length = static_cast<size_t>(len);
        int spin = 0;
        while (!queue.push(msg)) {
            if (++spin > 1000) spin = 0;
        }
    }

    close(client_sock);
    std::cout << "[recv] Closed client socket" << std::endl;
}