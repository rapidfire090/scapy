#include <iostream>
#include <thread>
#include <cstring>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

// Profile Selector
enum class Profile {
    RELAY,
    FIX_TO_BINARY,
};

// Structures
struct BinaryLogin {
    char msg_type;       // 'L'
    char username[6];    // Username
    char password[10];   // Password
};

struct BinaryOrder {
    char msg_type;       // 'O'
    char side;           // 'B' (buy) or 'S' (sell)
    int32_t quantity;    // Quantity (network byte order)
    char symbol[8];      // Symbol (padded)
};

struct FixOrder {
    std::string cl_ord_id;
    char side;
    int quantity;
    std::string symbol;
};

FixOrder parse_fix_new_order(const std::string& fix_msg) {
    FixOrder order;
    size_t start = 0;
    while (start < fix_msg.size()) {
        size_t end = fix_msg.find('\x01', start);
        std::string field = fix_msg.substr(start, end - start);

        if (field.rfind("11=", 0) == 0) order.cl_ord_id = field.substr(3);
        else if (field.rfind("54=", 0) == 0) order.side = field[3];
        else if (field.rfind("38=", 0) == 0) order.quantity = std::stoi(field.substr(3));
        else if (field.rfind("55=", 0) == 0) order.symbol = field.substr(3);

        if (end == std::string::npos) break;
        start = end + 1;
    }
    return order;
}

BinaryOrder convert_to_binary(const FixOrder& order) {
    BinaryOrder b {};
    b.msg_type = 'O';
    b.side = (order.side == '1') ? 'B' : 'S';
    b.quantity = htonl(order.quantity);
    std::strncpy(b.symbol, order.symbol.c_str(), sizeof(b.symbol));
    return b;
}

void handle_connection(int client_sock, const char* forward_ip, int forward_port, Profile profile) {
    int forward_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (forward_sock < 0) {
        perror("socket (forward)");
        close(client_sock);
        return;
    }

    sockaddr_in forward_addr {};
    forward_addr.sin_family = AF_INET;
    forward_addr.sin_port = htons(forward_port);
    inet_pton(AF_INET, forward_ip, &forward_addr.sin_addr);

    if (connect(forward_sock, (sockaddr*)&forward_addr, sizeof(forward_addr)) < 0) {
        perror("connect (forward)");
        close(client_sock);
        close(forward_sock);
        return;
    }

    if (profile == Profile::FIX_TO_BINARY) {
        // Send login message
        BinaryLogin login {};
        login.msg_type = 'L';
        std::strncpy(login.username, "TEST", sizeof(login.username));
        std::strncpy(login.password, "12345678", sizeof(login.password));
        send(forward_sock, &login, sizeof(login), 0);

        // Wait for ACK or NAK
        char ack_buffer[7];
        ssize_t ack_len = recv(forward_sock, ack_buffer, sizeof(ack_buffer), MSG_WAITALL);
        if (ack_len < 1) {
            std::cerr << "No response to login.\n";
            close(client_sock);
            close(forward_sock);
            return;
        }
        if (ack_buffer[0] == 'N') {
            std::cerr << "Login rejected by forward server.\n";
            close(client_sock);
            close(forward_sock);
            return;
        } else if (ack_buffer[0] == 'A') {
            std::string session_id(ack_buffer + 1, 6);
            std::cout << "Login successful. Session ID: [" << session_id << "]\n";
        } else {
            std::cerr << "Unexpected login response.\n";
            close(client_sock);
            close(forward_sock);
            return;
        }
    }

    char buffer[4096];
    ssize_t len;
    while ((len = recv(client_sock, buffer, sizeof(buffer), 0)) > 0) {
        if (profile == Profile::RELAY) {
            send(forward_sock, buffer, len, 0);
        } else if (profile == Profile::FIX_TO_BINARY) {
            std::string fix(buffer, len);
            FixOrder order = parse_fix_new_order(fix);
            BinaryOrder bin = convert_to_binary(order);
            send(forward_sock, &bin, sizeof(bin), 0);
        }
    }

    close(client_sock);
    close(forward_sock);
}

int main(int argc, char* argv[]) {
    if (argc != 6) {
        std::cerr << "Usage: " << argv[0] << " <profile> <listen_ip> <listen_port> <forward_ip> <forward_port>\n";
        return 1;
    }

    std::string profile_str = argv[1];
    Profile profile;
    if (profile_str == "relay") profile = Profile::RELAY;
    else if (profile_str == "fix_to_binary") profile = Profile::FIX_TO_BINARY;
    else {
        std::cerr << "Unknown profile: " << profile_str << "\n";
        return 1;
    }

    const char* listen_ip = argv[2];
    int listen_port = std::stoi(argv[3]);
    const char* forward_ip = argv[4];
    int forward_port = std::stoi(argv[5]);

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
              << " forwarding to " << forward_ip << ":" << forward_port
              << " using profile: " << profile_str << std::endl;

    while (true) {
        sockaddr_in client_addr {};
        socklen_t addrlen = sizeof(client_addr);
        int client_sock = accept(listen_sock, (sockaddr*)&client_addr, &addrlen);
        if (client_sock >= 0) {
            std::thread(handle_connection, client_sock, forward_ip, forward_port, profile).detach();
        }
    }

    close(listen_sock);
    return 0;
}
