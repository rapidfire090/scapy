#include <iostream>
#include <sstream>
#include <cstring>
#include <iomanip>
#include <thread>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#pragma pack(push, 1)

struct OuchLoginRequest {
    char message_type;              // 'U'
    char username[6];
    char password[20];
    char requested_session[4];
    char requested_sequence_number[20];
};

struct OuchAccepted {
    char message_type;              // 'A'
    char session_id[6];
};

struct OuchNewOrder {
    char message_type;                   // 'O'
    char order_token[14];
    char buy_sell_indicator;              // 'B' or 'S'
    uint32_t shares;                      // Unsigned int (network byte order)
    char stock[8];
    uint32_t price;                       // Unsigned int (price * 10000) (network byte order)
    uint32_t time_in_force;               // Unsigned int (network byte order)
    char firm[4];
    char display;
    char capacity;
    char intermarket_sweep_eligibility;
    uint32_t minimum_quantity;            // Unsigned int (network byte order)
    char cross_type;
    char customer_type;

    OuchNewOrder() {
        memset(this, 0, sizeof(*this));
    }
};

#pragma pack(pop)

// Parse a field from a FIX message
std::string parse_fix_value(const std::string& fix_msg, const std::string& tag) {
    size_t pos = fix_msg.find(tag);
    if (pos == std::string::npos) return "";
    size_t start = pos + tag.length();
    size_t end = fix_msg.find('\x01', start);
    return fix_msg.substr(start, end - start);
}

void process_connection(int client_sock, const char* forward_ip, int forward_port) {
    int forward_sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in forward_addr {};
    forward_addr.sin_family = AF_INET;
    forward_addr.sin_port = htons(forward_port);
    inet_pton(AF_INET, forward_ip, &forward_addr.sin_addr);

    if (connect(forward_sock, (sockaddr*)&forward_addr, sizeof(forward_addr)) < 0) {
        perror("connect to forward");
        close(client_sock);
        return;
    }

    // Send true OUCH 5.0 login
    OuchLoginRequest login {};
    login.message_type = 'U';
    std::memcpy(login.username, "USER01", 6);
    std::memcpy(login.password, "PASSWORD1234567890    ", 20); // pad to 20
    std::memset(login.requested_session, ' ', 4);
    std::memset(login.requested_sequence_number, ' ', 20);
    login.requested_sequence_number[0] = '0';

    send(forward_sock, &login, sizeof(login), 0);

    // Wait for ACK
    OuchAccepted ack {};
    ssize_t ack_len = recv(forward_sock, &ack, sizeof(ack), MSG_WAITALL);
    if (ack_len != sizeof(ack) || ack.message_type != 'A') {
        std::cerr << "Logon failed or invalid ACK" << std::endl;
        close(client_sock);
        close(forward_sock);
        return;
    }
    std::cout << "Login accepted. Session ID: " << std::string(ack.session_id, 6) << std::endl;

    char buffer[4096];
    ssize_t len;
    int order_counter = 1;
    while ((len = recv(client_sock, buffer, sizeof(buffer), 0)) > 0) {
        std::string fix(buffer, len);

        if (parse_fix_value(fix, "35=") == "D") {
            OuchNewOrder order;
            order.message_type = 'O';

            std::ostringstream token;
            token << "ORD" << std::setw(10) << std::setfill('0') << order_counter++;
            std::string token_str = token.str();
            std::memcpy(order.order_token, token_str.c_str(), std::min(token_str.size(), sizeof(order.order_token)));

            order.buy_sell_indicator = (parse_fix_value(fix, "54=") == "1") ? 'B' : 'S';
            order.shares = htonl(std::stoi(parse_fix_value(fix, "38=")));

            std::memset(order.stock, ' ', sizeof(order.stock));
            std::string symbol = parse_fix_value(fix, "55=");
            std::memcpy(order.stock, symbol.c_str(), std::min(symbol.size(), sizeof(order.stock)));

            order.price = htonl(1000000); // $100.00
            order.time_in_force = htonl(3600); // 1 hour

            std::memcpy(order.firm, "FIRM", 4);
            order.display = 'Y';
            order.capacity = 'A';
            order.intermarket_sweep_eligibility = 'N';
            order.minimum_quantity = htonl(0);
            order.cross_type = 'N';
            order.customer_type = 'R';

            send(forward_sock, &order, sizeof(order), 0);
            std::cout << "Sent OUCH NewOrder mapped from FIX" << std::endl;
        }
    }

    close(client_sock);
    close(forward_sock);
}

int main(int argc, char* argv[]) {
    if (argc != 5) {
        std::cerr << "Usage: " << argv[0] << " <listen_ip> <listen_port> <forward_ip> <forward_port>\n";
        return 1;
    }

    const char* listen_ip = argv[1];
    int listen_port = std::stoi(argv[2]);
    const char* forward_ip = argv[3];
    int forward_port = std::stoi(argv[4]);

    int listen_sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in listen_addr {};
    listen_addr.sin_family = AF_INET;
    listen_addr.sin_port = htons(listen_port);
    inet_pton(AF_INET, listen_ip, &listen_addr.sin_addr);

    bind(listen_sock, (sockaddr*)&listen_addr, sizeof(listen_addr));
    listen(listen_sock, 5);

    while (true) {
        sockaddr_in client_addr {};
        socklen_t client_len = sizeof(client_addr);
        int client_sock = accept(listen_sock, (sockaddr*)&client_addr, &client_len);
        if (client_sock >= 0) {
            std::thread(process_connection, client_sock, forward_ip, forward_port).detach();
        }
    }

    close(listen_sock);
    return 0;
}
