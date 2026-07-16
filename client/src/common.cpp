#include "common.hpp"

#include <cstring>
#include <iomanip>

namespace kv {

std::string DataValue::toString() const {
    return "[value=" + std::to_string(value) + ", version=" + std::to_string(version) + "]";
}

bool newerThan(const DataValue& first, const DataValue& second) {
    return first.version > second.version;
}

std::string clientMessageTypeToString(ClientMessage::Type type) {
    switch (type) {
        case ClientMessage::Type::GET: return "GET";
        case ClientMessage::Type::PUT: return "PUT";
        case ClientMessage::Type::ACK: return "ACK";
        case ClientMessage::Type::NACK: return "NACK";
    }
    return "GET";
}

static ClientMessage::Type clientMessageTypeFromString(const std::string& s) {
    if (s == "GET") return ClientMessage::Type::GET;
    if (s == "PUT") return ClientMessage::Type::PUT;
    if (s == "ACK") return ClientMessage::Type::ACK;
    if (s == "NACK") return ClientMessage::Type::NACK;
    throw std::runtime_error("Invalid client message type: " + s);
}

std::string serverMessageTypeToString(ServerMessage::Type type) {
    switch (type) {
        case ServerMessage::Type::REQUEST_CONNECTION: return "REQUEST_CONNECTION";
        case ServerMessage::Type::ACK_CONNECTION: return "ACK_CONNECTION";
        case ServerMessage::Type::CONNECTION_EXISTS: return "CONNECTION_EXISTS";
        case ServerMessage::Type::REPLICATE: return "REPLICATE";
        case ServerMessage::Type::ACK_REPLICATE: return "ACK_REPLICATE";
        case ServerMessage::Type::SYNC: return "SYNC";
        case ServerMessage::Type::ACK_SYNC: return "ACK_SYNC";
    }
    return "REQUEST_CONNECTION";
}

static ServerMessage::Type serverMessageTypeFromString(const std::string& s) {
    if (s == "REQUEST_CONNECTION") return ServerMessage::Type::REQUEST_CONNECTION;
    if (s == "ACK_CONNECTION") return ServerMessage::Type::ACK_CONNECTION;
    if (s == "CONNECTION_EXISTS") return ServerMessage::Type::CONNECTION_EXISTS;
    if (s == "REPLICATE") return ServerMessage::Type::REPLICATE;
    if (s == "ACK_REPLICATE") return ServerMessage::Type::ACK_REPLICATE;
    if (s == "SYNC") return ServerMessage::Type::SYNC;
    if (s == "ACK_SYNC") return ServerMessage::Type::ACK_SYNC;
    throw std::runtime_error("Invalid server message type: " + s);
}

std::string ClientMessage::toString() const {
    return "[message_type=" + clientMessageTypeToString(type) + ", content=" + content +
           ", key=" + std::to_string(key) + ", value=" + std::to_string(value) +
           ", serverIndex=" + std::to_string(serverIndex) + "]";
}

std::string ServerMessage::toString() const {
    return "[message_type=" + serverMessageTypeToString(type) + ", serverIndex=" +
           std::to_string(serverIndex) + "]";
}

std::string ClientRequest::toString() const {
    return "[id=" + id + ", requestorIndex=" + std::to_string(requestorIndex) +
           ", key=" + std::to_string(key) + ", dataValue=" + dataValue.toString() + "]";
}

std::vector<std::string> split(const std::string& input, char delim) {
    std::vector<std::string> parts;
    std::stringstream ss(input);
    std::string item;
    while (std::getline(ss, item, delim)) parts.push_back(item);
    if (!input.empty() && input.back() == delim) parts.emplace_back();
    return parts;
}

std::string encodeText(const std::string& s) {
    static const char* hex = "0123456789ABCDEF";
    std::string out;
    for (unsigned char c : s) {
        if (std::isalnum(c) || c == '_' || c == '-' || c == '.' || c == ' ') {
            out.push_back(static_cast<char>(c));
        } else {
            out.push_back('%');
            out.push_back(hex[c >> 4]);
            out.push_back(hex[c & 0x0F]);
        }
    }
    return out;
}

static int hexValue(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    return -1;
}

std::string decodeText(const std::string& s) {
    std::string out;
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '%' && i + 2 < s.size()) {
            int a = hexValue(s[i + 1]);
            int b = hexValue(s[i + 2]);
            if (a >= 0 && b >= 0) {
                out.push_back(static_cast<char>((a << 4) | b));
                i += 2;
                continue;
            }
        }
        out.push_back(s[i]);
    }
    return out;
}

static std::string serializeDataMap(const std::unordered_map<int, DataValue>& data) {
    std::ostringstream os;
    bool first = true;
    for (const auto& entry : data) {
        if (!first) os << ',';
        first = false;
        os << entry.first << ':' << entry.second.value << ':' << entry.second.version;
    }
    return os.str();
}

static std::unordered_map<int, DataValue> parseDataMap(const std::string& s) {
    std::unordered_map<int, DataValue> data;
    if (s.empty()) return data;
    for (const std::string& item : split(s, ',')) {
        if (item.empty()) continue;
        auto p = split(item, ':');
        if (p.size() != 3) continue;
        data[std::stoi(p[0])] = DataValue(std::stoi(p[1]), std::stoi(p[2]));
    }
    return data;
}

std::string serializeClientMessage(const ClientMessage& message) {
    std::ostringstream os;
    os << "C|" << clientMessageTypeToString(message.type) << '|' << message.key << '|'
       << message.value << '|' << message.serverIndex << '|' << encodeText(message.content);
    return os.str();
}

ClientMessage parseClientMessage(const std::string& payload) {
    auto p = split(payload, '|');
    if (p.size() < 6 || p[0] != "C") throw std::runtime_error("Malformed client message");
    ClientMessage m;
    m.type = clientMessageTypeFromString(p[1]);
    m.key = std::stoi(p[2]);
    m.value = std::stoi(p[3]);
    m.serverIndex = std::stoi(p[4]);
    m.content = decodeText(p[5]);
    return m;
}

std::string serializeServerMessage(const ServerMessage& message) {
    std::ostringstream os;
    os << "S|" << serverMessageTypeToString(message.type) << '|'
       << message.serverIndex << '|' << message.replicateForIndex << '|'
       << encodeText(message.requestUuid) << '|' << message.key << '|'
       << message.dataValue.value << '|' << message.dataValue.version << '|'
       << serializeDataMap(message.data);
    return os.str();
}

ServerMessage parseServerMessage(const std::string& payload) {
    auto p = split(payload, '|');
    if (p.size() < 9 || p[0] != "S") throw std::runtime_error("Malformed server message");
    ServerMessage m;
    m.type = serverMessageTypeFromString(p[1]);
    m.serverIndex = std::stoi(p[2]);
    m.replicateForIndex = std::stoi(p[3]);
    m.requestUuid = decodeText(p[4]);
    m.key = std::stoi(p[5]);
    m.dataValue = DataValue(std::stoi(p[6]), std::stoi(p[7]));
    m.data = parseDataMap(p[8]);
    return m;
}

SocketConnection::SocketConnection(int fd) : fd_(fd) {}

SocketConnection::~SocketConnection() { close(); }

SocketConnection::SocketConnection(SocketConnection&& other) noexcept : fd_(other.fd_) {
    other.fd_ = -1;
}

SocketConnection& SocketConnection::operator=(SocketConnection&& other) noexcept {
    if (this != &other) {
        close();
        fd_ = other.fd_;
        other.fd_ = -1;
    }
    return *this;
}

void SocketConnection::close() {
    if (fd_ >= 0) {
        ::shutdown(fd_, SHUT_RDWR);
        ::close(fd_);
        fd_ = -1;
    }
}

void SocketConnection::setReadTimeoutMillis(int millis) {
    timeval tv{};
    tv.tv_sec = millis / 1000;
    tv.tv_usec = (millis % 1000) * 1000;
    setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
}

bool SocketConnection::sendAll(const char* buffer, size_t bytes) {
    size_t sent = 0;
    while (sent < bytes) {
        ssize_t n = ::send(fd_, buffer + sent, bytes - sent, 0);
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

bool SocketConnection::recvAll(char* buffer, size_t bytes) {
    size_t received = 0;
    while (received < bytes) {
        ssize_t n = ::recv(fd_, buffer + received, bytes - received, 0);
        if (n <= 0) return false;
        received += static_cast<size_t>(n);
    }
    return true;
}

void SocketConnection::sendFrame(const std::string& payload) {
    if (fd_ < 0) throw std::runtime_error("Socket is closed");
    uint32_t len = htonl(static_cast<uint32_t>(payload.size()));
    if (!sendAll(reinterpret_cast<const char*>(&len), sizeof(len)) || !sendAll(payload.data(), payload.size())) {
        throw std::runtime_error("Failed to send frame");
    }
}

std::string SocketConnection::recvFrame() {
    if (fd_ < 0) throw std::runtime_error("Socket is closed");
    uint32_t netLen = 0;
    if (!recvAll(reinterpret_cast<char*>(&netLen), sizeof(netLen))) throw std::runtime_error("Failed to read frame length");
    uint32_t len = ntohl(netLen);
    if (len > 10 * 1024 * 1024) throw std::runtime_error("Frame too large");
    std::string payload(len, '\0');
    if (len > 0 && !recvAll(payload.data(), len)) throw std::runtime_error("Failed to read frame payload");
    return payload;
}

void ClientConnection::send(const ClientMessage& message) { conn_.sendFrame(serializeClientMessage(message)); }
ClientMessage ClientConnection::recv() { return parseClientMessage(conn_.recvFrame()); }
void ServerConnection::send(const ServerMessage& message) { conn_.sendFrame(serializeServerMessage(message)); }
ServerMessage ServerConnection::recv() { return parseServerMessage(conn_.recvFrame()); }

std::vector<std::string> readLines(const std::string& filename) {
    std::ifstream in(filename);
    if (!in) throw std::runtime_error("Unable to open " + filename);
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (!line.empty()) lines.push_back(line);
    }
    return lines;
}

std::string currentHostname() {
    char buffer[256]{};
    if (gethostname(buffer, sizeof(buffer) - 1) != 0) throw std::runtime_error("gethostname failed");
    return std::string(buffer);
}

static addrinfo* resolveHost(const std::string& hostname, int port, int flags) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = flags;
    addrinfo* result = nullptr;
    std::string portStr = std::to_string(port);
    int rc = getaddrinfo(hostname.c_str(), portStr.c_str(), &hints, &result);
    if (rc != 0) throw std::runtime_error(gai_strerror(rc));
    return result;
}

int connectToHost(const std::string& hostname, int port) {
    addrinfo* result = resolveHost(hostname, port, 0);
    int sock = -1;
    for (addrinfo* rp = result; rp != nullptr; rp = rp->ai_next) {
        sock = ::socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (sock < 0) continue;
        if (::connect(sock, rp->ai_addr, rp->ai_addrlen) == 0) break;
        ::close(sock);
        sock = -1;
    }
    freeaddrinfo(result);
    if (sock < 0) throw std::runtime_error("Could not connect to " + hostname + ":" + std::to_string(port));
    return sock;
}

int bindServerSocket(const std::string& hostname, int port, int backlog) {
    addrinfo* result = resolveHost(hostname, port, AI_PASSIVE);
    int sock = -1;
    int yes = 1;
    for (addrinfo* rp = result; rp != nullptr; rp = rp->ai_next) {
        sock = ::socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (sock < 0) continue;
        setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
        if (::bind(sock, rp->ai_addr, rp->ai_addrlen) == 0 && ::listen(sock, backlog) == 0) break;
        ::close(sock);
        sock = -1;
    }
    freeaddrinfo(result);
    if (sock < 0) throw std::runtime_error("Could not bind to " + hostname + ":" + std::to_string(port));
    return sock;
}

std::string generateUuid() {
    static thread_local std::mt19937_64 rng{std::random_device{}()};
    std::ostringstream os;
    os << std::hex << std::setfill('0') << std::setw(16) << rng() << std::setw(16) << rng();
    return os.str();
}

std::vector<int> generateForwardConnectionList(int serverIndex, int size) {
    std::vector<int> possibilities;
    for (int i = 1; i < size; ++i) possibilities.push_back((serverIndex + i) % size);
    return possibilities;
}

bool inReplicaSet(int candidate, int serverIndex, int size) {
    return candidate == (serverIndex + 1) % size || candidate == (serverIndex + 2) % size;
}

DataValue updateToGreaterVersion(const std::unordered_map<int, DataValue>& replica, int key, const DataValue& newDataValue) {
    auto it = replica.find(key);
    if (it == replica.end()) return newDataValue;
    return newerThan(newDataValue, it->second) ? newDataValue : it->second;
}

std::unordered_map<int, DataValue> mergeData(std::unordered_map<int, DataValue> first,
                                             const std::unordered_map<int, DataValue>& second,
                                             int hashValue,
                                             int modulus) {
    int normalizedHash = modIndex(hashValue, modulus);
    for (const auto& entry : second) {
        int key = entry.first;
        if (modIndex(key, modulus) != normalizedHash) continue;
        auto it = first.find(key);
        if (it == first.end() || newerThan(entry.second, it->second)) first[key] = entry.second;
    }
    return first;
}

int modIndex(int value, int modulus) {
    int r = value % modulus;
    return r < 0 ? r + modulus : r;
}

} // namespace kv
