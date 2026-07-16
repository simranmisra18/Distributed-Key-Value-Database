#pragma once

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace kv {

constexpr int CLIENT_CONNECTION_PORT = 12121;
constexpr int SERVER_CONNECTION_PORT = 12122;
constexpr const char* HOSTNAMES_FILENAME = "server-hostnames.txt";

struct DataValue {
    int value{-1};
    int version{0};

    DataValue() = default;
    explicit DataValue(int v) : value(v), version(0) {}
    DataValue(int v, int ver) : value(v), version(ver) {}

    std::string toString() const;
};

bool newerThan(const DataValue& first, const DataValue& second);

struct ClientMessage {
    enum class Type { GET, PUT, ACK, NACK };
    Type type{Type::GET};
    int key{-1};
    int value{-1};
    int serverIndex{-1};
    std::string content;

    std::string toString() const;
};

struct ServerMessage {
    enum class Type {
        REQUEST_CONNECTION,
        ACK_CONNECTION,
        CONNECTION_EXISTS,
        REPLICATE,
        ACK_REPLICATE,
        SYNC,
        ACK_SYNC
    };

    Type type{Type::REQUEST_CONNECTION};
    int serverIndex{-1};
    int replicateForIndex{-1};
    std::string requestUuid;
    int key{-1};
    DataValue dataValue;
    std::unordered_map<int, DataValue> data;

    std::string toString() const;
};

struct ClientRequest {
    std::string id;
    int requestorIndex{-1};
    int key{-1};
    DataValue dataValue;

    std::string toString() const;
};

struct ClientResponse {
    std::string id;
    int requestorIndex{-1};
    bool succeeded{false};
    std::string content;
    int key{-1};
    int value{-1};
};

template <typename T>
class ThreadSafeQueue {
public:
    void push(const T& value) {
        std::lock_guard<std::mutex> lock(mu_);
        q_.push(value);
        cv_.notify_one();
    }

    bool tryPop(T& out) {
        std::lock_guard<std::mutex> lock(mu_);
        if (q_.empty()) return false;
        out = q_.front();
        q_.pop();
        return true;
    }

    bool waitPopFor(T& out, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mu_);
        if (!cv_.wait_for(lock, timeout, [&] { return !q_.empty(); })) return false;
        out = q_.front();
        q_.pop();
        return true;
    }

private:
    std::mutex mu_;
    std::condition_variable cv_;
    std::queue<T> q_;
};

class SocketConnection {
public:
    explicit SocketConnection(int fd = -1);
    ~SocketConnection();

    SocketConnection(const SocketConnection&) = delete;
    SocketConnection& operator=(const SocketConnection&) = delete;

    SocketConnection(SocketConnection&& other) noexcept;
    SocketConnection& operator=(SocketConnection&& other) noexcept;

    int fd() const { return fd_; }
    bool isActive() const { return fd_ >= 0; }
    void close();
    void setReadTimeoutMillis(int millis);
    void sendFrame(const std::string& payload);
    std::string recvFrame();

private:
    int fd_{-1};
    bool recvAll(char* buffer, size_t bytes);
    bool sendAll(const char* buffer, size_t bytes);
};

class ClientConnection {
public:
    explicit ClientConnection(int fd = -1) : conn_(fd) {}
    bool isActive() const { return conn_.isActive(); }
    void close() { conn_.close(); }
    void send(const ClientMessage& message);
    ClientMessage recv();

private:
    SocketConnection conn_;
};

class ServerConnection {
public:
    explicit ServerConnection(int fd = -1, int serverIndex = -1) : conn_(fd), serverIndex_(serverIndex) {}
    bool isActive() const { return conn_.isActive(); }
    void close() { conn_.close(); }
    void setReadTimeoutMillis(int millis) { conn_.setReadTimeoutMillis(millis); }
    void send(const ServerMessage& message);
    ServerMessage recv();
    int getServerIndex() const { return serverIndex_; }
    void setServerIndex(int idx) { serverIndex_ = idx; }

private:
    SocketConnection conn_;
    int serverIndex_{-1};
};

std::vector<std::string> readLines(const std::string& filename);
std::string currentHostname();
int connectToHost(const std::string& hostname, int port);
int bindServerSocket(const std::string& hostname, int port, int backlog = 10);
std::string generateUuid();
std::vector<int> generateForwardConnectionList(int serverIndex, int size);
bool inReplicaSet(int candidate, int serverIndex, int size);
DataValue updateToGreaterVersion(const std::unordered_map<int, DataValue>& replica, int key, const DataValue& newDataValue);
std::unordered_map<int, DataValue> mergeData(std::unordered_map<int, DataValue> first,
                                             const std::unordered_map<int, DataValue>& second,
                                             int hashValue,
                                             int modulus);

ClientMessage parseClientMessage(const std::string& payload);
std::string serializeClientMessage(const ClientMessage& message);
ServerMessage parseServerMessage(const std::string& payload);
std::string serializeServerMessage(const ServerMessage& message);
std::string clientMessageTypeToString(ClientMessage::Type type);
std::string serverMessageTypeToString(ServerMessage::Type type);

std::vector<std::string> split(const std::string& input, char delim);
std::string encodeText(const std::string& s);
std::string decodeText(const std::string& s);
int modIndex(int value, int modulus);

} // namespace kv
