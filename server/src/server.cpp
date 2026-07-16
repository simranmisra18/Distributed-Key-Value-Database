#include "common.hpp"

#include <csignal>

using namespace kv;

using Replica = std::unordered_map<int, DataValue>;

static std::atomic<bool> g_done{false};
static void onSignal(int) { g_done = true; }

class PersistentStore {
public:
    explicit PersistentStore(std::string fileName) : fileName_(std::move(fileName)) {}

    void reorganizeFile(const Replica& data, bool append) {
        std::lock_guard<std::mutex> lock(mu_);
        std::ofstream out(fileName_, append ? std::ios::app : std::ios::trunc);
        if (!out) {
            std::cerr << "ERROR: Unable to write to file " << fileName_ << std::endl;
            return;
        }
        for (const auto& entry : data) {
            out << entry.first << ' ' << entry.second.value << ' ' << entry.second.version << '\n';
        }
    }

    void appendToFile(int key, const DataValue& dataValue) {
        std::lock_guard<std::mutex> lock(mu_);
        std::ofstream out(fileName_, std::ios::app);
        if (!out) {
            std::cerr << "ERROR: Unable to write to file " << fileName_ << std::endl;
            return;
        }
        out << key << ' ' << dataValue.value << ' ' << dataValue.version << '\n';
    }

    std::vector<Replica> readFromFile(int serverIndex, int modulus) {
        std::vector<Replica> replicas(3);
        std::ifstream in(fileName_);
        if (!in) return replicas;
        int key = 0;
        int value = 0;
        int version = 0;
        while (in >> key >> value >> version) {
            int hashValue = modIndex(key, modulus);
            if (hashValue == serverIndex) replicas[0][key] = DataValue(value, version);
            else if ((hashValue + 1) % modulus == serverIndex) replicas[1][key] = DataValue(value, version);
            else replicas[2][key] = DataValue(value, version);
        }
        return replicas;
    }

private:
    std::string fileName_;
    std::mutex mu_;
};

class ServerState {
public:
    int serverIndex{-1};
    std::string hostname;
    std::vector<std::string> serverNames;
    std::vector<Replica> replicas{3};
    std::vector<std::mutex> replicaMutexes{3};
    ThreadSafeQueue<ClientRequest> writeQueue;
    ThreadSafeQueue<ClientResponse> responseQueue;
    std::atomic<int> previousServerIndex{-1};
    std::atomic<int> nextServerIndex{-1};

    std::mutex previousMutex;
    std::mutex nextMutex;
    std::shared_ptr<ServerConnection> previousConnection;
    std::shared_ptr<ServerConnection> nextConnection;

    std::mutex requestMapperMutex;
    std::unordered_map<std::string, std::shared_ptr<ClientConnection>> requestMapper;

    std::unique_ptr<PersistentStore> persistentStore;
};

static void putReplica(ServerState& state, int replicaIndex, int key, const DataValue& value) {
    std::lock_guard<std::mutex> lock(state.replicaMutexes[replicaIndex]);
    state.replicas[replicaIndex][key] = value;
}

static std::optional<DataValue> getReplicaValue(ServerState& state, int replicaIndex, int key) {
    std::lock_guard<std::mutex> lock(state.replicaMutexes[replicaIndex]);
    auto it = state.replicas[replicaIndex].find(key);
    if (it == state.replicas[replicaIndex].end()) return std::nullopt;
    return it->second;
}

static Replica snapshotReplica(ServerState& state, int replicaIndex) {
    std::lock_guard<std::mutex> lock(state.replicaMutexes[replicaIndex]);
    return state.replicas[replicaIndex];
}

static void mergeIntoReplica(ServerState& state, int replicaIndex, const Replica& incoming, int hashValue) {
    std::lock_guard<std::mutex> lock(state.replicaMutexes[replicaIndex]);
    state.replicas[replicaIndex] = mergeData(state.replicas[replicaIndex], incoming, hashValue, static_cast<int>(state.serverNames.size()));
}

static bool orderedBefore(const std::vector<int>& connectionList, int first, int second) {
    for (auto it = connectionList.rbegin(); it != connectionList.rend(); ++it) {
        if (*it == first) return true;
        if (*it == second) break;
    }
    return false;
}

static void performSync(ServerState& state, int replicateForIndex, const Replica& data) {
    try {
        auto next = [&] {
            std::lock_guard<std::mutex> lock(state.nextMutex);
            return state.nextConnection;
        }();
        if (!next) return;
        ServerMessage sync;
        sync.type = ServerMessage::Type::SYNC;
        sync.serverIndex = state.serverIndex;
        sync.replicateForIndex = replicateForIndex;
        sync.data = data;
        next->send(sync);
    } catch (const std::exception& e) {
        std::cerr << "Sync failed: " << e.what() << std::endl;
    }
}

static void handleIncomingMessages(ServerState& state, std::shared_ptr<ServerConnection> connection, const std::vector<int>& connectionList);

static void serviceWrites(ServerState& state) {
    while (!g_done) {
        ClientRequest req;
        if (!state.writeQueue.waitPopFor(req, std::chrono::milliseconds(50))) continue;
        try {
            std::cout << "Received writeRequest: " << req.toString() << std::endl;
            int size = static_cast<int>(state.serverNames.size());
            int hashValue = modIndex(req.key, size);
            auto next = [&] {
                std::lock_guard<std::mutex> lock(state.nextMutex);
                return state.nextConnection;
            }();
            auto prev = [&] {
                std::lock_guard<std::mutex> lock(state.previousMutex);
                return state.previousConnection;
            }();

            if (hashValue == state.serverIndex) {
                if (!next || !inReplicaSet(next->getServerIndex(), state.serverIndex, size)) {
                    state.responseQueue.push({req.id, state.serverIndex, false, "Less than 2 replicas online", req.key, req.dataValue.value});
                } else {
                    DataValue dataValue = updateToGreaterVersion(snapshotReplica(state, 0), req.key, req.dataValue);
                    std::cout << "Writing to primary replica: " << dataValue.toString() << std::endl;
                    putReplica(state, 0, req.key, dataValue);
                    state.persistentStore->appendToFile(req.key, dataValue);
                    ServerMessage msg;
                    msg.type = ServerMessage::Type::REPLICATE;
                    msg.serverIndex = state.serverIndex;
                    msg.replicateForIndex = req.requestorIndex;
                    msg.requestUuid = req.id;
                    msg.key = req.key;
                    msg.dataValue = dataValue;
                    next->send(msg);
                }
            } else if ((hashValue + 1) % size == state.serverIndex) {
                if (req.requestorIndex == state.serverIndex) {
                    std::cerr << "Primary offline; determining if 2 replicas are online for secondary write" << std::endl;
                    if (!next || next->getServerIndex() != (hashValue + 2) % size) {
                        state.responseQueue.push({req.id, state.serverIndex, false, "Less than 2 replicas online", req.key, req.dataValue.value});
                    } else {
                        DataValue dataValue = updateToGreaterVersion(snapshotReplica(state, 1), req.key, req.dataValue);
                        putReplica(state, 1, req.key, dataValue);
                        state.persistentStore->appendToFile(req.key, dataValue);
                        ServerMessage msg;
                        msg.type = ServerMessage::Type::REPLICATE;
                        msg.serverIndex = state.serverIndex;
                        msg.replicateForIndex = req.requestorIndex;
                        msg.requestUuid = req.id;
                        msg.key = req.key;
                        msg.dataValue = dataValue;
                        next->send(msg);
                    }
                } else {
                    DataValue dataValue = updateToGreaterVersion(snapshotReplica(state, 1), req.key, req.dataValue);
                    putReplica(state, 1, req.key, dataValue);
                    state.persistentStore->appendToFile(req.key, dataValue);
                    ServerMessage msg;
                    msg.serverIndex = state.serverIndex;
                    msg.replicateForIndex = req.requestorIndex;
                    msg.requestUuid = req.id;
                    msg.key = req.key;
                    msg.dataValue = dataValue;
                    if (!next || next->getServerIndex() != (hashValue + 2) % size) {
                        if (prev) {
                            msg.type = ServerMessage::Type::ACK_REPLICATE;
                            prev->send(msg);
                        }
                    } else {
                        msg.type = ServerMessage::Type::REPLICATE;
                        next->send(msg);
                    }
                }
            } else if ((hashValue + 2) % size == state.serverIndex) {
                DataValue dataValue = updateToGreaterVersion(snapshotReplica(state, 2), req.key, req.dataValue);
                putReplica(state, 2, req.key, dataValue);
                state.persistentStore->appendToFile(req.key, dataValue);
                if (prev) {
                    ServerMessage msg;
                    msg.type = ServerMessage::Type::ACK_REPLICATE;
                    msg.serverIndex = state.serverIndex;
                    msg.replicateForIndex = req.requestorIndex;
                    msg.requestUuid = req.id;
                    msg.key = req.key;
                    msg.dataValue = dataValue;
                    prev->send(msg);
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "Error servicing request: " << e.what() << std::endl;
        }
    }
}

static void respondToClients(ServerState& state) {
    while (!g_done) {
        ClientResponse resp;
        if (!state.responseQueue.waitPopFor(resp, std::chrono::milliseconds(50))) continue;
        ClientMessage msg;
        msg.type = resp.succeeded ? ClientMessage::Type::ACK : ClientMessage::Type::NACK;
        msg.key = resp.key;
        msg.value = resp.value;
        msg.serverIndex = state.serverIndex;
        msg.content = resp.content;
        std::shared_ptr<ClientConnection> client;
        {
            std::lock_guard<std::mutex> lock(state.requestMapperMutex);
            auto it = state.requestMapper.find(resp.id);
            if (it != state.requestMapper.end()) {
                client = it->second;
                state.requestMapper.erase(it);
            }
        }
        if (!client) continue;
        try {
            client->send(msg);
            client->close();
        } catch (const std::exception& e) {
            std::cerr << "Error sending message(" << msg.toString() << ") to client: " << e.what() << std::endl;
        }
    }
}

static void handleClient(ServerState& state, int clientFd) {
    auto client = std::make_shared<ClientConnection>(clientFd);
    try {
        ClientMessage message = client->recv();
        switch (message.type) {
            case ClientMessage::Type::GET: {
                std::cout << "Received GET: " << message.toString() << std::endl;
                std::optional<DataValue> dataValue;
                for (int i = 0; i < 3 && !dataValue; ++i) dataValue = getReplicaValue(state, i, message.key);
                ClientMessage response;
                response.key = message.key;
                response.serverIndex = state.serverIndex;
                if (dataValue) {
                    response.type = ClientMessage::Type::ACK;
                    response.value = dataValue->value;
                    response.content = "Success";
                } else {
                    response.type = ClientMessage::Type::NACK;
                    response.value = -1;
                    response.content = "Key '" + std::to_string(message.key) + "' not present in store";
                }
                std::cout << "Responding with: " << response.toString() << std::endl;
                client->send(response);
                client->close();
                break;
            }
            case ClientMessage::Type::PUT: {
                int key = message.key;
                int size = static_cast<int>(state.serverNames.size());
                int hashValue = modIndex(key, size);
                DataValue putValue(message.value);
                if (hashValue == state.serverIndex) {
                    auto existing = getReplicaValue(state, 0, key);
                    if (existing) putValue = DataValue(message.value, existing->version + 1);
                } else if ((hashValue + 1) % size == state.serverIndex) {
                    auto existing = getReplicaValue(state, 1, key);
                    if (existing) putValue = DataValue(message.value, existing->version + 1);
                }
                ClientRequest req{generateUuid(), state.serverIndex, key, putValue};
                std::cout << "Received PUT; submitted to writeQueue: " << req.toString() << std::endl;
                {
                    std::lock_guard<std::mutex> lock(state.requestMapperMutex);
                    state.requestMapper[req.id] = client;
                }
                state.writeQueue.push(req);
                break;
            }
            default:
                break;
        }
    } catch (const std::exception& e) {
        std::cerr << "Error handling client request: " << e.what() << std::endl;
        client->close();
    }
}

static void listenForClients(ServerState& state) {
    int serverFd = bindServerSocket(state.hostname, CLIENT_CONNECTION_PORT);
    std::cout << "Client-facing server started successfully at " << state.hostname << ':' << CLIENT_CONNECTION_PORT << std::endl;
    while (!g_done) {
        sockaddr_storage addr{};
        socklen_t len = sizeof(addr);
        int fd = accept(serverFd, reinterpret_cast<sockaddr*>(&addr), &len);
        if (fd < 0) continue;
        std::thread(handleClient, std::ref(state), fd).detach();
    }
    ::close(serverFd);
}

static void connectToServer(ServerState& state, int indexOfServer, const std::vector<int>& connectionList) {
    std::string connectHostname = state.serverNames.at(indexOfServer);
    try {
        int fd = connectToHost(connectHostname, SERVER_CONNECTION_PORT);
        auto connection = std::make_shared<ServerConnection>(fd);
        connection->setReadTimeoutMillis(250);
        std::cerr << "Sending REQUEST_CONNECTION to " << connectHostname << std::endl;
        ServerMessage request;
        request.type = ServerMessage::Type::REQUEST_CONNECTION;
        request.serverIndex = state.serverIndex;
        connection->send(request);
        handleIncomingMessages(state, connection, connectionList);
    } catch (...) {
        return;
    }
}

static void checkAndConnect(ServerState& state, std::vector<int> connectionList) {
    size_t connectionIndex = 0;
    while (!g_done) {
        {
            std::lock_guard<std::mutex> lock(state.nextMutex);
            if (state.nextConnection) {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
                continue;
            }
        }
        int indexOfServer = connectionList.at(connectionIndex);
        if (indexOfServer != state.serverIndex) connectToServer(state, indexOfServer, connectionList);
        connectionIndex = (connectionIndex + 1) % connectionList.size();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

static void handleServer(ServerState& state, int fd, const std::vector<int>& connectionList) {
    auto connection = std::make_shared<ServerConnection>(fd);
    try {
        handleIncomingMessages(state, connection, connectionList);
    } catch (...) {
        std::lock_guard<std::mutex> lock(state.previousMutex);
        if (state.previousConnection == connection) {
            state.previousConnection.reset();
            state.previousServerIndex = -1;
        }
    }
}

static void listenForServers(ServerState& state, const std::vector<int>& connectionList) {
    int serverFd = bindServerSocket(state.hostname, SERVER_CONNECTION_PORT);
    std::cout << "Server-facing server started successfully at " << state.hostname << ':' << SERVER_CONNECTION_PORT << std::endl;
    while (!g_done) {
        sockaddr_storage addr{};
        socklen_t len = sizeof(addr);
        int fd = accept(serverFd, reinterpret_cast<sockaddr*>(&addr), &len);
        if (fd < 0) continue;
        std::thread(handleServer, std::ref(state), fd, std::cref(connectionList)).detach();
    }
    ::close(serverFd);
}

static void handleIncomingMessages(ServerState& state, std::shared_ptr<ServerConnection> connection, const std::vector<int>& connectionList) {
    while (!g_done) {
        ServerMessage msg = connection->recv();
        connection->setServerIndex(msg.serverIndex);
        std::string connectionHostname = state.serverNames.at(msg.serverIndex);
        ServerMessage response;

        switch (msg.type) {
            case ServerMessage::Type::REQUEST_CONNECTION: {
                std::cerr << "Received REQUEST_CONNECTION from " << connectionHostname << std::endl;
                bool acceptConnection = false;
                {
                    std::lock_guard<std::mutex> lock(state.previousMutex);
                    if (!state.previousConnection || orderedBefore(connectionList, connection->getServerIndex(), state.previousConnection->getServerIndex())) {
                        if (state.previousConnection) {
                            ServerMessage exists;
                            exists.type = ServerMessage::Type::CONNECTION_EXISTS;
                            exists.serverIndex = state.serverIndex;
                            try { state.previousConnection->send(exists); state.previousConnection->close(); } catch (...) {}
                        }
                        state.previousConnection = connection;
                        state.previousServerIndex = connection->getServerIndex();
                        acceptConnection = true;
                    }
                }
                if (acceptConnection) {
                    response.type = ServerMessage::Type::ACK_CONNECTION;
                    response.serverIndex = state.serverIndex;
                    Replica syncData;
                    int size = static_cast<int>(state.serverNames.size());
                    if ((connection->getServerIndex() + 1) % size == state.serverIndex) {
                        Replica r1 = snapshotReplica(state, 1);
                        Replica r2 = snapshotReplica(state, 2);
                        syncData.insert(r1.begin(), r1.end());
                        syncData.insert(r2.begin(), r2.end());
                    } else if ((connection->getServerIndex() + 2) % size == state.serverIndex) {
                        syncData = snapshotReplica(state, 2);
                    }
                    response.data = syncData;
                    connection->send(response);
                } else {
                    response.type = ServerMessage::Type::CONNECTION_EXISTS;
                    response.serverIndex = state.serverIndex;
                    connection->send(response);
                    connection->close();
                    return;
                }
                break;
            }
            case ServerMessage::Type::ACK_CONNECTION: {
                std::cerr << "Received ACK_CONNECTION from " << connectionHostname << std::endl;
                {
                    std::lock_guard<std::mutex> lock(state.nextMutex);
                    state.nextConnection = connection;
                    state.nextServerIndex = connection->getServerIndex();
                }
                int size = static_cast<int>(state.serverNames.size());
                mergeIntoReplica(state, 0, msg.data, state.serverIndex);
                if (!snapshotReplica(state, 0).empty()) state.persistentStore->reorganizeFile(snapshotReplica(state, 0), true);
                mergeIntoReplica(state, 1, msg.data, state.serverIndex - 1);
                if (!snapshotReplica(state, 1).empty()) state.persistentStore->reorganizeFile(snapshotReplica(state, 1), true);

                if (inReplicaSet(connection->getServerIndex(), state.serverIndex, size)) {
                    Replica r0 = snapshotReplica(state, 0);
                    if (!r0.empty()) performSync(state, state.serverIndex, r0);
                    Replica r1 = snapshotReplica(state, 1);
                    if ((state.serverIndex + 1) % size == connection->getServerIndex() && !r1.empty()) performSync(state, state.serverIndex - 1, r1);
                }
                break;
            }
            case ServerMessage::Type::CONNECTION_EXISTS: {
                std::cerr << "Received CONNECTION_EXISTS from " << connectionHostname << std::endl;
                {
                    std::lock_guard<std::mutex> lock(state.nextMutex);
                    if (state.nextConnection == connection) state.nextConnection.reset();
                    state.nextServerIndex = -1;
                }
                connection->close();
                return;
            }
            case ServerMessage::Type::REPLICATE: {
                std::cout << "Received replicate request: " << msg.toString() << std::endl;
                state.writeQueue.push({msg.requestUuid, msg.replicateForIndex, msg.key, msg.dataValue});
                break;
            }
            case ServerMessage::Type::ACK_REPLICATE: {
                std::cerr << "Received ACK_REPLICATE: " << msg.toString() << std::endl;
                int size = static_cast<int>(state.serverNames.size());
                if (msg.replicateForIndex == state.serverIndex) {
                    state.responseQueue.push({msg.requestUuid, msg.replicateForIndex, true, "", msg.key, msg.dataValue.value});
                } else if ((msg.replicateForIndex + 1) % size == state.serverIndex) {
                    auto prev = [&] { std::lock_guard<std::mutex> lock(state.previousMutex); return state.previousConnection; }();
                    if (prev) {
                        ServerMessage ack = msg;
                        ack.serverIndex = state.serverIndex;
                        prev->send(ack);
                    }
                }
                break;
            }
            case ServerMessage::Type::SYNC: {
                std::cout << "Received SYNC from " << connectionHostname << std::endl;
                int size = static_cast<int>(state.serverNames.size());
                if ((msg.replicateForIndex + 1) % size == state.serverIndex) {
                    mergeIntoReplica(state, 1, msg.data, state.serverIndex - 1);
                    auto next = [&] { std::lock_guard<std::mutex> lock(state.nextMutex); return state.nextConnection; }();
                    if (next && (state.serverIndex + 1) % size == next->getServerIndex()) {
                        ServerMessage sync = msg;
                        sync.serverIndex = state.serverIndex;
                        next->send(sync);
                    }
                }
                if ((msg.replicateForIndex + 2) % size == state.serverIndex) {
                    mergeIntoReplica(state, 2, msg.data, state.serverIndex - 2);
                }
                response.type = ServerMessage::Type::ACK_SYNC;
                response.serverIndex = state.serverIndex;
                try { connection->send(response); } catch (...) {}
                break;
            }
            case ServerMessage::Type::ACK_SYNC:
                std::cerr << "Received ACK_SYNC from " << connectionHostname << std::endl;
                break;
        }
    }
}

int main(int argc, char** argv) {
    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    if (argc < 3) {
        std::cout << "Usage: ./server <server-hostnames-file-path> <hostname>" << std::endl;
        return -1;
    }

    ServerState state;
    std::string hostnamesFile = argv[1];
    state.hostname = argv[2];

    try {
        state.serverNames = readLines(hostnamesFile);
        auto it = std::find(state.serverNames.begin(), state.serverNames.end(), state.hostname);
        if (it == state.serverNames.end()) throw std::runtime_error("Hostname not mentioned in server-hostnames file");
        state.serverIndex = static_cast<int>(std::distance(state.serverNames.begin(), it));
    } catch (const std::exception& e) {
        std::cout << "Error reading server-hostnames file '" << hostnamesFile << "' due to reason: " << e.what() << std::endl;
        return -1;
    }

    std::vector<int> connectionList = generateForwardConnectionList(state.serverIndex, static_cast<int>(state.serverNames.size()));
    std::cerr << "Server " << state.serverIndex << " instantiated with connectionList: ";
    for (int idx : connectionList) std::cerr << idx << ' ';
    std::cerr << std::endl;

    state.persistentStore = std::make_unique<PersistentStore>(state.hostname + ".dat");
    auto persistentReplicas = state.persistentStore->readFromFile(state.serverIndex, static_cast<int>(state.serverNames.size()));
    for (int i = 0; i < 3; ++i) state.replicas[i] = persistentReplicas[i];
    state.persistentStore->reorganizeFile(state.replicas[0], false);
    state.persistentStore->reorganizeFile(state.replicas[1], true);
    state.persistentStore->reorganizeFile(state.replicas[2], true);

    try {
        std::thread clientListener(listenForClients, std::ref(state));
        std::thread serverListener(listenForServers, std::ref(state), std::cref(connectionList));
        std::thread connector(checkAndConnect, std::ref(state), connectionList);
        std::thread writer(serviceWrites, std::ref(state));
        std::thread responder(respondToClients, std::ref(state));

        while (!g_done) {
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
            int prev = state.previousServerIndex.load();
            int next = state.nextServerIndex.load();
            std::cout << "\rLocal Ring Status: ";
            if (prev == -1) std::cout << "(no connection) -> ";
            else std::cout << state.serverNames[prev] << " -> ";
            std::cout << state.hostname << " -> ";
            if (next == -1) std::cout << "(no connection)                   \r";
            else std::cout << state.serverNames[next] << "                  \r";
            std::cout.flush();
        }

        clientListener.detach();
        serverListener.detach();
        connector.detach();
        writer.detach();
        responder.detach();
    } catch (const std::exception& e) {
        std::cout << "Error starting server threads: " << e.what() << std::endl;
        return -1;
    }

    std::cerr << "Server concluded successfully" << std::endl;
    return 0;
}
