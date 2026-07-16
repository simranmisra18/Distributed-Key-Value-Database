#include "common.hpp"

using namespace kv;

static void exitWithError(const std::string& error) {
    std::cout << error << std::endl;
    std::exit(-1);
}

static bool equalsIgnoreCase(std::string a, std::string b) {
    std::transform(a.begin(), a.end(), a.begin(), ::toupper);
    std::transform(b.begin(), b.end(), b.begin(), ::toupper);
    return a == b;
}

int main(int argc, char** argv) {
    if (argc < 3) exitWithError("Usage: ./client <command> <arg1> <optional-arg2>");

    std::string command = argv[1];
    if (equalsIgnoreCase(command, "PUT") && argc < 4) {
        exitWithError("Usage: ./client <command> <arg1> <arg2>");
    }

    std::vector<std::string> hostnames;
    try {
        hostnames = readLines(HOSTNAMES_FILENAME);
    } catch (const std::exception& e) {
        exitWithError(std::string("Error reading hostnames from file '") + HOSTNAMES_FILENAME + "': " + e.what());
    }

    if (hostnames.empty()) exitWithError("No hostnames configured");

    ClientMessage clientMessage;
    int key = 0;
    try {
        key = std::stoi(argv[2]);
    } catch (...) {
        exitWithError(std::string("Error parsing argument as Integer: '") + argv[2] + "'");
    }

    int serverIndex = modIndex(key, static_cast<int>(hostnames.size()));
    std::vector<int> accessOrder = {serverIndex,
                                    (serverIndex + 1) % static_cast<int>(hostnames.size()),
                                    (serverIndex + 2) % static_cast<int>(hostnames.size())};
    bool isGet = false;

    if (equalsIgnoreCase(command, "GET")) {
        isGet = true;
        clientMessage.type = ClientMessage::Type::GET;
        clientMessage.key = key;
        std::random_device rd;
        std::mt19937 gen(rd());
        std::shuffle(accessOrder.begin(), accessOrder.end(), gen);
    } else if (equalsIgnoreCase(command, "PUT")) {
        int value = 0;
        try {
            value = std::stoi(argv[3]);
        } catch (...) {
            exitWithError(std::string("Error parsing argument(s) as Integer: '") + argv[2] + "', '" + argv[3] + "'");
        }
        clientMessage.type = ClientMessage::Type::PUT;
        clientMessage.key = key;
        clientMessage.value = value;
        if (accessOrder.size() > 2) accessOrder.resize(2);
    } else {
        exitWithError("Unrecognized command: " + command);
    }

    try {
        std::unique_ptr<ClientConnection> connection;
        bool successful = false;
        for (int accessIndex : accessOrder) {
            try {
                int fd = connectToHost(hostnames.at(accessIndex), CLIENT_CONNECTION_PORT);
                connection = std::make_unique<ClientConnection>(fd);
                successful = true;
                break;
            } catch (...) {
                connection.reset();
            }
        }

        if (!successful || !connection) throw std::runtime_error("No servers online");

        connection->send(clientMessage);
        ClientMessage response = connection->recv();
        switch (response.type) {
            case ClientMessage::Type::ACK:
                if (isGet) {
                    std::cout << "Success: GET " << response.key << " returned " << response.value
                              << " from server serverIndex=" << response.serverIndex << std::endl;
                } else {
                    std::cout << "Success: PUT " << response.key << " wrote " << response.value
                              << " at server serverIndex=" << response.serverIndex << std::endl;
                }
                break;
            case ClientMessage::Type::NACK:
                std::cout << "Failure; Reason: " << response.content
                          << " at serverIndex=" << response.serverIndex << std::endl;
                break;
            default:
                break;
        }
    } catch (const std::exception& e) {
        exitWithError("Error during message(" + clientMessage.toString() + ") send: " + e.what());
    }

    return 0;
}
