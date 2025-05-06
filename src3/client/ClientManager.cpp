#include "ClientManager.hpp"
#include <iostream>
#include <stdexcept>
#include <chrono>

namespace idioms
{
    namespace client
    {

        ClientManager::ClientManager(int mpiWorldSize, const std::string &dataDirectory, bool useSuffixMode)
            : worldSize(mpiWorldSize), dataDir(dataDirectory), useSuffixTreeMode(useSuffixMode),
              clientIdCounter(0), running(false)
        {

            // Create shared DART router
            router = std::make_shared<DARTRouter>(worldSize - 1); // Exclude client manager process

            std::cout << "Client Manager initialized with " << (worldSize - 1) << " server processes" << std::endl;
        }

        ClientManager::~ClientManager()
        {
            stop();
        }

        void ClientManager::start()
        {
            if (running.load())
            {
                return; // Already running
            }

            running.store(true);
            clientThread = std::thread(&ClientManager::processClientRequests, this);

            std::cout << "Client Manager started" << std::endl;
        }

        void ClientManager::stop()
        {
            if (!running.load())
            {
                return; // Already stopped
            }

            running.store(false);

            if (clientThread.joinable())
            {
                clientThread.join();
            }

            // Clean up clients
            std::lock_guard<std::mutex> lock(clientsMutex);
            clients.clear();

            std::cout << "Client Manager stopped" << std::endl;
        }

        int ClientManager::registerClient()
        {
            int clientId = assignClientId();

            // Create new client instance
            std::lock_guard<std::mutex> lock(clientsMutex);
            clients[clientId] = std::make_shared<mpi::MPIClient>(0, worldSize, useSuffixTreeMode);

            std::cout << "Registered client with ID " << clientId << std::endl;

            return clientId;
        }

        void ClientManager::unregisterClient(int clientId)
        {
            std::lock_guard<std::mutex> lock(clientsMutex);

            auto it = clients.find(clientId);
            if (it != clients.end())
            {
                clients.erase(it);
                std::cout << "Unregistered client with ID " << clientId << std::endl;
            }
        }

        std::shared_ptr<mpi::MPIClient> ClientManager::getClient(int clientId)
        {
            std::lock_guard<std::mutex> lock(clientsMutex);

            auto it = clients.find(clientId);
            if (it != clients.end())
            {
                return it->second;
            }

            throw std::runtime_error("Invalid client ID: " + std::to_string(clientId));
        }

        int ClientManager::getClientCount()
        {
            std::lock_guard<std::mutex> lock(clientsMutex);
            return clients.size();
        }

        std::vector<int> ClientManager::getActiveClientIds()
        {
            std::vector<int> clientIds;

            std::lock_guard<std::mutex> lock(clientsMutex);
            for (const auto &pair : clients)
            {
                clientIds.push_back(pair.first);
            }

            return clientIds;
        }

        bool ClientManager::isValidClientId(int clientId)
        {
            std::lock_guard<std::mutex> lock(clientsMutex);
            return clients.find(clientId) != clients.end();
        }

        int ClientManager::assignClientId()
        {
            return clientIdCounter.fetch_add(1);
        }

        void ClientManager::processClientRequests()
        {
            // In a real implementation, this would handle client requests from a queue
            // For now, we'll just periodically check for inactive clients

            while (running.load())
            {
                // Do any periodic tasks

                // Sleep to avoid busy-waiting
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }

    } // namespace client
} // namespace idioms