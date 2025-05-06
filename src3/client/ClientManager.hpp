#ifndef IDIOMS_CLIENT_MANAGER_HPP
#define IDIOMS_CLIENT_MANAGER_HPP

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <mutex>
#include <atomic>
#include <thread>
#include <mpi.h>
#include "../dart/DART.hpp"
#include "../mpi/MPIClient.hpp"

namespace idioms
{
    namespace client
    {

        /**
         * Manages multiple client instances and coordinates access to servers.
         * This class is responsible for managing client connections, load balancing,
         * and ensuring thread safety for concurrent client operations.
         */
        class ClientManager
        {
        private:
            int worldSize;                    // Total number of MPI processes
            std::string dataDir;              // Data directory
            bool useSuffixTreeMode;           // Whether to use suffix tree mode
            std::atomic<int> clientIdCounter; // Counter for client IDs

            std::shared_ptr<DARTRouter> router; // Shared DART router

            std::mutex clientsMutex;                                          // Mutex for clients map
            std::unordered_map<int, std::shared_ptr<mpi::MPIClient>> clients; // Map of client ID to client instance

            // Thread for handling client requests
            std::thread clientThread;
            std::atomic<bool> running;

            // Client request processing
            void processClientRequests();

            // Assign a client ID to a new client
            int assignClientId();

        public:
            /**
             * Constructor
             *
             * @param mpiWorldSize Total number of MPI processes
             * @param dataDirectory Directory for storing data
             * @param useSuffixMode Whether to use suffix tree mode
             */
            ClientManager(int mpiWorldSize, const std::string &dataDirectory, bool useSuffixMode = false);

            /**
             * Destructor
             */
            ~ClientManager();

            /**
             * Start the client manager
             */
            void start();

            /**
             * Stop the client manager
             */
            void stop();

            /**
             * Register a new client
             *
             * @return Client ID
             */
            int registerClient();

            /**
             * Unregister a client
             *
             * @param clientId Client ID
             */
            void unregisterClient(int clientId);

            /**
             * Get a client instance
             *
             * @param clientId Client ID
             * @return Shared pointer to client instance
             */
            std::shared_ptr<mpi::MPIClient> getClient(int clientId);

            /**
             * Get the number of active clients
             *
             * @return Number of active clients
             */
            int getClientCount();

            /**
             * Get a list of active client IDs
             *
             * @return Vector of active client IDs
             */
            std::vector<int> getActiveClientIds();

            /**
             * Check if a client ID is valid
             *
             * @param clientId Client ID
             * @return True if the client ID is valid, false otherwise
             */
            bool isValidClientId(int clientId);
        };

    } // namespace client
} // namespace idioms

#endif // IDIOMS_CLIENT_MANAGER_HPP