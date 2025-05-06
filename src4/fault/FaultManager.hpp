#ifndef IDIOMS_FAULT_MANAGER_HPP
#define IDIOMS_FAULT_MANAGER_HPP

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <mutex>
#include <thread>
#include <atomic>
#include <chrono>
#include <mpi.h>
#include "../dart/DART.hpp"

namespace idioms
{
    namespace fault
    {

        // Status of a server
        enum ServerStatus
        {
            ACTIVE,         // Server is active and responsive
            SUSPECT,        // Server is suspected to be down
            CONFIRMED_DOWN, // Server is confirmed to be down
            RECOVERING      // Server is in recovery process
        };

        /**
         * Manages fault detection and recovery for the IDIOMS distributed system.
         * Uses a heartbeat mechanism to detect server failures and coordinates
         * recovery operations.
         */
        class FaultManager
        {
        private:
            int rank;                  // MPI rank of this process
            int worldSize;             // Total number of MPI processes
            bool isClient;             // Whether this is the client process
            std::atomic<bool> running; // Whether the fault manager is running

            std::mutex statusMutex;                                                                    // Mutex for server status map
            std::unordered_map<int, ServerStatus> serverStatus;                                        // Status of each server
            std::unordered_map<int, std::chrono::time_point<std::chrono::steady_clock>> lastHeartbeat; // Last heartbeat time

            std::chrono::milliseconds heartbeatInterval;     // Interval between heartbeats
            std::chrono::milliseconds timeoutThreshold;      // Timeout threshold for suspecting a server
            std::chrono::milliseconds confirmationThreshold; // Threshold for confirming a server is down

            std::shared_ptr<DARTRouter> router; // DART router for coordinating recovery
            std::thread heartbeatThread;        // Thread for sending/checking heartbeats

            // Send a heartbeat to all servers
            void sendHeartbeats();

            // Check heartbeats from all servers
            void checkHeartbeats();

            // Heartbeat monitoring function (runs in a separate thread)
            void heartbeatMonitor();

            // Handle a heartbeat message
            void handleHeartbeat(int sourceRank);

            // Initiate recovery for a failed server
            void initiateRecovery(int failedRank);

            // Coordinate recovery among surviving servers
            void coordinateRecovery(int failedRank);

            // Participate in recovery as a server
            void participateInRecovery(int failedRank, int coordinatorRank);

            // Update DART router to exclude failed servers
            void updateRouter();

            // Redistribute data from failed server
            void redistributeData(int failedRank);

        public:
            /**
             * Constructor
             *
             * @param mpiRank MPI rank of this process
             * @param mpiWorldSize Total number of MPI processes
             * @param dartRouter Shared DART router instance
             * @param isClientProcess Whether this is the client process
             */
            FaultManager(int mpiRank, int mpiWorldSize,
                         std::shared_ptr<DARTRouter> dartRouter,
                         bool isClientProcess);

            /**
             * Destructor
             */
            ~FaultManager();

            /**
             * Start fault monitoring
             */
            void start();

            /**
             * Stop fault monitoring
             */
            void stop();

            /**
             * Check if a server is active
             *
             * @param serverRank MPI rank of the server to check
             * @return True if the server is active, false otherwise
             */
            bool isServerActive(int serverRank);

            /**
             * Get the status of a server
             *
             * @param serverRank MPI rank of the server to check
             * @return Status of the server
             */
            ServerStatus getServerStatus(int serverRank);

            /**
             * Get a list of active servers
             *
             * @return Vector of active server ranks
             */
            std::vector<int> getActiveServers();

            /**
             * Process a heartbeat from a server
             * Used by the MPI message handler when it receives a heartbeat
             *
             * @param serverRank MPI rank of the server that sent the heartbeat
             */
            void processHeartbeat(int serverRank);

            /**
             * Notify of a server failure
             * Used to manually mark a server as failed for testing
             *
             * @param serverRank MPI rank of the failed server
             */
            void notifyServerFailure(int serverRank);

            /**
             * Query if fault tolerance monitoring is active
             *
             * @return True if fault monitoring is active, false otherwise
             */
            bool isRunning() const;
        };

    } // namespace fault
} // namespace idioms

#endif // IDIOMS_FAULT_MANAGER_HPP