#include "FaultManager.hpp"
#include <iostream>
#include <stdexcept>
#include <algorithm>

namespace idioms
{
    namespace fault
    {

        // MPI tags for fault tolerance
        const int HEARTBEAT_TAG = 10;
        const int RECOVERY_TAG = 11;
        const int RECOVERY_COORD_TAG = 12;
        const int RECOVERY_COMPLETE_TAG = 13;

        // Heartbeat message structure
        struct HeartbeatMessage
        {
            int senderId;
            int64_t timestamp;
        };

        // Recovery message structure
        struct RecoveryMessage
        {
            int failedServerId;
            int coordinatorId;
        };

        FaultManager::FaultManager(int mpiRank, int mpiWorldSize,
                                   std::shared_ptr<DARTRouter> dartRouter,
                                   bool isClientProcess)
            : rank(mpiRank), worldSize(mpiWorldSize), isClient(isClientProcess),
              running(false), router(dartRouter),
              heartbeatInterval(std::chrono::milliseconds(500)), // 500ms between heartbeats
              timeoutThreshold(std::chrono::milliseconds(2000)), // 2s to suspect failure
              confirmationThreshold(std::chrono::milliseconds(5000))
        { // 5s to confirm failure

            // Initialize server status map
            for (int i = 1; i < worldSize; i++)
            { // Skip rank 0 (client)
                serverStatus[i] = ServerStatus::ACTIVE;
                // Initialize with current time
                lastHeartbeat[i] = std::chrono::steady_clock::now();
            }

            std::cout << "Fault Manager initialized on rank " << rank << std::endl;
        }

        FaultManager::~FaultManager()
        {
            stop();
        }

        void FaultManager::start()
        {
            if (running.load())
            {
                return; // Already running
            }

            running.store(true);

            // Start heartbeat thread
            heartbeatThread = std::thread(&FaultManager::heartbeatMonitor, this);

            std::cout << "Fault Manager started on rank " << rank << std::endl;
        }

        void FaultManager::stop()
        {
            if (!running.load())
            {
                return; // Already stopped
            }

            running.store(false);

            // Join heartbeat thread if it exists
            if (heartbeatThread.joinable())
            {
                heartbeatThread.join();
            }

            std::cout << "Fault Manager stopped on rank " << rank << std::endl;
        }

        bool FaultManager::isServerActive(int serverRank)
        {
            std::lock_guard<std::mutex> lock(statusMutex);
            auto it = serverStatus.find(serverRank);
            if (it != serverStatus.end())
            {
                return it->second == ServerStatus::ACTIVE;
            }
            return false;
        }

        ServerStatus FaultManager::getServerStatus(int serverRank)
        {
            std::lock_guard<std::mutex> lock(statusMutex);
            auto it = serverStatus.find(serverRank);
            if (it != serverStatus.end())
            {
                return it->second;
            }
            throw std::runtime_error("Server rank not found in status map");
        }

        std::vector<int> FaultManager::getActiveServers()
        {
            std::vector<int> activeServers;
            std::lock_guard<std::mutex> lock(statusMutex);

            for (const auto &entry : serverStatus)
            {
                if (entry.second == ServerStatus::ACTIVE)
                {
                    activeServers.push_back(entry.first);
                }
            }

            return activeServers;
        }

        void FaultManager::processHeartbeat(int serverRank)
        {
            std::lock_guard<std::mutex> lock(statusMutex);

            // Update last heartbeat time
            lastHeartbeat[serverRank] = std::chrono::steady_clock::now();

            // If server was suspect or recovering, mark it as active
            if (serverStatus[serverRank] == ServerStatus::SUSPECT ||
                serverStatus[serverRank] == ServerStatus::RECOVERING)
            {
                std::cout << "Server " << serverRank << " is back online" << std::endl;
                serverStatus[serverRank] = ServerStatus::ACTIVE;
            }
        }

        void FaultManager::notifyServerFailure(int serverRank)
        {
            std::lock_guard<std::mutex> lock(statusMutex);

            // Mark the server as confirmed down
            serverStatus[serverRank] = ServerStatus::CONFIRMED_DOWN;

            std::cout << "Server " << serverRank << " manually marked as failed" << std::endl;

            // Initiate recovery if this is the client
            if (isClient)
            {
                initiateRecovery(serverRank);
            }
        }

        bool FaultManager::isRunning() const
        {
            return running.load();
        }

        void FaultManager::heartbeatMonitor()
        {
            while (running.load())
            {
                if (isClient)
                {
                    // Client sends heartbeats to all servers
                    sendHeartbeats();

                    // Client checks heartbeats from all servers
                    checkHeartbeats();
                }
                else
                {
                    // Servers just send heartbeats to the client
                    // Send a heartbeat to the client (rank 0)
                    HeartbeatMessage msg;
                    msg.senderId = rank;
                    msg.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::system_clock::now().time_since_epoch())
                                        .count();

                    MPI_Send(&msg, sizeof(msg), MPI_BYTE, 0, HEARTBEAT_TAG, MPI_COMM_WORLD);
                }

                // Sleep for heartbeat interval
                std::this_thread::sleep_for(heartbeatInterval);
            }
        }

        void FaultManager::sendHeartbeats()
        {
            // Client sends heartbeats to all servers
            HeartbeatMessage msg;
            msg.senderId = rank;
            msg.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::system_clock::now().time_since_epoch())
                                .count();

            for (int i = 1; i < worldSize; i++)
            {
                // Only send to active or suspect servers
                std::lock_guard<std::mutex> lock(statusMutex);
                if (serverStatus[i] == ServerStatus::ACTIVE ||
                    serverStatus[i] == ServerStatus::SUSPECT)
                {
                    MPI_Send(&msg, sizeof(msg), MPI_BYTE, i, HEARTBEAT_TAG, MPI_COMM_WORLD);
                }
            }
        }

        void FaultManager::checkHeartbeats()
        {
            auto now = std::chrono::steady_clock::now();

            std::lock_guard<std::mutex> lock(statusMutex);

            for (auto &entry : serverStatus)
            {
                int serverRank = entry.first;
                ServerStatus &status = entry.second;

                // Skip confirmed down servers
                if (status == ServerStatus::CONFIRMED_DOWN)
                {
                    continue;
                }

                // Calculate time since last heartbeat
                auto timeSinceLastHeartbeat = now - lastHeartbeat[serverRank];

                if (status == ServerStatus::ACTIVE)
                {
                    // Check if server has timed out
                    if (timeSinceLastHeartbeat > timeoutThreshold)
                    {
                        std::cout << "Server " << serverRank << " is suspected to be down" << std::endl;
                        status = ServerStatus::SUSPECT;
                    }
                }
                else if (status == ServerStatus::SUSPECT)
                {
                    // Check if server has been suspect for too long
                    if (timeSinceLastHeartbeat > confirmationThreshold)
                    {
                        std::cout << "Server " << serverRank << " is confirmed to be down" << std::endl;
                        status = ServerStatus::CONFIRMED_DOWN;

                        // Initiate recovery
                        initiateRecovery(serverRank);
                    }
                }
            }
        }

        void FaultManager::initiateRecovery(int failedRank)
        {
            std::cout << "Initiating recovery for server " << failedRank << std::endl;

            // Get list of active servers for recovery
            std::vector<int> activeServers;
            for (const auto &entry : serverStatus)
            {
                if (entry.second == ServerStatus::ACTIVE)
                {
                    activeServers.push_back(entry.first);
                }
            }

            if (activeServers.empty())
            {
                std::cerr << "No active servers available for recovery" << std::endl;
                return;
            }

            // Select a coordinator for recovery (first active server)
            int coordinatorRank = activeServers[0];

            // Send recovery message to coordinator
            RecoveryMessage msg;
            msg.failedServerId = failedRank;
            msg.coordinatorId = coordinatorRank;

            std::cout << "Selecting server " << coordinatorRank << " as recovery coordinator" << std::endl;

            MPI_Send(&msg, sizeof(msg), MPI_BYTE, coordinatorRank, RECOVERY_COORD_TAG, MPI_COMM_WORLD);

            // Notify other active servers to participate in recovery
            for (size_t i = 1; i < activeServers.size(); i++)
            {
                int serverRank = activeServers[i];
                MPI_Send(&msg, sizeof(msg), MPI_BYTE, serverRank, RECOVERY_TAG, MPI_COMM_WORLD);
            }

            // Update DART router to exclude failed server
            updateRouter();
        }

        void FaultManager::coordinateRecovery(int failedRank)
        {
            std::cout << "Coordinating recovery for server " << failedRank << std::endl;

            // In a real implementation, this would:
            // 1. Determine which data needs to be redistributed
            // 2. Assign responsibility for different data partitions to different servers
            // 3. Monitor the recovery process and report back to client

            // For now, we'll just perform a basic redistribution
            redistributeData(failedRank);

            // Notify client that recovery is complete
            int msg = 1; // Simple success message
            MPI_Send(&msg, 1, MPI_INT, 0, RECOVERY_COMPLETE_TAG, MPI_COMM_WORLD);

            std::cout << "Recovery for server " << failedRank << " completed" << std::endl;
        }

        void FaultManager::participateInRecovery(int failedRank, int coordinatorRank)
        {
            std::cout << "Participating in recovery for server " << failedRank
                      << " coordinated by " << coordinatorRank << std::endl;

            // In a real implementation, this would:
            // 1. Receive instructions from coordinator
            // 2. Fetch assigned data partitions
            // 3. Update local index
            // 4. Report back to coordinator when done

            // For now, just update the router
            updateRouter();
        }

        void FaultManager::updateRouter()
        {
            // Get list of active server IDs (not MPI ranks)
            std::vector<int> activeServerIds;
            for (const auto &entry : serverStatus)
            {
                if (entry.second == ServerStatus::ACTIVE)
                {
                    // Convert MPI rank to server ID (subtract 1)
                    activeServerIds.push_back(entry.first - 1);
                }
            }

            // Update the DART router with the new server count
            int newServerCount = activeServerIds.size();

            std::cout << "Updating DART router with " << newServerCount << " active servers" << std::endl;

            // In a real implementation, we would update the router
            // router->remapServers(newServerCount);

            // For now, just simulate the update
            std::cout << "DART router updated" << std::endl;
        }

        void FaultManager::redistributeData(int failedRank)
        {
            // In a real implementation, this would:
            // 1. Determine which virtual nodes were assigned to the failed server
            // 2. Redistribute those virtual nodes to active servers
            // 3. Transfer the data for those virtual nodes

            std::cout << "Redistributing data from failed server " << failedRank << std::endl;

            // For now, just simulate the redistribution
            std::this_thread::sleep_for(std::chrono::milliseconds(500));

            std::cout << "Data redistribution completed" << std::endl;
        }

        void FaultManager::handleHeartbeat(int sourceRank)
        {
            // Update last heartbeat time
            processHeartbeat(sourceRank);
        }

    } // namespace fault
} // namespace idioms