#include "MPIClient.hpp"
#include <iostream>
#include <stdexcept>
#include <unordered_set>
#include <algorithm>

namespace idioms
{
    namespace mpi
    {

        MPIClient::MPIClient(int mpiRank, int mpiWorldSize, bool useSuffixMode)
            : rank(mpiRank), worldSize(mpiWorldSize), useSuffixTreeMode(useSuffixMode)
        {

            // First process is reserved for the client controller,
            // so there are worldSize - 1 actual server processes
            int numServers = worldSize - 1;

            // Create the DART router
            router = std::make_shared<DARTRouter>(numServers);

            std::cout << "MPI Client initialized with " << numServers << " servers" << std::endl;
        }

        MPIClient::~MPIClient()
        {
            // Clean up resources if needed
        }

        void MPIClient::create_md_index(const std::string &key, const std::string &value, int objectId)
        {
            // Determine which servers should store this index record
            std::vector<int> serverIds = router->getServersForKey(key);

            std::cout << "Distributing index for key '" << key << "' to servers: ";
            for (size_t i = 0; i < serverIds.size(); i++)
            {
                if (i > 0)
                    std::cout << ", ";
                std::cout << serverIds[i];

                // MPI ranks start at 0, but rank 0 is reserved for client
                // So server with ID 0 is at MPI rank 1, server with ID 1 is at MPI rank 2, etc.
                int serverRank = serverIds[i] + 1;

                // Create and send the message
                CreateIndexMessage msg(key, value, objectId);
                sendMessage(msg, serverRank, INDEX_TAG);

                // Wait for response
                auto response = receiveResponse(serverRank, RESULT_TAG);

                if (!response.success)
                {
                    std::cerr << "Failed to create index on server " << serverIds[i] << std::endl;
                }
            }
            std::cout << std::endl;
        }

        void MPIClient::delete_md_index(const std::string &key, const std::string &value, int objectId)
        {
            // Determine which servers should have this index record
            std::vector<int> serverIds = router->getServersForKey(key);

            std::cout << "Deleting index for key '" << key << "' from servers: ";
            for (size_t i = 0; i < serverIds.size(); i++)
            {
                if (i > 0)
                    std::cout << ", ";
                std::cout << serverIds[i];

                // MPI ranks start at 0, but rank 0 is reserved for client
                int serverRank = serverIds[i] + 1;

                // Create and send the message
                DeleteIndexMessage msg(key, value, objectId);
                sendMessage(msg, serverRank, INDEX_TAG);

                // Wait for response
                auto response = receiveResponse(serverRank, RESULT_TAG);

                if (!response.success)
                {
                    std::cerr << "Failed to delete index on server " << serverIds[i] << std::endl;
                }
            }
            std::cout << std::endl;
        }

        std::vector<int> MPIClient::findServersForQuery(const std::string &queryStr) const
        {
            // Determine which servers should receive this query
            std::vector<int> destinationServerIds = router->getDestinationServers(queryStr);

            std::cout << "Query: \"" << queryStr << "\" routed to servers: ";
            for (size_t i = 0; i < destinationServerIds.size(); i++)
            {
                if (i > 0)
                    std::cout << ", ";
                std::cout << destinationServerIds[i];
            }
            std::cout << std::endl;

            return destinationServerIds;
        }

        std::vector<int> MPIClient::md_search(const std::string &queryStr)
        {
            // Find which servers should receive this query
            std::vector<int> serverIds = findServersForQuery(queryStr);

            // Send query to selected servers and collect results
            std::unordered_set<int> resultSet;
            std::vector<int> handlingServers;

            std::cout << "Servers that can handle the query: ";
            bool first = true;

            for (int serverId : serverIds)
            {
                // MPI ranks start at 0, but rank 0 is reserved for client
                int serverRank = serverId + 1;

                // Create and send the message
                QueryMessage msg(queryStr);
                sendMessage(msg, serverRank, QUERY_TAG);

                // Wait for response
                auto response = receiveResponse(serverRank, RESULT_TAG);

                // If we got results, this server could handle the query
                if (!response.results.empty())
                {
                    if (!first)
                        std::cout << ", ";
                    std::cout << serverId;
                    first = false;
                    handlingServers.push_back(serverId);

                    // Merge results
                    resultSet.insert(response.results.begin(), response.results.end());
                }
            }

            if (first)
                std::cout << "None";
            std::cout << std::endl;

            // Convert set to vector for return
            std::vector<int> results(resultSet.begin(), resultSet.end());
            std::sort(results.begin(), results.end()); // Sort for deterministic output
            return results;
        }

        void MPIClient::checkpointAllIndices()
        {
            std::cout << "Checkpointing indices to disk..." << std::endl;

            // Send checkpoint command to all servers
            AdminMessage msg(CHECKPOINT);

            for (int i = 1; i < worldSize; i++)
            { // Skip rank 0 (client)
                sendMessage(msg, i, ADMIN_TAG);

                // Wait for response
                auto response = receiveResponse(i, ADMIN_TAG);

                if (!response.success)
                {
                    std::cerr << "Failed to checkpoint index on server " << (i - 1) << std::endl;
                }
            }

            std::cout << "Checkpoint complete." << std::endl;
        }

        void MPIClient::recoverAllIndices()
        {
            std::cout << "Recovering indices from disk..." << std::endl;

            // Send recover command to all servers
            AdminMessage msg(RECOVER);

            for (int i = 1; i < worldSize; i++)
            { // Skip rank 0 (client)
                sendMessage(msg, i, ADMIN_TAG);

                // Wait for response
                auto response = receiveResponse(i, ADMIN_TAG);

                if (!response.success)
                {
                    std::cerr << "Failed to recover index on server " << (i - 1) << std::endl;
                }
            }

            std::cout << "Recovery complete." << std::endl;
        }

        void MPIClient::shutdownAllServers()
        {
            std::cout << "Shutting down all servers..." << std::endl;

            // Send shutdown command to all servers
            AdminMessage msg(SHUTDOWN);

            for (int i = 1; i < worldSize; i++)
            { // Skip rank 0 (client)
                sendMessage(msg, i, ADMIN_TAG);

                // No need to wait for response
            }
        }

        void MPIClient::sendMessage(const Message &msg, int destRank, int tag)
        {
            // Serialize the message
            auto buffer = msg.serialize();

            // Send the message
            MPI_Send(buffer.data(), buffer.size(), MPI_CHAR, destRank, tag, MPI_COMM_WORLD);
        }

        ResponseMessage MPIClient::receiveResponse(int sourceRank, int tag)
        {
            MPI_Status status;
            int msgSize;

            // Probe for the message size
            MPI_Probe(sourceRank, tag, MPI_COMM_WORLD, &status);
            MPI_Get_count(&status, MPI_CHAR, &msgSize);

            // Receive the message
            std::vector<char> buffer(msgSize);
            MPI_Recv(buffer.data(), msgSize, MPI_CHAR, sourceRank, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            // Check if it's an error response
            MessageType type = Message::getType(buffer);
            if (type == ERROR_RESPONSE)
            {
                auto errorMsg = ErrorResponseMessage::deserialize(buffer);
                throw std::runtime_error("Server error: " + errorMsg.errorMessage);
            }

            // Deserialize and return the response
            return ResponseMessage::deserialize(buffer);
        }

    } // namespace mpi
} // namespace idioms