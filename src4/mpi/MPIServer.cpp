#include "MPIServer.hpp"
#include <iostream>
#include <stdexcept>

namespace idioms
{
    namespace mpi
    {

        MPIServer::MPIServer(int mpiRank, int mpiWorldSize, const std::string &dataDirectory, bool useSuffixMode)
            : rank(mpiRank), worldSize(mpiWorldSize), dataDir(dataDirectory),
              useSuffixTreeMode(useSuffixMode), running(true)
        {

            // First process is reserved for the client controller,
            // so there are worldSize - 1 actual server processes
            int numServers = worldSize - 1;

            // Create the DART router
            router = std::make_shared<DARTRouter>(numServers);

            // Create the server implementation
            // Server rank in DART is 0-based, so we subtract 1 from MPI rank (which starts at 1 for servers)
            server = std::make_unique<DistributedIdiomsServer>(
                rank - 1, dataDir, router, useSuffixTreeMode);

            std::cout << "MPI Server " << rank << " initialized (DART server ID: " << (rank - 1) << ")" << std::endl;
        }

        MPIServer::~MPIServer()
        {
            shutdown();
        }

        void MPIServer::run()
        {
            std::cout << "MPI Server " << rank << " running..." << std::endl;

            MPI_Status status;
            int msgSize;

            while (running.load())
            {
                // Probe for any incoming message
                MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

                // Get the message size
                MPI_Get_count(&status, MPI_CHAR, &msgSize);

                // Read the message
                std::vector<char> buffer(msgSize);
                MPI_Recv(buffer.data(), msgSize, MPI_CHAR, status.MPI_SOURCE,
                         status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                // Handle the message
                try
                {
                    handleMessage(buffer, status.MPI_SOURCE);
                }
                catch (const std::exception &e)
                {
                    std::cerr << "Error handling message on server " << rank << ": "
                              << e.what() << std::endl;

                    // Send error response
                    sendErrorResponse(e.what(), status.MPI_SOURCE, RESULT_TAG);
                }
            }

            std::cout << "MPI Server " << rank << " shutting down..." << std::endl;
        }

        void MPIServer::shutdown()
        {
            running.store(false);
        }

        int MPIServer::getRank() const
        {
            return rank;
        }

        void MPIServer::handleMessage(const std::vector<char> &message, int sourceRank)
        {
            // Get the message type
            MessageType type = Message::getType(message);

            // Handle based on type
            switch (type)
            {
            case CREATE_INDEX:
            {
                auto msg = CreateIndexMessage::deserialize(message);
                handleCreateIndexMessage(msg, sourceRank);
                break;
            }
            case DELETE_INDEX:
            {
                auto msg = DeleteIndexMessage::deserialize(message);
                handleDeleteIndexMessage(msg, sourceRank);
                break;
            }
            case QUERY:
            {
                auto msg = QueryMessage::deserialize(message);
                handleQueryMessage(msg, sourceRank);
                break;
            }
            case CHECKPOINT:
            case RECOVER:
            case SHUTDOWN:
            {
                auto msg = AdminMessage::deserialize(message);
                handleAdminMessage(msg, sourceRank);
                break;
            }
            default:
                throw std::runtime_error("Unknown message type");
            }
        }

        void MPIServer::handleCreateIndexMessage(const CreateIndexMessage &msg, int sourceRank)
        {
            std::cout << "Server " << rank << " handling CREATE_INDEX for key '"
                      << msg.key << "'" << std::endl;

            // Add the indexed key to the server
            server->addIndexedKey(msg.key, msg.value, msg.objectId);

            // Send success response
            ResponseMessage response;
            response.success = true;
            sendResponse(response, sourceRank, RESULT_TAG);
        }

        void MPIServer::handleDeleteIndexMessage(const DeleteIndexMessage &msg, int sourceRank)
        {
            std::cout << "Server " << rank << " handling DELETE_INDEX for key '"
                      << msg.key << "'" << std::endl;

            // Remove the indexed key from the server
            server->removeIndexedKey(msg.key, msg.value, msg.objectId);

            // Send success response
            ResponseMessage response;
            response.success = true;
            sendResponse(response, sourceRank, RESULT_TAG);
        }

        void MPIServer::handleQueryMessage(const QueryMessage &msg, int sourceRank)
        {
            std::cout << "Server " << rank << " handling QUERY: '"
                      << msg.queryStr << "'" << std::endl;

            // Check if this server can handle the query
            if (!server->canHandleQuery(msg.queryStr))
            {
                std::cout << "Server " << rank << " cannot handle query '"
                          << msg.queryStr << "'" << std::endl;

                // Send empty response
                ResponseMessage response;
                response.success = true;
                sendResponse(response, sourceRank, RESULT_TAG);
                return;
            }

            // Execute the query
            std::vector<int> results = server->executeQuery(msg.queryStr);

            std::cout << "Server " << rank << " found " << results.size()
                      << " results for query '" << msg.queryStr << "'" << std::endl;

            // Send results
            ResponseMessage response(results);
            sendResponse(response, sourceRank, RESULT_TAG);
        }

        void MPIServer::handleAdminMessage(const AdminMessage &msg, int sourceRank)
        {
            std::cout << "Server " << rank << " handling admin message type "
                      << msg.type << std::endl;

            bool success = true;

            switch (msg.type)
            {
            case CHECKPOINT:
                success = server->checkpointIndex();
                break;
            case RECOVER:
                success = server->recoverIndex();
                break;
            case SHUTDOWN:
                shutdown();
                break;
            default:
                throw std::runtime_error("Unknown admin message type");
            }

            // Send response
            ResponseMessage response;
            response.success = success;
            sendResponse(response, sourceRank, ADMIN_TAG);
        }

        void MPIServer::sendResponse(const ResponseMessage &response, int destRank, int tag)
        {
            // Serialize the response
            auto buffer = response.serialize();

            // Send the response
            MPI_Send(buffer.data(), buffer.size(), MPI_CHAR, destRank, tag, MPI_COMM_WORLD);
        }

        void MPIServer::sendErrorResponse(const std::string &errorMsg, int destRank, int tag)
        {
            // Create and serialize the error response
            ErrorResponseMessage response(errorMsg);
            auto buffer = response.serialize();

            // Send the error response
            MPI_Send(buffer.data(), buffer.size(), MPI_CHAR, destRank, tag, MPI_COMM_WORLD);
        }

    } // namespace mpi
} // namespace idioms