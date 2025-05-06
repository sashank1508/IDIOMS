#ifndef IDIOMS_MPI_SERVER_HPP
#define IDIOMS_MPI_SERVER_HPP

#include <string>
#include <memory>
#include <atomic>
#include <mpi.h>
#include "../server/Server.hpp"
#include "../dart/DART.hpp"
#include "MPICommon.hpp"

namespace idioms
{
    namespace mpi
    {

        /**
         * MPI server process that handles distributed IDIOMS operations.
         * This server runs as a separate MPI process and communicates with
         * the client through MPI messages.
         */
        class MPIServer
        {
        private:
            int rank;                  // MPI rank
            int worldSize;             // Total number of MPI processes
            std::string dataDir;       // Data directory
            bool useSuffixTreeMode;    // Whether to use suffix tree mode
            std::atomic<bool> running; // Server running flag

            // The actual server implementation
            std::unique_ptr<DistributedIdiomsServer> server;

            // Shared DART router
            std::shared_ptr<DARTRouter> router;

            // Handle a message from a client
            void handleMessage(const std::vector<char> &message, int sourceRank);

            // Handle specific message types
            void handleCreateIndexMessage(const CreateIndexMessage &msg, int sourceRank);
            void handleDeleteIndexMessage(const DeleteIndexMessage &msg, int sourceRank);
            void handleQueryMessage(const QueryMessage &msg, int sourceRank);
            void handleAdminMessage(const AdminMessage &msg, int sourceRank);

            // Send a response to a client
            void sendResponse(const ResponseMessage &response, int destRank, int tag);

            // Send an error response to a client
            void sendErrorResponse(const std::string &errorMsg, int destRank, int tag);

        public:
            /**
             * Constructor
             *
             * @param mpiRank MPI rank of this server process
             * @param mpiWorldSize Total number of MPI processes
             * @param dataDirectory Directory for storing server data
             * @param useSuffixMode Whether to use suffix tree mode
             */
            MPIServer(int mpiRank, int mpiWorldSize, const std::string &dataDirectory, bool useSuffixMode);

            /**
             * Destructor
             */
            ~MPIServer();

            /**
             * Run the server
             * This method blocks and listens for client messages until shutdown
             */
            void run();

            /**
             * Shutdown the server
             */
            void shutdown();

            /**
             * Get the server rank
             *
             * @return MPI rank of this server process
             */
            int getRank() const;
        };

    } // namespace mpi
} // namespace idioms

#endif // IDIOMS_MPI_SERVER_HPP