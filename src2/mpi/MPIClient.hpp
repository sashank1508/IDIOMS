#ifndef IDIOMS_MPI_CLIENT_HPP
#define IDIOMS_MPI_CLIENT_HPP

#include <string>
#include <vector>
#include <memory>
#include <mpi.h>
#include "../dart/DART.hpp"
#include "MPICommon.hpp"

namespace idioms
{
    namespace mpi
    {

        /**
         * MPI client for communicating with distributed IDIOMS servers.
         * This client uses MPI to communicate with server processes.
         */
        class MPIClient
        {
        private:
            int rank;                           // MPI rank
            int worldSize;                      // Total number of MPI processes
            bool useSuffixTreeMode;             // Whether to use suffix tree mode
            std::shared_ptr<DARTRouter> router; // DART router for query routing

            // Send a message to a server
            void sendMessage(const Message &msg, int destRank, int tag);

            // Receive a response from a server
            ResponseMessage receiveResponse(int sourceRank, int tag);

            // Find servers that can handle a query
            std::vector<int> findServersForQuery(const std::string &queryStr) const;

        public:
            /**
             * Constructor
             *
             * @param mpiRank MPI rank of this client process
             * @param mpiWorldSize Total number of MPI processes
             * @param useSuffixMode Whether to use suffix tree mode
             */
            MPIClient(int mpiRank, int mpiWorldSize, bool useSuffixMode = false);

            /**
             * Destructor
             */
            ~MPIClient();

            /**
             * Create a metadata index record
             *
             * @param key Metadata attribute key
             * @param value Metadata attribute value
             * @param objectId Object ID to associate with this metadata
             */
            void create_md_index(const std::string &key, const std::string &value, int objectId);

            /**
             * Delete a metadata index record
             *
             * @param key Metadata attribute key
             * @param value Metadata attribute value
             * @param objectId Object ID associated with this metadata
             */
            void delete_md_index(const std::string &key, const std::string &value, int objectId);

            /**
             * Perform a metadata search
             *
             * @param queryStr Query string in the format "key=value" with optional wildcards (*)
             * @return Vector of matching object IDs
             */
            std::vector<int> md_search(const std::string &queryStr);

            /**
             * Checkpoint all server indices to disk
             */
            void checkpointAllIndices();

            /**
             * Recover all server indices from disk
             */
            void recoverAllIndices();

            /**
             * Shutdown all servers
             */
            void shutdownAllServers();
        };

    } // namespace mpi
} // namespace idioms

#endif // IDIOMS_MPI_CLIENT_HPP