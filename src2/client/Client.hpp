#ifndef IDIOMS_CLIENT_HPP
#define IDIOMS_CLIENT_HPP

#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include "../dart/DART.hpp"

namespace idioms
{

    // Forward declarations
    class DistributedIdiomsServer;

    /**
     * Client for the distributed IDIOMS system
     * Responsible for routing requests to appropriate servers and aggregating results
     */
    class DistributedIdiomsClient
    {
    private:
        std::shared_ptr<DARTRouter> router;
        std::vector<std::shared_ptr<DistributedIdiomsServer>> servers;
        bool useSuffixTreeMode;

        // Internal method to track servers that can handle a specific query
        std::vector<int> findServersForQuery(const std::string &queryStr) const;

    public:
        /**
         * Constructor
         *
         * @param numServers Number of server instances to create/connect to
         * @param dataDirectory Base directory for server data
         * @param useSuffixMode Whether to use suffix-tree mode for efficient infix queries
         */
        DistributedIdiomsClient(int numServers, const std::string &dataDirectory,
                                bool useSuffixMode = false);

        /**
         * Destructor
         */
        ~DistributedIdiomsClient();

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
    };

} // namespace idioms

#endif // IDIOMS_CLIENT_HPP