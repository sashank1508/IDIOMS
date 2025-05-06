#ifndef IDIOMS_SERVER_HPP
#define IDIOMS_SERVER_HPP

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <mutex>
#include "../dart/DART.hpp"
#include "../index/Trie.hpp"

namespace idioms
{

    /**
     * A server in the IDIOMS distributed system
     * Responsible for storing and querying a partition of the distributed metadata index
     */
    class DistributedIdiomsServer
    {
    private:
        int serverId;
        std::string dataDir;
        std::shared_ptr<DARTRouter> router;
        bool useSuffixTreeMode;
        std::mutex indexMutex;

        // In-memory trie-based index
        std::unique_ptr<KeyTrie> keyTrie;

        // Map of object ID to its metadata (key-value pairs) for more efficient lookup
        std::unordered_map<int, std::vector<std::pair<std::string, std::string>>> objectMetadata;

    public:
        /**
         * Constructor
         *
         * @param id Server ID
         * @param dataDirectory Directory for storing index data
         * @param dartRouter Shared DART router instance
         * @param useSuffixMode Whether to use suffix-tree mode for efficient infix queries
         */
        DistributedIdiomsServer(int id, const std::string &dataDirectory,
                                std::shared_ptr<DARTRouter> dartRouter,
                                bool useSuffixMode = false);

        /**
         * Add an indexed key to this server
         *
         * @param key Metadata attribute key
         * @param value Metadata attribute value
         * @param objectId Object ID to associate with this metadata
         */
        void addIndexedKey(const std::string &key, const std::string &value, int objectId);

        /**
         * Remove an indexed key from this server
         *
         * @param key Metadata attribute key
         * @param value Metadata attribute value
         * @param objectId Object ID associated with this metadata
         */
        void removeIndexedKey(const std::string &key, const std::string &value, int objectId);

        /**
         * Check if this server has a specific key
         *
         * @param key Metadata attribute key
         * @return True if this server has at least one object with this key
         */
        bool hasKey(const std::string &key) const;

        /**
         * Check if this server has a specific suffix
         *
         * @param suffix Key suffix to check
         * @return True if this server has at least one key ending with the suffix
         */
        bool hasSuffix(const std::string &suffix) const;

        /**
         * Check if this server can handle a specific query
         *
         * @param query Query string to check
         * @return True if this server can process this query
         */
        bool canHandleQuery(const std::string &query) const;

        /**
         * Execute a query on this server
         *
         * @param query Query string to execute
         * @return Vector of matching object IDs
         */
        std::vector<int> executeQuery(const std::string &query) const;

        /**
         * Checkpoint index to disk
         *
         * @return True if successful, false otherwise
         */
        bool checkpointIndex();

        /**
         * Recover index from disk
         *
         * @return True if successful, false otherwise
         */
        bool recoverIndex();

        /**
         * Get the server ID
         *
         * @return Server ID
         */
        int getId() const;
    };

} // namespace idioms

#endif // IDIOMS_SERVER_HPP