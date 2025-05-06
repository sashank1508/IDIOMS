#ifndef IDIOMS_SERVER_HPP
#define IDIOMS_SERVER_HPP

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <mutex>
#include "../dart/DART.hpp"

namespace idioms
{

    /**
     * A server in the IDIOMS distributed system
     */
    class DistributedIdiomsServer
    {
    private:
        int serverId;
        std::string dataDir;
        std::shared_ptr<DARTRouter> router;
        std::mutex indexMutex;

        // In a real implementation, this would be the IndexManager with in-memory tries
        // For this demo, we'll just track the keys and suffixes
        std::vector<std::string> indexedKeys;
        std::vector<std::string> indexedSuffixes;

        // Map of object ID to its metadata (key-value pairs)
        std::unordered_map<int, std::vector<std::pair<std::string, std::string>>> objectMetadata;

    public:
        DistributedIdiomsServer(int id, const std::string &dataDirectory,
                                std::shared_ptr<DARTRouter> dartRouter);

        // Add an indexed key to this server
        void addIndexedKey(const std::string &key, const std::string &value, int objectId);

        // Check if this server has a specific key
        bool hasKey(const std::string &key) const;

        // Check if this server has a specific suffix
        bool hasSuffix(const std::string &suffix) const;

        // Check if this server can handle a query
        bool canHandleQuery(const std::string &query) const;

        // Execute a query on this server
        std::vector<int> executeQuery(const std::string &query) const;

        // Checkpoint index to disk
        bool checkpointIndex();

        // Recover index from disk
        bool recoverIndex();

        // Get the server ID
        int getId() const;
    };

    /**
     * Client for the distributed IDIOMS system
     */
    class DistributedIdiomsClient
    {
    private:
        std::shared_ptr<DARTRouter> router;
        std::vector<std::shared_ptr<DistributedIdiomsServer>> servers;
        bool useSuffixTreeMode;

    public:
        DistributedIdiomsClient(int numServers, const std::string &dataDirectory,
                                bool useSuffixMode = false);

        // Create a metadata index record
        void create_md_index(const std::string &key, const std::string &value, int objectId);

        // Delete a metadata index record
        void delete_md_index(const std::string &key, const std::string &value, int objectId);

        // Perform a metadata search
        std::vector<int> md_search(const std::string &queryStr);

        // Checkpoint all server indices
        void checkpointAllIndices();

        // Recover all server indices
        void recoverAllIndices();
    };

    // Utility function to print object IDs
    void print_object_ids(const std::vector<int> &objectIds);

} // namespace idioms

#endif // IDIOMS_SERVER_HPP