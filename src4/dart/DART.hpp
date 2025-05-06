#ifndef IDIOMS_DART_HPP
#define IDIOMS_DART_HPP

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <cstdint>

namespace idioms
{

    /**
     * A consistent hash function for DART node mapping
     */
    class ConsistentHash
    {
    private:
        static const int RING_SIZE = 40; // Number of hash positions per server
        int numServers;
        std::vector<std::pair<uint64_t, int>> ring; // Hash ring: (position, server_id)

        // FNV-1a hash function for strings
        uint64_t hash(const std::string &key, int seed = 0) const;

        // Helper to sort the ring
        void sortRing();

    public:
        ConsistentHash(int numServers);

        // Find the server responsible for a given key
        int getServer(const std::string &key) const;

        // Get a list of servers for replication
        std::vector<int> getReplicaServers(const std::string &key, int replicationFactor) const;
    };

    /**
     * Class representing a virtual node in the DART system
     */
    class VirtualNode
    {
    private:
        uint32_t id;
        std::string prefix;

    public:
        VirtualNode(uint32_t id, const std::string &prefix);

        uint32_t getId() const;
        const std::string &getPrefix() const;

        // Check if a key belongs to this virtual node
        bool containsKey(const std::string &key) const;
    };

    /**
     * The DART router responsible for distributing index records and routing queries
     */
    class DARTRouter
    {
    protected:
        int numServers;
        int replicationFactor;
        ConsistentHash serverMap;
        std::vector<VirtualNode> virtualNodes;

        // Map from virtual node ID to server ID
        std::unordered_map<uint32_t, int> virtualNodeToServer;

        // Map from server ID to list of virtual node IDs
        std::unordered_map<int, std::vector<uint32_t>> serverToVirtualNodes;

        // Number of virtual nodes in the system
        static const int NUM_VIRTUAL_NODES = 256;

        // FNV-1a hash function for strings
        uint32_t hash(const std::string &key) const;

        // Initialize virtual nodes with different prefixes
        void initializeVirtualNodes();

    public:
        DARTRouter(int numServers, double replicationRatio = 0.1);

        // Get the virtual node ID for a key
        uint32_t getVirtualNodeId(const std::string &key) const;

        // Get the server ID for a given virtual node
        int getServerForVirtualNode(uint32_t virtualNodeId) const;

        // Get all virtual node IDs assigned to a server
        std::vector<uint32_t> getVirtualNodesForServer(int serverId) const;

        // Get the server IDs for a given key (including replicas)
        virtual std::vector<int> getServersForKey(const std::string &key) const;

        // Get servers for prefix query
        std::vector<int> getServersForPrefixQuery(const std::string &prefix) const;

        // Get servers for suffix query
        std::vector<int> getServersForSuffixQuery(const std::string &suffix) const;

        // Get servers for infix query
        std::vector<int> getServersForInfixQuery(const std::string &infix) const;

        // Get servers for wildcard query
        std::vector<int> getServersForWildcardQuery() const;

        // Get destination servers for a query
        virtual std::vector<int> getDestinationServers(const std::string &query) const;

        // Save the virtual node to server mapping for persistence
        bool saveMapping(const std::string &filename) const;

        // Load the virtual node to server mapping from persistence
        bool loadMapping(const std::string &filename);

        // Remap virtual nodes to servers after server count change
        void remapServers(int newNumServers);
    };

} // namespace idioms

#endif // IDIOMS_DART_HPP