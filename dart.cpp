#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <cstdint>
#include <algorithm>
#include <random>
#include <sstream>
#include <fstream>
#include <memory>
#include <chrono>
#include <thread>
#include <mutex>
#include <unordered_set>

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
    uint64_t hash(const std::string &key, int seed = 0) const
    {
        uint64_t hash = 14695981039346656037ULL + seed;
        for (char c : key)
        {
            hash = (hash ^ c) * 1099511628211ULL;
        }
        return hash;
    }

    // Helper to sort the ring
    void sortRing()
    {
        std::sort(ring.begin(), ring.end(),
                  [](const auto &a, const auto &b)
                  { return a.first < b.first; });
    }

public:
    ConsistentHash(int numServers) : numServers(numServers)
    {
        // Initialize the hash ring with multiple positions for each server
        for (int server = 0; server < numServers; server++)
        {
            for (int i = 0; i < RING_SIZE; i++)
            {
                std::string key = "server" + std::to_string(server) + "_" + std::to_string(i);
                uint64_t position = hash(key);
                ring.push_back({position, server});
            }
        }
        sortRing();
    }

    // Find the server responsible for a given key
    int getServer(const std::string &key) const
    {
        if (ring.empty())
        {
            return 0;
        }

        uint64_t keyHash = hash(key);

        // Find the first position in the ring >= keyHash
        auto it = std::lower_bound(ring.begin(), ring.end(),
                                   std::make_pair(keyHash, 0),
                                   [](const auto &a, const auto &b)
                                   { return a.first < b.first; });

        // If we reached the end, wrap around to the beginning
        if (it == ring.end())
        {
            it = ring.begin();
        }

        return it->second;
    }

    // Get a list of servers for replication
    std::vector<int> getReplicaServers(const std::string &key, int replicationFactor) const
    {
        std::unordered_set<int> uniqueServers;
        std::vector<int> servers;
        uniqueServers.reserve(replicationFactor + 1);

        if (ring.empty() || replicationFactor <= 0)
        {
            return servers;
        }

        uint64_t keyHash = hash(key);

        // Find the first position in the ring >= keyHash
        auto it = std::lower_bound(ring.begin(), ring.end(),
                                   std::make_pair(keyHash, 0),
                                   [](const auto &a, const auto &b)
                                   { return a.first < b.first; });

        // If we reached the end, wrap around to the beginning
        if (it == ring.end())
        {
            it = ring.begin();
        }

        // Add the primary server
        servers.push_back(it->second);
        uniqueServers.insert(it->second);

        // Add replica servers, advancing around the ring
        while (uniqueServers.size() < std::min(replicationFactor + 1, numServers))
        {
            ++it;
            if (it == ring.end())
            {
                it = ring.begin();
            }

            if (uniqueServers.find(it->second) == uniqueServers.end())
            {
                uniqueServers.insert(it->second);
                servers.push_back(it->second);
            }
        }

        return servers;
    }
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
    VirtualNode(uint32_t id, const std::string &prefix)
        : id(id), prefix(prefix) {}

    uint32_t getId() const { return id; }
    const std::string &getPrefix() const { return prefix; }

    // Check if a key belongs to this virtual node
    bool containsKey(const std::string &key) const
    {
        return key.find(prefix) == 0; // Check if key starts with prefix
    }
};

/**
 * The DART router responsible for distributing index records and routing queries
 */
class DARTRouter
{
private:
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
    uint32_t hash(const std::string &key) const
    {
        uint32_t hash = 2166136261;
        for (char c : key)
        {
            hash = (hash ^ c) * 16777619;
        }
        return hash;
    }

    // Initialize virtual nodes with different prefixes
    void initializeVirtualNodes()
    {
        virtualNodes.clear();
        virtualNodeToServer.clear();
        serverToVirtualNodes.clear();

        // Create a set of virtual nodes with different prefixes
        // For simplicity, we'll use 1-character prefixes for this demo
        std::vector<std::string> prefixes;

        // Add single-character prefixes (a-z, A-Z, 0-9)
        for (char c = 'a'; c <= 'z'; c++)
        {
            prefixes.push_back(std::string(1, c));
        }
        for (char c = 'A'; c <= 'Z'; c++)
        {
            prefixes.push_back(std::string(1, c));
        }
        for (char c = '0'; c <= '9'; c++)
        {
            prefixes.push_back(std::string(1, c));
        }

        // Add special character prefixes
        std::string specialChars = "_-./,:;!@#$%^&*()";
        for (char c : specialChars)
        {
            prefixes.push_back(std::string(1, c));
        }

        // Add two-character prefixes for common combinations
        std::vector<std::string> commonPrefixes = {
            "St", "Fi", "Da", "Ti", "Us", "Pr", "Sp", "Ke", "Va", "Ex",
            "Co", "In", "Re", "De", "Tr", "Lo", "Po", "Pa", "Mo", "Se"};
        prefixes.insert(prefixes.end(), commonPrefixes.begin(), commonPrefixes.end());

        // Use empty prefix for keys that don't match any specific prefix
        prefixes.push_back("");

        // Create virtual nodes for each prefix
        // Ensure we have at least NUM_VIRTUAL_NODES
        while (virtualNodes.size() < NUM_VIRTUAL_NODES)
        {
            for (const auto &prefix : prefixes)
            {
                if (virtualNodes.size() >= NUM_VIRTUAL_NODES)
                    break;

                uint32_t id = virtualNodes.size();
                virtualNodes.emplace_back(id, prefix);
            }
        }

        // Assign virtual nodes to servers using consistent hashing
        for (const auto &vnode : virtualNodes)
        {
            std::string key = "vnode_" + std::to_string(vnode.getId());
            int serverId = serverMap.getServer(key);

            virtualNodeToServer[vnode.getId()] = serverId;
            serverToVirtualNodes[serverId].push_back(vnode.getId());
        }
    }

public:
    DARTRouter(int numServers, double replicationRatio = 0.1)
        : numServers(numServers), serverMap(numServers)
    {
        // Calculate replication factor based on ratio (floor(N/10) as in the paper)
        replicationFactor = static_cast<int>(numServers * replicationRatio);
        if (replicationFactor < 1)
            replicationFactor = 1;

        initializeVirtualNodes();

        std::cout << "DART initialized with " << numServers << " servers and replication factor "
                  << replicationFactor << std::endl;
        std::cout << "Total virtual nodes: " << virtualNodes.size() << std::endl;
    }

    // Get the virtual node ID for a key
    uint32_t getVirtualNodeId(const std::string &key) const
    {
        // Find the first virtual node whose prefix matches the key
        for (const auto &vnode : virtualNodes)
        {
            if (vnode.containsKey(key))
            {
                return vnode.getId();
            }
        }

        // If no match found, use hash to assign to a virtual node
        return hash(key) % virtualNodes.size();
    }

    // Get the server ID for a given virtual node
    int getServerForVirtualNode(uint32_t virtualNodeId) const
    {
        auto it = virtualNodeToServer.find(virtualNodeId);
        if (it != virtualNodeToServer.end())
        {
            return it->second;
        }
        return hash(std::to_string(virtualNodeId)) % numServers;
    }

    // Get all virtual node IDs assigned to a server
    std::vector<uint32_t> getVirtualNodesForServer(int serverId) const
    {
        auto it = serverToVirtualNodes.find(serverId);
        if (it != serverToVirtualNodes.end())
        {
            return it->second;
        }
        return {};
    }

    // Get the server IDs for a given key (including replicas)
    std::vector<int> getServersForKey(const std::string &key) const
    {
        std::vector<int> servers;
        uint32_t baseVirtualNodeId = getVirtualNodeId(key);

        // Get primary server
        int primaryServer = getServerForVirtualNode(baseVirtualNodeId);
        servers.push_back(primaryServer);

        // Add replica servers
        if (replicationFactor > 0)
        {
            std::vector<int> replicaServers =
                serverMap.getReplicaServers(key, replicationFactor);

            for (int serverId : replicaServers)
            {
                if (serverId != primaryServer &&
                    std::find(servers.begin(), servers.end(), serverId) == servers.end())
                {
                    servers.push_back(serverId);
                    if (servers.size() >= replicationFactor + 1)
                        break;
                }
            }
        }

        return servers;
    }

    // Get servers for prefix query
    std::vector<int> getServersForPrefixQuery(const std::string &prefix) const
    {
        std::vector<int> servers;
        std::unordered_set<int> uniqueServers;

        // Find all virtual nodes that might contain keys with the given prefix
        for (const auto &vnode : virtualNodes)
        {
            const std::string &nodePrefix = vnode.getPrefix();

            // If node prefix starts with query prefix or query prefix starts with node prefix
            if (nodePrefix.find(prefix) == 0 || prefix.find(nodePrefix) == 0)
            {
                int serverId = getServerForVirtualNode(vnode.getId());
                if (uniqueServers.find(serverId) == uniqueServers.end())
                {
                    uniqueServers.insert(serverId);
                    servers.push_back(serverId);
                }
            }
        }

        // If no servers found, query might match keys across all virtual nodes
        if (servers.empty())
        {
            for (int i = 0; i < numServers; i++)
            {
                servers.push_back(i);
            }
        }

        return servers;
    }

    // Get servers for suffix query
    std::vector<int> getServersForSuffixQuery(const std::string &suffix) const
    {
        // In suffix-tree mode, each suffix is indexed separately
        // Find the virtual node for this suffix
        uint32_t virtualNodeId = getVirtualNodeId(suffix);
        int primaryServer = getServerForVirtualNode(virtualNodeId);

        std::vector<int> servers = {primaryServer};

        // Add replicas
        if (replicationFactor > 0)
        {
            std::vector<int> replicaServers =
                serverMap.getReplicaServers(suffix, replicationFactor);

            for (int serverId : replicaServers)
            {
                if (serverId != primaryServer &&
                    std::find(servers.begin(), servers.end(), serverId) == servers.end())
                {
                    servers.push_back(serverId);
                    if (servers.size() >= replicationFactor + 1)
                        break;
                }
            }
        }

        return servers;
    }

    // Get servers for infix query
    std::vector<int> getServersForInfixQuery(const std::string &infix) const
    {
        // In suffix-tree mode, infix queries are handled by finding
        // all suffixes that begin with the infix
        return getServersForPrefixQuery(infix);
    }

    // Get servers for wildcard query
    std::vector<int> getServersForWildcardQuery() const
    {
        // Wildcard queries need to be sent to all servers
        std::vector<int> servers;
        for (int i = 0; i < numServers; i++)
        {
            servers.push_back(i);
        }
        return servers;
    }

    // Get destination servers for a query
    std::vector<int> getDestinationServers(const std::string &query) const
    {
        // Parse the query to determine type
        std::string keyPart;
        size_t pos = query.find('=');
        if (pos != std::string::npos)
        {
            keyPart = query.substr(0, pos);
        }
        else
        {
            keyPart = query;
        }

        // Determine query type based on key part
        if (keyPart == "*")
        {
            return getServersForWildcardQuery();
        }
        else if (keyPart.front() == '*' && keyPart.back() == '*' && keyPart.length() > 2)
        {
            // Infix query
            return getServersForInfixQuery(keyPart.substr(1, keyPart.length() - 2));
        }
        else if (keyPart.front() == '*')
        {
            // Suffix query
            return getServersForSuffixQuery(keyPart.substr(1));
        }
        else if (keyPart.back() == '*')
        {
            // Prefix query
            return getServersForPrefixQuery(keyPart.substr(0, keyPart.length() - 1));
        }
        else
        {
            // Exact query
            return getServersForKey(keyPart);
        }
    }

    // Save the virtual node to server mapping for persistence
    bool saveMapping(const std::string &filename) const
    {
        std::ofstream file(filename);
        if (!file.is_open())
        {
            return false;
        }

        // Write format version
        file << "DART_MAPPING_V1" << std::endl;
        file << numServers << " " << replicationFactor << std::endl;

        // Write virtual node count
        file << virtualNodes.size() << std::endl;

        // Write virtual node details
        for (const auto &vnode : virtualNodes)
        {
            file << vnode.getId() << " " << vnode.getPrefix() << std::endl;
        }

        // Write virtual node to server mapping
        for (const auto &[vnodeId, serverId] : virtualNodeToServer)
        {
            file << vnodeId << " " << serverId << std::endl;
        }

        return true;
    }

    // Load the virtual node to server mapping from persistence
    bool loadMapping(const std::string &filename)
    {
        std::ifstream file(filename);
        if (!file.is_open())
        {
            return false;
        }

        std::string version;
        file >> version;
        if (version != "DART_MAPPING_V1")
        {
            return false;
        }

        int storedNumServers, storedReplicationFactor;
        file >> storedNumServers >> storedReplicationFactor;

        // Only update if server count matches
        if (storedNumServers != numServers)
        {
            std::cout << "Warning: Stored server count (" << storedNumServers
                      << ") doesn't match current server count (" << numServers
                      << "). Remapping required." << std::endl;
            return false;
        }

        replicationFactor = storedReplicationFactor;

        // Clear existing data
        virtualNodes.clear();
        virtualNodeToServer.clear();
        serverToVirtualNodes.clear();

        // Read virtual node count
        int vnodeCount;
        file >> vnodeCount;

        // Read virtual node details
        for (int i = 0; i < vnodeCount; i++)
        {
            uint32_t id;
            std::string prefix;
            file >> id;
            file.ignore(); // Skip whitespace
            std::getline(file, prefix);
            virtualNodes.emplace_back(id, prefix);
        }

        // Read virtual node to server mapping
        while (!file.eof())
        {
            uint32_t vnodeId;
            int serverId;
            if (file >> vnodeId >> serverId)
            {
                virtualNodeToServer[vnodeId] = serverId;
                serverToVirtualNodes[serverId].push_back(vnodeId);
            }
        }

        return true;
    }

    // Remap virtual nodes to servers after server count change
    void remapServers(int newNumServers)
    {
        if (newNumServers <= 0)
        {
            return;
        }

        // Save previous mapping for data migration planning
        auto previousMapping = virtualNodeToServer;

        // Update server count
        numServers = newNumServers;

        // Recalculate replication factor
        replicationFactor = static_cast<int>(numServers * 0.1);
        if (replicationFactor < 1)
            replicationFactor = 1;

        // Create new server map
        serverMap = ConsistentHash(numServers);

        // Clear existing server to virtual node mapping
        serverToVirtualNodes.clear();

        // Reassign virtual nodes to servers
        for (const auto &vnode : virtualNodes)
        {
            std::string key = "vnode_" + std::to_string(vnode.getId());
            int serverId = serverMap.getServer(key);

            virtualNodeToServer[vnode.getId()] = serverId;
            serverToVirtualNodes[serverId].push_back(vnode.getId());
        }

        // Output migration plan
        std::cout << "Server count changed to " << numServers
                  << ". Replication factor adjusted to " << replicationFactor << std::endl;

        int migrationsNeeded = 0;
        for (const auto &vnode : virtualNodes)
        {
            uint32_t vnodeId = vnode.getId();
            int oldServerId = -1;
            auto it = previousMapping.find(vnodeId);
            if (it != previousMapping.end())
            {
                oldServerId = it->second;
            }

            int newServerId = virtualNodeToServer[vnodeId];

            if (oldServerId != -1 && oldServerId != newServerId)
            {
                migrationsNeeded++;
            }
        }

        std::cout << "Migration plan: " << migrationsNeeded
                  << " virtual nodes need to be migrated." << std::endl;
    }
};

/**
 * A mock IDIOMS server for distributed index storage
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
                            std::shared_ptr<DARTRouter> dartRouter)
        : serverId(id), dataDir(dataDirectory), router(dartRouter)
    {
        // Create data directory if it doesn't exist
        std::string serverDir = dataDir + "/server_" + std::to_string(serverId);
        std::string cmd = "mkdir -p " + serverDir;
        system(cmd.c_str());
    }

    void addIndexedKey(const std::string &key, const std::string &value, int objectId)
    {
        std::lock_guard<std::mutex> lock(indexMutex);

        indexedKeys.push_back(key);

        // Also track all suffixes for suffix-tree mode
        for (size_t i = 0; i < key.length(); i++)
        {
            indexedSuffixes.push_back(key.substr(i));
        }

        // Store object metadata
        objectMetadata[objectId].push_back({key, value});
    }

    bool hasKey(const std::string &key) const
    {
        return std::find(indexedKeys.begin(), indexedKeys.end(), key) != indexedKeys.end();
    }

    bool hasSuffix(const std::string &suffix) const
    {
        return std::find(indexedSuffixes.begin(), indexedSuffixes.end(), suffix) != indexedSuffixes.end();
    }

    bool canHandleQuery(const std::string &query) const
    {
        // In a real implementation, this would use the trie structure
        // to efficiently check if this server can handle the query

        std::string keyPart;
        size_t pos = query.find('=');
        if (pos != std::string::npos)
        {
            keyPart = query.substr(0, pos);
        }
        else
        {
            keyPart = query;
        }

        // Check if this server can handle the query
        if (keyPart == "*")
        {
            // Wildcard queries can be handled by any server
            return true;
        }
        else if (keyPart.front() == '*' && keyPart.back() == '*' && keyPart.length() > 2)
        {
            // Infix query - can handle if any suffix starts with the infix
            std::string infix = keyPart.substr(1, keyPart.length() - 2);
            for (const auto &suffix : indexedSuffixes)
            {
                if (suffix.find(infix) == 0)
                {
                    return true;
                }
            }
            return false;
        }
        else if (keyPart.front() == '*')
        {
            // Suffix query - can handle if the suffix is indexed
            std::string suffix = keyPart.substr(1);
            return hasSuffix(suffix);
        }
        else if (keyPart.back() == '*')
        {
            // Prefix query - can handle if any key starts with the prefix
            std::string prefix = keyPart.substr(0, keyPart.length() - 1);
            for (const auto &key : indexedKeys)
            {
                if (key.find(prefix) == 0)
                {
                    return true;
                }
            }
            return false;
        }
        else
        {
            // Exact query - can handle if the key is indexed
            return hasKey(keyPart);
        }
    }

    std::vector<int> executeQuery(const std::string &query) const
    {
        // In a real implementation, this would use the in-memory index to
        // efficiently find matching objects

        // For this demo, we'll use a simplified implementation
        std::vector<int> results;
        std::unordered_set<int> resultSet;

        std::string keyPart, valuePart;
        size_t pos = query.find('=');
        if (pos != std::string::npos)
        {
            keyPart = query.substr(0, pos);
            valuePart = query.substr(pos + 1);
        }
        else
        {
            keyPart = query;
            valuePart = "*";
        }

        // Check if key part contains wildcards
        bool keyHasPrefix = keyPart.back() == '*' && keyPart.length() > 1;
        bool keyHasSuffix = keyPart.front() == '*' && keyPart.length() > 1;
        bool keyIsWildcard = keyPart == "*";
        bool keyHasInfix = keyHasPrefix && keyHasSuffix && keyPart.length() > 2;

        std::string keyToken;
        if (keyHasInfix)
        {
            keyToken = keyPart.substr(1, keyPart.length() - 2);
        }
        else if (keyHasPrefix)
        {
            keyToken = keyPart.substr(0, keyPart.length() - 1);
        }
        else if (keyHasSuffix)
        {
            keyToken = keyPart.substr(1);
        }
        else if (!keyIsWildcard)
        {
            keyToken = keyPart;
        }

        // Check if value part contains wildcards
        bool valueHasPrefix = valuePart.back() == '*' && valuePart.length() > 1;
        bool valueHasSuffix = valuePart.front() == '*' && valuePart.length() > 1;
        bool valueIsWildcard = valuePart == "*";
        bool valueHasInfix = valueHasPrefix && valueHasSuffix && valuePart.length() > 2;

        std::string valueToken;
        if (valueHasInfix)
        {
            valueToken = valuePart.substr(1, valuePart.length() - 2);
        }
        else if (valueHasPrefix)
        {
            valueToken = valuePart.substr(0, valuePart.length() - 1);
        }
        else if (valueHasSuffix)
        {
            valueToken = valuePart.substr(1);
        }
        else if (!valueIsWildcard)
        {
            valueToken = valuePart;
        }

        // For each object, check if its metadata matches the query
        for (const auto &[objectId, metadata] : objectMetadata)
        {
            bool objectMatches = false;

            for (const auto &[key, value] : metadata)
            {
                bool keyMatch = false;
                if (keyIsWildcard)
                {
                    keyMatch = true;
                }
                else if (keyHasInfix)
                {
                    keyMatch = key.find(keyToken) != std::string::npos;
                }
                else if (keyHasPrefix)
                {
                    keyMatch = key.find(keyToken) == 0;
                }
                else if (keyHasSuffix)
                {
                    keyMatch = key.length() >= keyToken.length() &&
                               key.compare(key.length() - keyToken.length(),
                                           keyToken.length(), keyToken) == 0;
                }
                else
                {
                    keyMatch = key == keyToken;
                }

                bool valueMatch = false;
                if (valueIsWildcard)
                {
                    valueMatch = true;
                }
                else if (valueHasInfix)
                {
                    valueMatch = value.find(valueToken) != std::string::npos;
                }
                else if (valueHasPrefix)
                {
                    valueMatch = value.find(valueToken) == 0;
                }
                else if (valueHasSuffix)
                {
                    valueMatch = value.length() >= valueToken.length() &&
                                 value.compare(value.length() - valueToken.length(),
                                               valueToken.length(), valueToken) == 0;
                }
                else
                {
                    valueMatch = value == valueToken;
                }

                if (keyMatch && valueMatch)
                {
                    objectMatches = true;
                    break;
                }
            }

            if (objectMatches)
            {
                resultSet.insert(objectId);
            }
        }

        // Convert set to vector
        results.assign(resultSet.begin(), resultSet.end());
        std::sort(results.begin(), results.end());

        return results;
    }

    // Checkpoint index to disk
    bool checkpointIndex()
    {
        std::lock_guard<std::mutex> lock(indexMutex);

        std::string indexFile = dataDir + "/server_" + std::to_string(serverId) + "/index.dat";
        std::ofstream file(indexFile);
        if (!file.is_open())
        {
            return false;
        }

        // Write format version
        file << "IDIOMS_INDEX_V1" << std::endl;

        // Write indexed keys
        file << indexedKeys.size() << std::endl;
        for (const auto &key : indexedKeys)
        {
            file << key << std::endl;
        }

        // Write indexed suffixes
        file << indexedSuffixes.size() << std::endl;
        for (const auto &suffix : indexedSuffixes)
        {
            file << suffix << std::endl;
        }

        // Write object metadata
        file << objectMetadata.size() << std::endl;
        for (const auto &[objectId, metadata] : objectMetadata)
        {
            file << objectId << " " << metadata.size() << std::endl;
            for (const auto &[key, value] : metadata)
            {
                file << key << std::endl;
                file << value << std::endl;
            }
        }

        return true;
    }

    // Recover index from disk
    bool recoverIndex()
    {
        std::lock_guard<std::mutex> lock(indexMutex);

        std::string indexFile = dataDir + "/server_" + std::to_string(serverId) + "/index.dat";
        std::ifstream file(indexFile);
        if (!file.is_open())
        {
            return false;
        }

        // Read format version
        std::string version;
        std::getline(file, version);
        if (version != "IDIOMS_INDEX_V1")
        {
            return false;
        }

        // Clear existing data
        indexedKeys.clear();
        indexedSuffixes.clear();
        objectMetadata.clear();

        // Read indexed keys
        int keyCount;
        file >> keyCount;
        file.ignore(); // Skip newline

        for (int i = 0; i < keyCount; i++)
        {
            std::string key;
            std::getline(file, key);
            indexedKeys.push_back(key);
        }

        // Read indexed suffixes
        int suffixCount;
        file >> suffixCount;
        file.ignore(); // Skip newline

        for (int i = 0; i < suffixCount; i++)
        {
            std::string suffix;
            std::getline(file, suffix);
            indexedSuffixes.push_back(suffix);
        }

        // Read object metadata
        int objectCount;
        file >> objectCount;

        for (int i = 0; i < objectCount; i++)
        {
            int objectId, metadataCount;
            file >> objectId >> metadataCount;
            file.ignore(); // Skip newline

            for (int j = 0; j < metadataCount; j++)
            {
                std::string key, value;
                std::getline(file, key);
                std::getline(file, value);
                objectMetadata[objectId].push_back({key, value});
            }
        }

        return true;
    }

    int getId() const { return serverId; }
};

/**
 * Simulated client for distributed IDIOMS - handles routing
 * and server coordination
 */
class DistributedIdiomsClient
{
private:
    std::shared_ptr<DARTRouter> router;
    std::vector<std::shared_ptr<DistributedIdiomsServer>> servers;
    bool useSuffixTreeMode;

public:
    DistributedIdiomsClient(int numServers, const std::string &dataDirectory,
                            bool useSuffixMode = false)
        : useSuffixTreeMode(useSuffixMode)
    {

        // Create DART router
        router = std::make_shared<DARTRouter>(numServers);

        // Create server instances
        for (int i = 0; i < numServers; i++)
        {
            servers.push_back(std::make_shared<DistributedIdiomsServer>(
                i, dataDirectory, router));
        }
    }

    // Create a metadata index record
    void create_md_index(const std::string &key, const std::string &value, int objectId)
    {
        // Determine which servers should store this index record
        std::vector<int> serverIds = router->getServersForKey(key);

        std::cout << "Distributing index for key '" << key << "' to servers: ";
        for (size_t i = 0; i < serverIds.size(); i++)
        {
            if (i > 0)
                std::cout << ", ";
            std::cout << serverIds[i];

            // Send the index record to this server
            servers[serverIds[i]]->addIndexedKey(key, value, objectId);
        }
        std::cout << std::endl;
    }

    // Delete a metadata index record (simplified)
    void delete_md_index(const std::string &key, const std::string &value, int objectId)
    {
        // In a real implementation, we would route delete requests to the appropriate servers
        std::cout << "Delete operation would be routed to the same servers as create" << std::endl;
    }

    // Perform a metadata search
    std::vector<int> md_search(const std::string &queryStr)
    {
        // Determine which servers should receive this query
        std::vector<int> destinationServers = router->getDestinationServers(queryStr);

        std::cout << "Query: \"" << queryStr << "\" routed to servers: ";
        for (size_t i = 0; i < destinationServers.size(); i++)
        {
            if (i > 0)
                std::cout << ", ";
            std::cout << destinationServers[i];
        }
        std::cout << std::endl;

        // Send query to selected servers and collect results
        std::unordered_set<int> resultSet;
        std::cout << "Servers that can handle the query: ";
        bool first = true;

        for (int serverId : destinationServers)
        {
            if (servers[serverId]->canHandleQuery(queryStr))
            {
                if (!first)
                    std::cout << ", ";
                std::cout << serverId;
                first = false;

                // Execute query on this server
                std::vector<int> serverResults = servers[serverId]->executeQuery(queryStr);

                // Merge results
                resultSet.insert(serverResults.begin(), serverResults.end());
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

    // Checkpoint all server indices
    void checkpointAllIndices()
    {
        for (const auto &server : servers)
        {
            server->checkpointIndex();
        }
    }

    // Recover all server indices
    void recoverAllIndices()
    {
        for (const auto &server : servers)
        {
            server->recoverIndex();
        }
    }
};

/**
 * Utility function to print object IDs
 */
void print_object_ids(const std::vector<int> &objectIds)
{
    std::cout << "Found " << objectIds.size() << " objects: ";
    if (objectIds.empty())
    {
        std::cout << "None";
    }
    else
    {
        for (size_t i = 0; i < objectIds.size(); i++)
        {
            if (i > 0)
                std::cout << ", ";
            std::cout << objectIds[i];
        }
    }
    std::cout << std::endl;
}

/**
 * Simulation of IDIOMS with DART-based distribution
 */
int main()
{
    std::cout << "=== Distributed IDIOMS with DART Demo ===" << std::endl;

    // Create a data directory
    std::string dataDir = "./idioms_data";
    std::string cmd = "mkdir -p " + dataDir;
    system(cmd.c_str());

    // Create a client with 4 servers and suffix-tree mode enabled
    DistributedIdiomsClient client(4, dataDir, true);

    std::cout << "\n=== Inserting Example Metadata ===" << std::endl;

    // Microscopy data examples from the paper
    client.create_md_index("FILE_PATH", "/data/488nm.tif", 1001);
    client.create_md_index("FILE_PATH", "/data/561nm.tif", 1002);
    client.create_md_index("StageX", "100.00", 1001);
    client.create_md_index("StageY", "200.00", 1001);
    client.create_md_index("StageZ", "50.00", 1001);
    client.create_md_index("StageX", "300.00", 1002);
    client.create_md_index("StageY", "400.00", 1002);
    client.create_md_index("StageZ", "75.00", 1002);
    client.create_md_index("creation_date", "2023-05-26", 1001);
    client.create_md_index("creation_date", "2023-06-15", 1002);
    client.create_md_index("microscope", "LLSM-1", 1001);
    client.create_md_index("microscope", "LLSM-2", 1002);
    client.create_md_index("AUXILIARY_FILE", "/data/488nm_metadata.json", 1001);
    client.create_md_index("AUXILIARY_FILE", "/data/561nm_metadata.json", 1002);

    // Checkpoint the indices
    std::cout << "\n=== Checkpointing Indices ===" << std::endl;
    client.checkpointAllIndices();
    std::cout << "All indices checkpointed to disk" << std::endl;

    std::cout << "\n=== Performing Queries ===" << std::endl;

    // Example queries
    std::vector<std::string> queries = {
        "StageX=300.00",     // Exact query
        "Stage*=*",          // Prefix query
        "*PATH=*tif",        // Suffix query
        "*FILE*=*metadata*", // Infix query
        "Stage*=*00",        // Combined query types
        "*=*488*",           // Wildcard query
        "FILE_PATH=*",       // Debug query
        "*=*.tif"            // Debug query
    };

    for (const auto &query : queries)
    {
        std::cout << "\nQuery: \"" << query << "\"" << std::endl;
        print_object_ids(client.md_search(query));
    }

    return 0;
};