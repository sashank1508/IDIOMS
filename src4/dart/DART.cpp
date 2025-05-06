#include "DART.hpp"
#include <algorithm>
#include <fstream>
#include <iostream>

namespace idioms
{

    // ConsistentHash implementation
    uint64_t ConsistentHash::hash(const std::string &key, int seed) const
    {
        uint64_t hash = 14695981039346656037ULL + seed;
        for (char c : key)
        {
            hash = (hash ^ c) * 1099511628211ULL;
        }
        return hash;
    }

    void ConsistentHash::sortRing()
    {
        std::sort(ring.begin(), ring.end(),
                  [](const auto &a, const auto &b)
                  { return a.first < b.first; });
    }

    ConsistentHash::ConsistentHash(int numServers) : numServers(numServers)
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

    int ConsistentHash::getServer(const std::string &key) const
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

    std::vector<int> ConsistentHash::getReplicaServers(const std::string &key, int replicationFactor) const
    {
        std::vector<int> servers;
        std::unordered_set<int> uniqueServers;

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

    // VirtualNode implementation
    VirtualNode::VirtualNode(uint32_t id, const std::string &prefix)
        : id(id), prefix(prefix) {}

    uint32_t VirtualNode::getId() const
    {
        return id;
    }

    const std::string &VirtualNode::getPrefix() const
    {
        return prefix;
    }

    bool VirtualNode::containsKey(const std::string &key) const
    {
        return key.find(prefix) == 0; // Check if key starts with prefix
    }

    // DARTRouter implementation
    DARTRouter::DARTRouter(int numServers, double replicationRatio)
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

    uint32_t DARTRouter::hash(const std::string &key) const
    {
        uint32_t hash = 2166136261;
        for (char c : key)
        {
            hash = (hash ^ c) * 16777619;
        }
        return hash;
    }

    void DARTRouter::initializeVirtualNodes()
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

    uint32_t DARTRouter::getVirtualNodeId(const std::string &key) const
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

    int DARTRouter::getServerForVirtualNode(uint32_t virtualNodeId) const
    {
        auto it = virtualNodeToServer.find(virtualNodeId);
        if (it != virtualNodeToServer.end())
        {
            return it->second;
        }
        return hash(std::to_string(virtualNodeId)) % numServers;
    }

    std::vector<uint32_t> DARTRouter::getVirtualNodesForServer(int serverId) const
    {
        auto it = serverToVirtualNodes.find(serverId);
        if (it != serverToVirtualNodes.end())
        {
            return it->second;
        }
        return {};
    }

    std::vector<int> DARTRouter::getServersForKey(const std::string &key) const
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

    std::vector<int> DARTRouter::getServersForPrefixQuery(const std::string &prefix) const
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

    std::vector<int> DARTRouter::getServersForSuffixQuery(const std::string &suffix) const
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

    std::vector<int> DARTRouter::getServersForInfixQuery(const std::string &infix) const
    {
        // In suffix-tree mode, infix queries are handled by finding
        // all suffixes that begin with the infix
        return getServersForPrefixQuery(infix);
    }

    std::vector<int> DARTRouter::getServersForWildcardQuery() const
    {
        // Wildcard queries need to be sent to all servers
        std::vector<int> servers;
        for (int i = 0; i < numServers; i++)
        {
            servers.push_back(i);
        }
        return servers;
    }

    std::vector<int> DARTRouter::getDestinationServers(const std::string &query) const
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

    bool DARTRouter::saveMapping(const std::string &filename) const
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

    bool DARTRouter::loadMapping(const std::string &filename)
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

    void DARTRouter::remapServers(int newNumServers)
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
} // namespace idioms