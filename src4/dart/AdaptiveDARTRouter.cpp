#include "AdaptiveDARTRouter.hpp"
#include <iostream>

namespace idioms
{

    AdaptiveDARTRouter::AdaptiveDARTRouter(int numServers, double baseReplicationRatio,
                                           int maxReplicationFactor, double popularityThreshold,
                                           double decayFactor, bool enableAdaptiveReplication)
        : DARTRouter(numServers, baseReplicationRatio),
          adaptiveReplicationEnabled(enableAdaptiveReplication)
    {
        // Initialize popularity tracker
        popularityTracker = popularity::PopularityTrackerManager::initialize(
            static_cast<int>(numServers * baseReplicationRatio),
            maxReplicationFactor,
            popularityThreshold,
            decayFactor);

        std::cout << "AdaptiveDARTRouter initialized with adaptive replication "
                  << (adaptiveReplicationEnabled ? "enabled" : "disabled") << std::endl;
    }

    void AdaptiveDARTRouter::recordQuery(const std::string &keyPattern)
    {
        if (adaptiveReplicationEnabled)
        {
            popularityTracker->recordQuery(keyPattern);
        }
    }

    std::vector<int> AdaptiveDARTRouter::getServersForKey(const std::string &key) const
    {
        if (!adaptiveReplicationEnabled)
        {
            // Use the default behavior if adaptive replication is disabled
            return DARTRouter::getServersForKey(key);
        }

        std::vector<int> servers;
        uint32_t baseVirtualNodeId = getVirtualNodeId(key);

        // Get primary server
        int primaryServer = getServerForVirtualNode(baseVirtualNodeId);
        servers.push_back(primaryServer);

        // Get the adaptive replication factor
        int replicationFactor = popularityTracker->getReplicationFactor(key);

        // Add replica servers
        if (replicationFactor > 0)
        {
            // Instead of directly accessing serverMap, use the parent class's functionality
            // to get additional servers
            std::vector<int> allServers = DARTRouter::getServersForKey(key);

            for (int serverId : allServers)
            {
                if (serverId != primaryServer &&
                    std::find(servers.begin(), servers.end(), serverId) == servers.end())
                {
                    servers.push_back(serverId);
                    if (static_cast<int>(servers.size()) >= replicationFactor + 1)
                        break;
                }
            }
        }

        return servers;
    }

    std::vector<int> AdaptiveDARTRouter::getDestinationServers(const std::string &query) const
    {
        // Parse the query to get the key pattern
        std::string keyPattern;
        size_t pos = query.find('=');
        if (pos != std::string::npos)
        {
            keyPattern = query.substr(0, pos);
        }
        else
        {
            keyPattern = query;
        }

        // Record this query pattern for popularity tracking
        if (adaptiveReplicationEnabled)
        {
            double increment = 1.0;
            if (keyPattern.find('*') == std::string::npos)
            {
                // Exact matches get higher priority
                increment = 2.0;
            }
            // Using const_cast because this method is const but we need to record the query
            const_cast<AdaptiveDARTRouter *>(this)->recordQuery(keyPattern);
        }

        // Determine query type based on key part
        if (keyPattern == "*")
        {
            return getServersForWildcardQuery();
        }
        else if (keyPattern.front() == '*' && keyPattern.back() == '*' && keyPattern.length() > 2)
        {
            // Infix query
            return getServersForInfixQuery(keyPattern.substr(1, keyPattern.length() - 2));
        }
        else if (keyPattern.front() == '*')
        {
            // Suffix query
            return getServersForSuffixQuery(keyPattern.substr(1));
        }
        else if (keyPattern.back() == '*')
        {
            // Prefix query
            return getServersForPrefixQuery(keyPattern.substr(0, keyPattern.length() - 1));
        }
        else
        {
            // Exact query
            return getServersForKey(keyPattern);
        }
    }

    void AdaptiveDARTRouter::setAdaptiveReplicationEnabled(bool enable)
    {
        adaptiveReplicationEnabled = enable;
        std::cout << "Adaptive replication " << (enable ? "enabled" : "disabled") << std::endl;
    }

    std::vector<std::pair<std::string, double>> AdaptiveDARTRouter::getPopularityStats() const
    {
        return popularityTracker->getAllKeysSortedByPopularity();
    }

    int AdaptiveDARTRouter::getCurrentReplicationFactor(const std::string &keyPattern) const
    {
        return popularityTracker->getReplicationFactor(keyPattern);
    }

} // namespace idioms