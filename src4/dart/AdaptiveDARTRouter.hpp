#ifndef IDIOMS_ADAPTIVE_DART_ROUTER_HPP
#define IDIOMS_ADAPTIVE_DART_ROUTER_HPP

#include "DART.hpp"
#include "../popularity/PopularityTracker.hpp"
#include <memory>

namespace idioms
{

    /**
     * Enhanced version of DARTRouter that uses popularity tracking
     * for adaptive replication of popular metadata
     */
    class AdaptiveDARTRouter : public DARTRouter
    {
    private:
        // Reference to the popularity tracker
        std::shared_ptr<popularity::PopularityTracker> popularityTracker;

        // Whether to use adaptive replication
        bool adaptiveReplicationEnabled;

    public:
        /**
         * Constructor
         *
         * @param numServers Number of servers in the system
         * @param baseReplicationRatio Base replication ratio (passed to parent)
         * @param maxReplicationFactor Maximum replication factor for popular keys
         * @param popularityThreshold Popularity threshold for adaptive replication
         * @param decayFactor Decay factor for popularity scores
         * @param enableAdaptiveReplication Whether to enable adaptive replication
         */
        AdaptiveDARTRouter(int numServers, double baseReplicationRatio = 0.1,
                           int maxReplicationFactor = 8, double popularityThreshold = 5.0,
                           double decayFactor = 0.05, bool enableAdaptiveReplication = true);

        /**
         * Record a query for popularity tracking
         *
         * @param keyPattern The key pattern being queried
         */
        void recordQuery(const std::string &keyPattern);

        /**
         * Get servers for a key with adaptive replication based on popularity
         *
         * @param key The key to find servers for
         * @return Vector of server IDs to send the key to
         */
        std::vector<int> getServersForKey(const std::string &key) const override;

        /**
         * Get destination servers for a query with adaptive routing
         *
         * @param query The query string
         * @return Vector of server IDs to route the query to
         */
        std::vector<int> getDestinationServers(const std::string &query) const override;

        /**
         * Enable or disable adaptive replication
         *
         * @param enable Whether to enable adaptive replication
         */
        void setAdaptiveReplicationEnabled(bool enable);

        /**
         * Get popularity statistics for all tracked key patterns
         *
         * @return Vector of key patterns sorted by popularity
         */
        std::vector<std::pair<std::string, double>> getPopularityStats() const;

        /**
         * Get current replication factor for a key pattern
         *
         * @param keyPattern The key pattern to check
         * @return Current replication factor
         */
        int getCurrentReplicationFactor(const std::string &keyPattern) const;
    };

} // namespace idioms

#endif // IDIOMS_ADAPTIVE_DART_ROUTER_HPP