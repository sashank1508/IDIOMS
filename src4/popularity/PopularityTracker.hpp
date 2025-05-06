#ifndef IDIOMS_POPULARITY_TRACKER_HPP
#define IDIOMS_POPULARITY_TRACKER_HPP

#include <string>
#include <unordered_map>
#include <mutex>
#include <chrono>
#include <algorithm>
#include <memory>
#include <vector>

namespace idioms
{
    namespace popularity
    {

        /**
         * Class that tracks metadata key/value popularity for adaptive replication
         * Uses a time-decay mechanism to age popularity scores over time
         */
        class PopularityTracker
        {
        private:
            // Mutex for thread safety
            mutable std::mutex popularityMutex;

            // Map of metadata key patterns to their popularity scores
            std::unordered_map<std::string, double> keyPopularity;

            // Map of last access times for decay calculation
            std::unordered_map<std::string, std::chrono::time_point<std::chrono::steady_clock>> lastAccess;

            // Decay factor (per hour) - configurable
            double decayFactor;

            // Minimum popularity threshold to consider for adaptive replication
            double popularityThreshold;

            // Maximum replication factor allowed based on popularity
            int maxReplicationFactor;

            // Base replication factor (from DART)
            int baseReplicationFactor;

            // Calculate time decay for a key based on last access time
            double calculateDecay(const std::string &key) const;

            // Update popularity score with decay applied
            void updatePopularityWithDecay(const std::string &key, double increment);

        public:
            /**
             * Constructor
             *
             * @param baseReplication Base replication factor from DART
             * @param maxReplication Maximum replication factor allowed (default: 5)
             * @param threshold Popularity threshold for adaptive replication (default: 10.0)
             * @param decay Decay factor per hour (default: 0.1)
             */
            PopularityTracker(int baseReplication, int maxReplication = 5,
                              double threshold = 10.0, double decay = 0.1);

            /**
             * Record a query for a metadata key pattern
             *
             * @param keyPattern The key pattern queried (can include wildcards)
             * @param increment Amount to increment popularity (default: 1.0)
             */
            void recordQuery(const std::string &keyPattern, double increment = 1.0);

            /**
             * Get the adjusted replication factor for a key pattern
             *
             * @param keyPattern The key pattern to check
             * @return Adjusted replication factor based on popularity
             */
            int getReplicationFactor(const std::string &keyPattern) const;

            /**
             * Get current popularity score for a key pattern
             *
             * @param keyPattern The key pattern to check
             * @return Current popularity score (with decay applied)
             */
            double getPopularity(const std::string &keyPattern) const;

            /**
             * Get all tracked key patterns sorted by popularity
             *
             * @return Vector of key patterns sorted by popularity (highest first)
             */
            std::vector<std::pair<std::string, double>> getAllKeysSortedByPopularity() const;

            /**
             * Reset all popularity tracking data
             */
            void reset();
        };

        // Singleton holder for PopularityTracker to ensure global access
        class PopularityTrackerManager
        {
        private:
            static std::shared_ptr<PopularityTracker> instance;
            static std::mutex instanceMutex;

        public:
            /**
             * Initialize the global PopularityTracker instance
             *
             * @param baseReplication Base replication factor from DART
             * @param maxReplication Maximum replication factor allowed
             * @param threshold Popularity threshold for adaptive replication
             * @param decay Decay factor per hour
             * @return The created PopularityTracker instance
             */
            static std::shared_ptr<PopularityTracker> initialize(
                int baseReplication, int maxReplication = 5,
                double threshold = 10.0, double decay = 0.1);

            /**
             * Get the global PopularityTracker instance
             *
             * @return The global PopularityTracker instance
             * @throws std::runtime_error if not initialized
             */
            static std::shared_ptr<PopularityTracker> getInstance();
        };

    } // namespace popularity
} // namespace idioms

#endif // IDIOMS_POPULARITY_TRACKER_HPP