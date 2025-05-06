#include "PopularityTracker.hpp"
#include <iostream>
#include <cmath>
#include <vector>

namespace idioms
{
    namespace popularity
    {

        // Initialize singleton instance
        std::shared_ptr<PopularityTracker> PopularityTrackerManager::instance = nullptr;
        std::mutex PopularityTrackerManager::instanceMutex;

        PopularityTracker::PopularityTracker(int baseReplication, int maxReplication,
                                             double threshold, double decay)
            : decayFactor(decay),
              popularityThreshold(threshold),
              maxReplicationFactor(maxReplication),
              baseReplicationFactor(baseReplication)
        {
            std::cout << "PopularityTracker initialized with:"
                      << " baseReplication=" << baseReplicationFactor
                      << " maxReplication=" << maxReplicationFactor
                      << " threshold=" << popularityThreshold
                      << " decay=" << decayFactor << std::endl;
        }

        double PopularityTracker::calculateDecay(const std::string &key) const
        {
            auto now = std::chrono::steady_clock::now();
            auto it = lastAccess.find(key);

            if (it == lastAccess.end())
            {
                return 1.0; // No decay for first access
            }

            // Calculate hours since last access
            double hoursSinceLastAccess =
                std::chrono::duration_cast<std::chrono::milliseconds>(now - it->second).count() /
                (1000.0 * 60.0 * 60.0);

            // Calculate decay factor using exponential decay
            return std::exp(-decayFactor * hoursSinceLastAccess);
        }

        void PopularityTracker::updatePopularityWithDecay(const std::string &key, double increment)
        {
            // Calculate current decayed popularity
            double decay = calculateDecay(key);
            double currentPopularity = 0.0;

            auto it = keyPopularity.find(key);
            if (it != keyPopularity.end())
            {
                currentPopularity = it->second * decay;
            }

            // Update with new increment
            keyPopularity[key] = currentPopularity + increment;

            // Update last access time
            lastAccess[key] = std::chrono::steady_clock::now();
        }

        // void PopularityTracker::recordQuery(const std::string &keyPattern, double increment)
        // {
        //     std::lock_guard<std::mutex> lock(popularityMutex);
        //     updatePopularityWithDecay(keyPattern, increment);
        // }

        void PopularityTracker::recordQuery(const std::string &keyPattern, double increment)
        {
            std::lock_guard<std::mutex> lock(popularityMutex);

            // Apply bonus to already-popular keys (rich get richer)
            double actualIncrement = increment;
            auto it = keyPopularity.find(keyPattern);
            if (it != keyPopularity.end() && it->second > popularityThreshold)
            {
                // Exponential bonus for already popular keys
                actualIncrement *= (1.0 + std::log10(it->second / popularityThreshold));
            }

            updatePopularityWithDecay(keyPattern, actualIncrement);
        }

        int PopularityTracker::getReplicationFactor(const std::string &keyPattern) const
        {
            std::lock_guard<std::mutex> lock(popularityMutex);

            // Get current popularity (with decay)
            double popularity = getPopularity(keyPattern);

            // If popularity is below threshold, use base replication
            if (popularity < popularityThreshold)
            {
                return baseReplicationFactor;
            }

            // Calculate adaptive replication factor
            int adaptiveReplication = baseReplicationFactor +
                                      static_cast<int>(std::log10(popularity / popularityThreshold));

            // Ensure within bounds
            return std::min(adaptiveReplication, maxReplicationFactor);
        }

        double PopularityTracker::getPopularity(const std::string &keyPattern) const
        {
            // Note: This method assumes popularityMutex is already locked by caller

            auto it = keyPopularity.find(keyPattern);
            if (it == keyPopularity.end())
            {
                return 0.0;
            }

            // Apply decay
            double decay = calculateDecay(keyPattern);
            return it->second * decay;
        }

        std::vector<std::pair<std::string, double>> PopularityTracker::getAllKeysSortedByPopularity() const
        {
            std::lock_guard<std::mutex> lock(popularityMutex);

            // Create a vector for sorting
            std::vector<std::pair<std::string, double>> result;

            // Populate with decayed scores
            for (const auto &entry : keyPopularity)
            {
                double decayedPopularity = entry.second * calculateDecay(entry.first);
                if (decayedPopularity > 0.01)
                { // Filter out extremely unpopular keys
                    result.push_back({entry.first, decayedPopularity});
                }
            }

            // Sort by popularity (descending)
            std::sort(result.begin(), result.end(),
                      [](const auto &a, const auto &b)
                      { return a.second > b.second; });

            return result;
        }

        void PopularityTracker::reset()
        {
            std::lock_guard<std::mutex> lock(popularityMutex);
            keyPopularity.clear();
            lastAccess.clear();
        }

        // Static methods for the manager

        std::shared_ptr<PopularityTracker> PopularityTrackerManager::initialize(
            int baseReplication, int maxReplication, double threshold, double decay)
        {

            std::lock_guard<std::mutex> lock(instanceMutex);

            // Create a new instance
            instance = std::make_shared<PopularityTracker>(
                baseReplication, maxReplication, threshold, decay);

            return instance;
        }

        std::shared_ptr<PopularityTracker> PopularityTrackerManager::getInstance()
        {
            std::lock_guard<std::mutex> lock(instanceMutex);

            if (!instance)
            {
                throw std::runtime_error("PopularityTracker not initialized");
            }

            return instance;
        }

    } // namespace popularity
} // namespace idioms