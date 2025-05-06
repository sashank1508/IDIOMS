#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <chrono>
#include <random>
#include <algorithm>
#include <iomanip>
#include <fstream>
#include "client/Client.hpp"
#include "dart/AdaptiveDARTRouter.hpp"

// Global metadata tracking (for display purposes only)
std::unordered_map<int, std::vector<std::pair<std::string, std::string>>> objectMetadata;

// Add metadata to the tracking structure
void track_metadata(int objectId, const std::string &key, const std::string &value)
{
    objectMetadata[objectId].push_back(std::make_pair(key, value));
}

// Simplified result printing
void print_result_count(const std::vector<int> &objectIds, const std::string &query)
{
    std::cout << "Query: \"" << query << "\" - Found " << objectIds.size() << " results." << std::endl;
}

// Timing function template
template <typename Func>
double time_execution(Func func)
{
    auto start = std::chrono::high_resolution_clock::now();
    func();
    auto end = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double, std::milli> duration = end - start;
    return duration.count();
}

// Generate a skewed workload pattern with popular and unpopular keys
std::vector<std::string> generate_skewed_query_workload(int numQueries, double popularitySkew = 0.8)
{
    // Define query patterns
    std::vector<std::string> queryPatterns = {
        // Popular queries (80% of workload)
        "StageX=*", "StageY=*", "StageZ=*",
        "creation_date=*", "microscope=*",

        // Less popular queries (20% of workload)
        "FILE_PATH=*", "AUXILIARY_FILE=*",
        "coordinate*=*", "temperature=*", "pressure=*",
        "humidity=*", "light_intensity=*", "duration=*",
        "researcher=*", "project=*", "sample_id=*",
        "protocol=*", "magnification=*", "resolution=*",
        "wavelength=*"};

    // Set up random number generator
    std::random_device rd;
    std::mt19937 gen(rd());

    // Distribution for selecting popular vs. unpopular queries
    std::bernoulli_distribution popularDist(popularitySkew);

    // Distributions for selecting specific queries
    std::uniform_int_distribution<> popularQueryDist(0, 4); // First 5 are popular
    std::uniform_int_distribution<> unpopularQueryDist(5, queryPatterns.size() - 1);

    // Generate the workload
    std::vector<std::string> workload;
    workload.reserve(numQueries);

    for (int i = 0; i < numQueries; i++)
    {
        bool usePopularQuery = popularDist(gen);
        int queryIdx;

        if (usePopularQuery)
        {
            queryIdx = popularQueryDist(gen);
        }
        else
        {
            queryIdx = unpopularQueryDist(gen);
        }

        workload.push_back(queryPatterns[queryIdx]);
    }

    return workload;
}

class AdaptiveIdiomsClient : public idioms::DistributedIdiomsClient
{
private:
    std::shared_ptr<idioms::AdaptiveDARTRouter> adaptiveRouter;
    std::unordered_map<int, std::vector<std::pair<std::string, std::string>>> localMetadata;

public:
    AdaptiveIdiomsClient(int numServers, const std::string &dataDirectory, bool useSuffixMode = false)
        : idioms::DistributedIdiomsClient(numServers, dataDirectory, useSuffixMode)
    {

        // Create our adaptive router
        adaptiveRouter = std::make_shared<idioms::AdaptiveDARTRouter>(
            numServers, 0.1, 5, 5.0, 0.05, true); // Lower threshold, lower decay
    }

    void clear_metadata()
    {
        // We'll track what metadata we've stored locally to facilitate clearing
        localMetadata.clear();

        // We would ideally need to clear server-side metadata too, but for our benchmark
        // we'll simply reindex everything, which effectively overwrites the old metadata
    }

    // Override to use our adaptive router and track metadata
    void create_md_index(const std::string &key, const std::string &value, int objectId)
    {
        // Track locally for potential clearing/reindexing
        localMetadata[objectId].push_back({key, value});

        // Determine which servers should store this index record
        std::vector<int> serverIds = adaptiveRouter->getServersForKey(key);

        // For each server that should have this metadata
        for (size_t i = 0; i < serverIds.size(); i++)
        {
            // Call through to the base class method to handle the actual server operations
            idioms::DistributedIdiomsClient::create_md_index(key, value, objectId);
        }
    }

    // Override to use adaptive router
    std::vector<int> md_search(const std::string &queryStr)
    {
        // Parse the query to get the key pattern for popularity tracking
        std::string keyPattern;
        size_t pos = queryStr.find('=');
        if (pos != std::string::npos)
        {
            keyPattern = queryStr.substr(0, pos);
        }
        else
        {
            keyPattern = queryStr;
        }

        // Record this query for popularity tracking
        adaptiveRouter->recordQuery(keyPattern);

        // Use base class implementation for actual search
        return idioms::DistributedIdiomsClient::md_search(queryStr);
    }

    // Get popularity statistics
    std::vector<std::pair<std::string, double>> getPopularityStats() const
    {
        return adaptiveRouter->getPopularityStats();
    }

    // Toggle adaptive replication
    void setAdaptiveReplication(bool enable)
    {
        adaptiveRouter->setAdaptiveReplicationEnabled(enable);
    }

    // Get replication factor for a key
    int getReplicationFactor(const std::string &key) const
    {
        return adaptiveRouter->getCurrentReplicationFactor(key);
    }

    // Reindex all metadata with current replication factors
    void reindexMetadata()
    {
        // For each object
        for (const auto &entry : localMetadata)
        {
            int objectId = entry.first;
            const auto &metadataList = entry.second;

            // For each metadata item
            for (const auto &metadata : metadataList)
            {
                const std::string &key = metadata.first;
                const std::string &value = metadata.second;

                // We'll recreate the index with adaptive replication
                // No need to delete first as we're recreating from scratch
                this->create_md_index(key, value, objectId);
            }
        }
    }
};

// Run benchmark comparing standard and adaptive approaches
// Run benchmark comparing standard and adaptive approaches
void run_benchmark()
{
    // Create data directory
    std::string dataDir = "./idioms_data";
    std::string cmd = "mkdir -p " + dataDir;
    system(cmd.c_str());

    // Number of servers
    const int NUM_SERVERS = 8;

    std::cout << "=== IDIOMS Adaptive Query Distribution Benchmark (Complete Version) ===" << std::endl;

    // Initialize clients
    std::cout << "\n=== Initializing standard and adaptive clients ===" << std::endl;
    auto standardClient = idioms::DistributedIdiomsClient(NUM_SERVERS, dataDir, true);
    auto adaptiveClient = AdaptiveIdiomsClient(NUM_SERVERS, dataDir, true);

    // Initialize with metadata
    std::cout << "\n=== Loading metadata ===" << std::endl;

    // Create more diverse metadata (1000 objects with various attributes)
    const int NUM_OBJECTS = 1000;

    // Define some common metadata keys and value ranges
    std::vector<std::string> commonKeys = {
        "StageX", "StageY", "StageZ", "creation_date", "microscope"};

    std::vector<std::string> rareKeys = {
        "FILE_PATH", "AUXILIARY_FILE", "coordinate_x", "coordinate_y", "coordinate_z",
        "temperature", "pressure", "humidity", "light_intensity", "duration",
        "researcher", "project", "sample_id", "protocol", "magnification",
        "resolution", "wavelength"};

    // Create random number generators
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> commonKeyDist(0, commonKeys.size() - 1);
    std::uniform_int_distribution<> rareKeyDist(0, rareKeys.size() - 1);
    std::uniform_real_distribution<> valueDist(0.0, 1000.0);
    std::uniform_int_distribution<> dateDist(1, 28);
    std::uniform_int_distribution<> monthDist(1, 12);
    std::uniform_int_distribution<> yearDist(2020, 2023);

    // Clear existing metadata to start fresh
    objectMetadata.clear();

    // We don't have a clear_metadata method in the base class,
    // so we'll just create new client instances instead
    standardClient = idioms::DistributedIdiomsClient(NUM_SERVERS, dataDir, true);
    adaptiveClient = AdaptiveIdiomsClient(NUM_SERVERS, dataDir, true);

    // Create metadata for both clients
    for (int objectId = 1; objectId <= NUM_OBJECTS; objectId++)
    {
        // Add some common metadata (every object has these)
        for (const auto &key : commonKeys)
        {
            std::string value;

            if (key == "creation_date")
            {
                // Generate a date string
                value = std::to_string(yearDist(gen)) + "-" +
                        std::to_string(monthDist(gen)) + "-" +
                        std::to_string(dateDist(gen));
            }
            else if (key == "microscope")
            {
                // Generate a microscope name
                value = "LLSM-" + std::to_string(1 + objectId % 5);
            }
            else
            {
                // Generate a numeric value
                value = std::to_string(valueDist(gen));
            }

            // Add to both clients
            standardClient.create_md_index(key, value, objectId);
            adaptiveClient.create_md_index(key, value, objectId);

            // Track for our reference
            track_metadata(objectId, key, value);
        }

        // Add some rare metadata (each object has 2-3 of these)
        int numRareKeys = 2 + objectId % 2; // 2 or 3 rare keys per object
        for (int i = 0; i < numRareKeys; i++)
        {
            std::string key = rareKeys[rareKeyDist(gen)];
            std::string value;

            if (key.find("FILE_PATH") != std::string::npos ||
                key.find("AUXILIARY_FILE") != std::string::npos)
            {
                // Generate a file path
                value = "/data/object_" + std::to_string(objectId) +
                        "/file_" + std::to_string(i) + ".tif";
            }
            else
            {
                // Generate a numeric or string value
                value = std::to_string(valueDist(gen));
            }

            // Add to both clients
            standardClient.create_md_index(key, value, objectId);
            adaptiveClient.create_md_index(key, value, objectId);

            // Track for our reference
            track_metadata(objectId, key, value);
        }
    }

    std::cout << "Created " << NUM_OBJECTS << " objects with metadata" << std::endl;

    // Checkpoint indices
    std::cout << "\n=== Checkpointing Indices ===" << std::endl;
    standardClient.checkpointAllIndices();
    adaptiveClient.checkpointAllIndices();

    // Generate workload with skewed query patterns
    std::cout << "\n=== Generating skewed query workload ===" << std::endl;
    const int NUM_QUERIES = 1000;
    std::vector<std::string> workload = generate_skewed_query_workload(NUM_QUERIES, 0.9); // 90% popular queries

    std::cout << "Generated " << NUM_QUERIES << " queries with 90% bias towards popular keys" << std::endl;

    // Run warmup phase with adaptive client to build popularity scores
    std::cout << "\n=== Running warmup phase to build popularity scores ===" << std::endl;

    for (int i = 0; i < 500; i++)
    { // Extended warmup phase
        adaptiveClient.md_search(workload[i % workload.size()]);
    }

    // Print popularity stats after warmup
    std::cout << "\n=== Popularity statistics after warmup ===" << std::endl;
    auto stats = adaptiveClient.getPopularityStats();

    for (const auto &entry : stats)
    {
        std::cout << entry.first << ": " << std::fixed << std::setprecision(2) << entry.second
                  << " (replication factor: " << adaptiveClient.getReplicationFactor(entry.first) << ")"
                  << std::endl;
    }

    // NEW STEP: Clear and recreate clients with fresh servers
    std::cout << "\n=== Recreating clients for proper reindexing ===" << std::endl;

    // Store the popularity statistics for later use
    auto popularityStats = adaptiveClient.getPopularityStats();

    // Re-initialize clients
    standardClient = idioms::DistributedIdiomsClient(NUM_SERVERS, dataDir, true);
    adaptiveClient = AdaptiveIdiomsClient(NUM_SERVERS, dataDir, true);

    // Re-add all metadata to both clients
    std::cout << "\n=== Reindexing metadata with adaptive replication factors ===" << std::endl;
    for (const auto &objEntry : objectMetadata)
    {
        int objectId = objEntry.first;

        for (const auto &metaEntry : objEntry.second)
        {
            const std::string &key = metaEntry.first;
            const std::string &value = metaEntry.second;

            // Add to standard client (base replication)
            standardClient.create_md_index(key, value, objectId);

            // Add to adaptive client (with popularity-based replication)
            adaptiveClient.create_md_index(key, value, objectId);
        }
    }

    // Print updated replication factors
    std::cout << "\n=== Updated replication factors after reindexing ===" << std::endl;
    for (const auto &entry : popularityStats)
    {
        std::cout << entry.first << ": popularity = " << std::fixed << std::setprecision(2) << entry.second
                  << " (replication factor: " << adaptiveClient.getReplicationFactor(entry.first) << ")"
                  << std::endl;
    }

    // Benchmark phase
    std::cout << "\n=== Running benchmark ===" << std::endl;

    // Data structures for results
    struct QueryResult
    {
        std::string query;
        double executionTime;
        int resultCount;
    };

    std::vector<QueryResult> standardResults;
    std::vector<QueryResult> adaptiveResults;

    // Run benchmark with standard client
    std::cout << "\n--- Standard client benchmark ---" << std::endl;

    for (const auto &query : workload)
    {
        QueryResult result;
        result.query = query;

        result.executionTime = time_execution([&]()
                                              {
            auto queryResults = standardClient.md_search(query);
            result.resultCount = queryResults.size(); });

        standardResults.push_back(result);
    }

    // Run benchmark with adaptive client
    std::cout << "\n--- Adaptive client benchmark ---" << std::endl;

    for (const auto &query : workload)
    {
        QueryResult result;
        result.query = query;

        result.executionTime = time_execution([&]()
                                              {
            auto queryResults = adaptiveClient.md_search(query);
            result.resultCount = queryResults.size(); });

        adaptiveResults.push_back(result);
    }

    // Calculate statistics
    double standardTotalTime = 0.0;
    double adaptiveTotalTime = 0.0;

    for (const auto &result : standardResults)
    {
        standardTotalTime += result.executionTime;
    }

    for (const auto &result : adaptiveResults)
    {
        adaptiveTotalTime += result.executionTime;
    }

    // Group by query pattern
    std::unordered_map<std::string, std::vector<double>> standardQueryTimes;
    std::unordered_map<std::string, std::vector<double>> adaptiveQueryTimes;

    for (const auto &result : standardResults)
    {
        standardQueryTimes[result.query].push_back(result.executionTime);
    }

    for (const auto &result : adaptiveResults)
    {
        adaptiveQueryTimes[result.query].push_back(result.executionTime);
    }

    // Calculate average time by query pattern
    std::unordered_map<std::string, double> standardAvgTimes;
    std::unordered_map<std::string, double> adaptiveAvgTimes;

    for (const auto &entry : standardQueryTimes)
    {
        double sum = 0.0;
        for (double time : entry.second)
        {
            sum += time;
        }
        standardAvgTimes[entry.first] = sum / entry.second.size();
    }

    for (const auto &entry : adaptiveQueryTimes)
    {
        double sum = 0.0;
        for (double time : entry.second)
        {
            sum += time;
        }
        adaptiveAvgTimes[entry.first] = sum / entry.second.size();
    }

    // Print results
    std::cout << "\n=== Benchmark Results ===" << std::endl;
    std::cout << "Total queries: " << NUM_QUERIES << std::endl;
    std::cout << "Standard client total time: " << standardTotalTime << " ms" << std::endl;
    std::cout << "Adaptive client total time: " << adaptiveTotalTime << " ms" << std::endl;

    double percentImprovement = (standardTotalTime - adaptiveTotalTime) / standardTotalTime * 100.0;
    std::cout << "Overall improvement: " << std::fixed << std::setprecision(2)
              << percentImprovement << "%" << std::endl;

    // Print detailed results by query pattern
    std::cout << "\n=== Detailed Results by Query Pattern ===" << std::endl;
    std::cout << std::setw(25) << "Query Pattern" << " | "
              << std::setw(15) << "Standard (ms)" << " | "
              << std::setw(15) << "Adaptive (ms)" << " | "
              << std::setw(15) << "Improvement %" << " | "
              << std::setw(10) << "Replication" << std::endl;
    std::cout << std::string(85, '-') << std::endl;

    // Get popular queries for ordering
    std::vector<std::string> orderedQueries;

    for (const auto &entry : popularityStats)
    {
        if (standardAvgTimes.find(entry.first + "=*") != standardAvgTimes.end())
        {
            orderedQueries.push_back(entry.first + "=*");
        }
    }

    // Add remaining queries
    for (const auto &entry : standardAvgTimes)
    {
        if (std::find(orderedQueries.begin(), orderedQueries.end(), entry.first) == orderedQueries.end())
        {
            orderedQueries.push_back(entry.first);
        }
    }

    // Print in order of popularity
    for (const auto &query : orderedQueries)
    {
        double stdTime = standardAvgTimes[query];
        double adaptTime = adaptiveAvgTimes[query];
        double improvement = (stdTime - adaptTime) / stdTime * 100.0;

        // Extract key pattern for replication factor
        std::string keyPattern = query;
        size_t pos = query.find('=');
        if (pos != std::string::npos)
        {
            keyPattern = query.substr(0, pos);
        }

        int replicationFactor = adaptiveClient.getReplicationFactor(keyPattern);

        std::cout << std::setw(25) << query << " | "
                  << std::setw(15) << std::fixed << std::setprecision(2) << stdTime << " | "
                  << std::setw(15) << std::fixed << std::setprecision(2) << adaptTime << " | "
                  << std::setw(15) << std::fixed << std::setprecision(2) << improvement << " | "
                  << std::setw(10) << replicationFactor << std::endl;
    }

    // Save results to CSV for plotting
    std::ofstream resultsCsv("benchmark_results.csv");
    resultsCsv << "Query,StandardTime,AdaptiveTime,Improvement,ReplicationFactor,Popularity\n";

    for (const auto &query : orderedQueries)
    {
        double stdTime = standardAvgTimes[query];
        double adaptTime = adaptiveAvgTimes[query];
        double improvement = (stdTime - adaptTime) / stdTime * 100.0;

        // Extract key pattern for replication factor and popularity
        std::string keyPattern = query;
        size_t pos = query.find('=');
        if (pos != std::string::npos)
        {
            keyPattern = query.substr(0, pos);
        }

        int replicationFactor = adaptiveClient.getReplicationFactor(keyPattern);

        // Find popularity score
        double popularity = 0.0;
        for (const auto &entry : popularityStats)
        {
            if (entry.first == keyPattern)
            {
                popularity = entry.second;
                break;
            }
        }

        resultsCsv << query << ","
                   << stdTime << ","
                   << adaptTime << ","
                   << improvement << ","
                   << replicationFactor << ","
                   << popularity << "\n";
    }

    resultsCsv.close();
    std::cout << "\nResults saved to benchmark_results.csv" << std::endl;
}