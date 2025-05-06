#include "Client.hpp"
#include "../server/Server.hpp"
#include <algorithm>
#include <iostream>
#include <unordered_set>

namespace idioms
{

    DistributedIdiomsClient::DistributedIdiomsClient(int numServers, const std::string &dataDirectory,
                                                     bool useSuffixMode)
        : useSuffixTreeMode(useSuffixMode)
    {

        // Create DART router
        router = std::make_shared<DARTRouter>(numServers);

        // Create server instances
        for (int i = 0; i < numServers; i++)
        {
            servers.push_back(std::make_shared<DistributedIdiomsServer>(
                i, dataDirectory, router, useSuffixTreeMode));
        }
    }

    DistributedIdiomsClient::~DistributedIdiomsClient()
    {
        // Clean up any resources as needed
    }

    void DistributedIdiomsClient::create_md_index(const std::string &key, const std::string &value, int objectId)
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

    void DistributedIdiomsClient::delete_md_index(const std::string &key, const std::string &value, int objectId)
    {
        // Determine which servers should store this index record
        std::vector<int> serverIds = router->getServersForKey(key);

        std::cout << "Deleting index for key '" << key << "' from servers: ";
        for (size_t i = 0; i < serverIds.size(); i++)
        {
            if (i > 0)
                std::cout << ", ";
            std::cout << serverIds[i];

            // Send the delete request to this server
            servers[serverIds[i]]->removeIndexedKey(key, value, objectId);
        }
        std::cout << std::endl;
    }

    std::vector<int> DistributedIdiomsClient::findServersForQuery(const std::string &queryStr) const
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

        // Find servers that can actually handle this query
        std::vector<int> handlingServers;

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

                handlingServers.push_back(serverId);
            }
        }

        if (first)
            std::cout << "None";
        std::cout << std::endl;

        return handlingServers;
    }

    std::vector<int> DistributedIdiomsClient::md_search(const std::string &queryStr)
    {
        // Find which servers can handle this query
        std::vector<int> handlingServers = findServersForQuery(queryStr);

        // Send query to selected servers and collect results
        std::unordered_set<int> resultSet;

        for (int serverId : handlingServers)
        {
            // Execute query on this server
            std::vector<int> serverResults = servers[serverId]->executeQuery(queryStr);

            // Merge results
            resultSet.insert(serverResults.begin(), serverResults.end());
        }

        // Convert set to vector for return
        std::vector<int> results(resultSet.begin(), resultSet.end());
        std::sort(results.begin(), results.end()); // Sort for deterministic output
        return results;
    }

    void DistributedIdiomsClient::checkpointAllIndices()
    {
        std::cout << "Checkpointing indices to disk..." << std::endl;
        for (const auto &server : servers)
        {
            bool success = server->checkpointIndex();
            if (!success)
            {
                std::cerr << "Warning: Failed to checkpoint index for server "
                          << server->getId() << std::endl;
            }
        }
        std::cout << "Checkpoint complete." << std::endl;
    }

    void DistributedIdiomsClient::recoverAllIndices()
    {
        std::cout << "Recovering indices from disk..." << std::endl;
        for (const auto &server : servers)
        {
            bool success = server->recoverIndex();
            if (!success)
            {
                std::cerr << "Warning: Failed to recover index for server "
                          << server->getId() << std::endl;
            }
        }
        std::cout << "Recovery complete." << std::endl;
    }

} // namespace idioms