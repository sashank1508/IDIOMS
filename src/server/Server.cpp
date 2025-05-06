#include "Server.hpp"
#include <algorithm>
#include <fstream>
#include <iostream>
#include <sstream>

namespace idioms
{

    // DistributedIdiomsServer implementation
    DistributedIdiomsServer::DistributedIdiomsServer(int id, const std::string &dataDirectory,
                                                     std::shared_ptr<DARTRouter> dartRouter)
        : serverId(id), dataDir(dataDirectory), router(dartRouter)
    {
        // Create data directory if it doesn't exist
        std::string serverDir = dataDir + "/server_" + std::to_string(serverId);
        std::string cmd = "mkdir -p " + serverDir;
        system(cmd.c_str());
    }

    void DistributedIdiomsServer::addIndexedKey(const std::string &key, const std::string &value, int objectId)
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

    bool DistributedIdiomsServer::hasKey(const std::string &key) const
    {
        return std::find(indexedKeys.begin(), indexedKeys.end(), key) != indexedKeys.end();
    }

    bool DistributedIdiomsServer::hasSuffix(const std::string &suffix) const
    {
        return std::find(indexedSuffixes.begin(), indexedSuffixes.end(), suffix) != indexedSuffixes.end();
    }

    bool DistributedIdiomsServer::canHandleQuery(const std::string &query) const
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

    std::vector<int> DistributedIdiomsServer::executeQuery(const std::string &query) const
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

    bool DistributedIdiomsServer::checkpointIndex()
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

    bool DistributedIdiomsServer::recoverIndex()
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

    int DistributedIdiomsServer::getId() const
    {
        return serverId;
    }

    // DistributedIdiomsClient implementation
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
                i, dataDirectory, router));
        }
    }

    // Create a metadata index record
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

    // Delete a metadata index record (simplified)
    void DistributedIdiomsClient::delete_md_index(const std::string &key, const std::string &value, int objectId)
    {
        // In a real implementation, we would route delete requests to the appropriate servers
        std::cout << "Delete operation would be routed to the same servers as create" << std::endl;
    }

    // Perform a metadata search
    std::vector<int> DistributedIdiomsClient::md_search(const std::string &queryStr)
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
    void DistributedIdiomsClient::checkpointAllIndices()
    {
        for (const auto &server : servers)
        {
            server->checkpointIndex();
        }
    }

    // Recover all server indices
    void DistributedIdiomsClient::recoverAllIndices()
    {
        for (const auto &server : servers)
        {
            server->recoverIndex();
        }
    }

    // Utility function to print object IDs
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

} // namespace idioms