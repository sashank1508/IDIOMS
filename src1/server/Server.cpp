#include "Server.hpp"
#include <algorithm>
#include <fstream>
#include <iostream>
#include <sstream>

namespace idioms
{

    DistributedIdiomsServer::DistributedIdiomsServer(int id, const std::string &dataDirectory,
                                                     std::shared_ptr<DARTRouter> dartRouter, bool useSuffixMode)
        : serverId(id), dataDir(dataDirectory), router(dartRouter),
          useSuffixTreeMode(useSuffixMode)
    {

        // Create data directory if it doesn't exist
        std::string serverDir = dataDir + "/server_" + std::to_string(serverId);
        std::string cmd = "mkdir -p " + serverDir;
        system(cmd.c_str());

        // Initialize the trie-based index
        keyTrie = std::make_unique<KeyTrie>(useSuffixTreeMode);
    }

    void DistributedIdiomsServer::addIndexedKey(const std::string &key, const std::string &value, int objectId)
    {
        std::lock_guard<std::mutex> lock(indexMutex);

        // Add to the trie index
        std::shared_ptr<ValueTrie> valueTrie;

        if (useSuffixTreeMode)
        {
            valueTrie = keyTrie->insertKeyWithSuffixMode(key);
            valueTrie->insertValueWithSuffixMode(value, objectId);
        }
        else
        {
            valueTrie = keyTrie->insertKeyOnly(key);
            valueTrie->insertValue(value, objectId);
        }

        // Store in the object metadata map for easier lookup
        objectMetadata[objectId].push_back({key, value});
    }

    void DistributedIdiomsServer::removeIndexedKey(const std::string &key, const std::string &value, int objectId)
    {
        std::lock_guard<std::mutex> lock(indexMutex);

        // In a production system, we would:
        // 1. Look up the key in the trie
        // 2. Find the value in the second layer trie
        // 3. Remove the objectId from the set of objects
        // 4. If the object set becomes empty and there are no child nodes, remove the node

        // For this implementation, we'll just remove from the objectMetadata map
        auto it = objectMetadata.find(objectId);
        if (it != objectMetadata.end())
        {
            auto &metadataList = it->second;
            metadataList.erase(
                std::remove_if(metadataList.begin(), metadataList.end(),
                               [&](const auto &pair)
                               {
                                   return pair.first == key && pair.second == value;
                               }),
                metadataList.end());

            // If no metadata left for this object, remove the entry
            if (metadataList.empty())
            {
                objectMetadata.erase(it);
            }
        }

        std::cout << "Deleted metadata '" << key << "=" << value
                  << "' for object " << objectId << " on server " << serverId << std::endl;
    }

    bool DistributedIdiomsServer::hasKey(const std::string &key) const
    {
        // Check if the key exists in the trie
        return keyTrie->searchExactKey(key) != nullptr;
    }

    bool DistributedIdiomsServer::hasSuffix(const std::string &suffix) const
    {
        // In suffix-tree mode, we can check if a suffix is indexed
        if (!useSuffixTreeMode)
        {
            return false;
        }

        auto valueTries = keyTrie->searchKeySuffix(suffix);
        return !valueTries.empty();
    }

    bool DistributedIdiomsServer::canHandleQuery(const std::string &query) const
    {
        // Parse the query string
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

        // Check if this server can handle the query based on the key part
        if (keyPart == "*")
        {
            // Wildcard queries can be handled by any server
            return true;
        }
        else if (keyPart.front() == '*' && keyPart.back() == '*' && keyPart.length() > 2)
        {
            // Infix query
            std::string infix = keyPart.substr(1, keyPart.length() - 2);
            auto valueTries = keyTrie->searchKeyInfix(infix);
            return !valueTries.empty();
        }
        else if (keyPart.front() == '*')
        {
            // Suffix query
            std::string suffix = keyPart.substr(1);
            return hasSuffix(suffix);
        }
        else if (keyPart.back() == '*')
        {
            // Prefix query
            std::string prefix = keyPart.substr(0, keyPart.length() - 1);
            auto valueTries = keyTrie->searchKeyPrefix(prefix);
            return !valueTries.empty();
        }
        else
        {
            // Exact query
            return hasKey(keyPart);
        }
    }

    std::vector<int> DistributedIdiomsServer::executeQuery(const std::string &query) const
    {
        // Parse the query
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

        // Get value tries based on key condition
        std::vector<std::shared_ptr<ValueTrie>> valueTries;
        std::unordered_set<int> resultSet;

        // Determine key query type
        if (keyPart == "*")
        {
            // Wildcard key
            valueTries = keyTrie->getAllValueTries();
        }
        else if (keyPart.front() == '*' && keyPart.back() == '*' && keyPart.length() > 2)
        {
            // Infix key
            std::string infix = keyPart.substr(1, keyPart.length() - 2);
            valueTries = keyTrie->searchKeyInfix(infix);
        }
        else if (keyPart.front() == '*')
        {
            // Suffix key
            std::string suffix = keyPart.substr(1);
            valueTries = keyTrie->searchKeySuffix(suffix);
        }
        else if (keyPart.back() == '*')
        {
            // Prefix key
            std::string prefix = keyPart.substr(0, keyPart.length() - 1);
            valueTries = keyTrie->searchKeyPrefix(prefix);
        }
        else
        {
            // Exact key
            auto valueTrie = keyTrie->searchExactKey(keyPart);
            if (valueTrie)
            {
                valueTries.push_back(valueTrie);
            }
        }

        // Determine value query type
        for (const auto &valueTrie : valueTries)
        {
            if (valuePart == "*")
            {
                // Wildcard value
                auto objectIds = valueTrie->getAllObjectIds();
                resultSet.insert(objectIds.begin(), objectIds.end());
            }
            else if (valuePart.front() == '*' && valuePart.back() == '*' && valuePart.length() > 2)
            {
                // Infix value
                std::string infix = valuePart.substr(1, valuePart.length() - 2);
                auto objectIds = valueTrie->searchValueInfix(infix);
                resultSet.insert(objectIds.begin(), objectIds.end());
            }
            else if (valuePart.front() == '*')
            {
                // Suffix value
                std::string suffix = valuePart.substr(1);
                auto objectIds = valueTrie->searchValueSuffix(suffix);
                resultSet.insert(objectIds.begin(), objectIds.end());
            }
            else if (valuePart.back() == '*')
            {
                // Prefix value
                std::string prefix = valuePart.substr(0, valuePart.length() - 1);
                auto objectIds = valueTrie->searchValuePrefix(prefix);
                resultSet.insert(objectIds.begin(), objectIds.end());
            }
            else
            {
                // Exact value
                auto objectIds = valueTrie->searchExactValue(valuePart);
                resultSet.insert(objectIds.begin(), objectIds.end());
            }
        }

        // Convert set to vector
        std::vector<int> results(resultSet.begin(), resultSet.end());
        std::sort(results.begin(), results.end());

        return results;
    }

    bool DistributedIdiomsServer::checkpointIndex()
    {
        std::lock_guard<std::mutex> lock(indexMutex);

        std::string indexFile = dataDir + "/server_" + std::to_string(serverId) + "/index.dat";
        std::ofstream file(indexFile, std::ios::binary);
        if (!file.is_open())
        {
            return false;
        }

        // Write format version
        file << "IDIOMS_INDEX_V1" << std::endl;

        // Write server configuration
        file << serverId << " " << (useSuffixTreeMode ? 1 : 0) << std::endl;

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

        file.close();
        return true;
    }

    bool DistributedIdiomsServer::recoverIndex()
    {
        std::lock_guard<std::mutex> lock(indexMutex);

        std::string indexFile = dataDir + "/server_" + std::to_string(serverId) + "/index.dat";
        std::ifstream file(indexFile, std::ios::binary);
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

        // Read server configuration
        int storedServerId;
        int storedSuffixMode;
        file >> storedServerId >> storedSuffixMode;

        if (storedServerId != serverId)
        {
            std::cerr << "Warning: Stored server ID (" << storedServerId
                      << ") doesn't match current server ID (" << serverId
                      << ")." << std::endl;
            return false;
        }

        // Clear existing data
        objectMetadata.clear();
        keyTrie = std::make_unique<KeyTrie>(useSuffixTreeMode);

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

                // Recreate the index
                addIndexedKey(key, value, objectId);
            }
        }

        file.close();
        return true;
    }

    int DistributedIdiomsServer::getId() const
    {
        return serverId;
    }

} // namespace idioms