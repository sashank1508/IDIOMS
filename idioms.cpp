#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <algorithm>

// Store metadata in a global structure for retrieval
std::unordered_map<int, std::vector<std::pair<std::string, std::string>>> objectMetadata;

// Add metadata to the tracking structure
void track_metadata(int objectId, const std::string &key, const std::string &value)
{
    objectMetadata[objectId].push_back(std::make_pair(key, value));
}

// Forward declarations
class ValueTrie;

/**
 * Node for the first-layer trie that stores metadata keys
 */
class KeyTrieNode
{
public:
    std::unordered_map<char, std::shared_ptr<KeyTrieNode>> children;
    bool isEndOfKey;
    std::shared_ptr<ValueTrie> valueTrie; // Points to the second-layer trie for this key
    std::string fullKey;                  // Store the full key for this node (needed for suffix searches)

    KeyTrieNode() : isEndOfKey(false), valueTrie(nullptr) {}
};

/**
 * Node for the second-layer trie that stores metadata values
 */
class ValueTrieNode
{
public:
    std::unordered_map<char, std::shared_ptr<ValueTrieNode>> children;
    bool isEndOfValue;
    std::unordered_set<int> objectIds; // Objects with this key-value pair
    std::string fullValue;             // Store the full value for this node (needed for suffix searches)

    ValueTrieNode() : isEndOfValue(false) {}
};

/**
 * Second-layer trie for storing metadata values for a specific key
 */
class ValueTrie
{
private:
    std::shared_ptr<ValueTrieNode> root;
    bool useSuffixTreeMode;

    // Helper function to insert a value and its suffixes
    void insertValueWithSuffixes(const std::string &value, int objectId)
    {
        for (size_t i = 0; i < value.length(); i++)
        {
            std::string suffix = value.substr(i);
            insertValue(suffix, objectId, value); // Pass the full value for suffix node
        }
    }

    // Helper function to search by value prefix
    void searchByValuePrefix(std::shared_ptr<ValueTrieNode> node, const std::string &prefix,
                             size_t index, std::unordered_set<int> &results) const
    {
        if (index == prefix.length())
        {
            // If reached the end of prefix, collect all object IDs in this subtree
            collectAllObjectIds(node, results);
            return;
        }

        char c = prefix[index];
        if (c == '*')
        {
            // Wildcard: search all branches
            for (const auto &pair : node->children)
            {
                searchByValuePrefix(pair.second, prefix, index + 1, results);
            }
        }
        else
        {
            // Exact match
            if (node->children.find(c) != node->children.end())
            {
                searchByValuePrefix(node->children[c], prefix, index + 1, results);
            }
        }
    }

    // Helper function to search for values containing an infix
    void searchByValueInfix(std::shared_ptr<ValueTrieNode> node,
                            const std::string &infix, std::unordered_set<int> &results) const
    {
        // If this node has a full value and it contains the infix, add the object IDs
        if (node->isEndOfValue && !node->fullValue.empty())
        {
            if (node->fullValue.find(infix) != std::string::npos)
            {
                results.insert(node->objectIds.begin(), node->objectIds.end());
            }
        }

        // Recursively search all child nodes
        for (const auto &pair : node->children)
        {
            searchByValueInfix(pair.second, infix, results);
        }
    }

    // Helper function to search for values ending with a suffix
    void searchByValueSuffix(std::shared_ptr<ValueTrieNode> node,
                             const std::string &suffix, std::unordered_set<int> &results) const
    {
        // If this node has a full value and it ends with the suffix, add the object IDs
        if (node->isEndOfValue && !node->fullValue.empty())
        {
            if (node->fullValue.length() >= suffix.length() &&
                node->fullValue.compare(node->fullValue.length() - suffix.length(),
                                        suffix.length(), suffix) == 0)
            {
                results.insert(node->objectIds.begin(), node->objectIds.end());
            }
        }

        // Recursively search all child nodes
        for (const auto &pair : node->children)
        {
            searchByValueSuffix(pair.second, suffix, results);
        }
    }

    // Helper function to collect all object IDs in a subtree
    void collectAllObjectIds(std::shared_ptr<ValueTrieNode> node,
                             std::unordered_set<int> &results) const
    {
        if (node->isEndOfValue)
        {
            results.insert(node->objectIds.begin(), node->objectIds.end());
        }

        for (const auto &pair : node->children)
        {
            collectAllObjectIds(pair.second, results);
        }
    }

public:
    ValueTrie(bool useSuffixMode = false)
        : root(std::make_shared<ValueTrieNode>()), useSuffixTreeMode(useSuffixMode) {}

    // Insert a value for a specific object ID
    void insertValue(const std::string &value, int objectId, const std::string &fullValue = "")
    {
        std::shared_ptr<ValueTrieNode> current = root;

        for (char c : value)
        {
            if (current->children.find(c) == current->children.end())
            {
                current->children[c] = std::make_shared<ValueTrieNode>();
            }
            current = current->children[c];
        }

        current->isEndOfValue = true;
        current->objectIds.insert(objectId);

        // Store the full value for suffix and infix searches
        if (!fullValue.empty())
        {
            current->fullValue = fullValue;
        }
        else
        {
            current->fullValue = value;
        }
    }

    // Insert a value with all its suffixes (for suffix-tree mode)
    void insertValueWithSuffixMode(const std::string &value, int objectId)
    {
        insertValue(value, objectId); // Insert the full value first

        if (useSuffixTreeMode)
        {
            insertValueWithSuffixes(value, objectId);
        }
    }

    // Search for exact value match
    std::unordered_set<int> searchExactValue(const std::string &value) const
    {
        std::shared_ptr<ValueTrieNode> current = root;
        std::unordered_set<int> results;

        for (char c : value)
        {
            if (current->children.find(c) == current->children.end())
            {
                return results; // No match
            }
            current = current->children[c];
        }

        if (current->isEndOfValue)
        {
            return current->objectIds;
        }

        return results;
    }

    // Search for values with a specific prefix
    std::unordered_set<int> searchValuePrefix(const std::string &prefix) const
    {
        std::unordered_set<int> results;
        searchByValuePrefix(root, prefix, 0, results);
        return results;
    }

    // Search for values with a specific suffix
    std::unordered_set<int> searchValueSuffix(const std::string &suffix) const
    {
        std::unordered_set<int> results;

        if (useSuffixTreeMode)
        {
            // With suffix-tree mode, we can utilize the suffix nodes directly
            // But we still need to verify the full value ends with the suffix
            searchByValueSuffix(root, suffix, results);
        }
        else
        {
            // Without suffix-tree mode, we need to check all values
            // (This would be very inefficient in a real system)
            std::cout << "Warning: Suffix search without suffix-tree mode is inefficient" << std::endl;
            // Implementation would scan all values - omitted for simplicity
        }

        return results;
    }

    // Search for values containing an infix
    std::unordered_set<int> searchValueInfix(const std::string &infix) const
    {
        std::unordered_set<int> results;

        if (useSuffixTreeMode)
        {
            // With suffix-tree mode, we need to check if any stored full value contains the infix
            searchByValueInfix(root, infix, results);
        }
        else
        {
            // Without suffix-tree mode, we need to check all values
            // (This would be very inefficient in a real system)
            std::cout << "Warning: Infix search without suffix-tree mode is inefficient" << std::endl;
            // Implementation would scan all values - omitted for simplicity
        }

        return results;
    }

    // Get all object IDs in this trie
    std::unordered_set<int> getAllObjectIds() const
    {
        std::unordered_set<int> results;
        collectAllObjectIds(root, results);
        return results;
    }
};

/**
 * First-layer trie for storing metadata keys
 */
class KeyTrie
{
private:
    std::shared_ptr<KeyTrieNode> root;
    bool useSuffixTreeMode;

    // Helper function to insert a key and its suffixes
    void insertKeyWithSuffixes(const std::string &key)
    {
        for (size_t i = 0; i < key.length(); i++)
        {
            insertKeyOnly(key.substr(i), key); // Pass the full key for suffix nodes
        }
    }

    // Helper function to search by key prefix
    void searchByKeyPrefix(std::shared_ptr<KeyTrieNode> node, const std::string &prefix,
                           size_t index, std::vector<std::shared_ptr<ValueTrie>> &valueTries) const
    {
        if (index == prefix.length())
        {
            // If we've reached the end of the prefix, collect this node and all descendants
            collectAllValueTries(node, valueTries);
            return;
        }

        char c = prefix[index];
        if (c == '*')
        {
            // Wildcard: search all branches
            for (const auto &pair : node->children)
            {
                searchByKeyPrefix(pair.second, prefix, index + 1, valueTries);
            }
        }
        else
        {
            // Exact character match
            if (node->children.find(c) != node->children.end())
            {
                searchByKeyPrefix(node->children[c], prefix, index + 1, valueTries);
            }
        }
    }

    // Helper function to search for keys containing an infix
    void searchByKeyInfix(std::shared_ptr<KeyTrieNode> node,
                          const std::string &infix, std::vector<std::shared_ptr<ValueTrie>> &valueTries) const
    {
        // If this node has a full key and it contains the infix, add its value trie
        if (node->isEndOfKey && node->valueTrie && !node->fullKey.empty())
        {
            if (node->fullKey.find(infix) != std::string::npos)
            {
                valueTries.push_back(node->valueTrie);
            }
        }

        // Recursively search all child nodes
        for (const auto &pair : node->children)
        {
            searchByKeyInfix(pair.second, infix, valueTries);
        }
    }

    // Helper function to search for keys ending with a suffix
    void searchByKeySuffix(std::shared_ptr<KeyTrieNode> node,
                           const std::string &suffix, std::vector<std::shared_ptr<ValueTrie>> &valueTries) const
    {
        // If this node has a full key and it ends with the suffix, add its value trie
        if (node->isEndOfKey && node->valueTrie && !node->fullKey.empty())
        {
            if (node->fullKey.length() >= suffix.length() &&
                node->fullKey.compare(node->fullKey.length() - suffix.length(),
                                      suffix.length(), suffix) == 0)
            {
                valueTries.push_back(node->valueTrie);
            }
        }

        // Recursively search all child nodes
        for (const auto &pair : node->children)
        {
            searchByKeySuffix(pair.second, suffix, valueTries);
        }
    }

    // Helper function to collect all value tries in a subtree
    void collectAllValueTries(std::shared_ptr<KeyTrieNode> node,
                              std::vector<std::shared_ptr<ValueTrie>> &valueTries) const
    {
        if (node->isEndOfKey && node->valueTrie)
        {
            valueTries.push_back(node->valueTrie);
        }

        for (const auto &pair : node->children)
        {
            collectAllValueTries(pair.second, valueTries);
        }
    }

public:
    KeyTrie(bool useSuffixMode = false)
        : root(std::make_shared<KeyTrieNode>()), useSuffixTreeMode(useSuffixMode) {}

    // Insert a key only (without value)
    std::shared_ptr<ValueTrie> insertKeyOnly(const std::string &key, const std::string &fullKey = "")
    {
        std::shared_ptr<KeyTrieNode> current = root;

        for (char c : key)
        {
            if (current->children.find(c) == current->children.end())
            {
                current->children[c] = std::make_shared<KeyTrieNode>();
            }
            current = current->children[c];
        }

        current->isEndOfKey = true;

        if (!current->valueTrie)
        {
            current->valueTrie = std::make_shared<ValueTrie>(useSuffixTreeMode);
        }

        // Store the full key for suffix and infix searches
        if (!fullKey.empty())
        {
            current->fullKey = fullKey;
        }
        else
        {
            current->fullKey = key;
        }

        return current->valueTrie;
    }

    // Insert a key with all its suffixes (for suffix-tree mode)
    std::shared_ptr<ValueTrie> insertKeyWithSuffixMode(const std::string &key)
    {
        std::shared_ptr<ValueTrie> mainValueTrie = insertKeyOnly(key, key);

        if (useSuffixTreeMode)
        {
            // Insert all suffixes
            insertKeyWithSuffixes(key);
        }

        return mainValueTrie;
    }

    // Search for exact key match
    std::shared_ptr<ValueTrie> searchExactKey(const std::string &key) const
    {
        std::shared_ptr<KeyTrieNode> current = root;

        for (char c : key)
        {
            if (current->children.find(c) == current->children.end())
            {
                return nullptr; // No match
            }
            current = current->children[c];
        }

        if (current->isEndOfKey)
        {
            return current->valueTrie;
        }

        return nullptr;
    }

    // Search for keys with a specific prefix
    std::vector<std::shared_ptr<ValueTrie>> searchKeyPrefix(const std::string &prefix) const
    {
        std::vector<std::shared_ptr<ValueTrie>> results;
        searchByKeyPrefix(root, prefix, 0, results);
        return results;
    }

    // Search for keys with a specific suffix
    std::vector<std::shared_ptr<ValueTrie>> searchKeySuffix(const std::string &suffix) const
    {
        std::vector<std::shared_ptr<ValueTrie>> results;

        if (useSuffixTreeMode)
        {
            // With suffix-tree mode, we can search through indexed suffixes
            searchByKeySuffix(root, suffix, results);
        }
        else
        {
            // Without suffix-tree mode, we'd need to scan all keys
            std::cout << "Warning: Suffix search without suffix-tree mode is inefficient" << std::endl;
            // Implementation would scan all keys - omitted for simplicity
        }

        return results;
    }

    // Search for keys containing an infix
    std::vector<std::shared_ptr<ValueTrie>> searchKeyInfix(const std::string &infix) const
    {
        std::vector<std::shared_ptr<ValueTrie>> results;

        if (useSuffixTreeMode)
        {
            // With suffix-tree mode, we search for the infix in all stored keys
            searchByKeyInfix(root, infix, results);
        }
        else
        {
            // Without suffix-tree mode, we'd need to scan all keys
            std::cout << "Warning: Infix search without suffix-tree mode is inefficient" << std::endl;
            // Implementation would scan all keys - omitted for simplicity
        }

        return results;
    }

    // Get all value tries in this trie
    std::vector<std::shared_ptr<ValueTrie>> getAllValueTries() const
    {
        std::vector<std::shared_ptr<ValueTrie>> results;
        collectAllValueTries(root, results);
        return results;
    }
};

/**
 * Main IDIOMS client class that provides the metadata indexing and search API
 */
class IdiomsClient
{
private:
    KeyTrie keyTrie;
    bool useSuffixTreeMode;

    // Helper to parse a query condition
    struct QueryCondition
    {
        std::string keyPart;
        std::string valuePart;
        enum QueryType
        {
            EXACT,
            PREFIX,
            SUFFIX,
            INFIX,
            WILDCARD
        } keyType,
            valueType;
    };

    // Parse a query condition string into a structured condition
    QueryCondition parseQueryCondition(const std::string &queryStr)
    {
        QueryCondition condition;

        // Split by '='
        size_t pos = queryStr.find('=');
        if (pos == std::string::npos)
        {
            // Default to wildcard if no delimiter
            condition.keyPart = queryStr;
            condition.valuePart = "*";
        }
        else
        {
            condition.keyPart = queryStr.substr(0, pos);
            condition.valuePart = queryStr.substr(pos + 1);
        }

        // Determine key query type
        if (condition.keyPart == "*")
        {
            condition.keyType = QueryCondition::WILDCARD;
        }
        else if (condition.keyPart.front() == '*' && condition.keyPart.back() == '*' && condition.keyPart.length() > 2)
        {
            condition.keyType = QueryCondition::INFIX;
            // Remove * from both ends
            condition.keyPart = condition.keyPart.substr(1, condition.keyPart.length() - 2);
        }
        else if (condition.keyPart.front() == '*')
        {
            condition.keyType = QueryCondition::SUFFIX;
            // Remove * from front
            condition.keyPart = condition.keyPart.substr(1);
        }
        else if (condition.keyPart.back() == '*')
        {
            condition.keyType = QueryCondition::PREFIX;
            // Remove * from back
            condition.keyPart = condition.keyPart.substr(0, condition.keyPart.length() - 1);
        }
        else
        {
            condition.keyType = QueryCondition::EXACT;
        }

        // Determine value query type
        if (condition.valuePart == "*")
        {
            condition.valueType = QueryCondition::WILDCARD;
        }
        else if (condition.valuePart.front() == '*' && condition.valuePart.back() == '*' && condition.valuePart.length() > 2)
        {
            condition.valueType = QueryCondition::INFIX;
            // Remove * from both ends
            condition.valuePart = condition.valuePart.substr(1, condition.valuePart.length() - 2);
        }
        else if (condition.valuePart.front() == '*')
        {
            condition.valueType = QueryCondition::SUFFIX;
            // Remove * from front
            condition.valuePart = condition.valuePart.substr(1);
        }
        else if (condition.valuePart.back() == '*')
        {
            condition.valueType = QueryCondition::PREFIX;
            // Remove * from back
            condition.valuePart = condition.valuePart.substr(0, condition.valuePart.length() - 1);
        }
        else
        {
            condition.valueType = QueryCondition::EXACT;
        }

        return condition;
    }

public:
    IdiomsClient(bool useSuffixMode = false)
        : keyTrie(useSuffixMode), useSuffixTreeMode(useSuffixMode) {}

    // Create a metadata index record
    void create_md_index(const std::string &key, const std::string &value, int objectId)
    {
        std::shared_ptr<ValueTrie> valueTrie;

        if (useSuffixTreeMode)
        {
            valueTrie = keyTrie.insertKeyWithSuffixMode(key);
            valueTrie->insertValueWithSuffixMode(value, objectId);
        }
        else
        {
            valueTrie = keyTrie.insertKeyOnly(key);
            valueTrie->insertValue(value, objectId);
        }

        // Track this metadata for display purposes
        track_metadata(objectId, key, value);
    }

    // Delete a metadata index record (simplified version)
    void delete_md_index(const std::string &key, const std::string &value, int objectId)
    {
        // In a real implementation, we would:
        // 1. Find the key in the trie
        // 2. Find the value in the second layer
        // 3. Remove the objectId from the set
        // 4. If the set is empty and no children, remove the node
        // Simplified implementation omitted for brevity
        std::cout << "Deleting index for " << key << "=" << value << " (Object " << objectId << ")" << std::endl;
    }

    // Perform a metadata search
    std::vector<int> md_search(const std::string &queryStr)
    {
        QueryCondition condition = parseQueryCondition(queryStr);
        std::unordered_set<int> resultSet;

        // Get value tries based on key condition
        std::vector<std::shared_ptr<ValueTrie>> valueTries;

        switch (condition.keyType)
        {
        case QueryCondition::EXACT:
        {
            std::shared_ptr<ValueTrie> valueTrie = keyTrie.searchExactKey(condition.keyPart);
            if (valueTrie)
            {
                valueTries.push_back(valueTrie);
            }
            break;
        }
        case QueryCondition::PREFIX:
            valueTries = keyTrie.searchKeyPrefix(condition.keyPart);
            break;
        case QueryCondition::SUFFIX:
            valueTries = keyTrie.searchKeySuffix(condition.keyPart);
            break;
        case QueryCondition::INFIX:
            valueTries = keyTrie.searchKeyInfix(condition.keyPart);
            break;
        case QueryCondition::WILDCARD:
            valueTries = keyTrie.getAllValueTries();
            break;
        }

        // Search in value tries based on value condition
        for (const auto &valueTrie : valueTries)
        {
            std::unordered_set<int> valueResults;

            switch (condition.valueType)
            {
            case QueryCondition::EXACT:
                valueResults = valueTrie->searchExactValue(condition.valuePart);
                break;
            case QueryCondition::PREFIX:
                valueResults = valueTrie->searchValuePrefix(condition.valuePart);
                break;
            case QueryCondition::SUFFIX:
                valueResults = valueTrie->searchValueSuffix(condition.valuePart);
                break;
            case QueryCondition::INFIX:
                valueResults = valueTrie->searchValueInfix(condition.valuePart);
                break;
            case QueryCondition::WILDCARD:
                valueResults = valueTrie->getAllObjectIds();
                break;
            }

            // Merge results
            resultSet.insert(valueResults.begin(), valueResults.end());
        }

        // Convert set to vector for return
        std::vector<int> results(resultSet.begin(), resultSet.end());
        std::sort(results.begin(), results.end()); // Sort for deterministic output
        return results;
    }
};

// Utility function to print object IDs and matching metadata
void print_object_ids(const std::vector<int> &objectIds, const std::string &query = "")
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

        // Print the matched key-value pairs for each object
        std::cout << std::endl
                  << "Matched metadata entries:";

        if (!query.empty())
        {
            // Parse the query to determine what we're looking for
            size_t pos = query.find('=');
            std::string keyPart = (pos != std::string::npos) ? query.substr(0, pos) : query;
            std::string valuePart = (pos != std::string::npos) ? query.substr(pos + 1) : "*";

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

            // For each object, find metadata entries that match the query pattern
            for (int objectId : objectIds)
            {
                if (objectMetadata.find(objectId) != objectMetadata.end())
                {
                    bool found = false;
                    for (const auto &entry : objectMetadata[objectId])
                    {
                        const std::string &key = entry.first;
                        const std::string &value = entry.second;

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
                            if (!found)
                            {
                                std::cout << std::endl
                                          << "  Object " << objectId << ":";
                                found = true;
                            }
                            std::cout << std::endl
                                      << "    " << key << "=" << value;
                        }
                    }
                }
            }
        }
        else
        {
            // If no query is provided, just list all metadata for matched objects
            for (int objectId : objectIds)
            {
                if (objectMetadata.find(objectId) != objectMetadata.end())
                {
                    std::cout << std::endl
                              << "  Object " << objectId << ":";
                    for (const auto &entry : objectMetadata[objectId])
                    {
                        std::cout << std::endl
                                  << "    " << entry.first << "=" << entry.second;
                    }
                }
            }
        }
    }
    std::cout << std::endl;
}

int main()
{
    // Create a client with suffix-tree mode enabled
    IdiomsClient client(true);

    std::cout << "=== Initializing IDIOMS with example metadata ===" << std::endl;

    // Microscopy data examples from the paper
    client.create_md_index("FILE_PATH", "/data/488nm.tif", 1001);
    std::cout << "Created FILE_PATH=/data/488nm.tif for object 1001" << std::endl;

    client.create_md_index("FILE_PATH", "/data/561nm.tif", 1002);
    std::cout << "Created FILE_PATH=/data/561nm.tif for object 1002" << std::endl;

    client.create_md_index("StageX", "100.00", 1001);
    std::cout << "Created StageX=100.00 for object 1001" << std::endl;

    client.create_md_index("StageY", "200.00", 1001);
    std::cout << "Created StageY=200.00 for object 1001" << std::endl;

    client.create_md_index("StageZ", "50.00", 1001);
    std::cout << "Created StageZ=50.00 for object 1001" << std::endl;

    client.create_md_index("StageX", "300.00", 1002);
    std::cout << "Created StageX=300.00 for object 1002" << std::endl;

    client.create_md_index("StageY", "400.00", 1002);
    std::cout << "Created StageY=400.00 for object 1002" << std::endl;

    client.create_md_index("StageZ", "75.00", 1002);
    std::cout << "Created StageZ=75.00 for object 1002" << std::endl;

    client.create_md_index("creation_date", "2023-05-26", 1001);
    std::cout << "Created creation date=2023-05-26 for object 1001" << std::endl;

    client.create_md_index("creation_date", "2023-06-15", 1002);
    std::cout << "Created creation date=2023-06-15 for object 1002" << std::endl;

    client.create_md_index("microscope", "LLSM-1", 1001);
    std::cout << "Created microscope=LLSM-1 for object 1001" << std::endl;

    client.create_md_index("microscope", "LLSM-2", 1002);
    std::cout << "Created microscope=LLSM-2 for object 1002" << std::endl;

    client.create_md_index("AUXILIARY_FILE", "/data/488nm_metadata.json", 1001);
    std::cout << "Created AUXILIARY_FILE=/data/488nm_metadata.json for object 1001" << std::endl;

    client.create_md_index("AUXILIARY_FILE", "/data/561nm_metadata.json", 1002);
    std::cout << "Created AUXILIARY_FILE=/data/561nm_metadata.json for object 1002" << std::endl;

    // Perform some queries
    std::cout << "\n=== Performing Queries ===" << std::endl;

    // 1. Exact Query
    std::cout << "\n1. Exact Query: \"StageX=300.00\"" << std::endl;
    print_object_ids(client.md_search("StageX=300.00"), "StageX=300.00");

    // 2. Prefix Query
    std::cout << "\n2. Prefix Query: \"Stage*=*\"" << std::endl;
    print_object_ids(client.md_search("Stage*=*"), "Stage*=*");

    // 3. Suffix Query
    std::cout << "\n3. Suffix Query: \"*PATH=*tif\"" << std::endl;
    print_object_ids(client.md_search("*PATH=*tif"), "*PATH=*tif");

    // 4. Infix Query
    std::cout << "\n4. Infix Query: \"*FILE*=*metadata*\"" << std::endl;
    print_object_ids(client.md_search("*FILE*=*metadata*"), "*FILE*=*metadata*");

    // 5. Combined Query Types
    std::cout << "\n5. Combined Query Types: \"Stage*=*00\"" << std::endl;
    print_object_ids(client.md_search("Stage*=*00"), "Stage*=*00");

    // 6. Wildcard Query
    std::cout << "\n6. Wildcard Query: \"*=*488*\"" << std::endl;
    print_object_ids(client.md_search("*=*488*"), "*=*488*");

    // Add some additional debugging queries
    std::cout << "\n7. Debug Query: \"*FILE*=*\"" << std::endl;
    print_object_ids(client.md_search("*FILE*=*"), "*FILE*=*");

    std::cout << "\n8. Debug Query: \"*=*.tif\"" << std::endl;
    print_object_ids(client.md_search("*=*.tif"), "*=*.tif");

    return 0;
}