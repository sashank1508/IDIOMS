#include "Trie.hpp"
#include <iostream>

namespace idioms
{

    // KeyTrieNode implementation
    KeyTrieNode::KeyTrieNode() : isEndOfKey(false), valueTrie(nullptr) {}

    // ValueTrieNode implementation
    ValueTrieNode::ValueTrieNode() : isEndOfValue(false) {}

    // ValueTrie implementation
    ValueTrie::ValueTrie(bool useSuffixMode)
        : root(std::make_shared<ValueTrieNode>()), useSuffixTreeMode(useSuffixMode) {}

    void ValueTrie::insertValue(const std::string &value, int objectId, const std::string &fullValue)
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

    void ValueTrie::insertValueWithSuffixes(const std::string &value, int objectId)
    {
        for (size_t i = 0; i < value.length(); i++)
        {
            std::string suffix = value.substr(i);
            insertValue(suffix, objectId, value); // Pass the full value for suffix node
        }
    }

    void ValueTrie::insertValueWithSuffixMode(const std::string &value, int objectId)
    {
        insertValue(value, objectId); // Insert the full value first

        if (useSuffixTreeMode)
        {
            insertValueWithSuffixes(value, objectId);
        }
    }

    void ValueTrie::searchByValuePrefix(std::shared_ptr<ValueTrieNode> node, const std::string &prefix,
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

    void ValueTrie::searchByValueInfix(std::shared_ptr<ValueTrieNode> node,
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

    void ValueTrie::searchByValueSuffix(std::shared_ptr<ValueTrieNode> node,
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

    void ValueTrie::collectAllObjectIds(std::shared_ptr<ValueTrieNode> node,
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

    std::unordered_set<int> ValueTrie::searchExactValue(const std::string &value) const
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

    std::unordered_set<int> ValueTrie::searchValuePrefix(const std::string &prefix) const
    {
        std::unordered_set<int> results;
        searchByValuePrefix(root, prefix, 0, results);
        return results;
    }

    std::unordered_set<int> ValueTrie::searchValueSuffix(const std::string &suffix) const
    {
        std::unordered_set<int> results;

        if (useSuffixTreeMode)
        {
            // With suffix-tree mode, we can utilize the suffix nodes directly
            searchByValueSuffix(root, suffix, results);
        }
        else
        {
            // Without suffix-tree mode, we need to check all values
            std::cout << "Warning: Suffix search without suffix-tree mode is inefficient" << std::endl;
        }

        return results;
    }

    std::unordered_set<int> ValueTrie::searchValueInfix(const std::string &infix) const
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
            std::cout << "Warning: Infix search without suffix-tree mode is inefficient" << std::endl;
        }

        return results;
    }

    std::unordered_set<int> ValueTrie::getAllObjectIds() const
    {
        std::unordered_set<int> results;
        collectAllObjectIds(root, results);
        return results;
    }

    // KeyTrie implementation
    KeyTrie::KeyTrie(bool useSuffixMode)
        : root(std::make_shared<KeyTrieNode>()), useSuffixTreeMode(useSuffixMode) {}

    std::shared_ptr<ValueTrie> KeyTrie::insertKeyOnly(const std::string &key, const std::string &fullKey)
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

    void KeyTrie::insertKeyWithSuffixes(const std::string &key)
    {
        for (size_t i = 0; i < key.length(); i++)
        {
            insertKeyOnly(key.substr(i), key); // Pass the full key for suffix nodes
        }
    }

    std::shared_ptr<ValueTrie> KeyTrie::insertKeyWithSuffixMode(const std::string &key)
    {
        std::shared_ptr<ValueTrie> mainValueTrie = insertKeyOnly(key, key);

        if (useSuffixTreeMode)
        {
            // Insert all suffixes
            insertKeyWithSuffixes(key);
        }

        return mainValueTrie;
    }

    void KeyTrie::searchByKeyPrefix(std::shared_ptr<KeyTrieNode> node, const std::string &prefix,
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

    void KeyTrie::searchByKeyInfix(std::shared_ptr<KeyTrieNode> node,
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

    void KeyTrie::searchByKeySuffix(std::shared_ptr<KeyTrieNode> node,
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

    void KeyTrie::collectAllValueTries(std::shared_ptr<KeyTrieNode> node,
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

    std::shared_ptr<ValueTrie> KeyTrie::searchExactKey(const std::string &key) const
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

    std::vector<std::shared_ptr<ValueTrie>> KeyTrie::searchKeyPrefix(const std::string &prefix) const
    {
        std::vector<std::shared_ptr<ValueTrie>> results;
        searchByKeyPrefix(root, prefix, 0, results);
        return results;
    }

    std::vector<std::shared_ptr<ValueTrie>> KeyTrie::searchKeySuffix(const std::string &suffix) const
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
        }

        return results;
    }

    std::vector<std::shared_ptr<ValueTrie>> KeyTrie::searchKeyInfix(const std::string &infix) const
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
        }

        return results;
    }

    std::vector<std::shared_ptr<ValueTrie>> KeyTrie::getAllValueTries() const
    {
        std::vector<std::shared_ptr<ValueTrie>> results;
        collectAllValueTries(root, results);
        return results;
    }

} // namespace idioms