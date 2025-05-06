#ifndef IDIOMS_TRIE_HPP
#define IDIOMS_TRIE_HPP

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <memory>

namespace idioms
{

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
        std::string fullKey;                  // Store the full key for this node (for suffix/infix searches)

        KeyTrieNode();
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
        std::string fullValue;             // Store the full value for this node (for suffix/infix searches)

        ValueTrieNode();
    };

    /**
     * Second-layer trie for storing metadata values for a specific key
     */
    class ValueTrie
    {
    private:
        std::shared_ptr<ValueTrieNode> root;
        bool useSuffixTreeMode;

        // Helper functions
        void insertValueWithSuffixes(const std::string &value, int objectId);
        void searchByValuePrefix(std::shared_ptr<ValueTrieNode> node, const std::string &prefix,
                                 size_t index, std::unordered_set<int> &results) const;
        void searchByValueInfix(std::shared_ptr<ValueTrieNode> node,
                                const std::string &infix, std::unordered_set<int> &results) const;
        void searchByValueSuffix(std::shared_ptr<ValueTrieNode> node,
                                 const std::string &suffix, std::unordered_set<int> &results) const;
        void collectAllObjectIds(std::shared_ptr<ValueTrieNode> node,
                                 std::unordered_set<int> &results) const;

    public:
        ValueTrie(bool useSuffixMode = false);

        // Index operations
        void insertValue(const std::string &value, int objectId, const std::string &fullValue = "");
        void insertValueWithSuffixMode(const std::string &value, int objectId);

        // Query operations
        std::unordered_set<int> searchExactValue(const std::string &value) const;
        std::unordered_set<int> searchValuePrefix(const std::string &prefix) const;
        std::unordered_set<int> searchValueSuffix(const std::string &suffix) const;
        std::unordered_set<int> searchValueInfix(const std::string &infix) const;
        std::unordered_set<int> getAllObjectIds() const;
    };

    /**
     * First-layer trie for storing metadata keys
     */
    class KeyTrie
    {
    private:
        std::shared_ptr<KeyTrieNode> root;
        bool useSuffixTreeMode;

        // Helper functions
        void insertKeyWithSuffixes(const std::string &key);
        void searchByKeyPrefix(std::shared_ptr<KeyTrieNode> node, const std::string &prefix,
                               size_t index, std::vector<std::shared_ptr<ValueTrie>> &valueTries) const;
        void searchByKeyInfix(std::shared_ptr<KeyTrieNode> node,
                              const std::string &infix, std::vector<std::shared_ptr<ValueTrie>> &valueTries) const;
        void searchByKeySuffix(std::shared_ptr<KeyTrieNode> node,
                               const std::string &suffix, std::vector<std::shared_ptr<ValueTrie>> &valueTries) const;
        void collectAllValueTries(std::shared_ptr<KeyTrieNode> node,
                                  std::vector<std::shared_ptr<ValueTrie>> &valueTries) const;

    public:
        KeyTrie(bool useSuffixMode = false);

        // Index operations
        std::shared_ptr<ValueTrie> insertKeyOnly(const std::string &key, const std::string &fullKey = "");
        std::shared_ptr<ValueTrie> insertKeyWithSuffixMode(const std::string &key);

        // Query operations
        std::shared_ptr<ValueTrie> searchExactKey(const std::string &key) const;
        std::vector<std::shared_ptr<ValueTrie>> searchKeyPrefix(const std::string &prefix) const;
        std::vector<std::shared_ptr<ValueTrie>> searchKeySuffix(const std::string &suffix) const;
        std::vector<std::shared_ptr<ValueTrie>> searchKeyInfix(const std::string &infix) const;
        std::vector<std::shared_ptr<ValueTrie>> getAllValueTries() const;
    };

} // namespace idioms

#endif // IDIOMS_TRIE_HPP