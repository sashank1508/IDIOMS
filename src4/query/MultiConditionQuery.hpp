#ifndef IDIOMS_MULTI_CONDITION_QUERY_HPP
#define IDIOMS_MULTI_CONDITION_QUERY_HPP

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <functional>

namespace idioms
{
    namespace query
    {

        // Operator type for conditions
        enum OperatorType
        {
            EQUALS,        // =
            NOT_EQUALS,    // !=
            GREATER_THAN,  // >
            LESS_THAN,     // <
            GREATER_EQUAL, // >=
            LESS_EQUAL,    // <=
            CONTAINS,      // contains
            STARTS_WITH,   // starts with
            ENDS_WITH,     // ends with
            REGEX_MATCH    // matches regex
        };

        // Logical operator for combining conditions
        enum LogicalOperator
        {
            AND,
            OR
        };

        // A single condition in a query
        struct QueryCondition
        {
            std::string key;       // Metadata attribute key
            std::string value;     // Value to compare against
            OperatorType op;       // Operator for comparison
            bool keyHasWildcard;   // Whether key contains wildcards
            bool valueHasWildcard; // Whether value contains wildcards

            QueryCondition()
                : op(EQUALS), keyHasWildcard(false), valueHasWildcard(false) {}

            QueryCondition(const std::string &k, const std::string &v, OperatorType o)
                : key(k), value(v), op(o), keyHasWildcard(false), valueHasWildcard(false) {}

            // Convert to string representation
            std::string toString() const;

            // Check if a key-value pair matches this condition
            bool matches(const std::string &testKey, const std::string &testValue) const;

            // Create a condition from a string representation
            static QueryCondition fromString(const std::string &conditionStr);
        };

        // A multi-condition query
        class MultiConditionQuery
        {
        private:
            std::vector<QueryCondition> conditions; // List of conditions
            std::vector<LogicalOperator> operators; // List of logical operators

        public:
            /**
             * Default constructor
             */
            MultiConditionQuery();

            /**
             * Constructor with a single condition
             *
             * @param condition The query condition
             */
            MultiConditionQuery(const QueryCondition &condition);

            /**
             * Add a condition with a logical operator
             *
             * @param op Logical operator
             * @param condition Query condition
             */
            void addCondition(LogicalOperator op, const QueryCondition &condition);

            /**
             * Check if an object matches the query conditions
             *
             * @param objectMetadata Object's metadata attributes
             * @return True if the object matches, false otherwise
             */
            bool matches(const std::unordered_map<std::string, std::string> &objectMetadata) const;

            /**
             * Convert to a string representation
             *
             * @return String representation of the query
             */
            std::string toString() const;

            /**
             * Create a query from a string representation
             *
             * @param queryStr String representation of the query
             * @return MultiConditionQuery object
             */
            static MultiConditionQuery fromString(const std::string &queryStr);

            /**
             * Get all conditions in this query
             *
             * @return Vector of query conditions
             */
            const std::vector<QueryCondition> &getConditions() const;

            /**
             * Get all logical operators in this query
             *
             * @return Vector of logical operators
             */
            const std::vector<LogicalOperator> &getOperators() const;
        };

        // Utility functions

        // Check if a string contains a wildcard
        bool containsWildcard(const std::string &str);

        // Parse a key with wildcards
        std::pair<std::string, bool> parseKey(const std::string &key);

        // Parse a value with wildcards
        std::pair<std::string, bool> parseValue(const std::string &value);

        // Convert a string to a numeric value
        double parseNumeric(const std::string &str);

        // Check if a string represents a numeric value
        bool isNumeric(const std::string &str);

        // Normalize a wildcard pattern for matching
        std::string normalizeWildcardPattern(const std::string &pattern);

        // Match a string against a wildcard pattern
        bool matchWildcard(const std::string &str, const std::string &pattern);

    } // namespace query
} // namespace idioms

#endif // IDIOMS_MULTI_CONDITION_QUERY_HPP