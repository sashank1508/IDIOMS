#include "MultiConditionQuery.hpp"
#include <iostream>
#include <sstream>
#include <regex>
#include <cmath>
#include <algorithm>

namespace idioms
{
    namespace query
    {

        // QueryCondition implementation

        std::string QueryCondition::toString() const
        {
            std::string opStr;

            switch (op)
            {
            case EQUALS:
                opStr = "=";
                break;
            case NOT_EQUALS:
                opStr = "!=";
                break;
            case GREATER_THAN:
                opStr = ">";
                break;
            case LESS_THAN:
                opStr = "<";
                break;
            case GREATER_EQUAL:
                opStr = ">=";
                break;
            case LESS_EQUAL:
                opStr = "<=";
                break;
            case CONTAINS:
                opStr = "contains";
                break;
            case STARTS_WITH:
                opStr = "startsWith";
                break;
            case ENDS_WITH:
                opStr = "endsWith";
                break;
            case REGEX_MATCH:
                opStr = "~=";
                break;
            }

            return key + " " + opStr + " " + value;
        }

        bool QueryCondition::matches(const std::string &testKey, const std::string &testValue) const
        {
            // Check key match (with wildcards if needed)
            if (keyHasWildcard)
            {
                if (!matchWildcard(testKey, key))
                {
                    return false;
                }
            }
            else
            {
                if (testKey != key)
                {
                    return false;
                }
            }

            // Handle different operator types
            switch (op)
            {
            case EQUALS:
                if (valueHasWildcard)
                {
                    return matchWildcard(testValue, value);
                }
                else
                {
                    return testValue == value;
                }

            case NOT_EQUALS:
                if (valueHasWildcard)
                {
                    return !matchWildcard(testValue, value);
                }
                else
                {
                    return testValue != value;
                }

            case GREATER_THAN:
                if (isNumeric(testValue) && isNumeric(value))
                {
                    return parseNumeric(testValue) > parseNumeric(value);
                }
                else
                {
                    return testValue > value; // Lexicographic comparison
                }

            case LESS_THAN:
                if (isNumeric(testValue) && isNumeric(value))
                {
                    return parseNumeric(testValue) < parseNumeric(value);
                }
                else
                {
                    return testValue < value; // Lexicographic comparison
                }

            case GREATER_EQUAL:
                if (isNumeric(testValue) && isNumeric(value))
                {
                    return parseNumeric(testValue) >= parseNumeric(value);
                }
                else
                {
                    return testValue >= value; // Lexicographic comparison
                }

            case LESS_EQUAL:
                if (isNumeric(testValue) && isNumeric(value))
                {
                    return parseNumeric(testValue) <= parseNumeric(value);
                }
                else
                {
                    return testValue <= value; // Lexicographic comparison
                }

            case CONTAINS:
                return testValue.find(value) != std::string::npos;

            case STARTS_WITH:
                return testValue.find(value) == 0;

            case ENDS_WITH:
                return testValue.length() >= value.length() &&
                       testValue.compare(testValue.length() - value.length(),
                                         value.length(), value) == 0;

            case REGEX_MATCH:
                try
                {
                    std::regex pattern(value);
                    return std::regex_match(testValue, pattern);
                }
                catch (const std::regex_error &e)
                {
                    std::cerr << "Invalid regex pattern: " << value << std::endl;
                    return false;
                }
            }

            return false; // Should never reach here
        }

        QueryCondition QueryCondition::fromString(const std::string &conditionStr)
        {
            QueryCondition condition;

            // Find operator
            std::string opStr;
            size_t opStart = 0;
            size_t opEnd = 0;

            // Check for compound operators first
            if (conditionStr.find(">=") != std::string::npos)
            {
                opStart = conditionStr.find(">=");
                opEnd = opStart + 2;
                opStr = ">=";
                condition.op = GREATER_EQUAL;
            }
            else if (conditionStr.find("<=") != std::string::npos)
            {
                opStart = conditionStr.find("<=");
                opEnd = opStart + 2;
                opStr = "<=";
                condition.op = LESS_EQUAL;
            }
            else if (conditionStr.find("!=") != std::string::npos)
            {
                opStart = conditionStr.find("!=");
                opEnd = opStart + 2;
                opStr = "!=";
                condition.op = NOT_EQUALS;
            }
            else if (conditionStr.find("~=") != std::string::npos)
            {
                opStart = conditionStr.find("~=");
                opEnd = opStart + 2;
                opStr = "~=";
                condition.op = REGEX_MATCH;
            }
            else if (conditionStr.find("contains") != std::string::npos)
            {
                opStart = conditionStr.find("contains");
                opEnd = opStart + 8;
                opStr = "contains";
                condition.op = CONTAINS;
            }
            else if (conditionStr.find("startsWith") != std::string::npos)
            {
                opStart = conditionStr.find("startsWith");
                opEnd = opStart + 10;
                opStr = "startsWith";
                condition.op = STARTS_WITH;
            }
            else if (conditionStr.find("endsWith") != std::string::npos)
            {
                opStart = conditionStr.find("endsWith");
                opEnd = opStart + 8;
                opStr = "endsWith";
                condition.op = ENDS_WITH;
            }
            else if (conditionStr.find(">") != std::string::npos)
            {
                opStart = conditionStr.find(">");
                opEnd = opStart + 1;
                opStr = ">";
                condition.op = GREATER_THAN;
            }
            else if (conditionStr.find("<") != std::string::npos)
            {
                opStart = conditionStr.find("<");
                opEnd = opStart + 1;
                opStr = "<";
                condition.op = LESS_THAN;
            }
            else if (conditionStr.find("=") != std::string::npos)
            {
                opStart = conditionStr.find("=");
                opEnd = opStart + 1;
                opStr = "=";
                condition.op = EQUALS;
            }
            else
            {
                throw std::runtime_error("Invalid condition: no operator found");
            }

            // Extract key and value
            condition.key = conditionStr.substr(0, opStart);
            condition.value = conditionStr.substr(opEnd);

            // Trim whitespace
            condition.key.erase(0, condition.key.find_first_not_of(" \t"));
            condition.key.erase(condition.key.find_last_not_of(" \t") + 1);
            condition.value.erase(0, condition.value.find_first_not_of(" \t"));
            condition.value.erase(condition.value.find_last_not_of(" \t") + 1);

            // Check for wildcards
            condition.keyHasWildcard = containsWildcard(condition.key);
            condition.valueHasWildcard = containsWildcard(condition.value);

            return condition;
        }

        // MultiConditionQuery implementation

        MultiConditionQuery::MultiConditionQuery()
        {
            // Empty query
        }

        MultiConditionQuery::MultiConditionQuery(const QueryCondition &condition)
        {
            conditions.push_back(condition);
        }

        void MultiConditionQuery::addCondition(LogicalOperator op, const QueryCondition &condition)
        {
            if (!conditions.empty())
            {
                operators.push_back(op);
            }
            conditions.push_back(condition);
        }

        bool MultiConditionQuery::matches(const std::unordered_map<std::string, std::string> &objectMetadata) const
        {
            if (conditions.empty())
            {
                return true; // Empty query matches everything
            }

            bool result = false;

            // Check first condition
            for (const auto &entry : objectMetadata)
            {
                if (conditions[0].matches(entry.first, entry.second))
                {
                    result = true;
                    break;
                }
            }

            // Apply logical operators for remaining conditions
            for (size_t i = 0; i < operators.size(); i++)
            {
                bool conditionResult = false;

                // Check this condition
                for (const auto &entry : objectMetadata)
                {
                    if (conditions[i + 1].matches(entry.first, entry.second))
                    {
                        conditionResult = true;
                        break;
                    }
                }

                // Apply logical operator
                if (operators[i] == AND)
                {
                    result = result && conditionResult;
                }
                else
                { // OR
                    result = result || conditionResult;
                }

                // Short-circuit evaluation
                if ((operators[i] == AND && !result) ||
                    (operators[i] == OR && result))
                {
                    break;
                }
            }

            return result;
        }

        std::string MultiConditionQuery::toString() const
        {
            if (conditions.empty())
            {
                return "";
            }

            std::ostringstream oss;
            oss << conditions[0].toString();

            for (size_t i = 0; i < operators.size(); i++)
            {
                oss << " " << (operators[i] == AND ? "AND" : "OR") << " ";
                oss << conditions[i + 1].toString();
            }

            return oss.str();
        }

        MultiConditionQuery MultiConditionQuery::fromString(const std::string &queryStr)
        {
            MultiConditionQuery query;

            // Split by AND/OR
            std::string remaining = queryStr;
            size_t andPos, orPos;

            // Extract first condition
            andPos = remaining.find(" AND ");
            orPos = remaining.find(" OR ");

            if (andPos == std::string::npos && orPos == std::string::npos)
            {
                // Only one condition
                query.conditions.push_back(QueryCondition::fromString(remaining));
                return query;
            }

            // Extract first condition
            size_t opPos = std::min(andPos != std::string::npos ? andPos : std::string::npos,
                                    orPos != std::string::npos ? orPos : std::string::npos);

            std::string firstCondition = remaining.substr(0, opPos);
            query.conditions.push_back(QueryCondition::fromString(firstCondition));

            // Extract remaining conditions
            while (opPos != std::string::npos)
            {
                // Determine operator
                bool isAnd = (andPos != std::string::npos && andPos == opPos);
                LogicalOperator op = isAnd ? AND : OR;
                query.operators.push_back(op);

                // Update remaining string
                remaining = remaining.substr(opPos + (isAnd ? 5 : 4)); // Skip " AND " or " OR "

                // Find next operator
                andPos = remaining.find(" AND ");
                orPos = remaining.find(" OR ");

                if (andPos == std::string::npos && orPos == std::string::npos)
                {
                    // Last condition
                    query.conditions.push_back(QueryCondition::fromString(remaining));
                    break;
                }

                // Extract next condition
                opPos = std::min(andPos != std::string::npos ? andPos : std::string::npos,
                                 orPos != std::string::npos ? orPos : std::string::npos);

                std::string nextCondition = remaining.substr(0, opPos);
                query.conditions.push_back(QueryCondition::fromString(nextCondition));
            }

            return query;
        }

        const std::vector<QueryCondition> &MultiConditionQuery::getConditions() const
        {
            return conditions;
        }

        const std::vector<LogicalOperator> &MultiConditionQuery::getOperators() const
        {
            return operators;
        }

        // Utility functions implementation

        bool containsWildcard(const std::string &str)
        {
            return str.find('*') != std::string::npos || str.find('?') != std::string::npos;
        }

        std::pair<std::string, bool> parseKey(const std::string &key)
        {
            bool hasWildcard = containsWildcard(key);
            return {key, hasWildcard};
        }

        std::pair<std::string, bool> parseValue(const std::string &value)
        {
            bool hasWildcard = containsWildcard(value);
            return {value, hasWildcard};
        }

        double parseNumeric(const std::string &str)
        {
            try
            {
                return std::stod(str);
            }
            catch (const std::exception &e)
            {
                return 0.0;
            }
        }

        bool isNumeric(const std::string &str)
        {
            if (str.empty())
            {
                return false;
            }

            char *end = nullptr;
            std::strtod(str.c_str(), &end);

            return end != str.c_str() && *end == '\0' && str[0] != ' ';
        }

        std::string normalizeWildcardPattern(const std::string &pattern)
        {
            // Convert wildcard pattern to regex pattern
            std::string regexPattern = pattern;

            // Escape special regex characters
            std::string specialChars = ".+()[]{}\\|^$";
            for (char c : specialChars)
            {
                size_t pos = 0;
                while ((pos = regexPattern.find(c, pos)) != std::string::npos)
                {
                    regexPattern.insert(pos, "\\");
                    pos += 2;
                }
            }

            // Convert wildcard characters to regex
            size_t pos = 0;
            while ((pos = regexPattern.find('*', pos)) != std::string::npos)
            {
                regexPattern.replace(pos, 1, ".*");
                pos += 2;
            }

            pos = 0;
            while ((pos = regexPattern.find('?', pos)) != std::string::npos)
            {
                regexPattern.replace(pos, 1, ".");
                pos += 1;
            }

            return "^" + regexPattern + "$";
        }

        bool matchWildcard(const std::string &str, const std::string &pattern)
        {
            if (pattern == "*")
            {
                return true; // Match everything
            }

            // Convert wildcard pattern to regex
            std::string regexPattern = normalizeWildcardPattern(pattern);

            try
            {
                std::regex re(regexPattern);
                return std::regex_match(str, re);
            }
            catch (const std::regex_error &e)
            {
                std::cerr << "Invalid regex pattern: " << regexPattern << std::endl;
                return false;
            }
        }

    } // namespace query
} // namespace idioms