#include "RangeQuery.hpp"
#include <iostream>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <ctime>
#include <regex>

namespace idioms
{
    namespace query
    {

        RangeQuery::RangeQuery(const std::string &key, double min, double max)
            : key(key), minValue(min), maxValue(max), isDateRange(false), dateFormat("")
        {

            keyHasWildcard = containsWildcard(key);
        }

        RangeQuery::RangeQuery(const std::string &key, const std::string &minDate,
                               const std::string &maxDate, const std::string &format)
            : key(key), dateFormat(format), isDateRange(true)
        {

            keyHasWildcard = containsWildcard(key);

            // Convert dates to numeric values
            minValue = dateToNumeric(minDate);
            maxValue = dateToNumeric(maxDate);
        }

        bool RangeQuery::isInRange(const std::string &metadataKey, const std::string &metadataValue) const
        {
            // Check key match (with wildcards if needed)
            if (keyHasWildcard)
            {
                if (!matchWildcard(metadataKey, key))
                {
                    return false;
                }
            }
            else
            {
                if (metadataKey != key)
                {
                    return false;
                }
            }

            // Convert value to numeric
            double numericValue;

            if (isDateRange)
            {
                // Check if value is a valid date
                if (!isValidDateFormat(metadataValue))
                {
                    return false;
                }

                numericValue = dateToNumeric(metadataValue);
            }
            else
            {
                // Check if value is numeric
                if (!isNumeric(metadataValue))
                {
                    return false;
                }

                numericValue = parseNumeric(metadataValue);
            }

            // Check if value is in range
            return numericValue >= minValue && numericValue <= maxValue;
        }

        MultiConditionQuery RangeQuery::toMultiConditionQuery() const
        {
            // Create condition for min value
            QueryCondition minCondition;
            minCondition.key = key;
            minCondition.keyHasWildcard = keyHasWildcard;
            minCondition.op = GREATER_EQUAL;

            if (isDateRange)
            {
                minCondition.value = numericToDate(minValue);
            }
            else
            {
                std::ostringstream oss;
                oss << minValue;
                minCondition.value = oss.str();
            }

            // Create condition for max value
            QueryCondition maxCondition;
            maxCondition.key = key;
            maxCondition.keyHasWildcard = keyHasWildcard;
            maxCondition.op = LESS_EQUAL;

            if (isDateRange)
            {
                maxCondition.value = numericToDate(maxValue);
            }
            else
            {
                std::ostringstream oss;
                oss << maxValue;
                maxCondition.value = oss.str();
            }

            // Create query with both conditions
            MultiConditionQuery query(minCondition);
            query.addCondition(AND, maxCondition);

            return query;
        }

        std::string RangeQuery::toString() const
        {
            std::ostringstream oss;

            oss << key << " in range [";

            if (isDateRange)
            {
                oss << numericToDate(minValue) << " to " << numericToDate(maxValue);
            }
            else
            {
                oss << minValue << " to " << maxValue;
            }

            oss << "]";

            return oss.str();
        }

        RangeQuery RangeQuery::fromString(const std::string &queryStr)
        {
            // Parse format: "key in range [min to max]"
            std::regex rangePattern(R"((.+)\s+in\s+range\s+\[(.+)\s+to\s+(.+)\])");
            std::smatch matches;

            if (std::regex_match(queryStr, matches, rangePattern) && matches.size() == 4)
            {
                std::string key = matches[1].str();
                std::string minStr = matches[2].str();
                std::string maxStr = matches[3].str();

                // Trim whitespace
                key.erase(0, key.find_first_not_of(" \t"));
                key.erase(key.find_last_not_of(" \t") + 1);
                minStr.erase(0, minStr.find_first_not_of(" \t"));
                minStr.erase(minStr.find_last_not_of(" \t") + 1);
                maxStr.erase(0, maxStr.find_last_not_of("] \t") + 1);
                maxStr.erase(maxStr.find_last_not_of("] \t") + 1);

                // Check if this is a date range
                bool isDate = minStr.find('-') != std::string::npos ||
                              minStr.find('/') != std::string::npos;

                if (isDate)
                {
                    return RangeQuery(key, minStr, maxStr);
                }
                else
                {
                    double min = parseNumeric(minStr);
                    double max = parseNumeric(maxStr);
                    return RangeQuery(key, min, max);
                }
            }

            throw std::runtime_error("Invalid range query format: " + queryStr);
        }

        const std::string &RangeQuery::getKey() const
        {
            return key;
        }

        double RangeQuery::getMinValue() const
        {
            return minValue;
        }

        double RangeQuery::getMaxValue() const
        {
            return maxValue;
        }

        bool RangeQuery::isDateRangeQuery() const
        {
            return isDateRange;
        }

        const std::string &RangeQuery::getDateFormat() const
        {
            return dateFormat;
        }

        double RangeQuery::dateToNumeric(const std::string &dateStr) const
        {
            // Parse date string to numeric value
            // For simplicity, we'll convert to days since epoch

            struct tm tm = {};

            // Parse date based on format
            if (dateFormat == "YYYY-MM-DD" || dateFormat.empty())
            {
                // Default format: YYYY-MM-DD
                if (dateStr.length() != 10 || dateStr[4] != '-' || dateStr[7] != '-')
                {
                    throw std::runtime_error("Invalid date format: " + dateStr);
                }

                tm.tm_year = std::stoi(dateStr.substr(0, 4)) - 1900;
                tm.tm_mon = std::stoi(dateStr.substr(5, 2)) - 1;
                tm.tm_mday = std::stoi(dateStr.substr(8, 2));
            }
            else if (dateFormat == "MM/DD/YYYY")
            {
                if (dateStr.length() != 10 || dateStr[2] != '/' || dateStr[5] != '/')
                {
                    throw std::runtime_error("Invalid date format: " + dateStr);
                }

                tm.tm_mon = std::stoi(dateStr.substr(0, 2)) - 1;
                tm.tm_mday = std::stoi(dateStr.substr(3, 2));
                tm.tm_year = std::stoi(dateStr.substr(6, 4)) - 1900;
            }
            else if (dateFormat == "DD-MM-YYYY")
            {
                if (dateStr.length() != 10 || dateStr[2] != '-' || dateStr[5] != '-')
                {
                    throw std::runtime_error("Invalid date format: " + dateStr);
                }

                tm.tm_mday = std::stoi(dateStr.substr(0, 2));
                tm.tm_mon = std::stoi(dateStr.substr(3, 2)) - 1;
                tm.tm_year = std::stoi(dateStr.substr(6, 4)) - 1900;
            }
            else
            {
                throw std::runtime_error("Unsupported date format: " + dateFormat);
            }

            // Convert to time_t
            std::time_t time = std::mktime(&tm);

            // Convert to days since epoch
            return static_cast<double>(time) / (60 * 60 * 24);
        }

        std::string RangeQuery::numericToDate(double value) const
        {
            // Convert numeric value (days since epoch) to date string
            std::time_t time = static_cast<std::time_t>(value * 60 * 60 * 24);
            std::tm *tm = std::localtime(&time);

            std::ostringstream oss;

            // Format based on dateFormat
            if (dateFormat == "YYYY-MM-DD" || dateFormat.empty())
            {
                oss << std::setfill('0') << std::setw(4) << (tm->tm_year + 1900) << "-"
                    << std::setw(2) << (tm->tm_mon + 1) << "-"
                    << std::setw(2) << tm->tm_mday;
            }
            else if (dateFormat == "MM/DD/YYYY")
            {
                oss << std::setfill('0') << std::setw(2) << (tm->tm_mon + 1) << "/"
                    << std::setw(2) << tm->tm_mday << "/"
                    << std::setw(4) << (tm->tm_year + 1900);
            }
            else if (dateFormat == "DD-MM-YYYY")
            {
                oss << std::setfill('0') << std::setw(2) << tm->tm_mday << "-"
                    << std::setw(2) << (tm->tm_mon + 1) << "-"
                    << std::setw(4) << (tm->tm_year + 1900);
            }
            else
            {
                throw std::runtime_error("Unsupported date format: " + dateFormat);
            }

            return oss.str();
        }

        bool RangeQuery::isValidDateFormat(const std::string &dateStr) const
        {
            // Check if date string matches the expected format

            if (dateFormat == "YYYY-MM-DD" || dateFormat.empty())
            {
                std::regex dateRegex(R"(\d{4}-\d{2}-\d{2})");
                return std::regex_match(dateStr, dateRegex);
            }
            else if (dateFormat == "MM/DD/YYYY")
            {
                std::regex dateRegex(R"(\d{2}/\d{2}/\d{4})");
                return std::regex_match(dateStr, dateRegex);
            }
            else if (dateFormat == "DD-MM-YYYY")
            {
                std::regex dateRegex(R"(\d{2}-\d{2}-\d{4})");
                return std::regex_match(dateStr, dateRegex);
            }
            else
            {
                return false;
            }
        }

    } // namespace query
} // namespace idioms