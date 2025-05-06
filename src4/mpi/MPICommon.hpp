#ifndef IDIOMS_MPI_COMMON_HPP
#define IDIOMS_MPI_COMMON_HPP

#include <string>
#include <vector>
#include <mpi.h>

namespace idioms
{
    namespace mpi
    {

        // Message types for MPI communication
        enum MessageType
        {
            // Index operations
            CREATE_INDEX = 1,
            DELETE_INDEX = 2,

            // Query operations
            QUERY = 3,

            // Administrative operations
            CHECKPOINT = 4,
            RECOVER = 5,
            SHUTDOWN = 6,

            // Response messages
            RESPONSE = 7,
            ERROR_RESPONSE = 8,

            // Fault tolerance messages
            HEARTBEAT = 9,
            SERVER_FAILURE = 10,
            RECOVERY_REQUEST = 11,
            RECOVERY_COMPLETE = 12
        };

        // MPI tags
        const int ADMIN_TAG = 1;  // For administrative messages
        const int INDEX_TAG = 2;  // For index operations
        const int QUERY_TAG = 3;  // For query operations
        const int RESULT_TAG = 4; // For query results
        const int FAULT_TAG = 5;  // For fault tolerance messages

        // Serialization utilities

        // Serialize a string
        inline std::vector<char> serializeString(const std::string &str)
        {
            size_t size = str.size();
            std::vector<char> buffer(sizeof(size_t) + size);

            // Copy the size
            memcpy(buffer.data(), &size, sizeof(size_t));

            // Copy the string content
            memcpy(buffer.data() + sizeof(size_t), str.c_str(), size);

            return buffer;
        }

        // Deserialize a string
        inline std::string deserializeString(const std::vector<char> &buffer, size_t &offset)
        {
            // Read the size
            size_t size;
            memcpy(&size, buffer.data() + offset, sizeof(size_t));
            offset += sizeof(size_t);

            // Read the string content
            std::string str(buffer.data() + offset, buffer.data() + offset + size);
            offset += size;

            return str;
        }

        // Serialize a vector of integers
        inline std::vector<char> serializeIntVector(const std::vector<int> &vec)
        {
            size_t size = vec.size();
            std::vector<char> buffer(sizeof(size_t) + size * sizeof(int));

            // Copy the size
            memcpy(buffer.data(), &size, sizeof(size_t));

            // Copy the vector content
            if (size > 0)
            {
                memcpy(buffer.data() + sizeof(size_t), vec.data(), size * sizeof(int));
            }

            return buffer;
        }

        // Deserialize a vector of integers
        inline std::vector<int> deserializeIntVector(const std::vector<char> &buffer, size_t &offset)
        {
            // Read the size
            size_t size;
            memcpy(&size, buffer.data() + offset, sizeof(size_t));
            offset += sizeof(size_t);

            // Read the vector content
            std::vector<int> vec(size);
            if (size > 0)
            {
                memcpy(vec.data(), buffer.data() + offset, size * sizeof(int));
                offset += size * sizeof(int);
            }

            return vec;
        }

        // MPI message classes

        // Base message class
        struct Message
        {
            MessageType type;

            Message(MessageType t) : type(t) {}
            virtual ~Message() {}

            virtual std::vector<char> serialize() const
            {
                std::vector<char> buffer(sizeof(MessageType));
                memcpy(buffer.data(), &type, sizeof(MessageType));
                return buffer;
            }

            static MessageType getType(const std::vector<char> &buffer)
            {
                MessageType type;
                memcpy(&type, buffer.data(), sizeof(MessageType));
                return type;
            }
        };

        // Create index message
        struct CreateIndexMessage : public Message
        {
            std::string key;
            std::string value;
            int objectId;

            CreateIndexMessage() : Message(CREATE_INDEX) {}
            CreateIndexMessage(const std::string &k, const std::string &v, int id)
                : Message(CREATE_INDEX), key(k), value(v), objectId(id) {}

            std::vector<char> serialize() const override
            {
                auto baseBuffer = Message::serialize();
                auto keyBuffer = serializeString(key);
                auto valueBuffer = serializeString(value);

                std::vector<char> buffer(baseBuffer.size() + keyBuffer.size() + valueBuffer.size() + sizeof(int));

                size_t offset = 0;

                // Copy the base message
                memcpy(buffer.data() + offset, baseBuffer.data(), baseBuffer.size());
                offset += baseBuffer.size();

                // Copy the key
                memcpy(buffer.data() + offset, keyBuffer.data(), keyBuffer.size());
                offset += keyBuffer.size();

                // Copy the value
                memcpy(buffer.data() + offset, valueBuffer.data(), valueBuffer.size());
                offset += valueBuffer.size();

                // Copy the object ID
                memcpy(buffer.data() + offset, &objectId, sizeof(int));

                return buffer;
            }

            static CreateIndexMessage deserialize(const std::vector<char> &buffer)
            {
                CreateIndexMessage msg;

                size_t offset = sizeof(MessageType);

                // Read the key
                msg.key = deserializeString(buffer, offset);

                // Read the value
                msg.value = deserializeString(buffer, offset);

                // Read the object ID
                memcpy(&msg.objectId, buffer.data() + offset, sizeof(int));

                return msg;
            }
        };

        // Delete index message
        struct DeleteIndexMessage : public Message
        {
            std::string key;
            std::string value;
            int objectId;

            DeleteIndexMessage() : Message(DELETE_INDEX) {}
            DeleteIndexMessage(const std::string &k, const std::string &v, int id)
                : Message(DELETE_INDEX), key(k), value(v), objectId(id) {}

            std::vector<char> serialize() const override
            {
                auto baseBuffer = Message::serialize();
                auto keyBuffer = serializeString(key);
                auto valueBuffer = serializeString(value);

                std::vector<char> buffer(baseBuffer.size() + keyBuffer.size() + valueBuffer.size() + sizeof(int));

                size_t offset = 0;

                // Copy the base message
                memcpy(buffer.data() + offset, baseBuffer.data(), baseBuffer.size());
                offset += baseBuffer.size();

                // Copy the key
                memcpy(buffer.data() + offset, keyBuffer.data(), keyBuffer.size());
                offset += keyBuffer.size();

                // Copy the value
                memcpy(buffer.data() + offset, valueBuffer.data(), valueBuffer.size());
                offset += valueBuffer.size();

                // Copy the object ID
                memcpy(buffer.data() + offset, &objectId, sizeof(int));

                return buffer;
            }

            static DeleteIndexMessage deserialize(const std::vector<char> &buffer)
            {
                DeleteIndexMessage msg;

                size_t offset = sizeof(MessageType);

                // Read the key
                msg.key = deserializeString(buffer, offset);

                // Read the value
                msg.value = deserializeString(buffer, offset);

                // Read the object ID
                memcpy(&msg.objectId, buffer.data() + offset, sizeof(int));

                return msg;
            }
        };

        // Query message
        struct QueryMessage : public Message
        {
            std::string queryStr;

            QueryMessage() : Message(QUERY) {}
            QueryMessage(const std::string &q) : Message(QUERY), queryStr(q) {}

            std::vector<char> serialize() const override
            {
                auto baseBuffer = Message::serialize();
                auto queryBuffer = serializeString(queryStr);

                std::vector<char> buffer(baseBuffer.size() + queryBuffer.size());

                size_t offset = 0;

                // Copy the base message
                memcpy(buffer.data() + offset, baseBuffer.data(), baseBuffer.size());
                offset += baseBuffer.size();

                // Copy the query string
                memcpy(buffer.data() + offset, queryBuffer.data(), queryBuffer.size());

                return buffer;
            }

            static QueryMessage deserialize(const std::vector<char> &buffer)
            {
                QueryMessage msg;

                size_t offset = sizeof(MessageType);

                // Read the query string
                msg.queryStr = deserializeString(buffer, offset);

                return msg;
            }
        };

        // Administrative message
        struct AdminMessage : public Message
        {
            // No additional data for now

            AdminMessage(MessageType t) : Message(t) {}

            std::vector<char> serialize() const override
            {
                return Message::serialize();
            }

            static AdminMessage deserialize(const std::vector<char> &buffer)
            {
                MessageType type = getType(buffer);
                return AdminMessage(type);
            }
        };

        // Response message
        struct ResponseMessage : public Message
        {
            std::vector<int> results;
            bool success;

            ResponseMessage() : Message(RESPONSE), success(true) {}
            ResponseMessage(const std::vector<int> &r) : Message(RESPONSE), results(r), success(true) {}

            std::vector<char> serialize() const override
            {
                auto baseBuffer = Message::serialize();
                auto resultsBuffer = serializeIntVector(results);

                std::vector<char> buffer(baseBuffer.size() + resultsBuffer.size() + sizeof(bool));

                size_t offset = 0;

                // Copy the base message
                memcpy(buffer.data() + offset, baseBuffer.data(), baseBuffer.size());
                offset += baseBuffer.size();

                // Copy the success flag
                memcpy(buffer.data() + offset, &success, sizeof(bool));
                offset += sizeof(bool);

                // Copy the results
                memcpy(buffer.data() + offset, resultsBuffer.data(), resultsBuffer.size());

                return buffer;
            }

            static ResponseMessage deserialize(const std::vector<char> &buffer)
            {
                ResponseMessage msg;

                size_t offset = sizeof(MessageType);

                // Read the success flag
                memcpy(&msg.success, buffer.data() + offset, sizeof(bool));
                offset += sizeof(bool);

                // Read the results
                msg.results = deserializeIntVector(buffer, offset);

                return msg;
            }
        };

        // Error response message
        struct ErrorResponseMessage : public Message
        {
            std::string errorMessage;

            ErrorResponseMessage() : Message(ERROR_RESPONSE) {}
            ErrorResponseMessage(const std::string &error) : Message(ERROR_RESPONSE), errorMessage(error) {}

            std::vector<char> serialize() const override
            {
                auto baseBuffer = Message::serialize();
                auto errorBuffer = serializeString(errorMessage);

                std::vector<char> buffer(baseBuffer.size() + errorBuffer.size());

                size_t offset = 0;

                // Copy the base message
                memcpy(buffer.data() + offset, baseBuffer.data(), baseBuffer.size());
                offset += baseBuffer.size();

                // Copy the error message
                memcpy(buffer.data() + offset, errorBuffer.data(), errorBuffer.size());

                return buffer;
            }

            static ErrorResponseMessage deserialize(const std::vector<char> &buffer)
            {
                ErrorResponseMessage msg;

                size_t offset = sizeof(MessageType);

                // Read the error message
                msg.errorMessage = deserializeString(buffer, offset);

                return msg;
            }
        };

        // Heartbeat message
        struct HeartbeatMessage : public Message
        {
            int serverId;
            int64_t timestamp;

            HeartbeatMessage() : Message(HEARTBEAT), serverId(0), timestamp(0) {}
            HeartbeatMessage(int id, int64_t ts) : Message(HEARTBEAT), serverId(id), timestamp(ts) {}

            std::vector<char> serialize() const override
            {
                auto baseBuffer = Message::serialize();
                std::vector<char> buffer(baseBuffer.size() + sizeof(int) + sizeof(int64_t));

                size_t offset = 0;

                // Copy the base message
                memcpy(buffer.data() + offset, baseBuffer.data(), baseBuffer.size());
                offset += baseBuffer.size();

                // Copy the server ID
                memcpy(buffer.data() + offset, &serverId, sizeof(int));
                offset += sizeof(int);

                // Copy the timestamp
                memcpy(buffer.data() + offset, &timestamp, sizeof(int64_t));

                return buffer;
            }

            static HeartbeatMessage deserialize(const std::vector<char> &buffer)
            {
                HeartbeatMessage msg;

                size_t offset = sizeof(MessageType);

                // Read the server ID
                memcpy(&msg.serverId, buffer.data() + offset, sizeof(int));
                offset += sizeof(int);

                // Read the timestamp
                memcpy(&msg.timestamp, buffer.data() + offset, sizeof(int64_t));

                return msg;
            }
        };

        // Server failure message
        struct ServerFailureMessage : public Message
        {
            int failedServerId;

            ServerFailureMessage() : Message(SERVER_FAILURE), failedServerId(0) {}
            ServerFailureMessage(int id) : Message(SERVER_FAILURE), failedServerId(id) {}

            std::vector<char> serialize() const override
            {
                auto baseBuffer = Message::serialize();
                std::vector<char> buffer(baseBuffer.size() + sizeof(int));

                size_t offset = 0;

                // Copy the base message
                memcpy(buffer.data() + offset, baseBuffer.data(), baseBuffer.size());
                offset += baseBuffer.size();

                // Copy the failed server ID
                memcpy(buffer.data() + offset, &failedServerId, sizeof(int));

                return buffer;
            }

            static ServerFailureMessage deserialize(const std::vector<char> &buffer)
            {
                ServerFailureMessage msg;

                size_t offset = sizeof(MessageType);

                // Read the failed server ID
                memcpy(&msg.failedServerId, buffer.data() + offset, sizeof(int));

                return msg;
            }
        };

        // Recovery request message
        struct RecoveryRequestMessage : public Message
        {
            int failedServerId;
            int coordinatorId;

            RecoveryRequestMessage() : Message(RECOVERY_REQUEST), failedServerId(0), coordinatorId(0) {}
            RecoveryRequestMessage(int failed, int coordinator)
                : Message(RECOVERY_REQUEST), failedServerId(failed), coordinatorId(coordinator) {}

            std::vector<char> serialize() const override
            {
                auto baseBuffer = Message::serialize();
                std::vector<char> buffer(baseBuffer.size() + 2 * sizeof(int));

                size_t offset = 0;

                // Copy the base message
                memcpy(buffer.data() + offset, baseBuffer.data(), baseBuffer.size());
                offset += baseBuffer.size();

                // Copy the failed server ID
                memcpy(buffer.data() + offset, &failedServerId, sizeof(int));
                offset += sizeof(int);

                // Copy the coordinator ID
                memcpy(buffer.data() + offset, &coordinatorId, sizeof(int));

                return buffer;
            }

            static RecoveryRequestMessage deserialize(const std::vector<char> &buffer)
            {
                RecoveryRequestMessage msg;

                size_t offset = sizeof(MessageType);

                // Read the failed server ID
                memcpy(&msg.failedServerId, buffer.data() + offset, sizeof(int));
                offset += sizeof(int);

                // Read the coordinator ID
                memcpy(&msg.coordinatorId, buffer.data() + offset, sizeof(int));

                return msg;
            }
        };

        // Recovery complete message
        struct RecoveryCompleteMessage : public Message
        {
            int failedServerId;
            bool success;

            RecoveryCompleteMessage() : Message(RECOVERY_COMPLETE), failedServerId(0), success(false) {}
            RecoveryCompleteMessage(int id, bool succ)
                : Message(RECOVERY_COMPLETE), failedServerId(id), success(succ) {}

            std::vector<char> serialize() const override
            {
                auto baseBuffer = Message::serialize();
                std::vector<char> buffer(baseBuffer.size() + sizeof(int) + sizeof(bool));

                size_t offset = 0;

                // Copy the base message
                memcpy(buffer.data() + offset, baseBuffer.data(), baseBuffer.size());
                offset += baseBuffer.size();

                // Copy the failed server ID
                memcpy(buffer.data() + offset, &failedServerId, sizeof(int));
                offset += sizeof(int);

                // Copy the success flag
                memcpy(buffer.data() + offset, &success, sizeof(bool));

                return buffer;
            }

            static RecoveryCompleteMessage deserialize(const std::vector<char> &buffer)
            {
                RecoveryCompleteMessage msg;

                size_t offset = sizeof(MessageType);

                // Read the failed server ID
                memcpy(&msg.failedServerId, buffer.data() + offset, sizeof(int));
                offset += sizeof(int);

                // Read the success flag
                memcpy(&msg.success, buffer.data() + offset, sizeof(bool));

                return msg;
            }
        };

    } // namespace mpi
} // namespace idioms

#endif // IDIOMS_MPI_COMMON_HPP