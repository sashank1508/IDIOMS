#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <mpi.h>
#include "mpi/MPIServer.hpp"
#include "mpi/MPIClient.hpp"

// Global metadata tracking (for display purposes only)
std::unordered_map<int, std::vector<std::pair<std::string, std::string>>> objectMetadata;

// Add metadata to the tracking structure
void track_metadata(int objectId, const std::string &key, const std::string &value)
{
    objectMetadata[objectId].push_back(std::make_pair(key, value));
}

// Enhanced print function that shows matching metadata
void print_detailed_results(const std::vector<int> &objectIds, const std::string &query = "")
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

// Function to run as a server process
void runAsServer(int rank, int worldSize)
{
    // Create a data directory
    std::string dataDir = "./idioms_data";
    std::string cmd = "mkdir -p " + dataDir;
    system(cmd.c_str());

    // Create and run the server
    idioms::mpi::MPIServer server(rank, worldSize, dataDir, true); // true = use suffix tree mode
    server.run();
}

// Function to run as the client process
void runAsClient(int rank, int worldSize)
{
    std::cout << "\n=== IDIOMS MPI Client Starting ===" << std::endl;

    // Create the MPI client
    idioms::mpi::MPIClient client(rank, worldSize, true); // true = use suffix tree mode

    std::cout << "\n=== Initializing IDIOMS with example metadata ===" << std::endl;

    // Microscopy data examples from the paper
    std::cout << "Creating metadata records..." << std::endl;

    client.create_md_index("FILE_PATH", "/data/488nm.tif", 1001);
    track_metadata(1001, "FILE_PATH", "/data/488nm.tif");

    client.create_md_index("FILE_PATH", "/data/561nm.tif", 1002);
    track_metadata(1002, "FILE_PATH", "/data/561nm.tif");

    client.create_md_index("StageX", "100.00", 1001);
    track_metadata(1001, "StageX", "100.00");

    client.create_md_index("StageY", "200.00", 1001);
    track_metadata(1001, "StageY", "200.00");

    client.create_md_index("StageZ", "50.00", 1001);
    track_metadata(1001, "StageZ", "50.00");

    client.create_md_index("StageX", "300.00", 1002);
    track_metadata(1002, "StageX", "300.00");

    client.create_md_index("StageY", "400.00", 1002);
    track_metadata(1002, "StageY", "400.00");

    client.create_md_index("StageZ", "75.00", 1002);
    track_metadata(1002, "StageZ", "75.00");

    client.create_md_index("creation_date", "2023-05-26", 1001);
    track_metadata(1001, "creation_date", "2023-05-26");

    client.create_md_index("creation_date", "2023-06-15", 1002);
    track_metadata(1002, "creation_date", "2023-06-15");

    client.create_md_index("microscope", "LLSM-1", 1001);
    track_metadata(1001, "microscope", "LLSM-1");

    client.create_md_index("microscope", "LLSM-2", 1002);
    track_metadata(1002, "microscope", "LLSM-2");

    client.create_md_index("AUXILIARY_FILE", "/data/488nm_metadata.json", 1001);
    track_metadata(1001, "AUXILIARY_FILE", "/data/488nm_metadata.json");

    client.create_md_index("AUXILIARY_FILE", "/data/561nm_metadata.json", 1002);
    track_metadata(1002, "AUXILIARY_FILE", "/data/561nm_metadata.json");

    // Checkpoint the indices
    std::cout << "\n=== Checkpointing Indices ===" << std::endl;
    client.checkpointAllIndices();

    // Perform some queries
    std::cout << "\n=== Performing Queries ===" << std::endl;

    // 1. Exact Query
    std::cout << "\n1. Exact Query: \"StageX=300.00\"" << std::endl;
    print_detailed_results(client.md_search("StageX=300.00"), "StageX=300.00");

    // 2. Prefix Query
    std::cout << "\n2. Prefix Query: \"Stage*=*\"" << std::endl;
    print_detailed_results(client.md_search("Stage*=*"), "Stage*=*");

    // 3. Suffix Query
    std::cout << "\n3. Suffix Query: \"*PATH=*tif\"" << std::endl;
    print_detailed_results(client.md_search("*PATH=*tif"), "*PATH=*tif");

    // 4. Infix Query
    std::cout << "\n4. Infix Query: \"*FILE*=*metadata*\"" << std::endl;
    print_detailed_results(client.md_search("*FILE*=*metadata*"), "*FILE*=*metadata*");

    // 5. Combined Query Types
    std::cout << "\n5. Combined Query Types: \"Stage*=*00\"" << std::endl;
    print_detailed_results(client.md_search("Stage*=*00"), "Stage*=*00");

    // 6. Wildcard Query
    std::cout << "\n6. Wildcard Query: \"*=*488*\"" << std::endl;
    print_detailed_results(client.md_search("*=*488*"), "*=*488*");

    // 7. Test Delete Functionality
    std::cout << "\n7. Testing Delete Functionality" << std::endl;
    std::cout << "Adding a new record..." << std::endl;
    client.create_md_index("test_key", "test_value", 1003);
    track_metadata(1003, "test_key", "test_value");

    std::cout << "Querying for the new record..." << std::endl;
    print_detailed_results(client.md_search("test_key=test_value"), "test_key=test_value");

    std::cout << "Deleting the record..." << std::endl;
    client.delete_md_index("test_key", "test_value", 1003);

    std::cout << "Querying again after deletion..." << std::endl;
    print_detailed_results(client.md_search("test_key=test_value"), "test_key=test_value");

    // 8. Test Recovery
    std::cout << "\n8. Testing Recovery" << std::endl;
    client.recoverAllIndices();

    std::cout << "Querying after recovery..." << std::endl;
    print_detailed_results(client.md_search("Stage*=*"), "Stage*=*");

    // Shutdown all servers
    std::cout << "\n=== Shutting Down Servers ===" << std::endl;
    client.shutdownAllServers();

    std::cout << "MPI Client finished." << std::endl;
}

int main(int argc, char **argv)
{
    // Initialize MPI
    MPI_Init(&argc, &argv);

    // Get rank and world size
    int rank, worldSize;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);

    if (worldSize < 2)
    {
        std::cerr << "Error: This program requires at least 2 MPI processes." << std::endl;
        std::cerr << "Please run with: mpirun -np N ./idioms_mpi (where N >= 2)" << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
        return 1;
    }

    // Process with rank 0 is the client, others are servers
    if (rank == 0)
    {
        runAsClient(rank, worldSize);
    }
    else
    {
        runAsServer(rank, worldSize);
    }

    // Finalize MPI
    MPI_Finalize();

    return 0;
}