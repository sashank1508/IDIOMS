#include <iostream>
#include <vector>
#include <string>
#include <thread>
#include <chrono>
#include <mutex>
#include <mpi.h>
#include "client/ClientManager.hpp"
#include "mpi/MPIServer.hpp"

// Mutex for synchronized console output
std::mutex consoleMutex;

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

// Function to simulate a client making queries
void simulateClient(int clientId, std::shared_ptr<idioms::mpi::MPIClient> client)
{
    // Sample metadata to add
    struct MetadataItem
    {
        std::string key;
        std::string value;
        int objectId;
    };

    std::vector<MetadataItem> metadataItems = {
        {"FILE_PATH", "/data/client" + std::to_string(clientId) + "/image1.tif", 2000 + clientId * 100 + 1},
        {"FILE_PATH", "/data/client" + std::to_string(clientId) + "/image2.tif", 2000 + clientId * 100 + 2},
        {"StageX", std::to_string(100.0 + clientId * 10.0), 2000 + clientId * 100 + 1},
        {"StageY", std::to_string(200.0 + clientId * 10.0), 2000 + clientId * 100 + 1},
        {"StageZ", std::to_string(50.0 + clientId * 5.0), 2000 + clientId * 100 + 1},
        {"StageX", std::to_string(300.0 + clientId * 10.0), 2000 + clientId * 100 + 2},
        {"StageY", std::to_string(400.0 + clientId * 10.0), 2000 + clientId * 100 + 2},
        {"StageZ", std::to_string(75.0 + clientId * 5.0), 2000 + clientId * 100 + 2},
        {"creation_date", "2023-01-" + std::to_string(1 + clientId), 2000 + clientId * 100 + 1},
        {"creation_date", "2023-01-" + std::to_string(15 + clientId), 2000 + clientId * 100 + 2},
        {"client_id", std::to_string(clientId), 2000 + clientId * 100 + 1},
        {"client_id", std::to_string(clientId), 2000 + clientId * 100 + 2}};

    // Add metadata
    {
        std::lock_guard<std::mutex> lock(consoleMutex);
        std::cout << "Client " << clientId << ": Adding metadata..." << std::endl;
    }

    for (const auto &item : metadataItems)
    {
        client->create_md_index(item.key, item.value, item.objectId);

        // Small delay to avoid flooding the server
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Sample queries to execute
    std::vector<std::string> queries = {
        "StageX=" + std::to_string(300.0 + clientId * 10.0), // Exact query for this client's data
        "Stage*=*",                                          // Prefix query for all stage data
        "*PATH=*tif",                                        // Suffix query for all TIF files
        "*client_id=" + std::to_string(clientId),            // Query for this client's data
        "creation_date=2023-01-*"                            // Date prefix query
    };

    // Execute queries
    {
        std::lock_guard<std::mutex> lock(consoleMutex);
        std::cout << "Client " << clientId << ": Executing queries..." << std::endl;
    }

    for (const auto &query : queries)
    {
        // Execute query
        std::vector<int> results = client->md_search(query);

        // Print results
        {
            std::lock_guard<std::mutex> lock(consoleMutex);
            std::cout << "Client " << clientId << " - Query: \"" << query << "\" - Found "
                      << results.size() << " results: ";

            for (size_t i = 0; i < results.size(); i++)
            {
                if (i > 0)
                    std::cout << ", ";
                std::cout << results[i];
            }
            std::cout << std::endl;
        }

        // Small delay between queries
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // Checkpoint the index
    {
        std::lock_guard<std::mutex> lock(consoleMutex);
        std::cout << "Client " << clientId << ": Checkpointing index..." << std::endl;
    }

    client->checkpointAllIndices();

    {
        std::lock_guard<std::mutex> lock(consoleMutex);
        std::cout << "Client " << clientId << ": Finished simulation" << std::endl;
    }
}

// Function to run as the client manager process
void runAsClientManager(int rank, int worldSize)
{
    std::cout << "=== IDIOMS Multi-Client Demo ===" << std::endl;

    // Create the client manager
    std::string dataDir = "./idioms_data";
    idioms::client::ClientManager clientManager(worldSize, dataDir, true);

    // Start the client manager
    clientManager.start();

    // Register multiple clients
    const int NUM_CLIENTS = 5;
    std::vector<int> clientIds;
    std::vector<std::thread> clientThreads;

    std::cout << "Registering " << NUM_CLIENTS << " clients..." << std::endl;

    for (int i = 0; i < NUM_CLIENTS; i++)
    {
        int clientId = clientManager.registerClient();
        clientIds.push_back(clientId);

        auto client = clientManager.getClient(clientId);

        // Start a thread to simulate client activity
        clientThreads.emplace_back(simulateClient, clientId, client);

        // Small delay between client registrations
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    // Wait for all client threads to finish
    for (auto &thread : clientThreads)
    {
        thread.join();
    }

    // Unregister clients
    std::cout << "Unregistering clients..." << std::endl;

    for (int clientId : clientIds)
    {
        clientManager.unregisterClient(clientId);
    }

    // Stop the client manager
    clientManager.stop();

    // Shutdown servers
    std::cout << "Shutting down servers..." << std::endl;
    auto mainClient = std::make_shared<idioms::mpi::MPIClient>(rank, worldSize, true);
    mainClient->shutdownAllServers();

    std::cout << "Multi-Client Demo finished" << std::endl;
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
        std::cerr << "Please run with: mpirun -np N ./idioms_multi_client (where N >= 2)" << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
        return 1;
    }

    // Process with rank 0 is the client manager, others are servers
    if (rank == 0)
    {
        runAsClientManager(rank, worldSize);
    }
    else
    {
        runAsServer(rank, worldSize);
    }

    // Finalize MPI
    MPI_Finalize();

    return 0;
}