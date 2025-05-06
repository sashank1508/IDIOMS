#include "LeaderElection.hpp"
#include <iostream>
#include <algorithm>
#include <chrono>
#include <thread>

namespace idioms
{
    namespace fault
    {

        LeaderElection::LeaderElection(int mpiRank, int mpiWorldSize)
            : rank(mpiRank), worldSize(mpiWorldSize), running(false), currentLeader(-1)
        {

            // Nothing to initialize yet
        }

        LeaderElection::~LeaderElection()
        {
            // Clean up resources if needed
        }

        void LeaderElection::initialize()
        {
            // For simplicity, we'll use rank 0 as the initial leader
            currentLeader.store(0);

            std::cout << "Leader election initialized. Initial leader: " << currentLeader.load() << std::endl;
        }

        void LeaderElection::processElectionMessage(int messageType, int sourceRank)
        {
            ElectionMessage msg;
            msg.type = static_cast<ElectionMessageType>(messageType);
            msg.senderId = sourceRank;

            handleElectionMessage(msg, sourceRank);
        }

        int LeaderElection::getLeader() const
        {
            return currentLeader.load();
        }

        bool LeaderElection::isLeader() const
        {
            return rank == currentLeader.load();
        }

        void LeaderElection::initiateElection()
        {
            std::lock_guard<std::mutex> lock(electionMutex);

            if (running.load())
            {
                // Election already in progress
                return;
            }

            std::cout << "Initiating leader election on rank " << rank << std::endl;

            running.store(true);
            startElection();
        }

        void LeaderElection::startElection()
        {
            // Clear participants
            participants.clear();

            // Add self as participant
            participants.insert(rank);

            if (shouldBecomeLeader())
            {
                // This is the highest-ranked process, so it should become the leader
                declareVictory();
                return;
            }

            // Find all processes with higher rank
            int nextHigherRank = findNextHigherRank();
            if (nextHigherRank != -1)
            {
                // Send election message to higher-ranked process
                sendElectionMessage(ELECTION, nextHigherRank);
            }
            else
            {
                // No higher-ranked processes, so this process should become the leader
                declareVictory();
            }
        }

        void LeaderElection::handleElectionMessage(const ElectionMessage &msg, int sourceRank)
        {
            switch (msg.type)
            {
            case ELECTION:
                // Another process is initiating an election
                std::cout << "Received election message from " << sourceRank << std::endl;

                // Send an ALIVE message back
                sendElectionMessage(ALIVE, sourceRank);

                // Start our own election
                initiateElection();
                break;

            case VICTORY:
                // Another process has declared itself the leader
                std::cout << "Received victory message from " << sourceRank << std::endl;

                // Update the current leader
                currentLeader.store(sourceRank);

                // Election is complete
                running.store(false);
                break;

            case ALIVE:
                // Response to our election message
                std::cout << "Received alive message from " << sourceRank << std::endl;

                // Add to participants
                participants.insert(sourceRank);
                break;
            }
        }

        void LeaderElection::declareVictory()
        {
            std::cout << "Process " << rank << " declaring victory in leader election" << std::endl;

            // Update the current leader
            currentLeader.store(rank);

            // Notify all other processes
            for (int i = 0; i < worldSize; i++)
            {
                if (i != rank)
                {
                    sendElectionMessage(VICTORY, i);
                }
            }

            // Election is complete
            running.store(false);
        }

        bool LeaderElection::shouldBecomeLeader() const
        {
            // Check if this is the highest-ranked process among participants
            for (int i = rank + 1; i < worldSize; i++)
            {
                if (participants.find(i) != participants.end())
                {
                    // There's a higher-ranked participant
                    return false;
                }
            }

            // This is the highest-ranked participant
            return true;
        }

        int LeaderElection::findNextHigherRank() const
        {
            for (int i = rank + 1; i < worldSize; i++)
            {
                // In a real implementation, we would check if the process is active
                return i;
            }

            // No higher-ranked processes
            return -1;
        }

        std::vector<int> LeaderElection::findLowerRankedProcesses() const
        {
            std::vector<int> lowerRanked;

            for (int i = 0; i < rank; i++)
            {
                // In a real implementation, we would check if the process is active
                lowerRanked.push_back(i);
            }

            return lowerRanked;
        }

        void LeaderElection::sendElectionMessage(ElectionMessageType type, int destRank)
        {
            ElectionMessage msg;
            msg.type = type;
            msg.senderId = rank;

            // Send the message
            MPI_Send(&msg, sizeof(msg), MPI_BYTE, destRank, ELECTION_TAG, MPI_COMM_WORLD);
        }

    } // namespace fault
} // namespace idioms