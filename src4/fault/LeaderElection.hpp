#ifndef IDIOMS_LEADER_ELECTION_HPP
#define IDIOMS_LEADER_ELECTION_HPP

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <mutex>
#include <atomic>
#include <mpi.h>

namespace idioms
{
    namespace fault
    {

        /**
         * Implements a leader election algorithm for coordinating recovery
         * after server failures. Uses a bully algorithm approach.
         */
        class LeaderElection
        {
        private:
            int rank;                       // MPI rank of this process
            int worldSize;                  // Total number of MPI processes
            std::atomic<bool> running;      // Whether the election is running
            std::atomic<int> currentLeader; // The current leader rank

            std::mutex electionMutex;             // Mutex for election state
            std::unordered_set<int> participants; // Participants in the current election

            // MPI tags for leader election
            static const int ELECTION_TAG = 20;
            static const int COORDINATOR_TAG = 21;
            static const int ALIVE_TAG = 22;

            // Message types for leader election
            enum ElectionMessageType
            {
                ELECTION = 1, // Initiate election
                VICTORY = 2,  // Announce victory
                ALIVE = 3     // Response to election
            };

            // Message structure for leader election
            struct ElectionMessage
            {
                ElectionMessageType type;
                int senderId;
            };

            // Start a new election
            void startElection();

            // Receive and handle election messages
            void handleElectionMessage(const ElectionMessage &msg, int sourceRank);

            // Declare victory in an election
            void declareVictory();

            // Check if this process should become the leader
            bool shouldBecomeLeader() const;

            // Find the next higher-ranked process
            int findNextHigherRank() const;

            // Find all lower-ranked processes
            std::vector<int> findLowerRankedProcesses() const;

            // Send an election message to a specific rank
            void sendElectionMessage(ElectionMessageType type, int destRank);

        public:
            /**
             * Constructor
             *
             * @param mpiRank MPI rank of this process
             * @param mpiWorldSize Total number of MPI processes
             */
            LeaderElection(int mpiRank, int mpiWorldSize);

            /**
             * Destructor
             */
            ~LeaderElection();

            /**
             * Initialize leader election
             * This should be called at startup
             */
            void initialize();

            /**
             * Process an election message
             * Used by the MPI message handler when it receives an election message
             *
             * @param messageType Type of election message
             * @param sourceRank MPI rank of the sender
             */
            void processElectionMessage(int messageType, int sourceRank);

            /**
             * Get the current leader
             *
             * @return MPI rank of the current leader
             */
            int getLeader() const;

            /**
             * Check if this process is the leader
             *
             * @return True if this process is the leader, false otherwise
             */
            bool isLeader() const;

            /**
             * Initiate a new election
             * Called when leader failure is detected
             */
            void initiateElection();
        };

    } // namespace fault
} // namespace idioms

#endif // IDIOMS_LEADER_ELECTION_HPP