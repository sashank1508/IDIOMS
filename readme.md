# IDIOMS - Indexed Distributed Object Metadata System

## Project Overview

IDIOMS (Indexed Distributed Object Metadata System) is a distributed metadata indexing and query system designed to efficiently store and search key-value metadata attributes associated with objects across a distributed environment. The system is inspired by scientific computing workflows that need to manage and query large numbers of data objects with associated metadata.

The implementation offers three operational modes:

1. **Standalone Mode**: Single-process execution with multiple virtual servers
2. **MPI Mode**: Distributed execution across multiple processes with one client and multiple servers
3. **Multi-Client Mode**: Distributed execution with multiple concurrent clients

## System Architecture

### Core Components

1. **Two-Level Trie Index**:
   - First level KeyTrie indexes metadata attribute keys
   - Second level ValueTrie indexes values for each key
   - Optional suffix-tree mode enables efficient suffix and infix searching

2. **DART (Distributed Aggregated Routing Table)**:
   - Distributes metadata across servers using consistent hashing
   - Maps virtual nodes to physical servers
   - Provides intelligent query routing
   - Supports data replication for reliability

3. **Client-Server Architecture**:
   - Clients route requests to appropriate servers
   - Servers maintain local indices
   - Clients aggregate results from multiple servers

4. **Query System**:
   - Supports exact, prefix, suffix, infix, and wildcard queries
   - Handles compound queries with multiple conditions

### Component Diagram

```
+-------------------+       +-------------------+
|      Client       |       | Multi-Client Mgr  |
+-------------------+       +-------------------+
         |                           |
         |  Query/Index Requests     |
         v                           v
+-------------------+       +-------------------+
|    DART Router    |       | Client Threads    |
+-------------------+       +-------------------+
         |                           |
         |  Route to Servers         |
         v                           v
+--------+--------+--------+--------+--------+
|        |        |        |        |        |
v        v        v        v        v        v
+----+  +----+  +----+  +----+  +----+  +----+
|Srv1|  |Srv2|  |Srv3|  |Srv4|  |Srv5|  |Srv6|
+----+  +----+  +----+  +----+  +----+  +----+
  |       |       |       |       |       |
  v       v       v       v       v       v
+----+  +----+  +----+  +----+  +----+  +----+
|Trie|  |Trie|  |Trie|  |Trie|  |Trie|  |Trie|
+----+  +----+  +----+  +----+  +----+  +----+
```

## Key Features

1. **Efficient Metadata Indexing**:
   - Trie-based structure for fast key-value lookups
   - Support for suffix-tree mode enabling efficient infix and suffix queries

2. **Advanced Query Capabilities**:
   - Exact matches: `attribute=value`
   - Prefix queries: `prefix*=value` or `attribute=prefix*`
   - Suffix queries: `*suffix=value` or `attribute=*suffix`
   - Infix queries: `*substring*=value` or `attribute=*substring*`
   - Wildcard queries: `*=*`

3. **Distributed Architecture**:
   - Metadata distributed across multiple servers
   - Intelligent query routing
   - Result aggregation from multiple servers
   - Data replication for reliability

4. **Multiple Operational Modes**:
   - Standalone mode for testing and development
   - MPI-based distributed deployment
   - Multi-client concurrent operation

5. **Persistence**:
   - Checkpoint and recovery mechanisms
   - Index persistence to disk

## Implementation Details

### Core Data Structures

1. **KeyTrie/ValueTrie** (`index/Trie.cpp`, `Trie.hpp`):
   - Character-by-character trie implementation
   - Support for suffix-tree mode
   - Methods for different query types (exact, prefix, suffix, infix)

2. **DART Router** (`dart/DART.cpp`, `DART.hpp`):
   - ConsistentHash for server assignment
   - VirtualNode mapping for key ranges
   - Methods for determining servers for different query types

3. **Server Components** (`server/Server.cpp`, `Server.hpp`):
   - DistributedIdiomsServer manages a partition of the index
   - Methods for adding/removing metadata and executing queries
   - Checkpoint and recovery functions

4. **Client Components** (`client/Client.cpp`, `Client.hpp`):
   - API for creating, deleting, and querying metadata
   - Logic for routing requests to appropriate servers
   - Result aggregation across multiple servers

5. **MPI Components** (`mpi/MPIServer.cpp`, `MPIClient.cpp`, `MPICommon.hpp`):
   - Message passing infrastructure
   - Serialization/deserialization
   - MPI-based client-server communication

6. **Query Components** (`query/MultiConditionQuery.cpp`, `RangeQuery.cpp`):
   - Structures for representing and evaluating complex queries
   - Support for compound conditions and range queries

### Query Processing Flow

1. **Client parses query** (e.g., `"Stage*=*"`)
2. **DART router determines potential servers** (which servers might have matching data)
3. **Query distributed to selected servers** (via function call or MPI)
4. **Each server determines if it can handle the query**
5. **Servers execute query on local trie index**
6. **Results returned to client**
7. **Client aggregates results** (removes duplicates)
8. **Final result set returned to user**

### Metadata Insertion Flow

1. **Client receives key-value pair** (e.g., `"StageX", "100.00"` for object `1001`)
2. **DART router determines target servers** (based on consistent hashing)
3. **Metadata sent to selected servers** (including replicas)
4. **Servers index the metadata in local tries**
5. **Servers confirm successful indexing**

## Build and Run Instructions

### Prerequisites

- C++17 compatible compiler
- MPI implementation (MPICH or OpenMPI) for distributed modes
- pthread support

### Building

```bash
# Create necessary directories
mkdir -p bin/index bin/dart bin/server bin/client bin/mpi bin/query

# 1. Standalone Mode
# Compile core components
g++ -std=c++17 -Wall -Wextra -pthread -I. -c src3/index/Trie.cpp -o bin/index/Trie.o
g++ -std=c++17 -Wall -Wextra -pthread -I. -c src3/dart/DART.cpp -o bin/dart/DART.o
g++ -std=c++17 -Wall -Wextra -pthread -I. -c src3/server/Server.cpp -o bin/server/Server.o
g++ -std=c++17 -Wall -Wextra -pthread -I. -c src3/client/Client.cpp -o bin/client/Client.o
g++ -std=c++17 -Wall -Wextra -pthread -I. -c src3/query/MultiConditionQuery.cpp -o bin/query/MultiConditionQuery.o
g++ -std=c++17 -Wall -Wextra -pthread -I. -c src3/query/RangeQuery.cpp -o bin/query/RangeQuery.o

# Compile main program
g++ -std=c++17 -Wall -Wextra -pthread -I. -c src3/main.cpp -o bin/main.o

# Link everything together
g++ -std=c++17 bin/index/Trie.o bin/dart/DART.o bin/server/Server.o bin/client/Client.o bin/query/MultiConditionQuery.o bin/query/RangeQuery.o bin/main.o -o idioms_standalone

# 2. MPI Mode
# Compile core components
mpic++ -std=c++17 -Wall -Wextra -pthread -I. -c src3/index/Trie.cpp -o bin/index/Trie.o
mpic++ -std=c++17 -Wall -Wextra -pthread -I. -c src3/dart/DART.cpp -o bin/dart/DART.o
mpic++ -std=c++17 -Wall -Wextra -pthread -I. -c src3/server/Server.cpp -o bin/server/Server.o

# Compile MPI components
mpic++ -std=c++17 -Wall -Wextra -pthread -I. -c src3/mpi/MPIServer.cpp -o bin/mpi/MPIServer.o
mpic++ -std=c++17 -Wall -Wextra -pthread -I. -c src3/mpi/MPIClient.cpp -o bin/mpi/MPIClient.o

# Compile query components
mpic++ -std=c++17 -Wall -Wextra -pthread -I. -c src3/query/MultiConditionQuery.cpp -o bin/query/MultiConditionQuery.o
mpic++ -std=c++17 -Wall -Wextra -pthread -I. -c src3/query/RangeQuery.cpp -o bin/query/RangeQuery.o

# Compile main program
mpic++ -std=c++17 -Wall -Wextra -pthread -I. -c src3/mpi_main.cpp -o bin/mpi_main.o

# Link everything into the final executable
mpic++ -std=c++17 bin/index/Trie.o bin/dart/DART.o bin/server/Server.o bin/mpi/MPIServer.o bin/mpi/MPIClient.o bin/query/MultiConditionQuery.o bin/query/RangeQuery.o bin/mpi_main.o -o idioms_mpi

# 3. Multi-Client Mode
# Compile core components
mpic++ -std=c++17 -Wall -Wextra -pthread -I. -c src3/index/Trie.cpp -o bin/index/Trie.o
mpic++ -std=c++17 -Wall -Wextra -pthread -I. -c src3/dart/DART.cpp -o bin/dart/DART.o
mpic++ -std=c++17 -Wall -Wextra -pthread -I. -c src3/server/Server.cpp -o bin/server/Server.o

# Compile MPI components
mpic++ -std=c++17 -Wall -Wextra -pthread -I. -c src3/mpi/MPIServer.cpp -o bin/mpi/MPIServer.o
mpic++ -std=c++17 -Wall -Wextra -pthread -I. -c src3/mpi/MPIClient.cpp -o bin/mpi/MPIClient.o

# Compile client components
mpic++ -std=c++17 -Wall -Wextra -pthread -I. -c src3/client/Client.cpp -o bin/client/Client.o
mpic++ -std=c++17 -Wall -Wextra -pthread -I. -c src3/client/ClientManager.cpp -o bin/client/ClientManager.o

# Compile query components
mpic++ -std=c++17 -Wall -Wextra -pthread -I. -c src3/query/MultiConditionQuery.cpp -o bin/query/MultiConditionQuery.o
mpic++ -std=c++17 -Wall -Wextra -pthread -I. -c src3/query/RangeQuery.cpp -o bin/query/RangeQuery.o

# Compile main program
mpic++ -std=c++17 -Wall -Wextra -pthread -I. -c src3/multi_client_demo.cpp -o bin/multi_client_demo.o

# Link everything together
mpic++ -std=c++17 bin/index/Trie.o bin/dart/DART.o bin/server/Server.o bin/mpi/MPIServer.o bin/mpi/MPIClient.o bin/client/Client.o bin/client/ClientManager.o bin/query/MultiConditionQuery.o bin/query/RangeQuery.o bin/multi_client_demo.o -o idioms_multi_client
```

### Running

```bash
# Run standalone mode
./idioms_standalone

# Run MPI mode with 5 processes (1 client, 4 servers)
mpirun -np 5 ./idioms_mpi

# Run multi-client mode with 7 processes (1 client manager, 6 servers)
mpirun --oversubscribe -np 7 ./idioms_multi_client
```

## Example Queries

The IDIOMS system supports various query types:

1. **Exact Query**: `StageX=300.00`
   - Finds objects with StageX exactly equal to 300.00

2. **Prefix Query**: `Stage*=*`
   - Finds objects with any attribute starting with "Stage"

3. **Suffix Query**: `*PATH=*tif`
   - Finds objects with any attribute ending with "PATH" and value ending with "tif"

4. **Infix Query**: `*FILE*=*metadata*`
   - Finds objects with any attribute containing "FILE" and value containing "metadata"

## Performance Considerations

1. **Trie-based Index**:
   - Time complexity: O(k) for lookups, where k is length
   - Space complexity: O(n*m) where n is number of keys and m is average length

2. **Distributed Query Routing**:
   - DART router efficiently maps queries to relevant servers
   - Reduces unnecessary network traffic by targeting relevant servers

3. **Parallel Query Execution**:
   - Queries executed in parallel across multiple servers
   - Result aggregation provides unified view

4. **Suffix-Tree Mode**:
   - Enables efficient suffix and infix queries
   - Trade-off: Increases space complexity by indexing all suffixes

## Conclusion

The IDIOMS system provides a scalable and efficient distributed metadata indexing and query solution. It supports advanced query patterns while maintaining performance through intelligent distribution and trie-based indexing. The multi-modal design allows for flexible deployment from development to production environments.
