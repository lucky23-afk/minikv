# miniKV Architecture & Design

miniKV is a distributed, high-performance in-memory key-value store in Go (1.22+) designed with consistent hashing, Write-Ahead Logging (WAL) with CRC32 integrity, replication, gossip-based failure detection, and an embedded web dashboard.

## System Architecture

```mermaid
graph TD
    Client[Client / Browser / Loadgen] -->|HTTP / JSON or TCP| Coord[Coordinator & Gateway :8080]
    
    subgraph Ring [Consistent Hash Ring]
        Coord -->|Primary Write / Read| N1[Storage Node 1 :7000]
        Coord -->|Replication Stream| N2[Storage Node 2 :7010]
        Coord -->|Replication Stream| N3[Storage Node 3 :7020]
    end

    subgraph Internal Storage Node
        N1 --> HT[Sharded Hash Table 256 stripes]
        N1 --> LRU[O 1 LRU Eviction container/list]
        N1 --> TTL[Min-Heap Active TTL Sweeper]
        N1 --> WAL[Append-Only WAL + CRC32]
        WAL --> Snap[Periodic Snapshots]
    end

    N1 -.->|Gossip Health Heartbeats :7001| N2
    N2 -.->|Gossip Health Heartbeats :7011| N3
    N3 -.->|Gossip Health Heartbeats :7021| N1
```

## Consistent Hash Ring & Partitioning

```mermaid
flowchart LR
    Key[Key: user:1001] --> Hash[FNV-1a Hash]
    Hash --> Ring[Virtual Node Ring - 128 vnodes/node]
    Ring --> Node1[Node 1: Master Primary]
    Ring --> Node2[Node 2: Replica 1]
    Ring --> Node3[Node 3: Replica 2]
```

## Write Path & Replication Protocol

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant Coord as Coordinator
    participant Master as Node 1 (Primary)
    participant Replica as Node 2 (Replica)

    Client->>Coord: PUT /api/v1/kv/key1 {"value": "data", "sync": true}
    Coord->>Master: TCP PUT key1 ttl [payload]
    Master->>Master: Append WAL (CRC32 checked)
    Master->>Master: Update Sharded In-Memory Table & LRU
    Master-->>Coord: +OK (Seq: N)
    Coord->>Replica: TCP PUT key1 ttl [payload]
    Replica->>Replica: Apply to WAL & Store
    Replica-->>Coord: +OK (Ack Seq: N)
    Coord-->>Client: 200 OK {"primary": "node-1", "replicas": ["node-2"]}
```

## Recovery Flow

```mermaid
stateDiagram-v2
    [*] --> ReadLatestSnapshot: Startup
    ReadLatestSnapshot --> VerifyCRC: Verify Header & Checksum
    VerifyCRC --> PopulateStore: Load Non-Expired Keys
    PopulateStore --> ScanWAL: Replay Unflushed Logs
    ScanWAL --> SkipTornTail: Gracefully Handle Torn Final Record
    SkipTornTail --> StartSweeper: Launch Min-Heap TTL Sweeper
    StartSweeper --> Ready: Open TCP & HTTP Listeners
    Ready --> [*]
```
