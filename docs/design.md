# miniKV Architecture & System Design

This document details the internal design decisions, consistency models, failure recovery algorithms, and known operational limitations of miniKV.

---

## 1. Consistency Model & CAP Position

miniKV is categorized as an **AP (Available / Partition-Tolerant)** system under Eric Brewer's CAP theorem when operating under its default configuration.

### Default Asynchronous Replication
- Write operations return as soon as the designated primary storage node appends the record to its Write-Ahead Log and updates its in-memory state.
- Replication frames are streamed asynchronously to followers in the key's preference list.
- **Trade-off**: Maximizes write throughput and minimizes client-observed latency by isolating the client from network fluctuations among follower nodes.
- **Loss Window**: If the primary storage node crashes abruptly before in-flight replication frames reach followers, modifications made during that un-streamed interval (typically bounded by network latency, approximately 5ms to 50ms) can be lost.

### Selectable Synchronous Consistency
- For critical operations (e.g., balance updates or authentication state), clients can issue requests with `"sync": true` and an explicit quorum count `"sync_n": N`.
- In synchronous mode, the coordinator blocks until at least `N` follower nodes acknowledge receipt and application of the WAL sequence number before returning HTTP 200 OK to the client.
- **Failover Guarantee**: In sync mode, if the primary crashes, the promoted follower is guaranteed to hold the exact acknowledged write sequence, ensuring zero data loss.

---

## 2. Consistent Hashing & Ring Rebalancing

### Ring Layout
- The ring maps the 64-bit integer space (`0` to `2^64 - 1`) using the FNV-1a non-cryptographic hash algorithm.
- To prevent hot-spotting and ensure uniform key distribution, each physical node is assigned `128` virtual nodes (configurable).
- Virtual node positions are sorted by hash in a contiguous slice. Key lookup uses binary search (`sort.Search`), achieving O(log(V * N)) lookup complexity where `V` is virtual node count and `N` is physical node count.

### Rebalance Dynamics
- When node `N+1` joins the cluster, it intercepts a subset of virtual node arcs.
- Mathematically, only approximately `1 / (N + 1)` fraction of existing keys map to the new node; all other `N / (N + 1)` keys remain pinned to their existing physical nodes.
- When an existing node departs or fails, only keys belonging to that node's partitions re-route to the next surviving physical node on the ring.
- **In-flight Key Migration**: Keys during a live rebalance are looked up in order of the preference list. If the newly mapped primary does not yet hold the key, the coordinator falls back to the previous replica nodes in the chain.

---

## 3. Write-Ahead Log (WAL) & Crash Recovery

### Frame Wire Format
Every log write is framed as an atomic binary record:
```
+----------------+----------------+--------------------------+----------------+
|  Magic (4B)    |  Length (4B)   |   Payload (N bytes)      |   CRC32 (4B)   |
|  0xABCD1234    |  uint32        |   Record binary payload  |   IEEE 802.3   |
+----------------+----------------+--------------------------+----------------+
```

Payload layout:
`[RecordType:1][KeyLen:2][Key:K][ValueLen:4][Value:V][ExpiresAt:8][Seq:8]`

### Fsync Policies
1. `always`: Every append calls `f.Sync()` immediately. Zero data loss on power cutoff or kernel panic.
2. `every-second`: A dedicated background worker flushes uncommitted pages once every 1,000 milliseconds.
3. `none`: Relies entirely on the operating system page cache. Maximum write throughput.

### Recovery Algorithm
On node startup:
1. **Load Snapshot**: Look up the latest snapshot file (`*.snap`). Read the header, verify the full-file CRC32 checksum, and populate live in-memory keys. Expired keys (`ExpiresAt <= now`) are discarded during load.
2. **Replay WAL**: Scan `wal.log` sequentially starting after the snapshot sequence number.
3. **CRC Verification & Torn Write Truncation**:
   - For each frame, read header and payload.
   - Compute CRC32 and compare against the stored tail checksum.
   - If a frame is torn (e.g. truncated midway through writing when power was cut), the recovery scanner halts cleanly, skips the corrupt tail, and truncates the WAL file to the boundary of the last valid frame.
4. **Resumption**: The node initializes its active sequence counter to `LastSeq + 1` and opens the file in append mode.

---

## 4. Eviction & Expiration Strategies

### O(1) LRU Cache Eviction
- Implemented with Go's standard `container/list` doubly linked list and a pointer map `map[string]*list.Element`.
- New entries or reads move the element to the front of the list.
- When `max-entries` or `max-bytes` thresholds are exceeded, elements are evicted from the tail of the list in O(1) time.
- Evictions invoke an `OnEvict` hook that decrements shard memory allocations, removes entries from the active TTL index, and increments atomic stats counters.

### Dual-Strategy TTL Engine
1. **Lazy Expiration on Access**: During `GET` or `EXISTS`, the entry's `ExpiresAt` nanosecond timestamp is compared against current time. If expired, it is deleted and treated as a cache miss.
2. **Active Min-Heap Sweeper**:
   - Every shard maintains a binary min-heap ordered by `ExpiresAt` ascending.
   - A background sweeper ticker runs every 100 milliseconds.
   - It peeks at the top of the heap. If `top.ExpiresAt <= now`, it pops the entry and removes it from the hash table.
   - If the earliest entry has not expired, the sweeper returns immediately without performing any table scans.

---

## 5. Gossip Failure Detector

- Nodes monitor peers using periodic HTTP heartbeats sent every 500 milliseconds with a 300 millisecond timeout.
- Three liveness states:
  - `Alive`: Node responds to health probes within timeout.
  - `Suspect`: A node fails a health check; flagged as suspect for up to 2,000 milliseconds.
  - `Dead`: If a suspect node fails to respond within the suspect window, it is transitioned to `Dead`.
- When declared `Dead`, the coordinator unregisters the node from the hash ring, preventing future request dispatch, and promotes the most up-to-date replica.
- Flapping mitigation: If a node responds while suspect, its suspect timer resets and it immediately transitions back to `Alive`.

---

## 6. Known Limitations

In the interest of engineering transparency, miniKV is a pedagogical and high-performance demonstration system with the following deliberate limitations:

1. **No Distributed Multi-Key Transactions**: All operations are atomic at the single-key level. Multi-key batch operations (`MGET`/`MSET`) across different hash partitions do not support two-phase commit (2PC).
2. **No Linearizability Across Dynamic Partitions**: Under network split conditions, asynchronous replication allows split-brain writes if a partition survives on both sides before gossip convergence.
3. **HTTP Gossip**: Gossip operates over HTTP instead of UDP SWIM gossip. While robust and firewall-friendly, HTTP polling introduces slightly higher network overhead per health check than raw UDP gossip frames.
4. **In-Memory Store Bound**: While persisted to disk via WAL and snapshot, all active keys and values must fit in node RAM. Cold values are not paged to disk beyond LRU eviction.
