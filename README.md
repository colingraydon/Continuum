# Continuum

A distributed key-value store built on consistent hashing, gossip-based membership, and vector clock conflict resolution - written in Go.

Continuum implements the core data layer used in systems like Cassandra and Dynamo: a hash ring that maps keys to nodes with minimal disruption when topology changes, a gossip protocol that propagates membership without a central coordinator, a replication layer that fans writes out to N nodes and resolves conflicts using vector clocks, a Merkle tree anti-entropy system that detects and repairs divergent replicas in the background, a hinted handoff layer that buffers writes for temporarily unreachable replicas and replays them on recovery, and a read repair layer that pushes the canonical merged value back to stale replicas after every quorum read. It exposes an HTTP API, Prometheus metrics, and a Grafana dashboard out of the box.

---

## Architecture

Continuum is organized into seven layers:

### Core Ring (`internal/ring`)

The routing engine. Implements the hash ring with a Red-Black Tree, virtual nodes, murmur3 hashing, and atomic key counters. Has no knowledge of HTTP, gossip, or storage - it is a pure routing library. Membership is driven entirely by the gossip layer via callbacks; the ring is never mutated directly.

### Gossip Protocol (`internal/gossip`)

Handles cluster membership and failure detection without a central coordinator. Each node maintains a `MemberList` - the single source of truth for membership state. Nodes exchange member lists on a 1-second interval with up to 3 random peers (fanout), propagating membership changes across the cluster in O(log n) rounds.

The gossip layer drives the ring: when membership changes (alive, suspect, dead), a callback updates the ring so routing always reflects current cluster state.

### KV Store (`internal/store`)

In-memory key-value storage with vector clock versioning. Each entry holds a value, a `VectorClockVersion`, and a precomputed murmur3 hash of the value used by the anti-entropy layer. Conflict resolution uses the standard Lamport partial order: an incoming write is dropped if the existing entry's clock dominates it, applied as the sole value if it dominates the existing entry, or appended as a sibling if the two clocks are concurrent.

Deletes are implemented as tombstones: a `Deleted` sibling written at an incremented vector clock. Tombstones participate in conflict resolution identically to value writes. A tombstone with a dominating clock wins, a stale tombstone is dropped, and a concurrent write/delete produces siblings. This prevents resurrection: a stale replica that missed a delete cannot revive the key through anti-entropy or a consistent read.

### Anti-Entropy (`internal/antientropy`)

Background repair layer that detects and corrects divergent replicas using Merkle trees. The primary node for each vnode range maintains one Merkle tree, partitioned into 16 hash-range buckets. Every 30 seconds, one vnode is selected at random and compared against each of its replicas. Sync is **bidirectional**: for each divergent bucket, the primary both pulls entries from the replica that it is missing or behind on, and pushes entries to the replica that it has but the replica lacks. Vector clock semantics handle all cases - dominated entries are silently dropped, concurrent entries become siblings.

Replicas do not maintain persistent trees. Instead, they compute bucket and root hashes on-the-fly from their store when the primary asks, which keeps the replica code path simple and avoids synchronization overhead.

**Tombstone GC** runs every 5 minutes. Uncontested tombstones (single-sibling, deleted, no concurrent live sibling) older than 1 hour are purged from the store and removed from the primary's Merkle trees. See the design decision below for the safety reasoning behind the TTL choice.

### Hinted Handoff (`internal/hintstore`)

Durability layer that closes the gap between quorum writes and full replication. When a coordinator fans a write out to its replica set and a replica is unreachable, the write is buffered as a *hint* in the coordinator's local store, tagged with the intended recipient. When gossip detects that the node has recovered (alive transition), the coordinator drains its hint buffer and replays each write to the recovered node as a normal replica sub-write.

The hint store is bounded: a per-node cap of 10,000 entries evicts the oldest hints when full, and hints older than 1 hour are expired and discarded. Anti-entropy is the safety net — any keys whose hints were lost will be repaired within the next sync cycle.

Hint delivery is triggered by the gossip `onChange` callback rather than a polling loop, so recovery latency is bounded by gossip convergence time (O(log n) rounds at 1-second intervals) rather than a fixed poll interval.

### Read Repair (`api`)

Inline repair layer that piggybacks on quorum reads. After the coordinator collects R responses and merges the sibling sets into a canonical result, it compares each replica's response against the survivors. A replica is stale if its sibling set is a proper subset of, or missing, any surviving entry (matched by equal vector clocks). Stale replicas are repaired asynchronously in a background goroutine so the client's read latency is not affected.

For the coordinator itself the repair is a direct store write. For remote replicas it uses the same proxied write path as hinted handoff (`X-Proxied-From`). Failures are logged; anti-entropy covers any keys that could not be repaired immediately.

### Data Migration (`api`)

Handles data movement when nodes join or leave the ring. On join, a new node marks itself bootstrapping via gossip, then pulls all keys in its primary vnode ranges from existing replicas using the existing sync endpoints, merging entries through the standard vector clock path. Coordinators exclude bootstrapping nodes from read replica sets and the bootstrapping node itself rejects coordinator-role requests with 503 until migration completes. When bootstrapping finishes, every peer receives the state change via gossip and evicts keys that now belong to the new node. On planned departure, a node pushes all locally-held keys to its alive successors in a single batched request per target before the HTTP server drains.

### Stats Aggregator (`internal/stats`)

A composition layer that combines ring statistics (vnode distribution, key counts, variance) with gossip membership status (alive/suspect/dead node counts) into a single unified view. Keeps the ring package free of membership concerns.

### HTTP API (`api`)

The transport layer. Exposes ring, gossip, and storage operations over HTTP, instruments all requests via Prometheus middleware, and wires all internal packages together. Handlers are thin - they delegate to the appropriate internal package and serialize the response.

---

## Gossip Protocol

### Membership lifecycle

Nodes transition through three states: **alive → suspect → dead**.

- A node is marked **alive** when it joins the cluster (via `POST /nodes` or gossip peer exchange) and its heartbeat is propagating
- A node is marked **suspect** when its heartbeat hasn't been updated within 5 seconds (`staleThreshold`) - it may be slow or partitioned
- A node is marked **dead** when it remains suspect past a second stale check - it is removed from the ring and stops receiving traffic

Recovery is automatic: if a dead or suspect node starts gossiping again with a higher heartbeat, it transitions back to alive and is re-added to the ring.

### Peer discovery and convergence

Each node runs three background loops:

1. **Gossip loop** (1s interval) - increments its own heartbeat, selects up to 3 random alive peers, and pushes its full member list to each. A new member propagates to the full cluster in O(log n) rounds.
2. **Receive loop** - handles incoming gossip messages. Merges the peer's member list using a last-write-wins strategy based on heartbeat: a member update is only accepted if the incoming heartbeat is strictly higher than what's known locally.
3. **Stale loop** (1s interval) - checks every non-self member's `UpdatedAt` timestamp. Members not heard from in 5 seconds transition alive → suspect → dead.

### Bootstrapping

New nodes specify one or more seed nodes via the `SEED_NODES` environment variable. On startup, the joining node marks itself as bootstrapping and sends its member list to each seed, triggering a gossip exchange. Within a few seconds the new node's membership has propagated to the full cluster.

While bootstrapping, the joining node rejects coordinator-role reads and writes with 503. Other nodes exclude it from read replica sets. It still accepts replica sub-requests (identified by `X-Proxied-From`) so it can receive writes during migration.

Once the HTTP server is up and the ring is populated from gossip, the joining node pulls its primary vnode ranges from existing replicas. On completion it clears the bootstrapping flag, which propagates via gossip. Each peer that receives the transition evicts keys that now belong to the new node.

---

## Replication

When a write arrives at any node, it:

1. Determines the replica set for the key using `GetReplicationNodes(key, factor)` - the N consecutive distinct nodes clockwise from the key's ring position
2. Increments its own vector clock counter and stores the value locally (self counts as one acknowledgment)
3. Fans out the write to all other replicas in parallel, waiting for W acknowledgments before returning 204
4. Returns 503 if quorum cannot be reached

Replica nodes store the write as-is without further fan-out, identified by the `X-Proxied-From` header.

Replication factor is configured via `REPLICATION_FACTOR` (default: 3). Write quorum W is configured via `WRITE_QUORUM` (default: majority, `floor(RF/2) + 1`). Inter-node replication calls use a dedicated HTTP client with a configurable timeout so a slow or unresponsive replica does not block the coordinator indefinitely.

### Consistent reads

When a read arrives at any node, it fans out to the full replica set and waits for R responses. The response with the highest vector clock is returned to the client. If R replicas cannot be reached, the request returns 503.

Read quorum R is configured via `READ_QUORUM` (default: majority). Setting `READ_QUORUM=1` gives lowest-latency reads but may return stale data; `READ_QUORUM=REPLICATION_FACTOR` gives the strongest consistency guarantee at the cost of latency.

---

## Vector Clocks

Each write carries a vector clock - a map from node ID to a logical counter. The writing node increments its own counter before storing and replicating.

```json
{ "clocks": { "node1": 3, "node2": 1 } }
```

This clock means: node1 has coordinated 3 writes, node2 has coordinated 1, and any replica that received all of them would accept this as the current version.

**Conflict detection** uses the standard partial order:

- Clock A **happens-before** B if every counter in A is ≤ B's and at least one is strictly less → B wins, A is dropped
- **Concurrent** clocks (neither happens-before the other) → both values are kept as **siblings** and returned on the next read
- **Equal** clocks → idempotent write, no change

When siblings are present, the read response contains a `siblings` array instead of a single `value`. A subsequent write whose clock dominates all sibling clocks resolves the conflict.

### Siblings vs. last-write-wins

The alternative to sibling surfacing is last-write-wins (LWW): on concurrent writes, keep the value with the highest timestamp and silently discard the other. Cassandra uses LWW by default. It is simple and produces no conflicts visible to the client, but it loses writes - if two clients update the same key concurrently, one update disappears with no indication that it happened.

Sibling surfacing (this system, Riak, Dynamo) never discards a write silently. Concurrent writes are preserved and the conflict is made visible. The tradeoff is that reads can return multiple values, and the application must know what to do with them - merge two shopping carts by union, pick the higher counter, defer to a CRDT, or surface the conflict to a user. This pushes complexity to the application layer, which is where the semantics to resolve it correctly live.

---

## Hinted Handoff

### The durability gap

With RF=3 and W=2, a write returns 204 once two replicas acknowledge it. If the third replica is down at write time, it misses the write entirely. Anti-entropy will repair it within the next 30-second sync cycle. Hinted handoff closes that window.

### How it works

When the coordinator fans a write to its replica set and a replica call fails, the write is buffered locally as a hint:

```
hint {
    key:     "user:123"
    value:   "alice"
    clocks:  {"node1": 3}
    deleted: false
    for:     "node3"
}
```

Hints survive in-memory on the coordinator. When gossip detects that node3 has recovered (alive transition), the coordinator drains its hint buffer for node3 and replays each write via a normal replica sub-write (`X-Proxied-From`). The receiving node applies them through the standard vector clock conflict resolution path — stale hints are silently dropped, concurrent hints become siblings.

### Quorum interaction

Hinted replicas **never count toward quorum**. A coordinator that already has W acks does not inflate that count with a speculative "I'll retry node3 later." The 204 reflects actual durability at W replicas; the hint is a best-effort repair on top of that.

### Fan-out implementation

The coordinator starts goroutines for all non-self replicas simultaneously. The result collection loop breaks as soon as W acks are received, rather than waiting for all replicas. For in-flight goroutines that hadn't reported when quorum was met, a background goroutine drains their results and records hints for any failures. This keeps write latency bounded by the W-th fastest replica, not the slowest.

### Graceful shutdown

On `SIGINT`/`SIGTERM`, before draining in-flight HTTP requests, the coordinator iterates all pending hints and delivers any that target currently-alive nodes. This prevents hint loss on planned restarts.

### Limits and safety

| Parameter | Value | Rationale |
| --------- | ----- | --------- |
| Per-node cap | 10,000 hints | Bounds memory; excess hints are evicted oldest-first |
| Hint TTL | 1 hour | Anti-entropy repairs anything older within 2 sync cycles |
| Coordinator restart | Hints lost | In-memory only; anti-entropy is the durable fallback |

The residual risk is a node that recovers after the coordinator that held its hints has restarted. In that case the replica relies on anti-entropy for repair — the same guarantee that existed before hinted handoff was added.

---

## Request flow

A write (`PUT /keys/:key`) flows like this:

1. Request hits `metricsMiddleware` - records latency and request count
2. `PutKey` handler extracts the key and decodes `{"value": "...", "clocks": {...}}`
3. Incoming clock is incremented for this node; value is stored in the local `Store`
4. `ring.GetReplicationNodes(key, factor)` returns the replica set
5. Goroutines fan the write out to each non-self replica with `X-Proxied-From` set
6. Coordinator waits for W acknowledgments (self counts as one); returns 204 on quorum, 503 if quorum cannot be reached

A key lookup (`GET /keys/:key`) flows like this:

1. `GetNode` handler extracts the key
2. `ring.GetReplicationNodes(key, factor)` returns the replica set
3. Goroutines fan the read out to each replica with `X-Proxied-From` set; each replica returns its local entry with its vector clock
4. The coordinator waits for R responses and merges the sibling sets into the canonical result
5. Any replica whose response was dominated by or missing a surviving entry is repaired asynchronously in a background goroutine
6. The merged result is returned to the client

---

## How key lookup works

1. Hash the key using Murmur3 to get a position on the ring (0 to 2^32)
2. Find the first virtual node with hash ≥ key hash using a Red-Black Tree ceiling lookup - O(log n)
3. If no vnode found, wrap around to the first vnode on the ring
4. If a health filter is set, walk forward skipping dead/suspect nodes
5. Return the physical node that vnode belongs to

---

## Benchmarks

Measured on Apple M3 Max:

| Operation           | Throughput   | Latency   |
| ------------------- | ------------ | --------- |
| GetNode (3 nodes)   | ~9M ops/sec  | 112 ns/op |
| GetNode (100 nodes) | ~5M ops/sec  | 201 ns/op |
| GetNode (parallel)  | ~6M ops/sec  | 160 ns/op |
| AddNode             | ~8K ops/sec  | 116 µs/op |
| RemoveNode          | ~10K ops/sec | 101 µs/op |

**Replica count impact on lookup latency:**

| Replicas | Latency   |
| -------- | --------- |
| 10       | 96 ns/op  |
| 50       | 105 ns/op |
| 150      | 114 ns/op |
| 500      | 129 ns/op |

Going from 10 to 500 vnodes adds only 33ns to lookup latency - the distribution benefit of more vnodes is essentially free at read time.

**Concurrent reads vs mixed reads/writes:**

| Workload             | Latency   |
| -------------------- | --------- |
| Pure reads           | 160 ns/op |
| Mixed reads + writes | 940 ns/op |

The 6x slowdown on mixed workloads is expected - write lock acquisition blocks concurrent readers. Node changes are rare in production so this tradeoff is acceptable.

---

## Design Decisions

### MemberList as single source of truth

The ring is a pure routing layer - it has no opinion on membership. All ring mutations flow through a single callback on `MemberList`, so gossip-discovered members, manually registered members (`POST /nodes`), and manually removed members (`DELETE /nodes/:id`) all take the same path. This eliminates the class of bugs where ring and membership state diverge.

### Vector clocks over LWW timestamps

Last-write-wins timestamps are simple but lose writes silently when two clients write to the same key concurrently. Vector clocks track causality per-node, so concurrent writes are detectable rather than silently discarded. When concurrency is detected, both values are preserved as siblings and returned to the reader - no write is lost, and the application can resolve the conflict with full information.

### Precomputed value hashes

Each store entry carries `Hash uint32` - a murmur3 hash of the value, computed at write time and updated via an `onUpdate` callback whenever a write is accepted. The anti-entropy manager registers this callback to keep its Merkle trees current. Computing at write time means tree updates are incremental and cheap - no full scan is needed after each write.

### Red-Black Tree

The ring uses a Red-Black Tree (via `emirpasic/gods`) to store virtual nodes sorted by hash. This gives O(log n) for insert, delete, and ceiling lookup. A sorted slice would give O(log n) lookup via binary search but O(n) insert/delete due to element shifting. The RBT's `Ceiling()` operation also maps directly to the ring's successor lookup semantics.

### Murmur3

Murmur3 is faster than cryptographic hashes (MD5, SHA) and has better distribution than FNV-32a for short strings. It's the same hash function Cassandra uses for consistent hashing. Since security is not a requirement here, the non-cryptographic nature is a non-issue.

### sync.RWMutex

The ring uses `sync.RWMutex` rather than a plain mutex. `RWMutex` allows unlimited concurrent readers with exclusive writers - correct for a ring where key lookups vastly outnumber topology changes.

### Atomic key counters

Per-node key counts use `sync/atomic.Int64` rather than incrementing under the write lock. This keeps `GetNode` on the read lock path so multiple goroutines can look up keys concurrently.

### Bidirectional sync and tombstone GC

The alternative to TTL-based GC is per-replica confirmation tracking - each primary records which replicas have acknowledged a tombstone and only purges it once every replica has confirmed receipt. This is strictly safer but adds meaningful complexity: the primary must persist acknowledgment state, handle replica churn (nodes joining and leaving mid-tracking), and reason about what "all replicas" means in a dynamic cluster.

Bidirectional sync makes the simpler TTL approach viable. Because the primary both pulls from and pushes to each replica on every sync cycle, a tombstone written on any node propagates outward without waiting for the replica to initiate a sync. With a 30-second sync interval, a 1-hour TTL provides roughly 120 sync cycles of headroom before GC runs - more than enough for tombstones to reach every live replica.

The residual risk is key resurrection: a node partitioned for longer than 1 hour reconnects with a stale live value after the primary has already GC'd its tombstone. For this in-memory store, the risk is theoretical - a node partitioned that long will have restarted and lost all state before reconnecting. For a persistent store, the TTL would need to be substantially longer - or replaced with per-replica confirmation tracking - to remain safe across restarts.

### Data migration: pull on join, push on leave

When a node joins, it could receive data via push from existing nodes (each existing node detects the join and sends relevant keys) or via pull (the joining node fetches its own ranges). Pull is simpler: only the joining node knows which ranges it owns, so it can issue targeted requests without every existing node needing to compute what to send. It also avoids redundant pushes when multiple existing nodes each send the same range.

On departure the direction reverses. The leaving node knows exactly what it holds and which nodes are its successors, so a single batched push per target is more efficient than waiting for anti-entropy to repair the gap.

### Hinted handoff: event-driven delivery over polling

Hint delivery is triggered by the gossip `onChange` callback (alive transition) rather than a background loop that polls member state on a fixed interval. This means recovery latency tracks gossip convergence — typically a few seconds — rather than the polling period. The tradeoff is tighter coupling between the gossip and handler layers, managed by passing a delivery function through `main.go` rather than letting either package import the other.

### Read repair: async over sync

Read repair could be synchronous — wait for all stale replicas to ack before returning to the client. This would guarantee that the next read from any replica sees the updated value. The tradeoff is that the client's read latency is now bounded by the slowest stale replica rather than the R-th fastest responding one. Since stale replicas are likely slower (they may have been partitioned or lagging), this is the worst case to wait on. Async repair accepts a short window where a stale replica could be read again, but that window is bounded by how fast the background goroutine completes — typically sub-millisecond for local writes and a single network round-trip for remote ones.

### Read repair: always repair even when siblings exist

When a quorum read surfaces a conflict (concurrent siblings), the temptation is to skip repair and let the application resolve the conflict first. The problem is that skipping leaves different replicas with different subsets of siblings — one might have `{alice}` and another `{bob}`, when both should see `{alice, bob}`. Repairing even during a conflict ensures all replicas converge to the same sibling set, so the application sees a consistent conflict regardless of which replica it reads from next.

### Hinted handoff: background goroutine for post-quorum failures

The coordinator breaks out of the replica result collection loop as soon as W acks are received. Goroutines for the remaining replicas are still in-flight at that point. Rather than waiting for all of them (which would add up to `REPLICA_TIMEOUT_MS` to write latency in the worst case) or ignoring them (which would miss failures that occurred after the quorum break), a background goroutine is spawned to drain the remaining results from the buffered channel and record hints for any failures. The client receives 204 immediately after quorum; hint bookkeeping happens concurrently.

### Callback pattern

The ring accepts a `SetUpdateCallback` rather than importing Prometheus directly, and the gossip `MemberList` accepts an `onChange` callback rather than holding a ring reference. Both keep internal packages free of external dependencies and make integration points explicit.

---

## API

### Write a value

```bash
curl -X PUT http://localhost:8080/keys/user:123 \
  -H "Content-Type: application/json" \
  -d '{"value": "alice"}'
```

Returns 204. The write is stored locally and fanned out to all replica nodes. An optional `clocks` field can be passed to forward an existing vector clock; if omitted, the receiving node's clock is used as the base.

### Read a value

```bash
curl http://localhost:8080/keys/user:123
```

Returns the primary replica node and the value with the highest vector clock across R replicas. Any node can serve any read - the coordinator fans out to the replica set internally.

```json
{
  "id": "node2",
  "address": "10.0.0.2:8080",
  "status": "alive",
  "value": "alice"
}
```

### Add a node

```bash
curl -X POST http://localhost:8080/nodes \
  -H "Content-Type: application/json" \
  -d '{"id": "node1", "address": "10.0.0.1:8080"}'
```

### Remove a node

```bash
curl -X DELETE http://localhost:8080/nodes/node1
```

### List all nodes

```bash
curl http://localhost:8080/nodes
```

### Get replication nodes

```bash
curl -X POST http://localhost:8080/replicate \
  -H "Content-Type: application/json" \
  -d '{"key": "user:123", "factor": 3}'
```

Returns the N nodes that own replicas of this key - useful for topology inspection.

### Health check

```bash
curl http://localhost:8080/health
```

```json
{
  "status": "ok",
  "total_nodes": 3,
  "healthy_nodes": 3,
  "suspect_nodes": 0,
  "dead_nodes": 0,
  "uptime": "4h32m10s"
}
```

### Get ring stats

```bash
curl http://localhost:8080/stats
```

```json
{
  "total_nodes": 3,
  "total_vnodes": 450,
  "healthy_nodes": 2,
  "suspect_nodes": 1,
  "dead_nodes": 0,
  "distribution": [
    {
      "node_id": "node1",
      "address": "10.0.0.1:8080",
      "vnode_count": 150,
      "key_count": 342,
      "percentage": 34.2
    }
  ],
  "most_loaded": "node1",
  "least_loaded": "node3",
  "variance": 4.22
}
```

### Exchange gossip state

```bash
curl -X POST http://localhost:8080/gossip \
  -H "Content-Type: application/json" \
  -d '{"members": [...]}'
```

Used internally by the gossip protocol. Merges the provided member list and returns this node's current view.

### Get sync state (anti-entropy)

```bash
curl "http://localhost:8080/sync?vnode=<endHash>"
```

Returns the Merkle tree state for a vnode range, computed on-the-fly from this node's store. Used by primary nodes to compare against replicas during background sync.

```json
{
  "root": 3829104721,
  "buckets": [1234567890, 0, 987654321, ...]
}
```

### Sync keys (anti-entropy)

```bash
curl -X POST http://localhost:8080/sync/keys \
  -H "Content-Type: application/json" \
  -d '{"keys": ["user:123", "user:456"]}'
```

Returns the full sibling sets for the requested keys, including vector clocks and tombstone state. Used by the primary to fetch a replica's version of divergent keys during repair.

### Sync bucket keys (anti-entropy)

```bash
curl "http://localhost:8080/sync/bucket-keys?vnode=<endHash>&bucket=<0-15>"
```

Returns the key names in a specific Merkle bucket within a vnode range. Used by the primary during bidirectional sync to discover keys the replica holds that the primary does not.

### Push sync entries (anti-entropy)

```bash
curl -X POST http://localhost:8080/sync/push \
  -H "Content-Type: application/json" \
  -d '{"entries": {"user:123": [{"value": "alice", "clocks": {"node1": 1}}]}}'
```

Applies a batch of entries from the primary to the local store. The replica accepts entries whose vector clocks are newer than or concurrent with what it already holds, using the same conflict resolution logic as a live write.

### Prometheus metrics

```bash
curl http://localhost:8080/metrics
```

---

## Running

### Local

```bash
make run
```

### Docker (3-node cluster with Prometheus + Grafana)

```bash
make docker-run
```

Starts three Continuum nodes that discover each other via gossip. `node1` acts as the seed; `node2` and `node3` bootstrap from it.

| Service    | Address                               |
| ---------- | ------------------------------------- |
| node1 API  | `http://localhost:8080`               |
| node2 API  | `http://localhost:8082`               |
| node3 API  | `http://localhost:8083`               |
| Prometheus | `http://localhost:9090`               |
| Grafana    | `http://localhost:3000` (admin/admin) |

In Grafana, add `http://prometheus:9090` as a Prometheus data source and query:

- `continuum_ring_node_count`
- `continuum_ring_key_lookups_total`
- `continuum_ring_distribution_variance`
- `rate(continuum_http_request_duration_seconds_sum[1m])`
- `continuum_ring_healthy_nodes`
- `continuum_ring_suspect_nodes`
- `continuum_ring_dead_nodes`
- `continuum_ring_vnode_count`
- `rate(continuum_http_requests_total[1m])`

---

## Environment Variables

| Variable             | Default          | Description                                                       |
| -------------------- | ---------------- | ----------------------------------------------------------------- |
| `SELF_ID`            | `SELF_ADDRESS`   | Node identifier                                                   |
| `SELF_ADDRESS`       | `localhost:8080` | HTTP address including port                                       |
| `GOSSIP_PORT`        | `8081`           | UDP port for gossip                                               |
| `REPLICAS`           | `150`            | Virtual nodes per physical node                                   |
| `REPLICATION_FACTOR` | `3`              | Number of replicas per key                                        |
| `WRITE_QUORUM`       | majority         | Replica acks required before returning 204                        |
| `READ_QUORUM`        | majority         | Replica responses required for a consistent read                  |
| `REPLICA_TIMEOUT_MS` | `500`            | Timeout in milliseconds for inter-node replication and read calls |
| `SEED_NODES`         | -                | Comma-separated HTTP addresses to bootstrap from                  |

---

## Development

```bash
make test      # run all tests
make e2e       # run end-to-end tests
make bench     # run benchmarks
make lint      # run golangci-lint
make coverage  # generate coverage report
```

### Generate test traffic

```bash
./scripts/traffic.sh http://localhost:8080 1000
```

---

## Metrics

| Metric                                    | Type      | Description                            |
| ----------------------------------------- | --------- | -------------------------------------- |
| `continuum_http_requests_total`           | Counter   | Request count by method, path, status  |
| `continuum_http_request_duration_seconds` | Histogram | Request latency by method and path     |
| `continuum_ring_node_count`               | Gauge     | Current physical node count            |
| `continuum_ring_vnode_count`              | Gauge     | Current virtual node count             |
| `continuum_ring_key_lookups_total`        | Counter   | Total key lookups performed            |
| `continuum_ring_distribution_variance`    | Gauge     | Key distribution variance across nodes |
| `continuum_ring_healthy_nodes`            | Gauge     | Nodes currently alive per gossip       |
| `continuum_ring_suspect_nodes`            | Gauge     | Nodes currently suspect per gossip     |
| `continuum_ring_dead_nodes`               | Gauge     | Nodes currently dead per gossip        |

---

## Shutdown

Continuum shuts down gracefully on `SIGINT` or `SIGTERM`:

1. Pushes all locally-held keys to alive successor nodes so data survives the departure.
2. Marks self as dead in the member list and broadcasts the updated state to all alive peers (not just the usual fanout of 3). Peers remove this node from their rings immediately rather than waiting up to 10 seconds for the stale threshold.
3. Stops accepting new HTTP connections.
4. Drains in-flight requests with a 30-second timeout.
5. Stops the gossip transport.

---

## What's Next

- **Persistence** - write-ahead log and snapshot-on-shutdown so state survives restarts. A prerequisite for making the tombstone GC safety argument hold across node restarts (currently it relies on restarted nodes losing all in-memory state).
- **Weighted vnodes** - nodes with higher capacity receive proportionally more vnodes for heterogeneous clusters
- **Architecture diagram**
