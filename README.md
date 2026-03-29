# Continuum

A distributed key-value store built on consistent hashing, gossip-based membership, and vector clock conflict resolution — written in Go.

Continuum implements the core data layer used in systems like Cassandra and Dynamo: a hash ring that maps keys to nodes with minimal disruption when topology changes, a gossip protocol that propagates membership without a central coordinator, and a replication layer that fans writes out to N nodes and resolves conflicts using vector clocks. It exposes an HTTP API, Prometheus metrics, and a Grafana dashboard out of the box.

---

## Architecture

Continuum is organized into five layers:

### Core Ring (`internal/ring`)
The routing engine. Implements the hash ring with a Red-Black Tree, virtual nodes, murmur3 hashing, and atomic key counters. Has no knowledge of HTTP, gossip, or storage — it is a pure routing library. Membership is driven entirely by the gossip layer via callbacks; the ring is never mutated directly.

### Gossip Protocol (`internal/gossip`)
Handles cluster membership and failure detection without a central coordinator. Each node maintains a `MemberList` — the single source of truth for membership state. Nodes exchange member lists on a 1-second interval with up to 3 random peers (fanout), propagating membership changes across the cluster in O(log n) rounds.

The gossip layer drives the ring: when membership changes (alive, suspect, dead), a callback updates the ring so routing always reflects current cluster state.

### KV Store (`internal/store`)
In-memory key-value storage with vector clock versioning. Each entry holds a value, a `VectorClockVersion`, and a precomputed murmur3 hash of the value (reserved for future Merkle tree anti-entropy). Conflict resolution uses the standard Lamport partial order: a write is accepted only if the existing entry's clock happens-before the incoming one. Concurrent writes keep the existing value.

### Stats Aggregator (`internal/stats`)
A composition layer that combines ring statistics (vnode distribution, key counts, variance) with gossip membership status (alive/suspect/dead node counts) into a single unified view. Keeps the ring package free of membership concerns.

### HTTP API (`api`)
The transport layer. Exposes ring, gossip, and storage operations over HTTP, instruments all requests via Prometheus middleware, and wires all internal packages together. Handlers are thin — they delegate to the appropriate internal package and serialize the response.

---

## Gossip Protocol

### Membership lifecycle

Nodes transition through three states: **alive → suspect → dead**.

- A node is marked **alive** when it joins the cluster (via `POST /nodes` or gossip peer exchange) and its heartbeat is propagating
- A node is marked **suspect** when its heartbeat hasn't been updated within 5 seconds (`staleThreshold`) — it may be slow or partitioned
- A node is marked **dead** when it remains suspect past a second stale check — it is removed from the ring and stops receiving traffic

Recovery is automatic: if a dead or suspect node starts gossiping again with a higher heartbeat, it transitions back to alive and is re-added to the ring.

### Peer discovery and convergence

Each node runs three background loops:

1. **Gossip loop** (1s interval) — increments its own heartbeat, selects up to 3 random alive peers, and pushes its full member list to each. A new member propagates to the full cluster in O(log n) rounds.
2. **Receive loop** — handles incoming gossip messages. Merges the peer's member list using a last-write-wins strategy based on heartbeat: a member update is only accepted if the incoming heartbeat is strictly higher than what's known locally.
3. **Stale loop** (1s interval) — checks every non-self member's `UpdatedAt` timestamp. Members not heard from in 5 seconds transition alive → suspect → dead.

### Bootstrapping

New nodes specify one or more seed nodes via the `SEED_NODES` environment variable. On startup, the node sends its member list to each seed, which triggers a gossip exchange. Within a few seconds the new node's membership has propagated to the full cluster.

---

## Replication

When a write arrives at any node, it:

1. Determines the replica set for the key using `GetReplicationNodes(key, factor)` — the N consecutive distinct nodes clockwise from the key's ring position
2. Increments its own vector clock counter and stores the value locally (self counts as one acknowledgment)
3. Fans out the write to all other replicas in parallel, waiting for W acknowledgments before returning 204
4. Returns 503 if quorum cannot be reached

Replica nodes store the write as-is without further fan-out, identified by the `X-Proxied-From` header.

Replication factor is configured via `REPLICATION_FACTOR` (default: 3). Write quorum W is configured via `WRITE_QUORUM` (default: majority — `floor(RF/2) + 1`).

### Consistent reads

When a read arrives at any node, it fans out to the full replica set and waits for R responses. The response with the highest vector clock is returned to the client. If R replicas cannot be reached, the request returns 503.

Read quorum R is configured via `READ_QUORUM` (default: majority). Setting `READ_QUORUM=1` gives lowest-latency reads but may return stale data; `READ_QUORUM=REPLICATION_FACTOR` gives the strongest consistency guarantee at the cost of latency.

---

## Vector Clocks

Each write carries a vector clock — a map from node ID to a logical counter. The writing node increments its own counter before storing and replicating.

```json
{"clocks": {"node1": 3, "node2": 1}}
```

This clock means: node1 has coordinated 3 writes, node2 has coordinated 1, and any replica that received all of them would accept this as the current version.

**Conflict resolution** uses the standard partial order:

- Clock A **happens-before** B if every counter in A is ≤ B's and at least one is strictly less → B wins
- **Concurrent** clocks (neither happens-before the other) → existing value is kept
- **Equal** clocks → existing value is kept

The `Version` interface (`HappensBefore(other Version) bool`) is the only contract the store depends on. Swapping in a different conflict resolution strategy — or surfacing concurrent writes to the client as siblings — requires only a new type implementing that interface.

---

## Request flow

A write (`PUT /keys/:key`) flows like this:

1. Request hits `metricsMiddleware` — records latency and request count
2. `PutKey` handler extracts the key and decodes `{"value": "...", "clocks": {...}}`
3. Incoming clock is incremented for this node; value is stored in the local `Store`
4. `ring.GetReplicationNodes(key, factor)` returns the replica set
5. Goroutines fan the write out to each non-self replica with `X-Proxied-From` set
6. Coordinator waits for W acknowledgments (self counts as one); returns 204 on quorum, 503 if quorum cannot be reached

A key lookup (`GET /keys/:key`) flows like this:

1. `GetNode` handler extracts the key
2. `ring.GetReplicationNodes(key, factor)` returns the replica set
3. Goroutines fan the read out to each replica with `X-Proxied-From` set; each replica returns its local entry with its vector clock
4. The coordinator waits for R responses, picks the entry with the highest vector clock, and returns it

---

## How key lookup works

1. Hash the key using Murmur3 to get a position on the ring (0 to 2^32)
2. Find the first virtual node with hash ≥ key hash using a Red-Black Tree ceiling lookup — O(log n)
3. If no vnode found, wrap around to the first vnode on the ring
4. If a health filter is set, walk forward skipping dead/suspect nodes
5. Return the physical node that vnode belongs to

---

## Benchmarks

Measured on Apple M3 Max:

| Operation | Throughput | Latency |
|---|---|---|
| GetNode (3 nodes) | ~9M ops/sec | 112 ns/op |
| GetNode (100 nodes) | ~5M ops/sec | 201 ns/op |
| GetNode (parallel) | ~6M ops/sec | 160 ns/op |
| AddNode | ~8K ops/sec | 116 µs/op |
| RemoveNode | ~10K ops/sec | 101 µs/op |

**Replica count impact on lookup latency:**

| Replicas | Latency |
|---|---|
| 10 | 96 ns/op |
| 50 | 105 ns/op |
| 150 | 114 ns/op |
| 500 | 129 ns/op |

Going from 10 to 500 vnodes adds only 33ns to lookup latency — the distribution benefit of more vnodes is essentially free at read time.

**Concurrent reads vs mixed reads/writes:**

| Workload | Latency |
|---|---|
| Pure reads | 160 ns/op |
| Mixed reads + writes | 940 ns/op |

The 6x slowdown on mixed workloads is expected — write lock acquisition blocks concurrent readers. Node changes are rare in production so this tradeoff is acceptable.

---

## Design Decisions

### MemberList as single source of truth

The ring is a pure routing layer — it has no opinion on membership. All ring mutations flow through a single callback on `MemberList`, so gossip-discovered members, manually registered members (`POST /nodes`), and manually removed members (`DELETE /nodes/:id`) all take the same path. This eliminates the class of bugs where ring and membership state diverge.

### Vector clocks over LWW timestamps

Last-write-wins timestamps are simple but lose writes silently when two clients write to the same key concurrently. Vector clocks track causality per-node, so concurrent writes are detectable rather than silently resolved by wall clock. The `Version` interface means the conflict strategy is swappable — the store has no dependency on the specific implementation.

### Precomputed value hashes

Each store entry carries `Hash uint32` — a murmur3 hash of the value, computed at write time. This is reserved for Merkle tree anti-entropy: when comparing replica state across nodes, leaf hashes let you identify divergent key ranges without transferring values. Computing at write time makes tree construction cheap.

### Red-Black Tree

The ring uses a Red-Black Tree (via `emirpasic/gods`) to store virtual nodes sorted by hash. This gives O(log n) for insert, delete, and ceiling lookup. A sorted slice would give O(log n) lookup via binary search but O(n) insert/delete due to element shifting. The RBT's `Ceiling()` operation also maps directly to the ring's successor lookup semantics.

### Murmur3

Murmur3 is faster than cryptographic hashes (MD5, SHA) and has better distribution than FNV-32a for short strings. It's the same hash function Cassandra uses for consistent hashing. Since security is not a requirement here, the non-cryptographic nature is a non-issue.

### sync.RWMutex

The ring uses `sync.RWMutex` rather than a plain mutex. `RWMutex` allows unlimited concurrent readers with exclusive writers — correct for a ring where key lookups vastly outnumber topology changes.

### Atomic key counters

Per-node key counts use `sync/atomic.Int64` rather than incrementing under the write lock. This keeps `GetNode` on the read lock path so multiple goroutines can look up keys concurrently.

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

Returns the primary replica node and the value with the highest vector clock across R replicas. Any node can serve any read — the coordinator fans out to the replica set internally.

```json
{"id": "node2", "address": "10.0.0.2:8080", "status": "alive", "value": "alice"}
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

Returns the N nodes that own replicas of this key — useful for topology inspection.

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
      "percentage": 34.20
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

| Service | Address |
|---|---|
| node1 API | `http://localhost:8080` |
| node2 API | `http://localhost:8082` |
| node3 API | `http://localhost:8083` |
| Prometheus | `http://localhost:9090` |
| Grafana | `http://localhost:3000` (admin/admin) |

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

| Variable | Default | Description |
|---|---|---|
| `SELF_ID` | `SELF_ADDRESS` | Node identifier |
| `SELF_ADDRESS` | `localhost:8080` | HTTP address including port |
| `GOSSIP_PORT` | `8081` | UDP port for gossip |
| `REPLICAS` | `150` | Virtual nodes per physical node |
| `REPLICATION_FACTOR` | `3` | Number of replicas per key |
| `WRITE_QUORUM` | majority | Replica acks required before returning 204 |
| `READ_QUORUM` | majority | Replica responses required for a consistent read |
| `SEED_NODES` | — | Comma-separated HTTP addresses to bootstrap from |

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

| Metric | Type | Description |
|---|---|---|
| `continuum_http_requests_total` | Counter | Request count by method, path, status |
| `continuum_http_request_duration_seconds` | Histogram | Request latency by method and path |
| `continuum_ring_node_count` | Gauge | Current physical node count |
| `continuum_ring_vnode_count` | Gauge | Current virtual node count |
| `continuum_ring_key_lookups_total` | Counter | Total key lookups performed |
| `continuum_ring_distribution_variance` | Gauge | Key distribution variance across nodes |
| `continuum_ring_healthy_nodes` | Gauge | Nodes currently alive per gossip |
| `continuum_ring_suspect_nodes` | Gauge | Nodes currently suspect per gossip |
| `continuum_ring_dead_nodes` | Gauge | Nodes currently dead per gossip |

---

## What's Next

- **Conflict surfacing** — return concurrent writes as siblings rather than silently keeping the existing value
- **Merkle anti-entropy** — use precomputed value hashes to efficiently detect and repair divergent replicas
- **Graceful shutdown** — drain in-flight requests and gossip `MemberDead` for self before exit
- **Persistence** — snapshot ring and KV state to disk on shutdown, reload on startup
- **Weighted vnodes** — nodes with higher capacity receive proportionally more vnodes for heterogeneous clusters
- **Architecture diagram**
