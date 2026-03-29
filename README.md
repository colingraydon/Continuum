# Continuum

A distributed consistent hashing ring with virtual nodes and gossip-based peer discovery, built in Go.

Continuum implements the core data routing layer used in distributed systems like Cassandra and DynamoDB - a hash ring that maps keys to nodes with minimal key movement when the cluster topology changes. Nodes discover each other via a gossip protocol, converge on ring state without a central coordinator, and route requests to the correct peer when a key lookup arrives at the wrong node. It exposes an HTTP API, Prometheus metrics, and a Grafana dashboard out of the box.

---

## Architecture

Continuum is organized into four layers:

### Core Ring (`internal/ring`)
The distributed systems engine. Implements the hash ring with a Red-Black Tree, virtual nodes, murmur3 hashing, and atomic key counters. Has no knowledge of HTTP, gossip, or metrics - it is a pure data routing library. Membership is driven entirely by the gossip layer via callbacks; the ring is never mutated directly.

### Gossip Protocol (`internal/gossip`)
Handles cluster membership and failure detection without a central coordinator. Each node maintains a `MemberList` - the single source of truth for membership state. Nodes exchange member lists on a 1-second interval with up to 3 random peers (fanout), propagating membership changes across the cluster in O(log n) rounds.

The gossip layer drives the ring: when membership changes (alive, suspect, dead), a callback updates the ring so routing always reflects the current member state.

### Stats Aggregator (`internal/stats`)
A composition layer that combines ring statistics (vnode distribution, key counts, variance) with gossip membership status (alive/suspect/dead node counts) into a single unified view. Keeps the ring package free of membership concerns.

### HTTP API (`api`)
The transport layer. Exposes ring and gossip operations over HTTP, instruments all requests via Prometheus middleware, and wires the ring, gossip layer, and aggregator together. Handlers are thin - they delegate to the appropriate internal package and serialize the response.

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
3. **Stale loop** (1s interval) - checks every non-self member's `UpdatedAt` timestamp. Members not heard from in 5 seconds transition suspect → dead.

### Bootstrapping

New nodes specify one or more seed nodes via the `SEED_NODES` environment variable. On startup, the node sends its member list to each seed, which triggers a gossip exchange. Within a few seconds the new node's membership has propagated to the full cluster.

### Peer routing

When a key lookup (`GET /keys/:key`) arrives at a node that isn't responsible for that key, the request is proxied to the correct peer rather than returning an error. The proxy sets an `X-Proxied-From` header to prevent forwarding loops - a node that receives a proxied request always serves it directly.

---

## Request flow

A key lookup (`GET /keys/:key`) flows like this:

1. Request hits `metricsMiddleware` - records latency and request count
2. `GetNode` handler extracts the key from the path
3. `ring.GetNode(key)` hashes the key with murmur3, finds the ceiling vnode in the RBT, increments the atomic key counter, returns the physical node
4. If the responsible node is not self, the request is proxied to the peer's address
5. If this node is responsible (or the request was already proxied), the response is serialized with node ID, address, and gossip status

---

## How key lookup works

1. Hash the key using Murmur3 to get a position on the ring (0 to 2^32)
2. Find the first virtual node with hash ≥ key hash using a Red-Black Tree ceiling lookup - O(log n)
3. If no vnode found, wrap around to the first vnode on the ring
4. Return the physical node that vnode belongs to

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

Going from 10 to 500 vnodes adds only 33ns to lookup latency - the distribution benefit of more vnodes is essentially free at read time.

**Concurrent reads vs mixed reads/writes:**

| Workload | Latency |
|---|---|
| Pure reads | 160 ns/op |
| Mixed reads + writes | 940 ns/op |

The 6x slowdown on mixed workloads is expected - write lock acquisition blocks concurrent readers. Node changes are rare in production so this tradeoff is acceptable.

---

## Design Decisions

### MemberList as single source of truth

The ring is a pure routing layer - it has no opinion on membership. All ring mutations flow through a single callback on `MemberList`, so gossip-discovered members, manually registered members (`POST /nodes`), and manually removed members (`DELETE /nodes/:id`) all take the same path. This eliminates the class of bugs where the ring and membership state diverge.

### Red-Black Tree

The ring uses a Red-Black Tree (via `emirpasic/gods`) to store virtual nodes sorted by hash. This gives O(log n) for insert, delete, and ceiling lookup. A sorted slice would give O(log n) lookup via binary search but O(n) insert/delete due to element shifting. Either works since node changes are rare, but the RBT is more correct under write load and the `Ceiling()` operation maps directly to the ring's successor lookup semantics.

### Murmur3 Hash Function

Murmur3 is faster than cryptographic hashes (MD5, SHA) and has better distribution than FNV-32a for short strings. It's the same hash function Cassandra uses for consistent hashing. Since security is not a requirement here, the non-cryptographic nature is a non-issue.

### sync.RWMutex

The ring uses `sync.RWMutex` rather than a plain mutex or lock-free structure. `RWMutex` allows unlimited concurrent readers with exclusive writers - correct for a ring where key lookups vastly outnumber topology changes.

### Atomic Key Counters

Per-node key counts use `sync/atomic.Int64` rather than incrementing under the write lock. This keeps `GetNode` on the read lock path so multiple goroutines can look up keys concurrently. Using a write lock for counting would serialize all lookups - a significant regression at high TPS.

### Callback Pattern for Metrics and Ring

The ring accepts a `SetUpdateCallback` rather than importing Prometheus directly, and the gossip `MemberList` accepts an `onChange` callback rather than holding a ring reference. Both keep internal packages free of external dependencies and make the integration points explicit.

### Configurable Replica Count

Replica count is read from the `REPLICAS` environment variable with a default of 150.

---

## API

### Add a node
```bash
curl -X POST http://localhost:8080/nodes \
  -H "Content-Type: application/json" \
  -d '{"id": "node1", "address": "10.0.0.1"}'
```

### Remove a node
```bash
curl -X DELETE http://localhost:8080/nodes/node1
```

### List all nodes
```bash
curl http://localhost:8080/nodes
```

### Look up a key
```bash
curl http://localhost:8080/keys/user:123
```

Returns the node responsible for the key. If this node is not responsible, the request is automatically proxied to the correct peer.

```json
{"id": "node2", "address": "10.0.0.2", "status": "alive"}
```

### Get replication nodes
```bash
curl -X POST http://localhost:8080/replicate \
  -H "Content-Type: application/json" \
  -d '{"key": "user:123", "factor": 3}'
```

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

Node status reflects gossip membership state: `healthy_nodes` are alive per gossip, `suspect_nodes` haven't been heard from recently, `dead_nodes` have been removed from the ring.

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
      "address": "10.0.0.1",
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

Used internally by the gossip protocol. Merges the provided member list into this node's view and returns the node's current member list.

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

- **Architecture Diagram** - use Lucid to generate an architectural diagram for the readme
- **Persistence** - ring state survives restarts via a simple JSON snapshot on shutdown and reload on startup
- **Weighted nodes** - nodes with higher capacity receive proportionally more vnodes, allowing heterogeneous clusters
- **Suspect-aware routing** - exclude suspect nodes from key routing in addition to dead nodes, with configurable fallback behavior
