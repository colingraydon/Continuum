# Continuum

A distributed consistent hashing ring with virtual nodes, built in Go.

Continuum implements the core data routing layer used in distributed systems like Cassandra and DynamoDB - a hash ring that maps keys to nodes with minimal key movement when the cluster topology changes. It exposes an HTTP API, Prometheus metrics, and a Grafana dashboard out of the box.

---

## Architecture

Continuum is organized into four layers:

### Core Ring (`internal/ring`)
The distributed systems engine. Implements the hash ring with a Red-Black Tree, virtual nodes, murmur3 hashing, and atomic key counters. Has no knowledge of HTTP, health, or metrics - it is a pure data routing library.

### Health Checker (`internal/health`)
A standalone component that periodically pings each node's `/health` endpoint and tracks a three-state lifecycle: healthy → suspect → dead. Designed to plug directly into a gossip protocol - the `OnStatusChange` callback is the integration point where gossip peer reports would replace direct HTTP pings.

### Stats Aggregator (`internal/stats`)
A composition layer that combines ring statistics (vnode distribution, key counts, variance) with health status (healthy/suspect/dead node counts) into a single unified view. Keeps the ring package free of health concerns.

### HTTP API (`api`)
The transport layer. Exposes ring operations over HTTP, instruments all requests via Prometheus middleware, and wires the ring, health checker, and aggregator together. Handlers are thin - they delegate to the appropriate internal package and serialize the response.

### Request flow

A key lookup (`GET /keys/:key`) flows like this:

1. Request hits `metricsMiddleware` - records latency and request count
2. `GetNode` handler extracts the key from the path
3. `ring.GetNode(key)` hashes the key with murmur3, finds the ceiling vnode in the RBT, increments the atomic key counter, returns the physical node
4. `checker.GetStatus(nodeID)` returns the node's current health status
5. Response is serialized with node ID, address, and status

### How key lookup works

1. Hash the key using Murmur3 to get a position on the ring (0 to 2^32)
2. Find the first virtual node with hash ≥ key hash using a Red-Black Tree ceiling lookup - O(log n)
3. If no vnode found, wrap around to the first vnode on the ring
4. Return the physical node that vnode belongs to

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

### Red-Black Tree

The ring uses a Red-Black Tree (via `emirpasic/gods`) to store virtual nodes sorted by hash. This gives O(log n) for insert, delete, and ceiling lookup. A sorted slice would give O(log n) lookup via binary search but O(n) insert/delete due to element shifting. Either works since node changes are rare, but the RBT is more correct under write load and the `Ceiling()` operation maps directly to the ring's successor lookup semantics.

### Murmur3 Hash Function

Murmur3 is faster than cryptographic hashes (MD5, SHA) and has better distribution than FNV-32a for short strings. It's the same hash function Cassandra uses for consistent hashing. Since security is not a requirement here, the non-cryptographic nature is a non-issue.

### sync.RWMutex

The ring uses `sync.RWMutex` rather than a plain mutex or lock-free structure. `RWMutex` allows unlimited concurrent readers with exclusive writers - correct for a ring where key lookups vastly outnumber topology changes.

### Atomic Key Counters

Per-node key counts use `sync/atomic.Int64` rather than incrementing under the write lock. This keeps `GetNode` on the read lock path so multiple goroutines can look up keys concurrently. Using a write lock for counting would serialize all lookups - a significant regression at high TPS.

### Callback Pattern for Metrics

The ring accepts a `SetUpdateCallback` rather than importing Prometheus directly. This keeps `internal/ring` as a pure distributed systems package with no API or metrics dependencies. The metrics concern lives entirely in the `api` package, making the ring reusable as a library.

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

### Look up a key
```bash
curl http://localhost:8080/keys/user:123
```

### Get replication nodes
```bash
curl -X POST http://localhost:8080/replicate \
  -H "Content-Type: application/json" \
  -d '{"key": "user:123", "factor": 3}'
```

### Get ring stats
```bash
curl http://localhost:8080/stats
```

```json
{
  "total_nodes": 3,
  "total_vnodes": 450,
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

### Docker (with Prometheus + Grafana)
```bash
make docker-run
```

- API: `http://localhost:8080`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (admin/admin)

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

---

## What's Next

- **Architecture Diagram** - use Lucid to generate an architectural diagram for the readme
- **Gossip protocol** - nodes discover each other and converge on ring state without a central coordinator
- **Node health checks** - background goroutine pings nodes and removes dead ones automatically
- **Persistence** - ring state survives restarts
- **Weighted nodes** - nodes with higher capacity receive proportionally more vnodes