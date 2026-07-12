# The Hash Ring (`internal/ring`)

> The routing engine. Given a key, find the node responsible for it. Nothing else.

## Overview

The ring owns one responsibility - mapping keys to nodes with O(log n) lookups and minimal key movement when the cluster topology changes. It has no knowledge of HTTP, gossip, or storage.

Membership is driven entirely by the gossip layer through a callback registered in `main.go`. The ring is never mutated directly by any other package. When a node joins, dies, or recovers, gossip fires `AddZonedNode` or `RemoveNode` on the ring - the ring has no opinion on why.

## How It Works

### Virtual Nodes

Physical nodes are represented as multiple virtual nodes (vnodes) distributed across the ring. The number of vnodes per node is set by `REPLICAS` (default 150). Each vnode is placed at position `murmur3(nodeID + "#" + index)` on the 2^32 integer ring.

More vnodes per node means a smoother key distribution and less imbalance when a node is added or removed. With 150 vnodes per node in a 3-node cluster, each node owns roughly 150 out of 450 ring positions. When a fourth node joins, it claims roughly 112 positions from its neighbors, redistributing about 25% of the key space - compared to 33% if there were only 3 positions.

### Key Lookup

```
hash(key) → position [0, 2^32)
↓
Red-Black Tree Ceiling(position) → first vnode ≥ position
↓
If none found → wrap to first vnode on ring
↓
If health filter set → skip dead/suspect nodes, walk forward
↓
Return physical node for that vnode
```

The `Ceiling()` operation on the RBT is O(log n) where n is the total vnode count. Wrap-around handles the ring being a logical circle despite being stored as a linear sorted structure.

### Weighted Nodes

A node with weight W receives `round(REPLICAS * W)` vnodes, minimum 1.

A node with `SELF_WEIGHT=2.0` gets twice as many vnodes as a default node and handles roughly twice the key space. A node with `SELF_WEIGHT=0.5` gets half. This allows heterogeneous hardware - a node with more memory or compute can absorb proportionally more load.

### Replication Node Walk

`GetReplicationNodes(key, n)` returns N distinct physical nodes clockwise from a key's ring position. It walks forward through vnodes, collecting physical nodes, skipping vnodes that belong to already-collected nodes, until N distinct nodes are found or the ring is exhausted.

When nodes carry zones (see below), the walk is zone-aware: a node whose zone is already represented in the set is passed over while unvisited nodes might still bring new zones. The first clockwise owner is always taken, so the primary for a key never changes because of zones. If the walk exhausts every node before the set fills - fewer zones than replicas - the passed-over nodes take the remaining slots in ring order, so the replica set never shrinks below `min(n, nodeCount)`.

`GetHealthyReplicationNodes(key, n)` is the sloppy-quorum variant the coordinator read/write paths use. The intended owners come from the zone-aware walk ignoring health, so every node agrees on who owns a key and hints land on real owners. Unhealthy owners are then replaced by healthy substitutes drawn from the rest of the ring in walk order, preferring zones the healthy set does not already cover, and the unhealthy owners are returned separately as the nodes to hint. With no health filter installed it behaves identically to `GetReplicationNodes`.

### Zones

`AddZonedNode(id, address, zone, weight)` labels a node with the failure domain it lives in - a rack, an availability zone, or a data center, whatever granularity a deployment cares about losing at once. The label comes from the node's own `SELF_ZONE` setting and travels to every other node through gossip membership, so each node's local ring computes the same zone-aware placement without any coordination.

An empty zone means unzoned: the node fills any slot and never conflicts with another node. A cluster that sets no zones anywhere gets exactly the pre-zone placement, and a mixed cluster (some nodes zoned, some not) degrades gracefully rather than failing.

### Key Counters

Each physical node carries an atomic `int64` key counter. It increments on each successful single-node lookup (`GetNode`); the replica-set walk (`GetReplicationNodes`) does not touch it. The counter is discarded along with the node when it is removed from the ring. These feed the `/stats` load distribution report and the Prometheus variance gauge.

## Design Decisions

### Zone Spreading in the Walk over a Placement Table

**Choice:** Zone awareness is a filter applied during the clockwise walk - defer nodes whose zone is already represented, fall back to them if zones run out - rather than a separately maintained placement table.

This is the shape of Cassandra's NetworkTopologyStrategy: placement stays a pure function of the ring plus membership metadata, so any node can compute any key's replica set locally and all nodes agree as long as gossip has converged. A placement table (explicit key-range-to-replica assignments) would allow more balanced placement but needs its own replication, versioning, and repair story - the class of problem the consistent hash ring exists to avoid.

**Tradeoff:** Zone-aware placement changes which nodes own which keys the moment zones are configured, exactly like adding a node does. Enabling zones on an existing cluster shifts replica sets, and anti-entropy must repair the moved ranges before the old copies are only found on non-owners. Deferred-then-fallback selection can also skew load toward zones with fewer nodes, since a small zone is picked once per set regardless of its size; balanced zones keep this moot.

**Choice:** `emirpasic/gods` Red-Black Tree, keyed by vnode hash.

A sorted slice gives O(log n) lookups via binary search but O(n) inserts and deletes - element shifting. With 150 vnodes per node and a 10-node cluster, the slice has 1,500 entries. Every join or departure requires shifting up to 150 entries. The RBT gives O(log n) for all three operations. Its `Ceiling()` method also maps directly to the ring's successor semantics without any adaptation.

**Tradeoff:** The RBT has higher per-operation constant overhead than a slice - pointer chasing, color-bit checks, rotations on write. For a system where reads (lookups) vastly outnumber writes (topology changes), a slice would win on raw lookup speed. The choice favors low worst-case write cost over maximum read throughput, which is the right call for a routing layer that must handle node failures without stalling.

### Murmur3 over Cryptographic Hashes

**Choice:** `murmur3` - the same hash function Cassandra uses for consistent hashing.

Murmur3 is faster than MD5 or SHA-1 and has better distribution than FNV-32a for short strings. Security is not a requirement - these hashes determine ring positions and value fingerprints, not authentication or integrity guarantees.

**Tradeoff:** Murmur3 has known collision attacks. An adversary who controls key names can craft inputs that all hash to the same vnode, overloading a single node. For a multi-tenant system with untrusted clients, a cryptographic hash or a keyed PRF would be needed. For this use case the risk is acceptable.

### sync.RWMutex over a Plain Mutex

**Choice:** `sync.RWMutex` for the ring's internal state.

Lookups happen concurrently across all active goroutines handling HTTP requests. Topology changes (add/remove node) are rare - they happen only when gossip detects membership changes. RWMutex allows unlimited concurrent readers with exclusive writers.

**Tradeoff:** RWMutex has higher overhead than a plain mutex on uncontended writes due to reader tracking bookkeeping. In a write-heavy scenario - constant topology churn - this overhead compounds. In a stable cluster the write path is cold enough that the overhead is negligible.

### Atomic Key Counters over Lock-Protected Counters

**Choice:** `sync/atomic.Int64` per physical node for key count tracking.

Incrementing a counter under the ring's write lock would block all concurrent readers during what is otherwise a purely statistical operation. Atomic operations allow counter updates to run concurrently with ongoing lookups at no synchronization cost.

**Tradeoff:** Atomic counters are not composed with the RBT mutation in a single transaction. There is a brief window where a counter reflects a lookup whose underlying ring state has since changed. This is acceptable - key counts are used for load reporting and Prometheus metrics, not routing decisions. Approximate counts are sufficient.

### Callback Pattern over Direct Prometheus Import

**Choice:** `SetUpdateCallback(fn func(nodeCount, vnodeCount int))` instead of importing Prometheus directly.

The ring receives a callback at construction time. On every topology change, it fires `fn(nodeCount, vnodeCount)`. The caller (`main.go`) wires this to `api.UpdateRingMetrics`. The ring itself has no dependency on Prometheus, HTTP, or the API package.

**Tradeoff:** More wiring boilerplate in `main.go`. Each integration point must be explicitly connected by the caller rather than happening automatically. The payoff is that the ring package can be tested in isolation without mocking Prometheus or the HTTP layer.

## See Also

- [Gossip](gossip.md) - fires `AddZonedNode` and `RemoveNode` on membership changes
- [Replication](replication.md) - calls `GetHealthyReplicationNodes` to build replica sets for reads and writes
- [Anti-Entropy](antientropy.md) - queries the ring for vnode ranges to sync
- [Operations](operations.md) - `REPLICAS`, `SELF_WEIGHT`, and `SELF_ZONE` env vars
