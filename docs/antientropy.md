# Anti-Entropy (`internal/antientropy`)

> Background repair layer that detects and corrects divergent replicas using Merkle trees.

## Overview

Anti-entropy is the durable safety net beneath hinted handoff and read repair. Hinted handoff closes the gap for replicas that missed writes while briefly down. Read repair fixes stale replicas inline on reads. Anti-entropy covers everything else - replicas that diverged while hints were lost, nodes that restarted and lost in-memory hints, or any divergence that slipped through the other two layers.

The primary node for each vnode range owns an in-memory Merkle tree for that range. Every sync tick (`SYNC_INTERVAL_MS`, default 30 seconds), the next vnode range in a deterministic round-robin order is compared against each of its replicas. Divergent buckets are reconciled bidirectionally. When ring membership changes the set of ranges this node is primary for, the manager detects it at the next tick and rebuilds its trees from a store scan.

## How It Works

### Merkle Trees

Each Merkle tree covers one vnode range and is partitioned into 16 hash buckets. Keys within the vnode range are assigned to buckets by `keyHash % 16`. Each bucket's hash is a murmur3 digest of its sorted (key, entry-hash) pairs, where an entry hash folds together each sibling's value hash and a canonical hash of its vector clock. The tree's root hash is a murmur3 digest of the 16 bucket hashes in order.

Comparing two Merkle trees requires sending 16 bucket hashes (64 bytes) plus the root hash. If the root hashes match, the replicas are in sync and no further work is done. If the root hashes differ, the per-bucket comparison identifies exactly which buckets diverged, bounding the sync work to only the affected keys.

**Why 16 buckets:** 16 is a power of 2 that divides the 2^32 hash space evenly. It is small enough that comparing bucket hashes across the network is cheap (a single small JSON payload per replica), and large enough to give granular divergence detection. With 16 buckets, a single divergent key triggers a sync of 1/16 of the vnode range rather than the entire range.

### Precomputed Value Hashes

Each store entry carries a `Hash uint32` - a murmur3 hash of the value computed at write time. The entry hash fed to the trees XORs each sibling's value hash with a canonical hash of its vector clock, so two replicas holding the same value at different clocks still hash differently - without the clock component, that divergence would be invisible to sync forever (a fault-harness finding). When the anti-entropy manager registers an `onUpdate` callback with the store, it is notified of every accepted write with the key and its new entry hash. The Merkle tree is updated incrementally - no full scan needed after each write.

### Sync Cycle

Every sync tick, the manager first re-derives its primary ranges from the ring and rebuilds its trees if membership changed them (a cheap comparison otherwise), then advances a round-robin cursor over the sorted vnode ends and runs `syncWithReplica` against each replica in that range's replica set. Cycling guarantees every primary range is synced exactly once per `N` ticks, where `N` is the number of primary vnodes; random selection had an unbounded worst case.

`syncWithReplica` fetches the replica's Merkle state (`GET /sync?vnode=<endHash>`) - the replica computes its bucket hashes on-the-fly from its store. It then compares each bucket hash against the primary's local tree. For each divergent bucket, `syncBucket` is called.

### Bidirectional Sync

For each divergent bucket, the primary:

1. Fetches the replica's key list for that bucket (`GET /sync/bucket-keys`)
2. Computes the union of local keys and remote keys
3. Pushes entries the replica is missing or behind on via `POST /sync/push`
4. Fetches the replica's versions of keys the primary is missing or behind on via `POST /sync/keys`, then merges them locally

Vector clock semantics handle all cases throughout - dominated entries are dropped silently, concurrent entries become siblings, idempotent entries are no-ops.

### Tombstone GC

Every 5 minutes, `GCTombstones` runs on the store. An entry is eligible for GC if:

- It has exactly one sibling
- That sibling is a tombstone (`Deleted=true`)
- The tombstone is older than `GCTTL` (24 hours)

Eligible entries are purged from the store and removed from the primary's Merkle trees via the `onEvict` callback.

The 24-hour TTL provides roughly 2,880 anti-entropy sync cycles of headroom (30-second interval × 2,880 ≈ 24 hours) for tombstones to propagate to all live replicas before GC runs.

> **Failure Mode - Tombstone Resurrection**
>
> If a node is partitioned for longer than `GCTTL` and the primary GCs a tombstone during that window, a naive reconnect would resurrect the deleted key from the stale live value the partitioned node still holds. With persistence enabled (`DATA_DIR`), the recovery driver enforces this invariant in code: a node whose last clean shutdown is older than `GCTTL` discards its local data and re-bootstraps from peers, so it can never reintroduce a value that other replicas have already GC'd. With persistence disabled, the same property holds for the trivial reason that an in-memory node loses all state on restart. See [docs/persistence.md](persistence.md) for the recovery flow.

## Design Decisions

### Primary-Driven Sync over Gossip-Driven Sync

**Choice:** The vnode primary initiates all sync. Replicas respond passively.

The alternative is gossip-driven sync where any node can initiate repair with any other. Primary-driven sync is simpler - only one node per vnode range runs the comparison logic, so there are no races between two nodes simultaneously syncing the same range in opposite directions. The primary is also the node with the authoritative Merkle tree in memory, so it is the natural initiator.

**Tradeoff:** If the primary is down, anti-entropy for that vnode range does not run until the primary recovers or a successor is elected. Read repair and hinted handoff are the fallbacks during that window. In the current design there is no automatic failover of the primary role - it is determined by ring position, which changes only when nodes join or leave.

### On-the-Fly Replica Trees over Persistent Replica Trees

**Choice:** Replicas compute their bucket hashes on-the-fly from their store when queried. They do not maintain persistent Merkle trees.

The alternative is for every node to maintain a full Merkle tree for every vnode range it holds, whether it is the primary or a replica. This would halve the sync work per round (no need to fetch and compute bucket hashes on demand) but triples the total memory used for Merkle trees in a 3-node cluster.

On-the-fly computation keeps the replica code path simple - a replica just scans its store for keys in the requested range and XORs their hashes. This computation is O(keys in range) but is done lazily and only when the primary asks. In practice the computation is fast enough that it does not noticeably affect the sync cycle duration.

**Tradeoff:** On-the-fly computation means the replica is doing O(n) work per sync request rather than O(1) (lookup precomputed hash). For large vnode ranges with many keys, this could be slow. The current design is appropriate for clusters where key counts per vnode range are in the thousands, not millions.

### TTL-Based Tombstone GC over Per-Replica Confirmation Tracking

**Choice:** Purge tombstones after `GCTTL` (24 hours), relying on anti-entropy coverage plus the recovery driver's downtime gate.

Per-replica confirmation tracking would have the primary record which replicas have acknowledged a tombstone and purge it only when all live replicas have confirmed receipt. This is strictly safer - no resurrection risk. But it requires the primary to persist ack state, handle replica churn (nodes joining mid-tracking), and define what "all replicas" means dynamically.

Bidirectional sync makes the TTL approach viable. Because the primary pushes tombstones to replicas on every sync round (not just pulling from them), a tombstone written on any node propagates outward within a few sync cycles regardless of whether the replica ever initiates contact. 2,880 sync cycles of headroom before GC is more than enough for any live replica to receive the tombstone.

**Tradeoff:** A node partitioned (or shut down) for longer than `GCTTL` would risk resurrecting GC'd tombstones if it rejoined with stale live values. With persistence enabled, the recovery driver enforces a max-downtime invariant: such a node refuses to load its local data and re-bootstraps from peers instead. The trade is that any writes it accepted that hadn't reached quorum are lost - the same risk Cassandra accepts with `gc_grace_seconds`.

### One Vnode per Tick, Round-Robin

**Choice:** One vnode synced per tick (`SYNC_INTERVAL_MS`, default 30 seconds), cycling deterministically through the sorted primary vnode ends.

A shorter interval would detect divergence faster but add more background HTTP traffic between nodes. A longer interval would reduce traffic but widen the window during which replicas are inconsistent. 30 seconds is a reasonable default for a system where hinted handoff and read repair handle the acute cases - anti-entropy is the slow but thorough backstop. Round-robin (rather than the original random sampling, a fault-harness finding) bounds a full keyspace pass at exactly `vnodes x interval` instead of a coupon-collector expected time with an unbounded tail.

**Tradeoff:** A full pass still scales linearly with vnode count: a node primary for 150 vnodes at the default interval takes 75 minutes per pass. Tune `SYNC_INTERVAL_MS` or `REPLICAS` when faster convergence matters; prioritizing ranges with known divergence or syncing several vnodes per tick are the next steps if that isn't enough.

### Primary Ranges Rebuilt on Membership Change

**Choice:** Each sync tick re-derives the primary ranges from the ring and, only when they changed, rebuilds the Merkle trees with one `KeyHashes` scan.

Ranges were previously computed once at startup, so a node that started alone considered itself primary for the whole keyspace forever (another fault-harness finding). Comparing the range set is cheap enough to do every tick; the expensive scan only runs on actual membership events, which are rare.

**Tradeoff:** A rebuild discards incremental tree state and rescans the store, which is O(total data). A future refinement could diff the old and new range sets and move only the affected keys.

## See Also

- [Ring](ring.md) - provides vnode ranges and replica sets
- [KV Store](../internal/store/store.go) - source of truth for all entries; fires `onUpdate` and `onEvict` callbacks
- [Read Repair](read-repair.md) - inline complement that catches divergence on the read path
- [Hinted Handoff](hinted-handoff.md) - the faster-reacting durability layer anti-entropy backs up
- [API](api.md) - sync endpoints (`/sync`, `/sync/keys`, `/sync/bucket-keys`, `/sync/push`)
