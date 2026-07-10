# Data Migration (`api`)

> Handles key movement when nodes join or leave the ring.

## Overview

When the cluster topology changes, keys need to move. A new node must acquire the keys in its primary vnode ranges before it can serve reads. A departing node must transfer its keys to successors before it goes offline. These two scenarios have opposite data flow directions - pull on join, push on leave.

## How It Works

### Join - Pull-Based Migration

When a new node starts with `SEED_NODES` set, it marks itself as bootstrapping and begins the following sequence:

1. Gossip exchange with seed nodes propagates the bootstrapping flag to the cluster
2. Existing nodes see the new node as bootstrapping and exclude it from coordinator-role read replica sets
3. The new node starts accepting replica sub-requests (`X-Proxied-From`) so it can receive writes during migration
4. The new node calls `handler.Bootstrap()`, which identifies its primary vnode ranges from the ring and, for each range and bucket, issues `GET /sync/bucket-keys` followed by `POST /sync/keys` to each existing replica to pull all entries in those ranges
5. Pulled entries are merged into the local store through the standard vector clock path
6. On completion, `ml.SetBootstrapping(selfID, false)` is called, propagating the state change via gossip

### Bootstrapping State Machine

```
start with SEED_NODES
        │
        ▼
[bootstrapping=true] ──► gossip propagates
        │
        │ data pull from replicas
        │
        ▼
[bootstrapping=false] ──► gossip propagates
        │
        ▼
each peer receives transition ──► CleanupStaleKeys()
```

While bootstrapping, the node:
- Rejects coordinator-role reads and writes with 503
- Accepts replica sub-requests (writes with `X-Proxied-From`)
- Is excluded from other nodes' read replica sets
- Is excluded from paxos CAS quorum voting (while still counting toward the quorum denominator, like dead members)

When bootstrapping clears, each peer that receives the `MemberBootstrapped` gossip event calls `CleanupStaleKeys`, which evicts keys from the local store that now belong to the new node's primary vnode ranges.

### Rejoin After a Downtime-Gate Wipe

The same state machine covers a second entry point: a node whose [downtime gate](persistence.md) discarded non-empty local storage. It marks itself bootstrapping even without `SEED_NODES`, waits for membership (peers arrive via seeds, gossip, or re-registration; a node still alone after a grace period serves standalone), and pulls with `BootstrapReplicaRanges()` instead of `Bootstrap()` - the **entire replica set**, not just primary ranges. A fresh joiner starts owning nothing, but a wiped rejoiner previously vouched for everything it replicates, and letting it vote in read sets or CAS quorums with absent state serves stale history (fault-harness finding #10).

### Leave - Push-Based Migration

On `SIGINT` or `SIGTERM`, before the HTTP server drains, the node calls `PushKeysToSuccessors`. It iterates all locally-held keys, finds each key's alive successor nodes via the ring (the nodes that will own those keys after this node is gone), and sends a batched `POST /sync/push` request per successor with all relevant entries.

The receiving nodes apply the entries through the standard vector clock merge path. After the push, gossip broadcasts this node as dead to the cluster, and successors update their rings.

## Design Decisions

### Pull on Join over Push on Join

**Choice:** The joining node pulls its own data from existing replicas.

The alternative is push-on-join: every existing node detects the join via gossip and proactively pushes all keys that should belong to the new node. Pull is simpler for two reasons.

First, only the joining node knows which ranges it owns (determined by its position on the ring). Existing nodes would each need to compute which of their keys now belong to the new node - a correct but redundant computation across all N existing nodes simultaneously.

Second, push-on-join leads to redundant data transfer. If a key has 3 replicas and the new node's primary range overlaps with all three, all three existing nodes might push the same key. The joining node would receive 3 copies of the same entry and need to merge them. Pull eliminates this - the joining node requests from one replica per range.

**Tradeoff:** Pull requires the joining node to make N HTTP requests (one per vnode range, potentially one per replica). This is more chattier in terms of request count than a single push from one well-chosen node. For large clusters with many vnode ranges, the bootstrap time is proportional to the number of ranges and the size of their key sets.

### Push on Leave over Wait-for-Anti-Entropy

**Choice:** The departing node actively pushes its keys to successors before shutting down.

The alternative is a graceful drain without data migration - the node stops accepting traffic, lets HTTP requests drain, and relies on anti-entropy to detect the divergence and repair it after the node is gone. The problem is that after the node is removed from all rings, anti-entropy runs only on the remaining nodes. Keys that were exclusively on the departing node (with no replica copies elsewhere) would be lost permanently. Keys with replicas are safe, but a planned departure should not be indistinguishable from a sudden crash.

Push-on-leave also reduces the anti-entropy catch-up window. Instead of waiting up to 30 seconds for the next sync cycle to detect missing keys on successors, the successors receive the keys synchronously during shutdown.

**Tradeoff:** Shutdown is slower - it blocks until `PushKeysToSuccessors` completes, which involves HTTP requests to all alive successors. If a successor is slow or temporarily unreachable, the shutdown holds until the request times out. The 30-second HTTP drain timeout in `main.go` bounds the worst case.

### Bootstrapping Flag over a Separate Coordinator Registry

**Choice:** A `Bootstrapping` field on the member entry in the gossip `MemberList` signals readiness state.

The alternative is a separate in-memory registry in the coordinator that tracks which nodes are bootstrapping. The registry would need to be kept in sync with gossip and consulted on every read fan-out. A field on the member entry is cheaper - it is already propagated by gossip to all nodes and is consulted naturally as part of reading the member list for replica set selection.

**Tradeoff:** The bootstrapping flag is part of the gossip protocol surface. Any change to its semantics requires updating all nodes simultaneously. The flag is a binary state - there is no way to express partial bootstrap completion (e.g., "50% of ranges migrated"). For a more sophisticated migration protocol with progress tracking, a separate registry would be needed.

## See Also

- [Gossip](gossip.md) - propagates the bootstrapping flag and the alive/bootstrapped transitions
- [Ring](ring.md) - determines vnode range ownership for both pull and push
- [Anti-Entropy](antientropy.md) - the background fallback for anything missed during migration
- [API](api.md) - sync endpoints used during both join pull and leave push
