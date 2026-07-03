# Gossip Protocol (`internal/gossip`)

> Cluster membership and failure detection without a central coordinator.

## Overview

The gossip layer owns one thing - answering the question "which nodes are alive right now?" It maintains a `MemberList` that is the single source of truth for cluster membership. Everything that depends on membership state - the ring, hinted handoff delivery, bootstrapping - reads from the `MemberList` or registers a callback to be notified when it changes.

The gossip layer does not directly modify the ring, the store, or the hint buffer. It fires an `onChange` callback when a member's status changes, and the caller (`main.go`) wires that callback to the appropriate downstream actions.

## How It Works

### Transport

Gossip messages are sent over **UDP**. Each message is a JSON-encoded member list. The transport uses a single `UDPConn` with a 65,536-byte receive buffer - the maximum UDP payload size. If a member list grows large enough that a single message exceeds 65KB, the message is dropped. In practice this is not a concern until a cluster reaches hundreds of nodes.

UDP is fire-and-forget. A gossip message that is lost in transit is not retried. The protocol tolerates this because membership updates propagate redundantly - the same information arrives from multiple peers within a few seconds.

### MemberList

Each member entry carries:
- `ID` - the node's identifier
- `Address` - the node's HTTP address
- `Status` - `alive`, `suspect`, or `dead`
- `Incarnation` - the node's epoch, advanced only by the node itself; the primary precedence key when merging gossip (see [Incarnation Numbers](#incarnation-numbers))
- `Heartbeat` - a logical counter the node increments every gossip tick; breaks ties within an incarnation
- `UpdatedAt` - the local timestamp of the last accepted update
- `Weight` - the node's capacity weight for vnode allocation
- `Bootstrapping` - whether the node is still migrating data on join

The `MemberList` is also the source of the health filter installed on the ring. The ring calls `ml.Get(id)` to check whether a node is alive before routing to it.

### Membership Lifecycle

```
        join
          │
          ▼
       [alive]  ◄──────────── heartbeat resumes
          │                         │
     5s no heartbeat                │
          │                         │
          ▼                         │
      [suspect]                     │
          │                         │
     5s still no heartbeat          │
          │                         │
          ▼
        [dead] ──────────── removed from ring
```

- A node is marked **alive** when it sends its first gossip message or when a peer reports it alive with a superseding `(incarnation, heartbeat)`
- A node transitions to **suspect** if no heartbeat update arrives within 5 seconds
- A node transitions to **dead** if it remains suspect for another 5 seconds - it is removed from the ring and stops receiving coordinator-role traffic
- Recovery is automatic - a dead or suspect node that gossips again supersedes its stale entry once its `(incarnation, heartbeat)` outranks it. A crash-restarted node whose heartbeat reset to zero relies on its incarnation for this (see [Incarnation Numbers](#incarnation-numbers))

> **Failure Mode - Network Partition**
>
> If a network partition isolates a subset of nodes, both sides will eventually mark the other side's nodes as dead and remove them from their rings. Each partition continues serving traffic for the keys it can reach quorum on. When the partition heals, gossip reconverges within O(log n) rounds and the rings are repopulated. Keys that diverged during the partition will be reconciled by anti-entropy and read repair.

> **Failure Mode - Split Brain**
>
> With an even-sized cluster and a symmetric partition (N/2 on each side), neither side can reach write quorum for keys whose replica set spans the partition. Both sides return 503 for those writes rather than accepting divergent writes. This is a deliberate choice - availability for partitioned writes would require relaxing quorum, which is a configuration decision left to the operator via `WRITE_QUORUM`.

### Three Gossip Loops

**Gossip loop** (1-second interval) - each node increments its own heartbeat, selects up to 3 random alive peers, and pushes its full member list to each. A membership change propagates to the full cluster in O(log n) gossip rounds, where n is the cluster size. With fanout=3 and a 10-node cluster, any update reaches all nodes within 3-4 rounds (3-4 seconds).

**Receive loop** - handles incoming gossip messages. For each member in the incoming list, the update is accepted only if its `(incarnation, heartbeat)` supersedes what is known locally: incarnation dominates, and heartbeat breaks ties within the same incarnation. This prevents stale gossip from reverting newer state while still letting a crash-restarted node reclaim its identity (see [Incarnation Numbers](#incarnation-numbers)).

**Stale loop** (1-second interval) - checks every non-self member's `UpdatedAt` timestamp. Members not heard from in 5 seconds transition alive → suspect → dead.

### Bootstrapping State

A joining node sets `Bootstrapping=true` on itself before it starts the gossip exchange with seed nodes. This flag propagates via gossip. While bootstrapping:

- Other nodes exclude the bootstrapping node from coordinator-role read replica sets
- The bootstrapping node rejects coordinator-role requests with 503
- The bootstrapping node still accepts replica sub-requests (identified by `X-Proxied-From`) so it can receive writes during migration

When data migration completes, the node sets `Bootstrapping=false`. This propagates via gossip. Each peer that receives the transition fires the `MemberBootstrapped` callback, which triggers key eviction for keys that now belong to the new node.

### Seed Nodes

`SEED_NODES` is a comma-separated list of existing node addresses. On startup, the joining node pushes its member list to each seed with the `WantReply` flag set. A seed that receives a `WantReply` message pushes its own member list straight back to the sender. Within a few rounds, the joining node's membership has propagated to the full cluster, and the existing cluster's membership has propagated back to the new node.

The reply matters most on **rejoin**. A brand-new node is unknown to its seeds, so they naturally start gossiping to it. But a node that crashed and stayed down long enough to be marked `dead` is excluded from every peer's alive set (`selectPeers` reads `GetAlive`), so no peer would gossip to it - it would never receive the cluster view, nor the stale entry it needs to refute. The bootstrap reply closes that gap: the seed answers even a node it holds `dead`. The reply carries `WantReply=false`, so it never triggers a reply of its own - a bootstrap is at most two datagrams per seed.

### Incarnation Numbers

Each node carries an **incarnation** number - an epoch that only the node itself advances. When merging gossip about a member, incarnation is the primary precedence key and heartbeat only breaks ties within the same incarnation.

This exists to fix crash-rejoin. A restarted node's heartbeat resets to zero, but peers still hold its pre-crash heartbeat (potentially in the hundreds). Under a heartbeat-only rule, the restarted node's gossip would be rejected until its counter climbed back past the stale value - hundreds of seconds. Incarnation decouples "which view is newer" from the reset-prone heartbeat.

The node learns its correct epoch two ways:

- **Persistence (primary, when `DATA_DIR` is set).** The incarnation is stored on disk, Cassandra-generation style. On restart the node loads the last value, advances past it, and installs it before gossiping — so its very first gossip already dominates any stale entry, with no peer round trip. A refutation-driven advance at runtime is persisted through the same store before it propagates. The write is atomic (temp + fsync + rename) and monotonic. A missing or corrupt file is treated as a fresh start (epoch 0), which simply falls back to refutation.
- **Refutation (fallback, always available).** When the node receives gossip carrying its own entry — either a stale higher incarnation that peers remember, or a suspect/dead claim at its current incarnation — it advances its incarnation just past the stale value and keeps asserting `alive`. Its next gossip round then supersedes the stale entry everywhere within a round or two. An `alive` echo at the node's current incarnation is ignored, so the incarnation does not grow without bound in steady state.

Refutation only fires when the node *receives* gossip about itself, which is why the [seed bootstrap reply](#seed-nodes) is part of the same fix: it guarantees a rejoining node gets that inbound message even after peers have buried it. Persistence removes the dependency entirely for the common case — a node with `DATA_DIR` never needs a peer to remember it in order to reclaim a fresh epoch. Without `DATA_DIR`, the node runs in refutation-only mode and behaves as before.

## Design Decisions

### UDP over TCP for Gossip Messages

**Choice:** UDP with JSON-encoded full member lists.

TCP would add connection setup overhead for every gossip round. Since gossip is inherently lossy-tolerant - any dropped message is re-sent within 1 second by the next gossip tick - the reliability guarantees of TCP are unnecessary overhead. UDP is stateless and cheap. The 65KB payload limit is generous for member lists in clusters up to a few hundred nodes.

**Tradeoff:** Lost gossip messages mean membership updates take slightly longer to propagate. In the worst case (consistent packet loss between two nodes), a node could be marked suspect even while healthy. The 5-second stale threshold provides enough headroom for transient packet loss. Sustained packet loss is failure detection working correctly.

### `(Incarnation, Heartbeat)` LWW for Membership over Vector Clocks

**Choice:** Last-write-wins ordered first by incarnation, then by a monotonically increasing heartbeat counter.

Membership state is simple - alive, suspect, or dead. The most recent observation of a node's liveness is always the correct one. Vector clocks track concurrent writes to shared values, but membership state is not a shared mutable value - it is an observation, and the most recent observation wins. The `(incarnation, heartbeat)` pair provides a total order over observations of the same node with no coordination.

Heartbeat alone is not enough because a crashed and restarted node starts its heartbeat from 0, and gossip still holds the high pre-crash heartbeat - so under a heartbeat-only rule the restarted node is ignored until its counter climbs back past the stale value (hundreds of seconds). The incarnation number, advanced by the node via refutation on restart, supersedes the stale entry immediately. See [Incarnation Numbers](#incarnation-numbers).

**Tradeoff:** With `DATA_DIR` set, the incarnation is persisted (atomic, monotonic), so a restarted node reclaims a fresh epoch from disk with no peer round trip. Without it, the node runs in refutation-only mode and depends on receiving gossip that carries its own stale entry — the [seed bootstrap reply](#seed-nodes) guarantees this on rejoin.

### Event-Driven Ring Updates over Polling

**Choice:** The ring is updated synchronously in the `onChange` callback when gossip fires a membership transition.

The alternative is a polling loop that periodically reads the `MemberList` and syncs the ring to match. Polling adds latency proportional to the poll interval and wastes CPU when nothing has changed. The callback approach means the ring is updated within the same gossip processing tick that detected the change - typically within 1 second of the actual event.

**Tradeoff:** The callback is called on the gossip receive goroutine. A slow or blocking callback would delay gossip processing. The ring's `AddWeightedNode` and `RemoveNode` operations are fast (O(log n) RBT mutations), so this is not a concern in practice.

### 5-Second Stale Threshold

**Choice:** A member is suspected after 5 seconds without a heartbeat.

With a 1-second gossip interval and fanout=3, a node that is alive will have its heartbeat delivered to the full cluster within a few rounds. 5 seconds provides headroom for transient packet loss or CPU spikes. A threshold shorter than 5 seconds would produce false positives - healthy nodes being suspected during brief overload. A threshold longer than 5 seconds would delay failure detection and keep dead nodes in the ring longer, routing traffic to an address that cannot respond.

**Tradeoff:** A node can be unreachable for up to 10 seconds before being removed from the ring (5s suspect + 5s dead). During that window, coordinators that route to it will time out and fail that replica's contribution to quorum. If the node is one of W required replicas, writes will fail until either the node recovers or another replica absorbs the write.

## See Also

- [Ring](ring.md) - updated on every alive/dead membership transition
- [Hinted Handoff](hinted-handoff.md) - hint delivery is triggered by the alive transition
- [Data Migration](data-migration.md) - bootstrapping state is propagated via gossip
- [Operations](operations.md) - `SEED_NODES`, `GOSSIP_PORT` env vars
