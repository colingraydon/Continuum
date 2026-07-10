# Replication (`api`)

> Fans writes to N replicas and reads from R, resolving conflicts through vector clock causality.

## Overview

The replication layer coordinates writes and reads across multiple nodes. Any node can serve as coordinator for any key - there is no designated primary for a key at the coordinator level (the anti-entropy layer has its own notion of a primary, but that is separate). A coordinator fans the operation to the replica set, waits for quorum, and returns to the client.

## How It Works

### Replica Set Selection (Sloppy Quorum)

`ring.GetHealthyReplicationNodes(key, replicationFactor)` walks clockwise from the key's ring position collecting the first N distinct nodes that gossip currently considers alive. When a strict replica is suspect or dead-but-still-seated, the walk skips it and pulls in the next healthy node as a substitute - Dynamo's sloppy quorum. Each skipped node is reported as an intended owner so the coordinator can buffer a hint for it. With every strict replica healthy, the result is exactly the strict set. Bootstrapping nodes are excluded from the read replica set but still accept replica sub-requests.

### Write Path

1. The coordinator increments its own vector clock entry for this key and stores the value locally. Self counts as one ack.
2. Goroutines fan the write to the healthy replica set simultaneously with `X-Proxied-From` set to the coordinator's ID.
3. The coordinator waits on a buffered channel for W acks. As soon as W are received (including self), it returns 204.
4. A background goroutine drains the remaining in-flight goroutines. Any replica that fails gets a hint buffered in the hint store, as does every unhealthy strict replica the walk skipped.
5. If quorum cannot be reached, the coordinator returns 503.

Replicas that receive a write with `X-Proxied-From` set store it without further fan-out and without buffering hints - they are in the replica role, not the coordinator role.

### Read Path

1. The coordinator fans a read to the same healthy replica set the write path uses, so a substitute that absorbed a sloppy write is included.
2. It waits for R responses.
3. All returned sibling sets are merged into a canonical result. Merging means taking the union of all siblings, then applying vector clock dominance - if sibling A's clock happens-before sibling B's clock, A is dropped. Only causally concurrent siblings survive.
4. Any replica whose response was a proper subset of the merged result is repaired asynchronously in a background goroutine (read repair).
5. The merged result is returned to the client.
6. If fewer than R replicas respond, the coordinator returns 503.

### Vector Clocks

Each write carries a `VectorClockVersion` - a map from node ID to a logical counter. The coordinator raises its own counter to the highest value it has locally issued for the key, then increments it, before storing and replicating. The raise is a uniqueness guarantee. A client retrying from a stale clock base through the same coordinator must never receive the exact version of an earlier write for a different value. Equal clocks are treated as idempotent duplicates everywhere (merge, repair, anti-entropy), so a collision would leave replicas permanently split on which value the version names (simulation finding #9).

```json
{ "clocks": { "node1": 3, "node2": 1 } }
```

This clock means node1 has coordinated 3 writes to this key, node2 has coordinated 1. Any replica that received all writes coordinated by both nodes would accept this as current.

**Conflict detection** uses the standard partial order:

- Clock A **happens-before** B if every counter in A is <= the corresponding counter in B and at least one is strictly less. B dominates - A is dropped.
- **Concurrent** clocks (neither happens-before the other) - both values are kept as siblings.
- **Equal** clocks - idempotent, no change.

### Siblings

When concurrent writes produce siblings, the read response returns a `siblings` array instead of a single `value`:

```json
{
  "siblings": [
    { "value": "alice", "clocks": { "node1": 2 } },
    { "value": "bob",   "clocks": { "node2": 1 } }
  ]
}
```

A subsequent write that passes a clock dominating all sibling clocks resolves the conflict. The application is responsible for choosing the resolution strategy - union, higher counter, CRDT merge, or user prompt.

### Quorum Configuration

| Parameter | Default | Meaning |
| --------- | ------- | ------- |
| `REPLICATION_FACTOR` | 3 | Replicas per key |
| `WRITE_QUORUM` | majority (`RF/2 + 1`) | Acks required before returning 204 |
| `READ_QUORUM` | majority (`RF/2 + 1`) | Responses required before returning result |

With RF=3 and W=R=2, the system tolerates one node failure for both reads and writes. Setting `READ_QUORUM=1` gives lowest-latency reads at the cost of potentially stale data. Setting `READ_QUORUM=REPLICATION_FACTOR` gives the strongest read consistency but fails if any replica is down.

### Per-Request Consistency

The process-configured W and R are defaults, not fixed limits: any key request can override them with `?consistency=one|quorum|all` (`one`=1, `quorum`=RF/2+1, `all`=RF; see [API](api.md)). This lets a single deployment serve mixed workloads - a session write can demand `all` while a dashboard read takes `one` - instead of forcing one durability/latency point per process. An unrecognized level is rejected with 400 before any local write, so a typo cannot half-apply. The resolved quorum is clamped to the available replica set exactly like the configured values.

> **Failure Mode - Quorum Not Met**
>
> If fewer than W replicas acknowledge a write, the coordinator returns 503. The write was not lost - it landed on however many replicas responded before quorum failed. Anti-entropy will propagate it. The client should treat 503 as "unknown state" and implement idempotent retry with the returned or a higher vector clock. With sloppy quorum this only happens when fewer than W *healthy* nodes exist anywhere on the ring, or when a replica that gossip still believes alive fails mid-request (e.g. in the seconds between a crash and the suspect verdict).

> **Failure Mode - Concurrent Writes**
>
> Two clients writing to the same key simultaneously will produce siblings if their writes land on different coordinators. Neither write is lost. The next read surfaces both values. This is the correct behavior - surfacing the conflict gives the application the information needed to resolve it correctly.

## Design Decisions

### Vector Clocks over Last-Write-Wins

**Choice:** Per-node logical counters that track causality.

Last-write-wins (LWW) is simpler - on conflict, keep the value with the higher wall-clock timestamp and discard the other. Cassandra uses LWW by default. The problem is that LWW silently loses writes. Two clients writing concurrently will have one update disappear with no indication to either client or the application.

Vector clocks detect concurrency without requiring synchronized clocks. If two writes are concurrent, both are preserved as siblings. No write is silently discarded. The conflict is made visible and can be resolved by the application with full information.

**Tradeoff:** Reads can return multiple values. The application must handle siblings - it cannot assume a single canonical value. This pushes conflict resolution logic to the client layer, which is where the domain semantics to resolve it correctly live (a shopping cart is merged by union, a counter is merged by max, etc). For applications that cannot handle siblings, LWW is simpler at the cost of occasional silent data loss.

### Sibling Surfacing over Silent Resolution

**Choice:** Return all causally concurrent siblings to the reader.

The alternative is server-side conflict resolution - pick a winner using a deterministic rule (e.g., lexicographic value, highest clock sum) and return only the winner. This avoids returning siblings to clients. The problem is that any deterministic rule that does not know the application semantics will sometimes pick the wrong value. A merge of two concurrent counter increments should produce the sum, not the lexicographically larger one.

**Tradeoff:** Clients must handle `siblings` arrays in responses. This is more complex than a guaranteed single-value response. The API surface is larger and client code must branch on the presence of siblings.

### Sloppy Quorum over Strict

**Choice:** The coordinator walks past unhealthy replicas to the next healthy nodes on the ring, counting the substitutes toward W and buffering a hint for each skipped intended owner.

Under a strict quorum, the replica set is fixed by ring position: if W of a key's N home replicas are down, the write is rejected even when the rest of the cluster is idle and healthy. The fault harness surfaced the practical consequence - a single suspect node made every key it hosted unwritable at W=RF until gossip removed it from the ring. Sloppy quorum restores Dynamo's "always writable" property: a write succeeds as long as *any* W healthy nodes exist, because substitutes fill in for unhealthy home replicas. The write is never silently redirected - each skipped owner gets a hint carrying the value, so hinted handoff (or anti-entropy as backstop) repairs the intended owner when it returns. Reads use the same healthy walk, so a substitute that absorbed a write serves it until repair completes.

**Tradeoff:** W no longer counts only home replicas, weakening read-your-write further at the margins: a read issued after gossip has re-verdicted a node can hit a different set than the write used, missing the substitute's copy until read repair or hint delivery closes the gap. Quorum clamping also engages earlier - the healthy walk excludes suspects immediately, so a shrunken cluster accepts writes at the suspect verdict rather than waiting for ring removal. Both are deliberate: this system consistently chooses availability plus convergence (hints, read repair, anti-entropy) over blocking.

### Background Goroutine for Post-Quorum Replica Failures

**Choice:** Drain remaining in-flight replica goroutines in a background goroutine after quorum is met.

The coordinator breaks out of the result collection loop as soon as W acks are received. Goroutines for the remaining replicas are still in-flight. Three options for handling them: wait for all (bad - adds up to `REPLICA_TIMEOUT_MS` to latency in the worst case), ignore them (bad - misses failures that occur after the quorum break and loses the chance to buffer hints), or drain them in the background (chosen). The client gets 204 at quorum and hint bookkeeping happens concurrently.

**Tradeoff:** The background goroutine adds a small amount of ongoing goroutine overhead per write when replicas are slow. In a healthy cluster where all replicas respond quickly, the goroutine exits almost immediately. The cost is bounded by `REPLICA_TIMEOUT_MS`.

### X-Proxied-From Header for Replica Identification

**Choice:** A single HTTP header distinguishes replica sub-requests from coordinator requests.

A replica that receives a write with `X-Proxied-From` set stores the value without fanning out further and without buffering hints. Without this distinction, a write received by a replica would be treated as a new coordinator write, triggering another fan-out and another round of vector clock increments.

**Tradeoff:** The header is an informal contract. Any caller that sets it will bypass fan-out and quorum checks. This is intentional - anti-entropy sync, read repair, and hinted handoff all need to write directly to replicas without triggering coordinator behavior. The header serves as a role signal, not an authorization mechanism.

## See Also

- [Ring](ring.md) - provides the healthy replica set via `GetHealthyReplicationNodes`
- [Hinted Handoff](hinted-handoff.md) - records hints for failed replica writes and skipped intended owners
- [Read Repair](read-repair.md) - repairs stale replicas after quorum reads
- [KV Store](https://github.com/colingraydon/Continuum/blob/main/internal/store/store.go) - implements the vector clock conflict resolution logic
- [Operations](operations.md) - `REPLICATION_FACTOR`, `WRITE_QUORUM`, `READ_QUORUM`, `REPLICA_TIMEOUT_MS`
