# Hinted Handoff (`internal/hintstore`)

> Durability buffer that closes the gap between quorum acknowledgment and full replication.

## Overview

With RF=3 and W=2, a write returns 204 once two replicas acknowledge it. If the third replica is temporarily down at write time, it misses the write. Anti-entropy will repair it within the next sync cycle - up to 30 seconds later. Hinted handoff closes that window by buffering the write locally and replaying it when the replica recovers.

## How It Works

### What Gets Buffered

Hints are buffered in the coordinator's local hint store in two cases: a replica call in the fan-out fails, or the sloppy-quorum walk skipped an unhealthy home replica upfront and wrote to a substitute instead (see [Replication](replication.md#sloppy-quorum-over-strict)). In the second case the skipped node is the intended owner, and its hint carries the full write:

```
hint {
    key:     "user:123"
    value:   "alice"
    clocks:  {"node1": 3}
    deleted: false
    for:     "node3"
}
```

The hint is tagged with the intended recipient node ID. Hints are not writes - they are deferred write intents. The coordinator's own write already landed on quorum; the hint exists only to repair the missing replica.

### Hint Delivery

Delivery is primarily event-driven: the gossip `onChange` callback fires `handler.DeliverHints(nodeID, address)` the moment a node transitions to alive (either recovering from dead or joining fresh). A background sweep (`runHintDelivery`, 30s tick) backs it up by calling `DeliverPendingHints`, which delivers buffered hints to any currently-alive target regardless of any membership edge. The sweep is what covers a target that never presents a dead→alive transition. The important case is an asymmetric partition: the isolated node keeps gossiping, so it never looks dead, while its inbound writes are dropped.

`DeliverHints` drains all hints tagged for that node from the hint store and replays each one as a normal replica sub-write (`X-Proxied-From` set to the coordinator's ID). The receiving node applies them through the standard vector clock conflict resolution path:

- Stale hints (the replica already has a higher clock) are silently dropped
- Concurrent hints become siblings
- Idempotent hints are no-ops

A hint that fails to deliver is re-buffered rather than dropped, with its original timestamp preserved so its TTL keeps counting from the original write. This makes the periodic sweep safe to run against a target that is still unreachable: a failed attempt costs the delivery try but does not discard the hint. Anti-entropy remains the backstop for any hint that ages out before its target becomes reachable.

### Graceful Shutdown Flush

On `SIGINT` or `SIGTERM`, before draining in-flight HTTP requests, the coordinator calls `DeliverPendingHints` (the same sweep the periodic timer drives). This iterates all pending hints and delivers any that target currently-alive nodes. This prevents hint loss on planned restarts - if the coordinator is restarting intentionally, it gets one chance to flush what it holds.

### Bounds and Limits

| Parameter | Value | Rationale |
| --------- | ----- | --------- |
| Per-node cap | 10,000 hints | Bounds memory; excess hints are evicted oldest-first |
| Hint TTL | 1 hour | Anti-entropy repairs anything missed within 2 sync cycles |
| Expiry check | Every 5 minutes | Runs in a background goroutine alongside the main server loop |

With `DATA_DIR` set, the hint store is backed by its own append-only log (layered on the shared segmented WAL: CRC framing, torn-tail recovery, group commit). Every stored hint is logged before it is buffered, drains and evictions append REMOVE records, and the log is periodically compacted to one STORE record per live hint. On restart the log is replayed to rebuild the buffer, dropping hints already older than the TTL. Without `DATA_DIR` the store is memory-only and hints are lost on restart, falling back to anti-entropy - the same guarantee that existed before hinted handoff was added.

> **Failure Mode - Coordinator Restart**
>
> With persistence enabled, buffered hints survive a coordinator crash: the hint log is replayed on startup and delivery resumes when the target node is next seen alive. In memory-only mode, hints are lost on restart and any affected key is repaired by anti-entropy within the next sync cycle (up to 30 seconds), so the window of inconsistency is bounded by the anti-entropy interval, not the hint TTL.

> **Failure Mode - Hint Buffer Full**
>
> If a node is down long enough that a coordinator accumulates more than 10,000 hints for it, the oldest hints are evicted. Those keys will not be replayed via handoff and must rely on anti-entropy for repair. The cap prevents unbounded memory growth on the coordinator when a downstream node is down for an extended period.

## Design Decisions

### Event-Driven Delivery, Periodic Sweep as Backstop

**Choice:** Deliver primarily on the gossip `onChange` alive transition, with a periodic sweep as a backstop.

Event-driven delivery is the fast path: polling alone adds latency proportional to the poll interval - if a node recovers at t=0 and the poll runs at t=29s, hints sit undelivered for 29 seconds, whereas the callback delivers within the same second gossip detects the recovery. But a pure alive-transition trigger has a blind spot the fault-injection harness surfaced. During an asymmetric partition the isolated node never looks dead, because its outbound gossip still flows, so it never presents a dead→alive edge and its hints are never triggered. Repair then falls entirely to anti-entropy. The 30s sweep closes that gap by delivering to any alive target on a timer, independent of membership edges, while the callback still handles the common recover-from-dead case with no added latency.

**Tradeoff:** The sweep retries against targets that may still be unreachable, so `DeliverHints` re-buffers failed deliveries (preserving the hint's timestamp so its TTL is not reset) rather than dropping them. The event-driven path keeps its tight coupling between gossip and the handler: the `onChange` callback is wired in `main.go` and passes a delivery function through to the handler, so neither package imports the other directly.

### Persistent Hint Log (with Anti-Entropy as Backstop)

**Choice:** With `DATA_DIR` set, hints are persisted to an append-only log under `DATA_DIR/hints`; without it, the store is memory-only.

The hint log reuses the segmented WAL (`internal/wal`) for CRC framing, torn-tail recovery, and group-commit fsyncs. Each `Store` writes an op-coded record; drains, cap evictions, and TTL expiry write `REMOVE` records carrying the affected hint sequence numbers, so removals are precise append-only events rather than full rewrites. The log is replayed on startup to rebuild the buffer, and rewrite-compacted once it accumulates enough superseded records (and on graceful shutdown). Hints already older than the TTL are dropped during replay, so a long coordinator downtime self-prunes stale hints without coupling to the storage downtime gate.

**Tradeoff:** Hint durability is best-effort (anti-entropy still backstops any loss), so a failed fsync is logged rather than surfaced. The drain path syncs its `REMOVE` record before delivering, so a crash mid-delivery cannot resurrect already-delivered hints on restart.

### Oldest-First Eviction under the Cap

**Choice:** When the 10,000-hint cap is reached, the oldest hints are evicted.

Oldest-first eviction prioritizes delivering the most recent writes. In a scenario where a replica has been down long enough to overflow the hint buffer, the most recent state of each key is more valuable than the oldest. Evicting oldest hints means the replay, if it happens, brings the replica closer to the current state faster.

**Tradeoff:** Oldest-first eviction can cause hint starvation for keys that are written infrequently but had their hint evicted before the replica recovered. Those keys fall back to anti-entropy. This is acceptable - the hint store is an optimization layer, not the durability guarantee. Anti-entropy is the durability guarantee.

### Hints Never Count toward Quorum

**Choice:** A buffered hint does not inflate the coordinator's acknowledged-replica count.

If hints counted toward quorum, a write could return 204 on quorum that includes "I'll retry node3 later" - but if the coordinator restarts before replaying, that write has only reached W-1 replicas. The 204 would be a lie. Quorum counts only actual acks. The hint is a best-effort repair on top of actual quorum.

**Tradeoff:** None. This is a correctness requirement, not a tradeoff. Counting hints toward quorum would violate the durability guarantee of the quorum acknowledgment.

## See Also

- [Gossip](gossip.md) - fires the alive transition that triggers hint delivery
- [Replication](replication.md) - records hints for failed replicas during fan-out
- [Anti-Entropy](antientropy.md) - durable fallback for hints that were lost or expired
- [Operations](operations.md) - hint store parameters are hardcoded; no env vars
