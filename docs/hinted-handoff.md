# Hinted Handoff (`internal/hintstore`)

> Durability buffer that closes the gap between quorum acknowledgment and full replication.

## Overview

With RF=3 and W=2, a write returns 204 once two replicas acknowledge it. If the third replica is temporarily down at write time, it misses the write. Anti-entropy will repair it within the next sync cycle - up to 30 seconds later. Hinted handoff closes that window by buffering the write locally and replaying it when the replica recovers.

## How It Works

### What Gets Buffered

When the coordinator fans a write to its replica set and a replica call fails, the write is buffered as a hint in the coordinator's local hint store:

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

Delivery is triggered by the gossip `onChange` callback, not a polling loop. When gossip detects that a node has transitioned to alive (either recovering from dead or joining fresh), the callback fires `handler.DeliverHints(nodeID, address)`.

`DeliverHints` drains all hints tagged for that node from the hint store and replays each one as a normal replica sub-write (`X-Proxied-From` set to the coordinator's ID). The receiving node applies them through the standard vector clock conflict resolution path:

- Stale hints (the replica already has a higher clock) are silently dropped
- Concurrent hints become siblings
- Idempotent hints are no-ops

### Graceful Shutdown Flush

On `SIGINT` or `SIGTERM`, before draining in-flight HTTP requests, the coordinator calls `FlushHints`. This iterates all pending hints and delivers any that target currently-alive nodes. This prevents hint loss on planned restarts - if the coordinator is restarting intentionally, it gets one chance to flush what it holds.

### Bounds and Limits

| Parameter | Value | Rationale |
| --------- | ----- | --------- |
| Per-node cap | 10,000 hints | Bounds memory; excess hints are evicted oldest-first |
| Hint TTL | 1 hour | Anti-entropy repairs anything missed within 2 sync cycles |
| Expiry check | Every 5 minutes | Runs in a background goroutine alongside the main server loop |

The hint store is entirely in-memory. If the coordinator restarts before delivering hints, those hints are lost. The residual risk is a node that recovers after the coordinator that held its hints has restarted. In that case, the replica relies on anti-entropy for repair - the same guarantee that existed before hinted handoff was added.

> **Failure Mode - Coordinator Restart**
>
> In-memory hints are lost on coordinator restart. Any key whose hint was lost will be repaired by anti-entropy within the next sync cycle (up to 30 seconds). The window of inconsistency is bounded by the anti-entropy interval, not the hint TTL. For persistent durability of hints, a write-ahead log would be needed.

> **Failure Mode - Hint Buffer Full**
>
> If a node is down long enough that a coordinator accumulates more than 10,000 hints for it, the oldest hints are evicted. Those keys will not be replayed via handoff and must rely on anti-entropy for repair. The cap prevents unbounded memory growth on the coordinator when a downstream node is down for an extended period.

## Design Decisions

### Event-Driven Delivery over Polling

**Choice:** Delivery triggered by the gossip `onChange` callback (alive transition).

The alternative is a background loop that periodically checks which nodes are alive and drains hints for them. Polling adds latency proportional to the poll interval - if a node recovers at t=0 and the poll runs at t=29s, hints sit undelivered for 29 seconds. With event-driven delivery, hints are delivered within the same second that gossip detects the recovery.

**Tradeoff:** Tighter coupling between gossip and the handler layer. The `onChange` callback is wired in `main.go` and passes a delivery function through to the handler, keeping neither package importing the other directly. This is more boilerplate than a polling approach but keeps the packages decoupled while still achieving event-driven delivery.

### In-Memory Store over Persistent Store

**Choice:** Hints live in memory only.

Persisting hints to disk would survive coordinator restarts and eliminate the "lost hints on restart" failure mode. The cost is complexity - WAL format, fsync timing, recovery logic, and the interaction with hint expiry and delivery. For this system, anti-entropy is the persistent fallback. The 30-second anti-entropy interval means the maximum inconsistency window after a coordinator restart is one sync cycle, which is acceptable.

**Tradeoff:** Coordinator restarts produce a brief consistency gap for keys whose hints were lost, repaired by anti-entropy. For a system with stronger durability requirements, the hint store should be backed by a WAL.

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
