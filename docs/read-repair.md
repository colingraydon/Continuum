# Read Repair (`api`)

> Inline repair that piggybacks on quorum reads to push the canonical value back to stale replicas.

## Overview

Read repair detects and fixes stale replicas as a side effect of every quorum read, at no additional latency cost to the client. It is the fastest-reacting repair layer - it triggers on the read path, within the same request that detected the divergence. Anti-entropy catches what read repair misses, but only on a 30-second cycle.

## How It Works

### Staleness Detection

After the coordinator collects R responses and merges all sibling sets into the canonical result, it compares each replica's response against the merged set. A replica is stale if its sibling set is a proper subset of the merged result - that is, it is missing at least one surviving sibling entry (matched by equal vector clocks).

A replica that returned no entry at all (key not found) when the merged result is non-empty is also stale.

### Repair Execution

Stale replicas are repaired asynchronously in a background goroutine so the client's read latency is not affected. Two paths:

- **Local node** - the coordinator itself was stale. The coordinator writes the merged result directly to its own store via a normal `store.Put` call.
- **Remote replica** - the coordinator sends the merged entry as a replica sub-write via `PUT /keys/:key` with `X-Proxied-From` set, reusing the same HTTP path as coordinator fan-out and hinted handoff delivery.

If the background repair write fails, it is logged and dropped. Anti-entropy covers any keys that could not be repaired immediately.

### What Gets Repaired

Only surviving siblings are pushed. A sibling that was dominated by another sibling in the merge phase was already "repaired" logically - the dominating sibling is the correct value, and any replica that holds only the dominated sibling will accept the dominating one when it receives it.

## Design Decisions

### Async Repair over Sync Repair

**Choice:** Repair stale replicas in a background goroutine. Return to the client immediately after the merge.

Synchronous repair would wait for all stale replicas to ack before returning the read result. This guarantees that the next read from any replica sees the repaired value. The tradeoff is that the client's read latency is now bounded by the slowest stale replica rather than the R-th fastest responding one. Since stale replicas are often slow (they may have been partitioned, lagging, or briefly down), this is the worst case to block on.

Async repair accepts a short window where a second read could still return stale data from an unrepaired replica. That window is bounded by how fast the background goroutine completes - typically sub-millisecond for local writes and one network round-trip for remote ones.

**Tradeoff:** After a read that triggered repair, there is a brief period during which a second read could still return stale data from the replica being repaired. For monotonic-read consistency, synchronous repair would be required. For this system, the async window is acceptable and anti-entropy provides the durable backstop.

### Always Repair Even When Siblings Exist

**Choice:** Repair stale replicas even when the merged result contains concurrent siblings.

The temptation when surfacing a conflict is to skip repair and let the application resolve the conflict first. The problem is that skipping leaves different replicas with different subsets of siblings. One replica might have `{alice}` and another `{bob}`, when both should see `{alice, bob}`. A subsequent read from either replica would surface an incomplete conflict view.

Repairing even during a conflict ensures all replicas converge to the same sibling set. The application sees a consistent conflict regardless of which replica it reads from next, and whichever resolution write it issues will be correctly compared against the full sibling set on every replica.

**Tradeoff:** Repair pushes siblings to replicas that may see them immediately discarded if the application sends a resolution write moments later. This is a small amount of wasted work. The alternative - skipping repair during conflicts - produces divergent sibling sets across replicas, which is a correctness problem.

### Reusing the X-Proxied-From Write Path

**Choice:** Remote repair uses `PUT /keys/:key` with `X-Proxied-From`, the same path used by coordinator fan-out and hinted handoff.

The alternative is a dedicated repair endpoint. Reusing the existing path means repair writes go through the same vector clock conflict resolution logic as any other replica sub-write. A repair that arrives after the application has already resolved the conflict will be correctly dominated and dropped. There is no special-casing required.

**Tradeoff:** Repair writes look identical to coordinator fan-out writes from the receiving replica's perspective. There is no way to distinguish a repair from a first-time replication in logs or metrics. Adding a distinct header (e.g., `X-Read-Repair`) would improve observability at the cost of a separate code path.

## See Also

- [Replication](replication.md) - the quorum read that triggers repair
- [Anti-Entropy](antientropy.md) - the background complement that catches what read repair misses
- [KV Store](../internal/store/store.go) - vector clock merge logic applied to repair writes
