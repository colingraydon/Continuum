# Client Consistency (`api`, `internal/store`)

> Conditional writes that refuse to create siblings, and session clocks that close the read-your-writes gap.

## Overview

The base system is eventually consistent: a sloppy-quorum write can land on replicas that a later quorum read never touches, and a concurrent write always produces siblings for the client to resolve. Two opt-in mechanisms tighten this per request, without changing the default path:

- **Conditional writes (CAS)** - `?cas=true` on PUT/DELETE turns the request's `clocks` field into a precondition. A write whose clock does not dominate the current state is rejected with 412 instead of becoming a sibling, giving lock-like semantics to clients that want them. Every CAS for a key routes through the key's primary replica, so writers racing through different coordinators serialize on one mutex instead of forking history.
- **Session guarantees** - every coordinator write and read returns the resulting vector clock in an `X-Session-Clock` response header. A client that sends that clock back on a later GET is guaranteed a result that dominates it (read-your-writes, and monotonic reads when the clock is advanced after each read), or a 503 if the cluster cannot currently prove it.

Both ride the existing vector clock machinery; neither adds a new consensus protocol.

## How It Works

### CAS Write Path

A normal write merges into the sibling set: dominated siblings are replaced, concurrent ones accumulate. A CAS write adds routing and one check before anything else happens:

1. **Routing.** The receiving coordinator resolves the key's *primary*: the first node on the strict (health-ignoring) ring walk, which every coordinator sharing the ring view resolves identically. If that is another node, the request is forwarded to it with an `X-CAS-Forwarded-From` header and the primary's verdict (status, body, `X-Session-Clock`) is relayed back verbatim. If the primary is not alive in the member list, or a forwarded request lands on a node that does not consider itself primary (diverging ring views), the request fails closed with 503 and the client retries.
2. **Versioning.** The primary increments its own counter on the client-supplied `clocks` to produce the write's version, exactly as for a normal write. Unlike a normal write, an absent `clocks` field is *not* bootstrapped from the current local entry - it is the precondition "no current value exists".
3. **Precondition.** Under the store mutex that serializes all writes to the key, the store verifies that this version causally dominates **every** existing sibling. Because the check demands domination of all siblings, a key in conflict cannot be CAS-written until the client has read and merged the full sibling set.
4. **Commit.** On success the write proceeds through the normal path (WAL append, memtable merge, replica fan-out, hints); the merge is guaranteed to resolve to exactly one sibling. On failure the request returns 412 with no side effects: nothing is logged, stored, or replicated.

Because every CAS for a key executes its check-then-write atomically on the same primary, two CAS writers racing through *any* pair of coordinators serialize: the loser's precondition is evaluated after the winner's commit and gets 412.

### Session Clock Flow

1. `PUT /keys/k` returns `X-Session-Clock: {"node1": 3}` - the clock of the write just performed. DELETE returns the tombstone's clock the same way.
2. The client stores that clock (per key) and sends it as an `X-Session-Clock` request header on a later `GET /keys/k`.
3. The coordinator performs the normal quorum read and merges sibling sets. It then folds all surviving clocks into their componentwise maximum and checks that this covers the session clock (every counter in the session clock is less than or equal to the observed one).
4. If the quorum result does not cover the session clock, the coordinator escalates once: it re-reads from **every** replica in the read set and re-merges. This closes the sloppy-quorum window where the initial R responses happened to come from replicas the write never reached.
5. If even the full replica set cannot produce a covering result, the read fails with 503. The client retries; hinted handoff or anti-entropy will deliver the missing write.

Every coordinator GET response returns the observed merged clock in `X-Session-Clock`, including the 404 served for a tombstone, so a client can advance its session clock after reads as well as writes and thereby get monotonic reads.

## Design Decisions

### Primary-Serialized CAS, Not Consensus

**Choice:** Every CAS for a key executes on the key's primary replica, which checks the precondition against its local store atomically under the store mutex. Other coordinators forward rather than check locally.

Cassandra implements conditional writes with Paxos rounds (lightweight transactions) because a coordinator-local check cannot order writes racing through *different* coordinators: each passes its own check and the writes still fork into siblings. Routing through a single primary closes that race without a consensus protocol: whichever ring node receives the request, the check-then-write runs on the same mutex, so concurrent CAS writers get exactly one 204 and the rest get 412. The choice of primary is deterministic (the strict ring walk ignores health, so a flapping replica cannot make two coordinators disagree about who is primary while the ring itself is stable), and CAS fails closed with 503 whenever that certainty is missing: primary not alive, primary unreachable, or a forwarded request landing on a node whose ring view disagrees with the forwarder's. A 503 is retryable; what it never does is silently fork history.

**Tradeoff:** CAS trades availability for this: it is CP-flavored in a system that is otherwise AP. While the primary for a key is down or partitioned, CAS writes to that key return 503 until membership converges and the ring walk names a new primary, whereas normal writes stay available through the sloppy quorum. One window remains open: nodes whose ring views diverge during membership churn can briefly name different primaries, and two CAS writes landing on both in that window can still fork. Closing it requires a consensus round per write (Paxos/Raft per key range), which stays on the roadmap; the mismatch check on forwarded requests narrows the window to direct client hits on a stale-view node, and the next read surfaces any fork as ordinary siblings.

### Fail Closed on Unsatisfiable Session Reads

**Choice:** A session read that cannot be covered even by the full replica set returns 503, not the freshest available result.

Returning the freshest available data with a "best effort" flag would make the header advisory and force every client to re-implement the staleness check. Failing closed makes the guarantee real: a 200 response provably dominates the session clock. The client's recovery is a plain retry, and the window is bounded by hint replay (event-driven, seconds) or anti-entropy (30-second cycle).

**Tradeoff:** A session clock can outlive the state that proves it. The clearest case is a deleted key whose tombstone has been garbage-collected: a client still carrying the tombstone clock gets 503 until it drops its session state. Session clocks should be treated as short-lived request context, not durable client state; their useful lifetime is well inside the tombstone GC TTL.

### Escalate Once to All Replicas, Not Incrementally

**Choice:** On a session miss the coordinator re-reads the entire read set in one round, rather than widening the quorum one replica at a time.

The miss already cost one round trip; incremental widening would cost up to RF minus R additional sequential rounds in the worst case. One parallel full-set round bounds the total at two round trips and reuses the existing fan-out machinery. The escalated read also feeds read repair with the most complete picture, so the replica that caused the miss is repaired as a side effect and the next session read usually succeeds at quorum.

**Tradeoff:** A session miss reads RF replicas even when R+1 would have sufficed. Misses are the rare path (they require the quorum read to dodge every replica the write reached), so the simpler bound wins.

## See Also

- [Replication](replication.md) - vector clocks, quorum, and the sibling model both features build on
- [Read Repair](read-repair.md) - repairs the stale replica that caused a session escalation
- [Hinted Handoff](hinted-handoff.md) - delivers the missed write that a failed session read is waiting for
- [API Reference](api.md) - request and response formats for `?cas=true` and `X-Session-Clock`
