# Client Consistency (`api`, `internal/paxos`, `internal/store`)

> Conditional writes that refuse to create siblings, serial reads that never
> regress, and session clocks that close the read-your-writes gap.

## Overview

The base system is eventually consistent: a sloppy-quorum write can land on replicas that a later quorum read never touches, and a concurrent write always produces siblings for the client to resolve. Three opt-in mechanisms tighten this per request, without changing the default path:

- **Conditional writes (CAS)** - `?cas=true` on PUT/DELETE turns the request's `clocks` field into a precondition. A write whose clock does not dominate the current state is rejected with 412 instead of becoming a sibling, giving lock-like semantics to clients that want them. Every CAS runs one single-decree Paxos round among the key's replica set (the Cassandra lightweight-transaction shape), so writers racing through *any* coordinators serialize on ballot order and majority intersection instead of forking history.
- **Serial reads** - `GET ?consistency=serial` runs the Paxos prepare phase: a linearizable read that observes every decided CAS write and never regresses, at the cost of a majority round trip and acceptor writes.
- **Session guarantees** - every coordinator write and read returns the resulting vector clock in an `X-Session-Clock` response header. A client that sends that clock back on a later GET is guaranteed a result that dominates it (read-your-writes, and monotonic reads when the clock is advanced after each read), or a 503 if the cluster cannot currently prove it.

Session guarantees ride the existing vector clock machinery; CAS and serial reads add one consensus round per operation, and nothing else.

## How It Works

### The CAS round

A normal write merges into the sibling set: dominated siblings are replaced, concurrent ones accumulate. A CAS write instead runs one Paxos round, coordinated by whichever node received the request, among the key's **strict** replica set (the health-ignoring ring walk - quorums must come from a stable set, and the healthy walk resizes with gossip verdicts). The round's majority is over the replication factor capped by total known membership *including dead members*: a partitioned node that has declared its peers dead must not shrink the quorum to itself (see the safety notes below).

1. **Prepare.** The coordinator mints a ballot (a wall-clock-seeded counter that folds in every rejection it has seen, tie-broken by node id) and asks every replica to promise it. Each promise carries the replica's committed state for the key - the prepare doubles as the quorum read - plus any accepted-but-uncommitted mutation from an earlier round and the highest ballot the replica has seen commit. The phase returns at a majority of promises; a rejection carrying a higher ballot triggers a bounded retry with a fresh ballot.
2. **Finish in-flight rounds.** If any promise carries an accepted mutation whose ballot lies *above every committed ballot the promises report*, a previous coordinator vanished between accept and commit - and that mutation may already hold a majority of accepts, i.e. be *decided* without having been applied. The coordinator re-proposes it under its own ballot, commits it, and (unless it was this request's own earlier attempt, which makes this success) returns a retryable 503: deciding a new mutation on top of an unfinished decision is how updates get lost. Accepted debris at or below a reported commit is the opposite case - the leftover of a round that *lost* - and is ignored: re-proposing it would overwrite the newer committed value.
3. **Precondition.** The write's version is the client's `clocks` plus the coordinator's own increment; an absent `clocks` field is *not* bootstrapped - it is the precondition "no current value exists". The version must causally dominate **every** committed sibling in the dominance-merge of the promises' states: the freshest state any majority can prove, so a replica that missed a write cannot vouch for a stale precondition. Failure returns 412 with no side effects - unless an earlier attempt of this same request already proposed, in which case no-side-effects can no longer be proven and the client gets a retryable 503 instead.
4. **Propose.** The mutation - key, value, tombstone flag, full version clock, ballot: self-contained, so any later coordinator can finish it verbatim - goes to every replica; a majority must accept. Acceptors reject ballots below their promise, which is what serializes racing writers.
5. **Commit.** The decided mutation is applied through the normal store path on every replica (its version dominates, so the merge resolves to one sibling) and the round's accepted state is cleared. **A majority of commit acks is required before the 204**, so serial reads can rely on intersection; stragglers are hinted and anti-entropy remains the backstop. The write's clock returns in `X-Session-Clock`.

A retry with the same precondition mints the same version, which is how the coordinator recognizes its own mutation if a rival round resurrected and committed it while this coordinator saw only timeouts: the committed sibling carries this request's exact version and value, and the answer is 204, not a false 412.

### Serial reads

`GET ?consistency=serial` runs step 1 and, when needed, step 2 - then dominance-merges the promises' committed states and responds like a normal read (single value, 404 for a tombstone, siblings for a conflict, observed clock in `X-Session-Clock`). Finishing in-flight rounds is not optional: a round accepted by a majority but committed to one replica is decided, and a plain quorum read that happens to include that replica shows the new value while the next read misses it - a regression. The prepare intersects the accept majority, so a serial read always learns of the decision and completes it before answering.

### Session Clock Flow

1. `PUT /keys/k` returns `X-Session-Clock: {"node1": 3}` - the clock of the write just performed. DELETE returns the tombstone's clock the same way.
2. The client stores that clock (per key) and sends it as an `X-Session-Clock` request header on a later `GET /keys/k`.
3. The coordinator performs the normal quorum read and merges sibling sets. It then folds all surviving clocks into their componentwise maximum and checks that this covers the session clock (every counter in the session clock is less than or equal to the observed one).
4. If the quorum result does not cover the session clock, the coordinator escalates once: it re-reads from **every** replica in the read set and re-merges. This closes the sloppy-quorum window where the initial R responses happened to come from replicas the write never reached.
5. If even the full replica set cannot produce a covering result, the read fails with 503. The client retries; hinted handoff or anti-entropy will deliver the missing write.

Every coordinator GET response returns the observed merged clock in `X-Session-Clock`, including the 404 served for a tombstone, so a client can advance its session clock after reads as well as writes and thereby get monotonic reads.

## Design Decisions

### Paxos Per Key, Not a Primary and Not Raft

**Choice:** Every CAS runs a single-decree Paxos round among the key's replicas; there is no designated primary and no long-lived leader. Acceptor state (promises, accepted mutations) is per-key and persisted to its own append-only log before every reply.

The previous design serialized CAS through the key's primary replica - one mutex, no consensus protocol. [History checking](history-checking.md) demonstrated exactly why Cassandra didn't stop there (fault-harness finding #7): membership churn moves the *primary role* without moving the *state*, so a new primary that missed the last acknowledged write served stale reads and accepted a CAS from the superseded value, forking history. Paxos replaces the "everyone agrees on the primary" assumption with majority intersection: no two rounds can both decide without sharing an acceptor, no matter how ring views churn - and the shared acceptor's promise arbitrates. Raft per token range was rejected because leader lifecycles would have to be reconciled with vnode ownership churn - the same failover problem again - plus log and snapshot machinery this system doesn't otherwise need; leaderless per-key rounds have no failover at all.

Three details carry the safety argument through the failure modes the harnesses actually produced:

- **Quorum denominators never shrink because nodes are down.** The majority is computed over the replication factor capped by total known membership *including dead members*. Without this, a node isolated by an asymmetric partition declares its peers dead, drops them from its ring, resolves a replica set of one, and becomes a disjoint "majority of one" - the fault harness forked exactly this way before the rule was added. A dead replica still counts toward the quorum size; it just cannot vote.
- **Promises survive crashes.** The acceptor fsyncs every state change to `DATA_DIR/paxos` (replayed and compacted to one record per key on open) before its reply leaves the node. A promise a crash can revoke is how two majorities accept conflicting values; in memory-only mode the acceptor is memory-only too, matching the store's durability.
- **A wiped store forfeits the vote until repaired.** Promises are only half of what a prepare reports - the other half is the replica's committed state, and a node whose [downtime gate](persistence.md) discarded its data can no longer vouch for the keys it replicates even though its promises survived. It rejoins as bootstrapping, excluded from voting (but still counted in the denominator) until it has pulled its entire replica set back; otherwise a prepare majority leaning on its absent state merges to stale history (finding #10).

**Tradeoff:** CAS is still CP-flavored in an otherwise-AP system - it now needs a *majority* of the key's replicas rather than one designated primary, which is strictly more available under failover (a dead replica no longer blocks CAS at all) but still fails closed under quorum loss. Each CAS costs three fan-out phases instead of one forwarded hop, each phase returning at majority; contended rounds duel on ballots and resolve by bounded retry with backoff, so under heavy contention clients see retryable 503s rather than a queue. One narrower window remains: majorities are computed against each coordinator's view of the replica set, so ring views diverging enough to produce *disjoint majorities within a single round* could still fork - that requires simultaneous membership disagreement about multiple nodes, not just about who leads. The linearizability checker that found finding #7 now runs as a hard assertion over kill, restart, and partition scenarios and has not found a violation since.

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
