# Design: Consensus-Backed CAS (`internal/paxos`, planned `api` wiring)

> Implementation plan for replacing primary-serialized conditional writes
> with a single-decree Paxos round per key, closing fault-harness finding #7.
> The replica-side acceptor is implemented and tested; this documents the
> protocol, the coordinator wiring that remains, and the test flips that
> prove it.

## Status

| Piece | State |
| ----- | ----- |
| `internal/paxos` acceptor (ballots, prepare/accept/commit, persistent log with replay + compaction) | **implemented, tested** (`feat/paxos-cas`) |
| Coordinator round, replica HTTP endpoints, serial reads (`api`) | designed below, not yet wired |
| Fault/sim scenario flips to hard assertions | blocked on the wiring |

## Why

[History checking](history-checking.md) upgraded the CAS contract from a
claim to a checked property, and the check failed exactly where
[Client Consistency](client-consistency.md) conceded it would: CAS
serializes against the *current primary's local state*, and membership churn
moves the primary role without moving the state. A new primary that missed
the last acknowledged write serves stale reads and accepts a CAS from the
superseded value, forking history (finding #7, reproduced reliably by
`LinearizableCASAcrossPrimaryFailover` / `...AcrossPartition` and by the
simulation harness).

The fix is the one Cassandra chose for lightweight transactions: no
long-lived leader to fail over at all. Each CAS runs one **single-decree
Paxos round** among the key's replica set. Any two rounds for a key must
each gather a majority, and two majorities always share an acceptor, so no
ring-view disagreement about *who coordinates* can fork history — the
intersection property replaces the "everyone agrees on the primary"
assumption. (Multi-Raft per token range was considered and rejected: leader
lifecycles would have to be reconciled with vnode ownership churn, which is
the same failover problem again, plus log and snapshot machinery this
system doesn't otherwise need.)

## Protocol

### Ballots

`paxos.Ballot{Counter, Node}`, ordered by counter then node id (so two
coordinators can never mint equal ballots). Coordinators mint counters from
`max(unix-nanos, last-seen+1)` — a per-handler `atomic.Uint64` high-water
mark, folded with every rejection's ballot (`observeBallot`) so retries
leapfrog the round that beat them.

### The round (coordinator side, `?cas=true` PUT/DELETE)

Replica set: the **strict** ring walk (`GetReplicationNodes`, health
filter ignored), majority = `len/2+1`. Paxos quorums must come from a
stable set; the healthy walk resizes with gossip verdicts, and quorums over
a shifting set lose the intersection guarantee. Fan every phase to all
replicas, count acks, self short-circuits to local calls.

1. **Prepare** `ballot` on all replicas. Each promise carries the
   replica's committed store entry for the key (the prepare doubles as the
   quorum read) plus any accepted-but-uncommitted mutation.
   - Fewer than majority promises and a higher ballot was seen → observe
     it, backoff, retry (bounded, ~3 attempts, then 503 `contended`).
   - Fewer than majority and no higher ballot → 503 `unavailable`.
2. **Finish in-flight rounds.** If any promise carries an accepted
   mutation, re-propose the highest-ballot one **under our ballot**,
   commit it, and return 503 `retry`: it may already have been decided
   (majority-accepted) without ever being applied, and deciding *our*
   mutation on top of an unfinished decision is how updates get lost. The
   client's retry then observes the settled state.
3. **Precondition.** Dominance-merge the committed entries from the
   promises (`mergeResponses` — the freshest state any majority can
   prove). The write's version is `client clocks + coordinator's own
   increment`; it must strictly dominate (`HappensBefore`) every merged
   sibling, else 412 with no side effects (a persisted promise is the only
   trace). Same client-facing semantics as today, evaluated against quorum
   state instead of one node's view.
4. **Propose** `Mutation{key, value, deleted, version.Clocks, ballot}` to
   all replicas; majority accepts required (rejection → observe ballot,
   retry loop).
5. **Commit**: apply on every replica — a plain `store.Put/Delete` at the
   mutation's version, which dominates and merges to one sibling — then
   clear the acceptor's round. Failures get hints (anti-entropy is the
   backstop); **majority commit acks required before the 204** so serial
   reads can rely on intersection. Partial commit → 503 (outcome unknown,
   a later round finishes it).
6. 204 + `X-Session-Clock: version`.

### Serial reads (`GET ?consistency=serial`)

Steps 1–2 only: majority prepare, finish any in-flight round, then
dominance-merge the promises' committed entries and respond through the
normal `writeKeyResponse` path (404 for tombstone, siblings for conflict,
session clock header).

The in-flight completion is not optional. A round accepted by a majority
but committed to one replica is *decided*; a plain quorum read that
happens to include that replica shows the new value, and a later one that
misses it shows the old — a read regression porcupine will flag. Prepare
intersects the accept majority, so the serial read always learns of the
decision and finishes it before answering.

### Mutation self-containment

`paxos.Mutation` carries the complete write (key, value, tombstone flag,
full vector clock version, ballot) so *any* coordinator that learns it from
a promise can finish the round verbatim. Re-commit is idempotent: an equal
clock merges as an idempotent write and is dropped.

## Safety notes and edge cases

- **Promise durability.** The acceptor fsyncs every state change before
  replying (own append-only log via the `wal` package, replayed and
  compacted to one record per key on open). A promise a crash can revoke
  is how two disjoint majorities accept conflicting values. Memory-only
  acceptors are only safe where the store is memory-only too — which is
  why the *simulation* harness (crash = total state loss by design) keeps
  its post-crash CAS check in detector mode, while the fault harness
  (DATA_DIR set) flips to a hard assertion.
- **Stale commits.** `Commit(key, ballot)` clears the accepted round only
  if the ballot matches; a slow round's commit must not erase a newer
  accept. Apply-to-store happens *before* the acceptor clear so a crash
  between them only re-commits idempotently.
- **412 vs races.** The precondition check runs between prepare and
  propose with no lock; the ballot machinery is what serializes. A rival
  that prepares a higher ballot in that window makes our propose fail, and
  the retry re-reads state — the check never needs the store mutex.
- **Remaining (narrower) window.** Majorities are taken over each
  coordinator's *view* of the replica set. Two views that disagree so much
  that disjoint "majorities" exist (e.g. `{A,B,C}` vs `{B,C,D}` picking
  `{A,B}` and `{C,D}`) could still fork. This needs simultaneous ring-view
  divergence *and* pathological quorum selection inside one round — far
  narrower than disagreeing about a single primary. State it honestly in
  the docs; the detector scenarios are the empirical check.
- **Mixed workloads.** Non-CAS writes to a CAS key still merge as
  siblings, exactly as before; CAS keys should be CAS-only, unchanged
  contract.
- **Cost.** CAS goes from 1 forwarded round trip to 3 phases (each one
  parallel fan-out); serial reads cost a prepare. Contention costs
  retries with backoff (ballot dueling). Benchmark a healthy-cluster CAS
  before/after and add it to `docs/benchmarks.md`.

## Wiring plan (`api`, `cmd`, tests)

Everything below existed as a working draft on `feat/paxos-cas` before it
was reverted in favor of this document; it re-derives in a session.

1. **Handler state**: `acceptor *paxos.Acceptor` (default
   `paxos.NewAcceptor()` in `NewHandler`), `ballotCounter atomic.Uint64`,
   `SetPaxosAcceptor` for main. `nextBallot`/`observeBallot` as above.
2. **Replica endpoints** (in `server.go`): `POST /paxos/prepare`
   (`{key, ballot}` → promise + committed `entry NodeResponse`),
   `POST /paxos/propose` (`Mutation` → promise), `POST /paxos/commit`
   (`Mutation` → 204, apply-then-clear). Phase requests ride `casClient`
   (2× replica timeout).
3. **Coordinator**: `coordinateWrite` replaces the
   `casRoutedElsewhere` branch with `paxosCAS(w, key, incoming, wr)`;
   `GetNode` intercepts `?consistency=serial` before `requestedQuorum`
   (which still rejects `serial` on writes).
4. **main.go**: `paxos.OpenAcceptor(DATA_DIR/paxos)` when persistent,
   `SetPaxosAcceptor` before `BuildMux`, `acceptor.Close()` after drain.
   Sim nodes keep the memory default.
5. **Removals**: `casRoutedElsewhere`, `forwardCAS`,
   `X-CAS-Forwarded-From`, `keyWrite.body`, the CAS branches of
   `applyLocalWrite`, and `store.PutCAS`/`DeleteCAS`/`ErrCASConflict` +
   `internal/store/cas_test.go` (the precondition moves to the
   coordinator against quorum-merged state).
6. **Test rework**: `api/cas_session_test.go` rewrites to paxos semantics
   — a single-node RF=1 handler is a majority of one, so
   insert/overwrite/412/delete flows test the full round in-process;
   forwarding/mismatch tests are deleted with the code. New unit tests:
   ballot dueling (two handlers, shared acceptors), resurrection of an
   accepted-uncommitted mutation, serial read finishing an in-flight
   round.
7. **The payoff flips**: fault workload reads switch from
   `?consistency=all` to `?consistency=serial`; scenarios 12/13 switch
   `expectKnownCASGap` → `verifyLinearizable`; sim keeps detector mode
   only for schedules containing a crash (memory-only amnesia, see safety
   notes) and asserts otherwise. Watch porcupine runtime on the flipped
   scenarios; shrink workloads if Undecided.
8. **Docs to update**: `client-consistency.md` (CAS section rewrite: the
   round, serial reads, the narrower remaining window),
   `fault-injection.md` finding #7 → *fixed by consensus-backed CAS*,
   `history-checking.md` detector paragraphs, README line-16 blurb
   ("conditional writes serialize through the key's primary replica" →
   paxos round) and roadmap entry, `api.md` (new endpoints +
   `consistency=serial`), this page's Status table.

## See Also

- [Client Consistency](client-consistency.md) - the current
  primary-serialized design this replaces
- [History Checking](history-checking.md) - the checker that motivated and
  will verify the change
- [Fault Injection](fault-injection.md) - finding #7
- [Simulation Testing](simulation.md) - the fast reproduction loop for
  churn scenarios
