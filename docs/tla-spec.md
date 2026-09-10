# TLA+ Specification

> A formal model of the replication and repair path, model-checked exhaustively
> by TLC. Where the fault and simulation harnesses *sample* executions of the
> real system, this enumerates *every* interleaving of a bounded model.

## Why a specification at all

Continuum already has unusually strong dynamic verification: a process-level
fault harness, a seeded in-process simulation, and a porcupine linearizability
checker over recorded histories. All three share a limitation — they observe the
executions they happen to produce. A schedule that would break an invariant only
gets caught if some seed reaches it.

TLC works the other way round. It runs no Continuum code at all. Instead it
takes a mathematical description of the protocol and explores the entire
reachable state space within configured bounds — every interleaving of writes,
crashes, recoveries, hint deliveries, repairs and GC passes — and either reports
that an invariant held in all of them or prints the shortest trace that breaks
it.

The trade is exact and worth stating plainly: **this verifies the design, not
the implementation.** A correct spec and a buggy Go implementation can coexist
happily. Closing that gap is what the roadmap's trace-conformance item is for;
it is not done. Until it is, this document describes a model that agrees with
the code because a human kept it that way.

## What is modelled

`specs/Replication.tla` covers a single key across a replica set:

| Mechanism | Modelled as |
| --------- | ----------- |
| Quorum write | fan-out to reachable replicas, acknowledge at `W` |
| Quorum clamp | `ClampQuorum`: settle for fewer than `W` when that is all that is live |
| Crash / partition | node leaves `up`, **keeps its data** |
| Recovery | node rejoins; the downtime gate may wipe it first |
| Permanent loss | node leaves and its data is destroyed |
| Hinted handoff | writes buffered for unreachable targets, replayed on return |
| Anti-entropy | pairwise reconcile, newest version wins |
| Delete | a write whose value is a tombstone |
| Tombstone GC | purge, subject to two guards (below) |

### Abstractions

Each of these is a deliberate loss of fidelity, chosen to keep the state space
finite. They are the reason a passing check is evidence rather than proof:

- **Vector clocks become a total order.** Writes carry an increasing version and
  reconciliation is last-writer-wins. This is sound for the two properties
  checked — both are about whether *some* copy of a value exists, not about how
  concurrent writes are ordered — but it means the model says nothing about
  sibling creation or conflict resolution. A spec for that would need the
  partial order.
- **One key.** Cross-key interactions (compaction, the shared memtable) are out
  of scope.
- **No clock.** TTLs cannot be expressed, so their *consequences* are stated
  directly as preconditions. The GC's convergence guard is the clearest case —
  see below.
- **The coordinator is implicit.** Fan-out is atomic in the model; a coordinator
  crashing mid-fan-out is not represented.
- **Bounded everything.** Three nodes, two values, three operations, one
  permanent failure. Bugs needing a fourth node or a fifth operation are outside
  what this run explores.

## The properties

### NoResurrection

> Once a delete's tombstone has been collected, no serving node holds the value
> that delete superseded.

This is the safety argument written in prose in
[persistence](persistence.md#tombstone-gc-safety), now stated formally. A node
that is *down* may still carry the old value on disk — that is unavoidable and
harmless, because it must discard the data before it can serve or spread it.

**Holds** in the checked configuration: 738,061 distinct states, exhaustive.

### Durability

> The newest acknowledged write survives, so long as no more than
> `|Nodes| - W` replicas have lost their data.

**Holds** under strict quorum: 135,799 distinct states, exhaustive.

Two things about this property are not obvious, and TLC is what made them
explicit.

## Why both GC guards

Tombstone collection has two preconditions, and the model checker shows **each
is load-bearing** — removing either one alone reachs a resurrection in seconds:

**Guard 1 — reachable replicas have converged.** A tombstone is only collected
once every reachable node holds it or something newer. This is what the TTL
buys: at a 24h GC TTL against a 30s sync interval,
[anti-entropy](antientropy.md) counts ~2,880 sync cycles of headroom for the
tombstone to reach every live replica.

The first version of this spec omitted that guard, on the assumption that the
downtime gate carried the whole argument. TLC produced a six-step counterexample:
a delete lands on two replicas while a third holds the old value and a pending
hint; GC collects the tombstone and drops the now-obsolete hint; the third node
is *up*, so the downtime gate never applies to it, and anti-entropy then spreads
its stale value back outward. The headroom is not a nicety — it is a
correctness precondition.

**Guard 2 — the downtime gate.** Every node unreachable at GC time must discard
its data before serving again. Delete this conjunct from `Recover` and TLC
immediately finds the trace the gate was built to prevent.

Neither guard alone suffices. That is a sharper claim than the prose made, and
it is the kind of claim a sampling harness cannot make at all.

## What the gate costs

Modelling durability surfaced something the documentation understated.

[persistence](persistence.md#tombstone-gc-safety) describes the downtime gate's
price as: *"a node down longer than `gcTTL` loses any writes it accepted that
hadn't reached quorum."* TLC found a trace where a write that **had** reached
full quorum is lost anyway:

1. A delete is acknowledged; `n1` then crashes.
2. `v1` is written to `{n2, n3}` and acknowledged — a full `W=2` quorum.
3. `n3` is permanently lost.
4. GC collects the tombstone. `n2` is down at that moment, so it is marked
   stale — the gate does not know `n2` holds a *newer* write.
5. `n2` returns, the gate wipes it, and the last copy of an acknowledged write
   is gone.

The gate is a blunt instrument: it discards a node's whole dataset, including
acknowledged writes the rest of the cluster no longer has. So **a gate wipe
consumes durability budget exactly as a permanent failure does**, and the honest
statement of the guarantee counts both:

```
Lost == failed \cup wiped
Durability == Cardinality(Lost) > |Nodes| - W  \/  <the write survives>
```

With that accounting the property holds exhaustively. Without it, the trace
above stands. This is not a bug to fix — the gate is preferable to resurrection,
and both alternatives were weighed in the [persistence
design](persistence.md#alternatives-considered) — but "one gate wipe plus one
permanent failure can lose a quorum-acknowledged write" is a materially
different operational statement from what the docs said, and it belongs in the
durability story rather than only in a model.

## The quorum clamp, as a checked statement

`ClampQuorum` models fault-harness finding #4: the write quorum clamps to the
live replica set, so a cluster that has lost most of its replicas acknowledges
writes with fewer copies than `W` rather than refusing them.

Turning it on makes `Durability` violable, and TLC produces the trace. That is
the point of having it as a constant rather than a comment: the cost of the
availability trade is now something the checker demonstrates on demand, instead
of a caveat a reader has to take on trust.

- `Replication.cfg` — clamp **on**, matching the code. Checks `NoResurrection`.
- `Strict.cfg` — clamp **off**. Additionally checks `Durability`.

## Running it

```bash
make spec                      # both configurations, ~20s
bash scripts/tlc.sh Replication   # one configuration
bash scripts/tlc.sh Strict
```

`scripts/tlc.sh` fetches a pinned `tla2tools.jar` into `.tlc/` on first use and
needs a JDK 17+. TLC exits non-zero on a violated invariant, which is what
allows the `spec` CI job to gate rather than merely report.

To explore further than CI does, raise the bounds in a config — `MaxOps = 4` or
a fourth node. Expect the state space to grow sharply; that is why the CI job
runs the small configurations and larger ones stay a manual exercise.

## Not covered

Stated so the boundary is explicit rather than implied:

- **Trace conformance** — checking that the running system's observed behavior
  actually refines this spec. The roadmap's stretch goal, and the thing that
  would close the design-versus-implementation gap.
- **Paxos CAS** — the conditional-write protocol has its own
  [design doc](paxos-cas-design.md) and porcupine coverage, but no spec.
- **Sibling semantics** — excluded by the total-order abstraction above.
- **Membership and gossip** — the model treats reachability as an oracle rather
  than something the cluster has to agree on.
- **Multi-DC** — one replica set, no data-center dimension.

## See Also

- [Persistence](persistence.md#tombstone-gc-safety) — the prose safety argument
  this formalizes
- [Anti-Entropy](antientropy.md) — the TTL headroom guard 1 depends on
- [Fault Injection](fault-injection.md) and [Simulation](simulation.md) — the
  sampling counterparts
- [Testing](testing.md) — where this sits in the verification stack
