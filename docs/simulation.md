# Simulation Testing (`tests/sim`)

> A whole cluster in one process behind a seeded in-memory network: fault
> schedules generated from a seed, thousands of checked operations per second,
> and a race detector that finally sees the entire system at once.

## Overview

The fault harness proves the system survives real process faults, but each
scenario costs tens of seconds (real sockets, real SIGKILLs, real gossip
timers) and its faults are hand-written. The simulation harness inverts the
trade: every node runs in one test process, wired together by an in-memory
network that decides each message's fate — partitions, drops, added latency —
from a single seeded RNG, with production timing intervals compressed from
seconds to milliseconds. One seeded run packs a fault schedule, ~2,000
verified operations, and full repair convergence into a few seconds of wall
clock, so a `make sim` sweeps more distinct fault interleavings than a full
fault-injection pass, and `SIM_SEEDS=100` before a risky merge is a
ten-minute soak instead of an overnight job.

The same checks run as in the fault harness, at the same strength: no
acknowledged write may be lost (durability), replicas must converge to
identical sibling sets (convergence, checked by direct store access — free
when the store is in-process), and CAS histories go through the porcupine
checker from [History Checking](history-checking.md) — a hard assertion on
healthy runs, detector mode for the known churn gap (finding #7) under
faults.

## How it works

### The network is the fault injector

`simNet` owns every edge. Node-to-node HTTP (replica fan-out, CAS forwarding,
hint delivery, anti-entropy sync, migration) rides an injected
`http.RoundTripper` that resolves the target node and serves the request
straight through its mux; gossip datagrams ride an in-memory `gossip.Conn`
whose payloads are JSON round-tripped like the real UDP codec. Each message
consults the seeded RNG and the current fault state for its (from, to) edge:

- **blocked** (partition/isolation): the sender waits until its own timeout
  fires, exactly like a real blackhole — hint buffering, quorum clamping, and
  CAS fail-closed all key off timeouts, not instant errors;
- **dropped** (seeded probability): same, or silently vanished for UDP;
- **delayed**: delivered after the configured latency, which also reorders
  gossip between edges.

The harness's own client traffic rides a reserved never-faulted edge: checks
must observe cluster state, not get lucky with the fault schedule.

### Real nodes, compressed time

Each node is the production wiring from `cmd/continuum` — real store, ring,
member list, gossiper, anti-entropy manager, hint store, HTTP handler — with
three injected seams (`api.HandlerConfig.Transport`, `gossip.Conn` +
`Gossiper.SetTiming`, `antientropy.SetHTTPTransport`) and second-scale
intervals shrunk ~40x: gossip every 25ms, suspicion at 250ms, anti-entropy
every 100ms. A full fault cycle — fault, suspect verdict, dead verdict, ring
re-route, heal, hint replay, Merkle repair — fits in under a second.

### Seeded schedules

A run's seed generates its fault plan: a sequence of episodes drawn from
symmetric partition, asymmetric isolation, message-drop burst, latency burst,
and crash+restart, each with random targets, timing, and duration. Failures
report the seed; `SIM_SEED=k make sim` replays that schedule. At most one
crash per run: the store is memory-only in simulation, so a crash is total
state loss, and two overlapping losses could legitimately destroy an
acknowledged W=2 write — that would blame the schedule, not the system.

## Design Decisions

### Seeded, not bit-deterministic

**Choice:** The fault schedule, workload key/target choices, and per-message
fault decisions all derive from the seed; the Go scheduler and wall-clock
timing stay real.

Full determinism — FoundationDB-style virtual time under a single-threaded
event loop — requires every component to take a logical clock and yield
points, a rewrite this codebase doesn't need to buy its value: what makes
seeds useful is that a seed reproduces the *shape* of a run (which faults,
where, in what order) and runs are cheap enough to retry a flaky
reproduction. In practice a violating seed re-violates reliably; the porcupine
visualization and node logs (`SIM_LOG=1`) carry the diagnosis from there.

**Tradeoff:** A race between the schedule and goroutine timing can make a
specific interleaving intermittent under one seed. The harness compensates
with volume — more seeds, not more precision per seed.

### In-process on purpose

**Choice:** All nodes share one process, and `make sim-race` runs the suite
under the race detector.

The fault harness can never do this: separate binaries hide cross-component
data races from `-race` forever, and per-package unit tests only see one
component at a time. The simulation's first run under `-race` immediately
caught a production bug that had survived every other layer of the pyramid
(finding #8 below).

**Tradeoff:** No process boundary means no coverage of SIGTERM shutdown
ordering, WAL crash recovery, or the downtime gate — that stays the fault
harness's and E2E layer's job. The two harnesses are complements, not
alternatives.

### Memory-only stores, crash as data loss

**Choice:** Simulated nodes run without a WAL or SSTables; a crash discards
the node's entire dataset, and its restart rejoins empty under the same
identity.

That is the harshest recovery scenario replication ever faces — everything
must come back through quorum intersection, hinted handoff, and anti-entropy,
with SWIM incarnation refutation reclaiming the node's identity. Disk
recovery paths are already covered at high volume by the store's randomized
model test and end-to-end by the fault harness.

## Findings

Kept here in the spirit of the [fault harness's findings
list](fault-injection.md#findings-the-harness-surfaced):

8. **Member snapshots were shared pointers** — *fixed by copy-on-read.*
   `MemberList.Get/GetAll/GetAlive` returned pointers to the internal member
   structs, which every mutator (heartbeat increments, suspect/dead verdicts,
   merges) writes in place under the list's lock. Readers outside the lock —
   the gossip round marshaling the member list, the stale checker, the ring's
   health filter, coordinator handlers — raced with every status change. The
   simulation harness's first `-race` run caught it (goroutine counts in one
   process finally let the detector see gossip and the data path
   simultaneously); process-level tests could never have. The getters now
   return snapshot copies, pinned by a concurrent-hammer regression test.

## Running it

```bash
make sim               # default: 3 seeds per scenario
make sim-race          # same suite under the race detector
SIM_SEEDS=50 make sim  # wider sweep
SIM_SEED=43 make sim   # replay one seed
SIM_LOG=1 SIM_SEED=43 make sim  # replay with node logs
```

## See Also

- [Fault Injection](fault-injection.md) - the process-level complement:
  real binaries, real signals, real sockets
- [History Checking](history-checking.md) - the porcupine checker both
  harnesses share
- [Testing](testing.md) - where simulation sits in the pyramid
