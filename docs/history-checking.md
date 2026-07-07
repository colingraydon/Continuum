# History Checking (`internal/histcheck`, `tests/fault`)

> Formal linearizability verification of recorded operation histories with
> [porcupine](https://github.com/anishathalye/porcupine), replacing
> spot-checked invariants with a checker that considers every legal
> interleaving.

## Overview

The fault harness's original invariants sample the system's final state:
after faults heal, the last acknowledged write per key must be visible
(durability) and replicas must agree (convergence). Those checks say nothing
about what clients *observed while the fault was live* — a history can end
converged and still have served an unserializable interleaving along the way.

History checking closes that gap for the one code path that claims strong
semantics: conditional writes. A workload of racing CAS clients records every
operation — input, output, invocation time, response time — and a
linearizability checker then searches for a sequential ordering of those
operations, consistent with real time, that a CAS register could have
executed. If none exists, the history is a proof of a consistency violation,
and porcupine renders an interactive visualization pinpointing the first
operation no ordering can explain.

The default sloppy-quorum path is *not* checked for linearizability: it is
eventually consistent by design, surfaces concurrent writes as siblings, and
no sequential register models it. Its contract stays covered by the
durability and convergence invariants.

## The sequential model

`internal/histcheck` models each key as an independent CAS register (porcupine
partitions the history per key, which turns one NP-hard search over thousands
of operations into many small ones):

- State: the register's current value, or absent.
- `read -> v` is legal iff the state is `v`; `read -> absent` iff absent.
- `cas(expected, new) -> 204` is legal iff the state equals `expected`
  (absent when the write sent no clocks), and sets the state to `new`.
- `cas(expected, new) -> 412` is legal iff the state does *not* equal
  `expected`, and changes nothing.
- A read that surfaces a sibling conflict is never legal: CAS refuses to
  create siblings, so siblings in a pure-CAS keyspace prove a forked write.

The workload maps the protocol onto that model: each client loops
`GET ?consistency=serial` (learning the current value and its merged clock
from `X-Session-Clock`), then `PUT ?cas=true` with that clock as
precondition.
Because every value written is unique and each distinct state has a distinct
vector clock, "the clock I read" and "the value I read" identify each other,
so the clock-domination precondition the store actually checks and the
value-equality precondition the model checks coincide.

### Unknown outcomes

A CAS that times out or returns 5xx may still have committed — a 503 for
missed write quorum is returned *after* the primary's local commit, and a
request stuck in a hung node's kernel buffer can apply seconds later. The
checker handles these with the standard open-interval technique: the
operation's return time is pushed past the end of the history, so the search
may linearize it at any point after its call — including after every observed
read, which is observationally identical to it never happening. A 412 needs
no such treatment: it guarantees no side effects.

## Scenarios and finding #7

| Scenario | Faults | Expectation |
| -------- | ------ | ----------- |
| `LinearizableCASHealthy` | none | Hard assertion: the history linearizes |
| `LinearizableCASAcrossPrimaryFailover` | SIGKILL + restart (downtime-gate wipe) | Hard assertion since bootstrapping rejoin (finding #10) |
| `LinearizableCASAcrossPartition` | asymmetric partition + heal | Hard assertion since paxos-backed CAS |

Under the original primary-serialized CAS design, the churn scenarios failed
the moment they existed: the checker made the documented concession concrete
as [finding #7](fault-injection.md#findings-the-harness-surfaced). CAS
serialized against the *current primary's local state*, and churn moved the
primary role without moving the state; a new primary that missed the last
acknowledged write served stale reads and accepted a CAS from the superseded
value, forking history. The diagnosis pass reports three mechanical
signatures of that root cause alongside the porcupine visualization:

- **forked generations** — two acknowledged CAS writes from the same expected
  value;
- **stale reads** — a value observed after the CAS that replaced it was
  acknowledged (values never repeat, so this is unambiguous);
- **conflict reads** — the forked siblings later surfacing to a client.

The scenarios ran as detectors — logging violations instead of failing —
until [paxos-backed CAS](client-consistency.md) closed the window; the
partition scenario became a hard assertion, and the failover scenario
immediately surfaced the *next* gap (finding #10: a downtime-gate wipe
under-replicated committed values until repair), detected it while
bootstrapping rejoin was built, and asserts linearizability again since.
The checker also drove the paxos fix itself, catching three protocol bugs
in the wiring that final-state checks cannot see:
the isolated-node "majority of one" fork (quorum denominators must include
dead members), the false 412 a retrying coordinator returned after a rival
round resurrected and committed its own mutation, and the resurrection of
superseded sub-majority debris over a newer commit (acceptors now track and
report their highest committed ballot so coordinators can tell an in-flight
round from a dead one's leftovers).

## Design Decisions

### Check CAS, not the sloppy-quorum path

**Choice:** Only conditional writes are checked for linearizability.

Linearizability is the contract CAS implies and the strongest claim the
system makes; checking it there turns "CAS gives lock-like semantics" into a
machine-verified property. The default path deliberately trades that for
availability — checking it against a register model would only re-discover
that design decision as a "violation".

**Tradeoff:** Session guarantees (read-your-writes, monotonic reads) are
weaker-than-linearizable properties the checker does not yet model; they
remain covered by unit tests only.

### Detectors for known gaps, hard assertions everywhere else

**Choice:** A scenario that reproduces a *documented, open* gap reports the
violation and keeps the suite green; once the gap is closed, one helper call
flips it into the regression test for the fix. The churn scenarios lived as
detectors for finding #7 and are hard assertions now; the simulation
harness's crash schedules remain the one detector left, because its
memory-only nodes legitimately lose their paxos promises with everything
else.

A permanently red test trains people to ignore the suite; a skipped test
stops observing the behavior. The detector keeps the reproduction running on
every pass, which is also what makes the flip trustworthy: the fix is
verified against the exact workload that broke the predecessor.

**Tradeoff:** While a gap is open, a *new* regression in the same path is
reported but does not fail CI. The healthy-cluster assertion bounds the
exposure: any regression visible without churn still fails hard.

### Client-observed histories only

**Choice:** The checker sees exactly what clients saw — status codes, bodies,
and wall-clock windows — with no instrumentation inside the store or the
coordinator.

That is what makes a violation meaningful: it is a client-visible consistency
breach, not an internal-state technicality. It also keeps the recorded
timestamps honest (one monotonic clock in the test process) and the checked
system identical to the shipped one.

**Tradeoff:** Diagnosis has to be reconstructed from the outside, which is
why the harness pairs every violation with the fork/stale-read scan, the
porcupine visualization, and a raw JSON dump of the history —
`go run ./cmd/histbisect <dump.json>` then binary-searches each key's
subhistory for the minimal failing prefix, which is how the false-412
retry bug in the paxos wiring was isolated.

## See Also

- [Fault Injection](fault-injection.md) - the harness these scenarios run on,
  and the findings list this feeds
- [Client Consistency](client-consistency.md) - the CAS design being checked,
  including the churn window it concedes
- [Testing](testing.md) - where history checking sits in the test pyramid
