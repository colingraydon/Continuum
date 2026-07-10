# Testing

> How the full test suite is layered, what each layer proves, and where a new
> test belongs.

## The pyramid

Each layer trades speed for realism. The lower layers run on every `go test`;
the heavier ones are opt-in behind build tags so the default loop stays fast.

| Layer | Command | Isolation | Runtime |
| ----- | ------- | --------- | ------- |
| Unit + package tests | `make test` | single package, in-process | seconds |
| Randomized store model test | part of `make test` | real files, one process | ~25s (reduced under `-short`) |
| In-process cluster tests | `make e2e` | multiple nodes, one process, real HTTP | seconds |
| Process-based E2E | `make e2e-integration` | real binaries, real signals | ~10s |
| Fault injection | `make fault` | real binaries + fault proxies | ~2-3 min |
| Seeded simulation | `make sim` | whole cluster, one process, in-memory network | ~20s (scales with `SIM_SEEDS`) |
| Benchmarks | `make bench` | micro | varies |

Supporting passes: `make test-race` (race detector across all packages),
`make lint` (golangci-lint), `make coverage` (HTML report).

## Layer 1: unit and package tests

Every package has co-located `_test.go` files covering its contract plus its
failure paths. The failure-path tests are deliberate and injected, not
incidental: the WAL tests corrupt frames and tear tails, the SSTable tests
truncate and bit-flip files, the hint log tests inject append/sync/close
errors through the `logWriter` seam, and the store durability tests inject
WAL failures through the `store.WAL` interface to prove
durable-before-visible ordering. If a package has an I/O boundary, there is a
seam for making that boundary fail.

## Layer 2: the randomized store model test

`TestStoreRandomOpsAgainstModel` (`internal/store/model_test.go`) drives the
whole LSM stack with random interleavings that no hand-written test would
enumerate: puts, deletes, forced flushes, compactions, and crash-reopens
(abandon the store without a final flush, then recover from the manifest'd
tables plus the WAL tail, exactly like a process kill).

Because the workload is single-writer with strictly increasing clocks, the
expected state is a plain map: after every operation and every recovery, the
store's visible value for a key must equal the model's exactly. Any
divergence pinpoints a bug in merge, flush, compaction, replay, or recovery.
Three fixed seeds run 1,500 operations each (one seed, 400 ops under
`-short`), so failures are reproducible: a failing seed is a bug report.

## Layer 3: in-process cluster tests

`make e2e` runs the `TestE2E*` tests in the `api` package. Each "node" is a
full handler stack (ring, member list, store) behind an `httptest.Server`,
so multiple nodes talk real HTTP to each other inside one process: quorum
fan-out, replica reads, conflict surfacing, membership propagation, and ring
convergence, with none of the process-management overhead. This is the
cheapest place to test coordinator/replica interaction logic.

## Layer 4: process-based E2E (`tests/e2e`, build tag `e2e`)

`TestMain` compiles the actual binary once, then scenarios spawn real
processes with real config env vars and talk to them over real sockets. This
layer covers what only a process boundary can prove: graceful shutdown on
SIGTERM, persistence across restart, WAL-only crash recovery, the downtime
gate, and hinted handoff between separately-started processes.

## Layer 5: fault injection (`tests/fault`, build tag `fault`)

The adversarial layer: clusters run behind harness-owned TCP/UDP proxies, and
scenarios inject crashes, hangs, asymmetric partitions, gossip packet loss,
and coordinator crashes with buffered hints, while a causal workload records
every acknowledged write. Two invariants are asserted after every scenario:
no acknowledged write is ever lost, and all replicas converge to identical
sibling sets. A second workload races CAS clients on shared keys and feeds
the recorded operation history through a porcupine linearizability checker
([History Checking](history-checking.md)), which both proves the healthy-path
CAS contract and reproduces the churn-window violation as finding #7. See
[Fault Injection](fault-injection.md) for the architecture, the scenario
catalog, and the system findings the harness surfaced.

## Layer 6: seeded simulation (`tests/sim`, build tag `sim`)

The volume layer: the whole cluster runs in one process behind a seeded
in-memory network, with production timing compressed ~40x, so one run packs a
generated fault schedule (partitions, isolation, drops, latency, crash with
total state loss) and thousands of checked operations into a few seconds.
Same invariants as the fault harness plus the porcupine check; failures
replay by seed. Being in-process, `make sim-race` gives the race detector its
only whole-system view - which is how finding #8 (shared member pointers) was
caught. See [Simulation Testing](simulation.md).

## Benchmarks

`benchmarks/` holds microbenchmarks for the hash ring and store
(`make bench`). Results and methodology live in
[Benchmarks](benchmarks.md); broader coverage (anti-entropy, gossip,
end-to-end latency) is on the roadmap.

## Build tags and why

`go test ./...` must stay fast and dependency-free, so the two suites that
spawn processes are tagged out of the default build:

```bash
go test ./...                                  # layers 1-3
go test -tags e2e   -timeout 120s ./tests/e2e/...
go test -tags fault -timeout 900s ./tests/fault/...
```

All fault-harness files are `_test.go` files behind the `fault` tag, so they
are invisible to normal builds, vet, and lint runs without the tag.

## CI mapping

Every push and PR to `main` runs (see `.github/workflows/ci.yml`):

- **test**: `go vet` + `go test` with coverage uploaded to Codecov
- **e2e-integration**: layer 4
- **fault-injection**: layer 5
- **lint**: golangci-lint
- **docker**: image build, gated on the jobs above

CodeQL runs as a separate scheduled workflow, and Dependabot keeps
dependencies fresh with auto-merged patch/minor PRs.

## Where does a new test belong?

Work down this list and stop at the first match:

1. Testing one package's logic or an error path it owns: package unit test,
   with an injected fault seam if the path is I/O.
2. Testing a storage-engine interaction across operations (flush vs replay vs
   compaction): extend the model test's operation mix, or add a scenario seed.
3. Testing coordinator/replica protocol behavior: in-process `TestE2E*` test
   in `api`.
4. Testing something only a real process shows (signals, restart, recovery
   ordering, config): `tests/e2e`.
5. Testing behavior under failure (kills, hangs, partitions, loss) or a
   durability/convergence guarantee: a `tests/fault` scenario composed from
   the existing cluster, fault, workload, and verifier pieces.
