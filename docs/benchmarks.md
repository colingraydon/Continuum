# Benchmarks

> Performance measurements across the stack: ring routing, the in-memory and
> LSM store paths, Merkle/anti-entropy costs, gossip codec, and full
> coordinator round trips on an in-process cluster.

## Scope and method

All numbers below are from one run of `make bench` on an Apple M3 Max (14 cores,
`goos: darwin`, `goarch: arm64`), Go's standard `testing.B` framework with
`-benchmem`. Suites live in `benchmarks/`:

| File | Covers |
| ---- | ------ |
| `ring_bench_test.go` | Key routing, topology changes, vnode-count impact |
| `store_bench_test.go` | In-memory store: puts, gets, conflicts, vector clocks |
| `store_lsm_bench_test.go` | Durable path: WAL group commit, SSTable reads, scans, GC pause |
| `antientropy_bench_test.go` | Merkle maintenance, sync-state serving, tree rebuild |
| `gossip_bench_test.go` | Message codec and member-list merge |
| `replication_bench_test.go` | In-process 3-node cluster: quorum fan-out latency by consistency level |

Numbers are hardware- and load-dependent; treat them as one machine's
snapshot and the *ratios* as the durable findings.

## Published dataset (percentiles)

`go test -bench` reports only means, so the citable dataset comes from a
dedicated harness: `make bench-report` (`cmd/benchreport`) runs each scenario
with per-operation timing, computes **exact nearest-rank percentiles** over
the recorded samples (p50/p90/p99/p999, plus min/mean/max and throughput),
and writes three provenance-stamped artifacts to `docs/data/`:

| File | Purpose |
| ---- | ------- |
| `benchmarks.json` | Latest snapshot; a static frontend fetches this directly |
| `benchmarks.csv` | Flat companion for spreadsheets |
| `history.ndjson` | One line per run, for trend charts over time |

Every snapshot embeds `git_commit`, `generated_at`, Go version, and the CPU
model, so any published number is pinnable to exactly what produced it.
Scenarios are microsecond-scale or slower, where individual timer readings
are meaningful - HTTP GET/PUT per consistency level (the user-facing key
lookup), SSTable point reads, durable writes with and without group commit,
sync-state serving, and scan pages. The one nanosecond-scale entry (raw ring
lookup) is measured over 1000-op batches and labeled accordingly.

Generate on a known machine, not shared CI: the dataset is meant to be
reproducible and citable, and CI runners are neither.

## Regression gate in CI

Every pull request runs the CPU-bound subset (`make bench-ci`) twice on the
same runner - once at the PR's base commit, once at its head - and compares
with `benchstat`. A statistically significant time regression above 20% fails
the build (`scripts/benchguard.sh`). Same-runner A/B comparison cancels most
shared-VM variance; the significance test filters the rest, so a noisy run
shows up as `~` and never fails the gate. The fsync-bound, cluster-setup, and
multi-millisecond benchmarks are excluded from the gate as too noisy or slow
for CI - they remain in `make bench` for local measurement.

## Hash ring

| Operation | Latency |
| --------- | ------- |
| GetNode, 3 nodes | 116 ns/op |
| GetNode, 100 nodes | 217 ns/op |
| GetNode, parallel | 151 ns/op |
| AddNode (150 vnodes) | 121 µs/op |
| RemoveNode | 108 µs/op |

Vnode count barely moves lookup latency — the RBT `Ceiling()` is O(log n)
with a small constant:

| Vnodes per node | Latency |
| --------------- | ------- |
| 10 | 96 ns/op |
| 50 | 108 ns/op |
| 150 | 117 ns/op |
| 500 | 133 ns/op |

Mixed read/write workloads pay for the write lock: pure concurrent reads run
at 177 ns/op, reads mixed with topology churn at 874 ns/op. In a stable
cluster the ring lives in the pure-read regime.

## KV store — in-memory path

| Operation | Latency |
| --------- | ------- |
| Put (memory only) | 495 ns/op |
| Get (memtable hit) | 131 ns/op |
| Put dropped as dominated | 79 ns/op |
| Put, 8 parallel writers | 595 ns/op |
| Mixed concurrent reads/writes | 359 ns/op |
| Vector clock Increment | 116 ns/op |
| Vector clock HappensBefore | 65 ns/op |

## KV store — durable path (WAL + SSTables)

**Group commit.** A single sequential writer pays a full fsync per write;
concurrent writers share fsyncs through `SyncUpTo` batching:

| Workload | Per-write latency | Throughput |
| -------- | ----------------- | ---------- |
| 1 writer (fsync per write) | 4.30 ms/op | ~230 writes/s |
| 8 parallel writers (group commit) | 0.57 ms/op | ~1,740 writes/s |

Group commit buys **~7.5×** aggregate write throughput at 8 workers — the
"before/after" the group-commit work promised. The ceiling is the device
fsync rate; more concurrency amortizes each fsync over more writes.

**Table reads.** Point reads that miss the memtable go to the SSTables:

| Read | Latency |
| ---- | ------- |
| Get from 1 table, no cache (bloom → index → 1 block read) | 1.9 µs/op |
| Get from 1 table through the block cache (steady state) | 0.55 µs/op |
| Get spread over 4 tables, no cache | 2.1 µs/op |

The multi-generation penalty is small because bloom filters short-circuit the
tables that don't hold the key (~1% false-positive rate at 10 bits/key). The
block cache is the before/after of the compression + cache work: an uncached
read pays a filesystem read, a CRC check, and block decompression on every
probe; a steady-state cached read skips all three and lands **~3.5× cheaper**.

**Scans and GC:**

| Operation | Latency |
| --------- | ------- |
| Scan page (100 keys of a 10k-key prefix, 1 table + memtable overlay) | 4.8 ms/op |
| Scan page (100 keys served from a 10k-key memtable, ordered overlay) | 10 µs/op |
| Tombstone GC pass over 10k aged tombstones | 0.8 ms pause |

The memtable overlay is now an ordered skiplist, so it seeks to the range start
and touches only the matching keys instead of sweeping and sorting the whole
memtable per scan. The table-backed scan number still reflects a documented
tradeoff: a page reads the *entire* prefix range from the SSTable before
cutting to `limit`, so paging through a large table prefix stays O(range) per
page. A k-way merging iterator with early termination across tables is the
known follow-up if scan volume grows.

## Anti-entropy and Merkle trees

| Operation | Latency |
| --------- | ------- |
| Tree Update (per-write onUpdate callback) | 108 ns/op |
| ComputeBucketHash over 1k entries | 102 µs/op |
| Serve sync state from maintained tree (10k-key store) | 117 µs/op |
| Serve sync state via store scan (the pre-#56 path) | 3.08 ms/op |
| Full tree rebuild on membership change (10k keys) | 2.4 ms/op |

Serving `GET /sync` from maintained trees is **~26× cheaper** than the
scan-and-hash fallback at 10k keys — and the gap grows linearly with data
size, since the scan is O(total data) while the tree read is O(range). This
is the before/after for the incremental-Merkle work; the fallback path still
exists (and is benchmarked) because a vnode without a tree falls back to it.

The 2.4 ms rebuild runs only on actual membership changes; the 108 ns
incremental update is what every write pays.

## Gossip

| Operation | Latency | Allocs |
| --------- | ------- | ------ |
| Marshal message, 10 members | 3.2 µs/op | 11 |
| Marshal message, 100 members | 31 µs/op | 101 |
| Unmarshal message, 10 members | 14 µs/op | 54 |
| Unmarshal message, 100 members | 130 µs/op | 417 |
| MemberList.Merge of a 100-member view | 0.86 µs/op | 0 |

At the default 1 s gossip interval with fanout 3, a 100-node cluster spends
roughly (31 µs + 3×130 µs) ≈ 0.4 ms/s of CPU on codec work per node - JSON is
nowhere near the bottleneck at this scale. The zero-alloc steady-state Merge
means an exchange that changes nothing is effectively free.

## Coordinator round trips (in-process 3-node cluster)

Real handlers, rings, member lists, and HTTP over loopback; RF=3, memory-only
stores. Measures the full coordinator path: local write/read, replica
fan-out, quorum wait, and (for reads) sibling merge.

| Request | Latency |
| ------- | ------- |
| PUT `consistency=one` | 60 µs/op |
| PUT `consistency=quorum` | 104 µs/op |
| PUT `consistency=all` | 111 µs/op |
| GET `consistency=one` | 64 µs/op |
| GET `consistency=quorum` | 102 µs/op |
| GET `consistency=all` | 110 µs/op |
| Scan page (100 keys, scatter-gather across 3 nodes) | 1.5 ms/op |

Waiting on one extra replica costs ~40 µs over loopback — on a real network
this becomes the inter-node RTT, which is the point of per-request
consistency: the client chooses how many RTTs each operation is worth.
`one` still fans out to all replicas (they must receive the write); it just
doesn't wait for them.

## Reading these numbers together

- A durable quorum write is dominated by the WAL fsync (~ms), not the fan-out
  (~0.1 ms). Group commit is what makes durable throughput usable.
- Reads are cheap at every layer (131 ns memtable, 0.55 µs cached / 1.9 µs
  uncached table, 100 µs quorum round trip); the block cache cut the middle
  layer ~3.5× for hot keys.
- Anti-entropy's background costs are all sub-ms except full rebuilds, which
  only membership changes trigger.
