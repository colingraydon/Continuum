# Benchmarks

> Performance measurements for the hash ring. Results are from `benchmarks/ring_bench_test.go`.

## Scope

These benchmarks cover the **hash ring** only (`internal/ring`). They measure the core routing operation - how fast the system can look up a key's responsible node given a populated ring. Store read/write throughput, gossip overhead, anti-entropy sync cost, and end-to-end HTTP latency are not yet benchmarked.

Run benchmarks with:
```bash
make bench
```

## Hash Ring - Lookup Throughput

Measured on Apple M3 Max.

| Operation | Throughput | Latency |
| --------- | ---------- | ------- |
| GetNode (3 nodes) | ~9M ops/sec | 112 ns/op |
| GetNode (100 nodes) | ~5M ops/sec | 201 ns/op |
| GetNode (parallel) | ~6M ops/sec | 160 ns/op |
| AddNode | ~8K ops/sec | 116 µs/op |
| RemoveNode | ~10K ops/sec | 101 µs/op |

## Vnode Count Impact on Lookup Latency

| Vnodes per node | Latency |
| --------------- | ------- |
| 10 | 96 ns/op |
| 50 | 105 ns/op |
| 150 | 114 ns/op |
| 500 | 129 ns/op |

Going from 10 to 500 vnodes adds only 33ns to lookup latency. The RBT `Ceiling()` operation is O(log n) in total vnode count, but the constant factor is small enough that the distribution benefit of more vnodes is essentially free at read time.

## Concurrent Read vs Mixed Read/Write

| Workload | Latency |
| -------- | ------- |
| Pure reads | 160 ns/op |
| Mixed reads + writes | 940 ns/op |

The 6x slowdown on mixed workloads comes from write lock acquisition blocking concurrent readers. In a stable cluster, topology changes (node add/remove) are rare - the ring is almost always in the pure-read regime.

## TODO

The following benchmark coverage does not yet exist:

- **KV Store** - `Put` and `Get` throughput under concurrent access, sibling accumulation cost, tombstone GC pause time
- **Anti-Entropy** - Merkle tree build time, bucket comparison cost, sync round duration under various key counts and divergence rates
- **Gossip** - Message serialization/deserialization throughput, convergence time across cluster sizes
- **End-to-End** - Full write and read latency through the HTTP layer with a live replica set, including quorum fan-out overhead
- **Replication fan-out** - Coordinator write latency at different quorum levels and replica counts
