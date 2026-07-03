# Continuum

[![CI](https://github.com/colingraydon/Continuum/actions/workflows/ci.yml/badge.svg)](https://github.com/colingraydon/Continuum/actions/workflows/ci.yml)
[![CodeQL](https://github.com/colingraydon/Continuum/actions/workflows/codeql.yml/badge.svg)](https://github.com/colingraydon/Continuum/actions/workflows/codeql.yml)
[![codecov](https://codecov.io/gh/colingraydon/Continuum/branch/main/graph/badge.svg)](https://codecov.io/gh/colingraydon/Continuum)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=colingraydon_Continuum&metric=alert_status)](https://sonarcloud.io/project/overview?id=colingraydon_Continuum)
[![Go Report Card](https://goreportcard.com/badge/github.com/colingraydon/continuum)](https://goreportcard.com/report/github.com/colingraydon/continuum)
[![Go](https://img.shields.io/badge/go-1.26-blue)](https://go.dev)
[![License](https://img.shields.io/github/license/colingraydon/Continuum)](LICENSE)

A distributed key-value store implementing the core data layer patterns from Cassandra and Dynamo - written in Go.

Continuum maps keys to nodes via a consistent hash ring, propagates cluster membership through a gossip protocol, fans writes to N replicas with quorum acknowledgment, surfaces concurrent writes as vector clock siblings rather than discarding them, and repairs divergent replicas through a combination of inline read repair, event-driven hinted handoff, and background Merkle tree anti-entropy. With `DATA_DIR` set, the store runs as an LSM engine: every write goes through a CRC-checked write-ahead log whose fsyncs are batched across concurrent writers by group commit, the memtable flushes to immutable SSTables with bloom filters on a size threshold, and reads merge across generations, so state survives restart and the dataset is no longer RAM-bound on the write path. Buffered hints are persisted to their own append-only log, so undelivered writes survive a coordinator crash rather than depending on anti-entropy alone.

---

## Architecture

```mermaid
flowchart TB
    Client([Client]) --> HTTP[HTTP Handlers]

    subgraph core [internal - core]
        direction LR
        Ring[Ring]
        Gossip[Gossip]
        Store[KV Store / Memtable]
        subgraph storage [LSM storage]
            direction LR
            WAL[WAL - group commit]
            SST[SSTables + Bloom]
            Compact[Compaction]
        end
    end

    subgraph bg [internal - background]
        direction LR
        AE[Anti-Entropy]
        HH[Hint Store]
        HintLog[Hint Log]
    end

    HTTP --> Ring
    HTTP --> Gossip
    HTTP --> Store
    Gossip --> Ring

    Store -->|append| WAL
    Store -->|flush| SST
    Compact -->|merge| SST

    Gossip --> HH
    Store --> AE
    HH -->|persist| HintLog

    AE -.->|sync| HTTP
    HH -.->|replay| HTTP
```

| Layer | Package | Role |
| ----- | ------- | ---- |
| Hash Ring | `internal/ring` | Routes keys to nodes via consistent hashing with a Red-Black Tree and virtual nodes |
| Gossip | `internal/gossip` | Cluster membership and failure detection over UDP without a central coordinator |
| KV Store | `internal/store` | LSM engine: vector clock versioning, tombstone deletes, memtable flush, merged reads |
| WAL + SSTables | `internal/wal`, `internal/sstable` | Crash-durable append-only log with CRC framing; immutable sorted tables with bloom filters |
| Anti-Entropy | `internal/antientropy` | Background Merkle-tree comparison and bidirectional repair of divergent replicas |
| Hint Store | `internal/hintstore` | Durability buffer that replays missed writes when a down replica recovers |
| HTTP API | `api` | Transport, read repair, data migration, Prometheus instrumentation |

---

## Quick Start

**Single node**

```bash
make run
```

**Three-node cluster with Prometheus and Grafana**

```bash
make docker-run
```

| Service | Address |
| ------- | ------- |
| node1 | `http://localhost:8080` |
| node2 | `http://localhost:8082` |
| node3 | `http://localhost:8083` |
| Prometheus | `http://localhost:9090` |
| Grafana | `http://localhost:3000` (admin / admin) |

**Write and read a key**

```bash
curl -X PUT http://localhost:8080/keys/user:123 \
  -H "Content-Type: application/json" \
  -d '{"value": "alice"}'

curl http://localhost:8080/keys/user:123
```

**Development commands**

```bash
make test      # unit and integration tests
make e2e       # end-to-end tests (spawns real processes)
make fault     # fault-injection suite (kills, hangs, partitions, packet loss)
make bench     # benchmarks
make lint      # golangci-lint
make coverage  # HTML coverage report
```

---

## Documentation

| Doc | Contents |
| --- | -------- |
| [Architecture](docs/architecture.md) | System diagram, layer map, write and read path narratives |
| [Hash Ring](docs/ring.md) | RBT, murmur3, virtual nodes, weighted allocation, health filter |
| [Gossip](docs/gossip.md) | UDP transport, membership lifecycle, failure detection, convergence |
| [Replication](docs/replication.md) | Vector clocks, quorum, sibling surfacing, fan-out implementation |
| [Anti-Entropy](docs/antientropy.md) | Merkle trees, bidirectional sync, tombstone GC safety argument |
| [Hinted Handoff](docs/hinted-handoff.md) | Durability gap, hint lifecycle, event-driven delivery, graceful flush |
| [Persistence](docs/persistence.md) | WAL framing, snapshot format, recovery flow, downtime gate |
| [SSTable](docs/sstable.md) | Immutable sorted table format: data blocks, sparse index, bloom filter, LSM roadmap |
| [Read Repair](docs/read-repair.md) | Async repair, always-repair-on-conflict, X-Proxied-From path reuse |
| [Data Migration](docs/data-migration.md) | Pull on join, push on leave, bootstrapping state machine |
| [Fault Injection](docs/fault-injection.md) | Process-level fault harness: proxies, kill/hang/partition scenarios, durability and convergence invariants |
| [Testing](docs/testing.md) | The full test pyramid: unit and fault-seam tests, randomized store model, in-process clusters, process E2E, fault injection |
| [API Reference](docs/api.md) | All endpoints with request/response examples and internal headers |
| [Operations](docs/operations.md) | Env vars, Docker setup, Makefile targets, Prometheus metrics |
| [Benchmarks](docs/benchmarks.md) | Hash ring throughput and latency measurements |

---

## What's Next

- **Benchmark coverage** - store, anti-entropy, gossip, and end-to-end latency benchmarks, including before/after numbers for group commit
- **Sharded store** - split the single store mutex into 256 shards to unlock parallel replay and parallel snapshot iteration
- **Anti-entropy cadence** - the sync loop repairs one random vnode per round, so keyspace repair time scales with vnode count; cycle deterministically and rebuild primary ranges on membership change (found by the [fault harness](docs/fault-injection.md#findings-the-harness-surfaced))
- **Gossip incarnation numbers** - a rejoining node's heartbeat restarts at zero, so peers ignore its gossip until it outruns its pre-crash counter; SWIM-style incarnations would fix crash rejoin (also a fault-harness finding)
- **Incremental Merkle state** - replicas currently recompute sync-state bucket hashes with a full `KeyHashes` scan on every anti-entropy request, which is O(total data) per sync now that the dataset is disk-resident; maintain persistent, incrementally updated per-vnode Merkle trees on all nodes instead
- **Streaming bootstrap and decommission** - node join pulls keys as one JSON batch per bucket and graceful shutdown materializes the entire dataset in memory for a single push per successor; replace both with chunked, resumable streaming so migration survives datasets larger than RAM
- **Sloppy quorum** - writes fan out only to the strict replica set, so a write is rejected when W of them are down even if healthy nodes exist; walk past failed replicas to the next healthy nodes on the ring, with hints marking the intended owner (the Dynamo "always writable" property)
- **Range scans** - SSTables are sorted and a k-way merge already exists in compaction, but nothing exposes ordered iteration; add a merged range iterator across memtable and tables, then a scatter-gather `GET /keys?prefix=` across vnodes
- **Per-request consistency levels** - R and W are fixed at process start; let clients pick ONE / QUORUM / ALL per request
- **SSTable block compression and block cache** - reads past the bloom filter hit the filesystem on every probe
