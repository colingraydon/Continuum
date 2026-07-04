# Operations

> Running, configuring, and observing a Continuum cluster.

## Environment Variables

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `SELF_ID` | value of `SELF_ADDRESS` | Node identifier used in vector clocks, gossip, and logs |
| `SELF_ADDRESS` | `localhost:8080` | HTTP address the node advertises to peers (and binds, unless `HTTP_BIND_PORT` is set) |
| `HTTP_BIND_PORT` | port of `SELF_ADDRESS` | Port the HTTP listener actually binds. Set when the advertised address differs from the local listener (NAT, port mapping, fault-injection proxies) |
| `GOSSIP_PORT` | `8081` | UDP port the gossip listener binds |
| `GOSSIP_ADVERTISE_ADDR` | host of `SELF_ADDRESS` + `GOSSIP_PORT` | UDP address peers send gossip to. Set when nodes use heterogeneous gossip ports (local multi-process clusters) or sit behind port mapping |
| `REPLICAS` | `150` | Virtual nodes per physical node |
| `REPLICATION_FACTOR` | `3` | Number of replicas per key |
| `WRITE_QUORUM` | majority (`RF/2 + 1`) | Replica acks required before returning 204; overridable per request with `?consistency=` |
| `READ_QUORUM` | majority (`RF/2 + 1`) | Replica responses required for a consistent read; overridable per request with `?consistency=` |
| `REPLICA_TIMEOUT_MS` | `500` | Timeout in milliseconds for inter-node replication and read calls |
| `SYNC_INTERVAL_MS` | `30000` | Interval between anti-entropy sync rounds |
| `HINT_DELIVERY_INTERVAL_MS` | `30000` | Interval between hint delivery sweeps to alive targets (backstops event-driven delivery) |
| `SEED_NODES` | (none) | Comma-separated HTTP addresses to bootstrap from on first join |
| `SELF_WEIGHT` | `1.0` | Capacity weight for vnode allocation; `2.0` gives twice the vnodes |
| `DATA_DIR` | (none) | Directory for WAL + SSTable persistence. Empty disables persistence (memory-only) |
| `MEMTABLE_MAX_BYTES` | `16777216` (16 MiB) | Memtable size that triggers a flush to an SSTable (requires `DATA_DIR`) |

**Notes on specific variables:**

`SELF_ID` defaults to the resolved value of `SELF_ADDRESS`, not the string "SELF_ADDRESS". If `SELF_ADDRESS=node1:8080` and `SELF_ID` is unset, the node identifies itself as `node1:8080`. Set `SELF_ID` explicitly when you want a stable, human-readable identity that is independent of the address.

`WRITE_QUORUM` and `READ_QUORUM` are validated at startup. If either exceeds `REPLICATION_FACTOR`, the process exits with a fatal error - quorum that can never be reached would cause all writes or reads to return 503 immediately.

`SEED_NODES` is only used at startup. It does not need to list every node in the cluster - one live seed is sufficient to join. If omitted, the node starts as a standalone single-node cluster.

`SELF_WEIGHT` follows the formula `round(REPLICAS * SELF_WEIGHT)` with a minimum of 1 vnode. A weight of `0.5` on a 150-replica configuration gives 75 vnodes and roughly half the key space.

`DATA_DIR` enables crash-durable persistence: every `PUT`/`DELETE`/`EVICT`/`GC` is appended to a write-ahead log and fsynced before the in-memory store is updated. When the memtable exceeds `MEMTABLE_MAX_BYTES` it is flushed to an immutable SSTable and the covered WAL segments are deleted; a final flush runs on graceful shutdown. On restart the node opens its SSTables and replays the WAL tail before joining gossip. A node whose last clean shutdown is older than `GCTTL` (24 h) discards its local data and re-bootstraps from peers, so the cluster cannot resurrect tombstones that other replicas have already purged. Pre-LSM data dirs (snapshot-based) are migrated to an SSTable automatically on first startup. See [docs/persistence.md](persistence.md) and [docs/sstable.md](sstable.md) for formats and recovery flow.

## Running Locally

**Single node**
```bash
make run
```

**Three-node cluster (Docker + Prometheus + Grafana)**
```bash
make docker-run
```

| Service | Address |
| ------- | ------- |
| node1 API | `http://localhost:8080` |
| node2 API | `http://localhost:8082` |
| node3 API | `http://localhost:8083` |
| Prometheus | `http://localhost:9090` |
| Grafana | `http://localhost:3000` (admin / admin) |

In Grafana, add `http://prometheus:9090` as a Prometheus data source.

## Makefile Targets

| Target | Description |
| ------ | ----------- |
| `make run` | Build and run a single node with default config |
| `make docker-run` | Start the 3-node Docker Compose cluster |
| `make test` | Run all unit and integration tests |
| `make test-race` | Run all tests under the race detector |
| `make e2e` | Run in-process cluster tests (`TestE2E*` in `api`) |
| `make e2e-integration` | Run process-based end-to-end tests (spawns real binaries) |
| `make fault` | Run the fault-injection suite (kills, hangs, partitions, packet loss) |
| `make bench` | Run all benchmarks with memory stats |
| `make bench-ci` | Run the CPU-bound benchmark subset used by the CI regression gate |
| `make bench-report` | Regenerate the published percentile dataset in `docs/data/` (run on a known machine) |
| `make lint` | Run golangci-lint |
| `make coverage` | Generate HTML coverage report |

## Generating Test Traffic

```bash
./scripts/traffic.sh http://localhost:8080 1000
```

Sends 1,000 random PUT requests to the specified node.

## Prometheus Metrics

Exposed at `GET /metrics` on each node's HTTP port.

| Metric | Type | Description |
| ------ | ---- | ----------- |
| `continuum_http_requests_total` | Counter | Request count by method, path, status |
| `continuum_http_request_duration_seconds` | Histogram | Request latency by method and path |
| `continuum_ring_node_count` | Gauge | Current physical node count |
| `continuum_ring_vnode_count` | Gauge | Current virtual node count |
| `continuum_ring_key_lookups_total` | Counter | Total key lookups performed |
| `continuum_ring_distribution_variance` | Gauge | Key distribution variance across nodes |
| `continuum_ring_healthy_nodes` | Gauge | Nodes currently alive per gossip |
| `continuum_ring_suspect_nodes` | Gauge | Nodes currently suspect per gossip |
| `continuum_ring_dead_nodes` | Gauge | Nodes currently dead per gossip |

**Useful Grafana queries**

```
rate(continuum_http_requests_total[1m])
rate(continuum_http_request_duration_seconds_sum[1m])
continuum_ring_healthy_nodes
continuum_ring_suspect_nodes
continuum_ring_dead_nodes
continuum_ring_distribution_variance
continuum_ring_key_lookups_total
```

## Shutdown

Continuum shuts down gracefully on `SIGINT` or `SIGTERM`:

1. Pushes all locally-held keys to alive successor nodes
2. Marks self as dead in the member list and broadcasts to all alive peers (not just the usual fanout of 3)
3. Flushes pending hints to any currently-alive nodes
4. Stops accepting new HTTP connections and drains in-flight requests with a 30-second timeout
5. **Finalizes persistence** (if `DATA_DIR` is set): flushes the memtable to a final SSTable, truncates WAL segments the table covers, closes the table readers and WAL, and writes `meta.json` with `last_clean_shutdown = now`
6. Stops the gossip transport

The push in step 1 and the flush in step 3 are best-effort - if a successor is unreachable, those keys and hints are not retried. Anti-entropy covers the gap.

Step 5 is what makes the downtime gate work: the gate compares `last_clean_shutdown` against `GCTTL` on the next start. A node that crashes still recovers by opening its SSTables and replaying the WAL tail, as long as its last clean shutdown is within `GCTTL`. If the last clean shutdown is older than `GCTTL` (or `meta.json` is missing entirely), the node discards its local data and re-bootstraps rather than risk resurrecting tombstones that peers have already purged.

## CI Pipeline

Four jobs run on every push and pull request to `main` (see [docs/testing.md](testing.md) for what each layer covers):

- **test** - `go vet` + `go test` with coverage upload to Codecov
- **e2e-integration** - process-based end-to-end tests with a 120-second timeout
- **fault-injection** - the fault-injection suite with a 900-second timeout
- **lint** - golangci-lint
- **bench-regression** (pull requests only) - runs the CPU-bound benchmark subset (`make bench-ci`) on the PR's base commit and then on its head **on the same runner**, compares with `benchstat`, and fails on statistically significant time regressions above 20% (`scripts/benchguard.sh`, threshold via `BENCH_REGRESSION_THRESHOLD`). Same-runner A/B cancels most shared-VM variance and benchstat's significance test filters the rest; insignificant deltas (`~`) never fail the gate. fsync-bound, cluster-setup, and multi-millisecond benchmarks are excluded as too noisy or slow for CI - run `make bench` locally for those.

**docker** runs after all of the above pass and verifies the image builds successfully.

**CodeQL** runs as a separate workflow on push, PR, and a weekly schedule (Mondays at 8am UTC). Results appear in the Security tab under Code scanning.

**Dependabot** opens grouped PRs weekly for Go module and GitHub Actions dependency updates. Patch and minor updates are auto-merged if CI passes.
