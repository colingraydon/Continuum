# Fault Injection

> How the fault-injection harness works, what invariants it asserts, and what
> it has found. The suite lives in `tests/fault` behind the `fault` build tag.

```bash
make fault    # ~3 minutes; spawns real processes and real sockets
```

## Why process-level

The harness tests the binary, not the packages: real processes, real HTTP
between coordinators and replicas, real UDP gossip, real fsyncs, real SIGKILL.
Everything the unit and in-process tests cannot see (recovery ordering across
restarts, kernel-level connection behavior against a stopped process, torn
processes mid-flush) is in scope here.

## Architecture

Each node runs behind two harness-owned proxies, and the node advertises the
proxy addresses instead of its own:

```
peers ──TCP──▶ tcpProxy ──▶ 127.0.0.1:HTTP_BIND_PORT   (HTTP: replication, sync, hints)
peers ──UDP──▶ udpProxy ──▶ 127.0.0.1:GOSSIP_PORT      (gossip datagrams)
harness ─────────────────▶ 127.0.0.1:HTTP_BIND_PORT    (direct: never faulted)
```

This uses two config knobs added for exactly this split: `HTTP_BIND_PORT`
(bind a different port than the advertised `SELF_ADDRESS`) and
`GOSSIP_ADVERTISE_ADDR` (advertise a gossip address instead of assuming every
node shares one gossip port). Because peers only know the advertised
addresses, all inter-node traffic is interceptable, while the harness keeps an
unfaulted side channel to every node - including partitioned ones.

### Fault vocabulary

| Fault | Mechanism | What peers observe |
| ----- | --------- | ------------------ |
| Crash | SIGKILL | Connection refused; no shutdown path ran |
| Graceful leave | SIGTERM | Push-on-leave, hint flush, clean WAL finalize |
| Hang | SIGSTOP / SIGCONT | Connections accepted by the kernel, then time out (distinct from refused) |
| Restart | new process, same identity, ports, and `DATA_DIR` | Recovery from disk |
| Inbound blackhole | TCP proxy severs and refuses; UDP proxy drops | Node unreachable, but its own outbound still flows (asymmetric partition) |
| Link latency | TCP proxy delays each forwarded chunk | Slow replica |
| Gossip loss | UDP proxy drops N‰ of datagrams | Lossy network |

Symmetric total isolation is approximated by SIGSTOP (the node can neither
send nor receive). The proxies are inbound-only, so pairwise partitions
between specific nodes are not expressible; asymmetric isolation of one node
plus hangs cover the interesting single-node cases.

## Workload and invariants

The workload runs concurrent causal read-modify-write clients: each worker
owns a disjoint key set (single writer per key), GETs to pick up current
vector clocks, PUTs with them, and records which sequence numbers were
acknowledged with 204. Single-writer keys make verification exact:

- **Durability**: after faults heal, a consistent read of every key must
  return a value at or beyond the last acknowledged sequence. An acked write
  may only ever be superseded by a *later* write on that key, never lost.
- **Convergence**: after faults heal and anti-entropy has run, every running
  replica of a key must hold an identical, non-empty sibling set (verified by
  direct replica reads that bypass coordinator merging and read repair).

## Scenario catalog

| Test | Fault | What it proves |
| ---- | ----- | -------------- |
| `ReplicaCrashRestartUnderLoad` | SIGKILL + restart mid-workload | Quorum keeps accepting writes; wiped node refills; no acked write lost |
| `CrashRecoveryFromWALAndTables` | clean stop, then SIGKILL | Recovery from SSTables + WAL tail, incl. tombstones and overwrites |
| `HungReplicaTimeoutsAndRecovery` | SIGSTOP 8s | Replica-timeout path buffers hints; suspect→alive transition delivers them |
| `AsymmetricPartitionHeals` | inbound blackhole 14s | Both sides keep writing; membership self-heals; siblings from the split merge everywhere |
| `HintLogSurvivesCoordinatorCrash` | blackhole replica, SIGKILL coordinator | Persistent hint log replays after crash and repairs the replica with anti-entropy disabled |
| `DecommissionPushesKeysToSuccessors` | SIGTERM | Push-on-leave moves solely-held keys to successors, no anti-entropy needed |
| `QuorumLossThenClampedAvailability` | SIGKILL 2 of 3 | Writes 503 while dead nodes hold ring seats, then resume once the ring shrinks |
| `GossipPacketLossStability` | 40% gossip drop | No false death verdicts; data path unaffected |

## Findings the harness surfaced

Things learned building and running this suite, kept here because they are
system behaviors, not test artifacts:

1. **Anti-entropy repairs one random vnode per sync round**
   (`syncRound` in `internal/antientropy/manager.go`). Time to cover the
   keyspace scales with vnode count: at the default `REPLICAS=150` and 30s
   interval, a given key's range is synced roughly every 75 minutes per node.
   The harness runs `REPLICAS=8` with a 2s interval so convergence is
   observable; production convergence expectations should be set accordingly
   (or the sync loop extended to cycle through vnodes rather than sample
   randomly).
2. **Anti-entropy primary ranges are computed once at startup** and never
   rebuilt on membership change. A node that starts alone considers itself
   primary for the whole keyspace forever; ranges do not adapt when nodes
   join or leave. Today this only over-syncs, but it will matter for any
   feature that relies on accurate primary ownership.
3. **A rejoining node's heartbeat restarts at zero**, so peers that remember
   its pre-crash heartbeat ignore its gossip until the counter catches up
   (roughly its previous uptime in seconds). The harness re-registers
   restarted nodes via `POST /nodes` to reset the stored entry; production
   rejoin after crash relies on the same catch-up or on re-registration.
   SWIM-style incarnation numbers would remove the wedge.
4. **Write quorum clamps to the live replica set** (`min(W, len(replicas))`).
   Once gossip removes dead members from the ring, a 3-node cluster that lost
   two nodes acknowledges single-copy writes. W is a consistency knob against
   the *current* ring, not a hard durability floor. `QuorumLossThenClampedAvailability`
   pins this behavior so any future change to it is a conscious one.
5. **Hints are only delivered on an alive *transition*.** During an
   asymmetric partition the isolated node never looks dead to its peers (its
   outbound gossip still flows), so hints buffered for it are never
   triggered for delivery; repair falls to anti-entropy. Delivery on a
   periodic timer, or on failed-then-succeeded probes, would close this.

## Extending the suite

New scenarios compose from the same pieces: `newCluster` (per-test topology
and quorum config), fault primitives on `cluster`/`node` (`kill`, `shutdown`,
`pause`, `resume`, `restart`, `isolate`, `heal`, proxy latency and drop
knobs), `newWorkload` for load with history, and `verifyDurability` /
`verifyConvergence` for the invariants. Keep scenarios deterministic where
possible (seeded writes, explicit fault windows) and lean on the invariant
checkers rather than sleeping and asserting exact states.

Related: the storage engine has a separate randomized model test
(`TestStoreRandomOpsAgainstModel` in `internal/store`) that drives random
put/delete/flush/compact/crash-reopen sequences against an in-memory model,
covering single-node LSM correctness at much higher operation counts than
process-level tests can.
