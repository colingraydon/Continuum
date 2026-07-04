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
| `QuorumLossThenClampedAvailability` | SIGKILL 2 of 3 | Writes 503 while gossip still believes the dead nodes alive, then resume once the healthy set shrinks (suspect verdict) |
| `GossipPacketLossStability` | 40% gossip drop | No false death verdicts; data path unaffected |
| `PeriodicHintDeliveryAcrossAsymmetricPartition` | inbound HTTP blackhole, gossip up, AE off | Periodic sweep delivers buffered hints with no dead→alive edge and no coordinator restart; failed sweeps re-buffer |
| `SloppyQuorumAlwaysWritable` | SIGKILL 1 of 4, RF=3, AE off | Once the victim is suspect, a `consistency=all` write succeeds via the next healthy node; the skipped owner's hint repairs it after restart |

## Findings the harness surfaced

Things learned building and running this suite, kept here because they are
system behaviors, not test artifacts:

1. **Anti-entropy repaired one random vnode per sync round** — *fixed by
   deterministic round-robin.* Random sampling gave keyspace coverage a
   coupon-collector expected time with an unbounded tail; `syncRound` now
   cycles through the sorted primary vnode ends, bounding a full pass at
   exactly `vnodes x interval`. A pass still scales with vnode count (150
   vnodes at the 30s default is 75 minutes), so the harness keeps running
   `REPLICAS=8` with a 2s interval to make convergence observable within
   test deadlines. See [Anti-Entropy](antientropy.md#one-vnode-per-tick-round-robin).
2. **Anti-entropy primary ranges were computed once at startup** — *fixed by
   rebuild-on-membership-change.* Each sync tick re-derives the primary
   ranges from the ring (a cheap comparison) and rebuilds the Merkle trees
   with one store scan only when membership actually changed them. A node
   that starts alone no longer considers itself primary for the whole
   keyspace forever.
3. **A rejoining node's heartbeat restarts at zero** — *fixed by SWIM-style
   incarnation numbers.* Previously peers that remembered a node's pre-crash
   heartbeat ignored its gossip until the counter caught up (roughly its
   previous uptime in seconds). Now each node carries an incarnation epoch that
   dominates heartbeat when merging; on restart the node refutes its stale
   entry by advancing its incarnation past what peers remember, and a seed
   bootstrap reply guarantees a node buried as `dead` still receives the gossip
   it needs to refute. See [Incarnation Numbers](gossip.md#incarnation-numbers).
   The harness still re-registers restarted nodes via `POST /nodes`, which now
   only speeds up an already-correct convergence.
4. **Write quorum clamps to the live replica set** (`min(W, len(replicas))`).
   A 3-node cluster that lost two nodes acknowledges single-copy writes. W is
   a consistency knob against the *current* ring, not a hard durability floor.
   `QuorumLossThenClampedAvailability` pins this behavior so any future change
   to it is a conscious one. Since sloppy quorum, the clamp engages at the
   suspect verdict (the healthy walk excludes suspects from the fan-out set)
   rather than waiting for the dead verdict to remove the node from the ring.
5. **Hints were only delivered on an alive *transition*** — *fixed by a
   periodic delivery sweep.* During an asymmetric partition the isolated node
   never looks dead to its peers (its outbound gossip still flows), so hints
   buffered for it were never triggered for delivery and repair fell to
   anti-entropy. A background sweep (`runHintDelivery`, 30s tick) now also
   delivers buffered hints to any currently-alive target, independent of any
   membership edge. To make retrying against a still-unreachable target safe,
   `DeliverHints` re-buffers any hint that fails to deliver instead of dropping
   it, preserving its original timestamp so its TTL keeps counting from the
   original write; anti-entropy remains the backstop for hints that age out
   before the target becomes reachable.
6. **Merkle hashes were clock-blind** — *fixed by folding vector clocks into
   entry hashes.* Two replicas holding the same value at different clocks
   (e.g. one received an earlier write attempt via a hint, the other holds
   the final acknowledged one) produced identical Merkle roots, so
   anti-entropy skipped them and the clock divergence persisted until the
   next write or read repair. Surfaced by `QuorumLossThenClampedAvailability`
   once the deterministic sync cadence made convergence timing reliable
   enough to expose it; the entry hash now XORs each sibling's value hash
   with a canonical hash of its vector clock.

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
