# Persistence

> **Status: implemented (v1).** WAL + snapshot persistence on the store, gated by the `DATA_DIR` environment variable. Hint store persistence and group commit are still future work.

## Why

Today every layer holds state in memory only. A graceful single-node restart survives because successors and anti-entropy refill the node, but a whole-cluster restart loses every key. The goal is to make the store crash-durable and the cluster cold-startable.

## What persists, what's derived

| Layer | Persists? | Why |
| ----- | --------- | --- |
| `internal/store` | **Yes** | Authoritative KV data. Vector clocks live inside `Sibling.Version`, so they ride along with entries. `tombstoneAges` persists too — restart must not reset TTL. |
| `internal/hintstore` | **Yes (v2)** | Currently lost on crash. Anti-entropy backstops it for keys that achieved quorum elsewhere. Deferred to a follow-up PR. |
| `internal/gossip` | No | MemberList reconstructs by gossiping with seed nodes. |
| `internal/ring` | No | Derived from gossip membership + config. |
| `internal/antientropy` | No | `rebuild()` repopulates Merkle trees from `store.KeyHashes()` at startup. |

## On-disk layout

```
DATA_DIR/
  meta.json                  node_id, last_clean_shutdown, snapshot_epoch, latest_seq
  snap/NNNNNNNN.snap         snapshot at sequence NNNNNNNN
  snap/NNNNNNNN.snap.tmp     in-flight; cleaned up at startup
  wal/NNNNNNNN.wal           segment starting at sequence NNNNNNNN
```

Segmented WAL: rotate at 64 MB. After a snapshot lands, segments fully covered by it get deleted.

## WAL record format

Length-prefixed, CRC32-checked binary frames. Fixed header so torn-tail detection is simple:

```
[crc32: 4][len: 4][seq: 8][payload: len-8]
```

CRC covers `len | seq | payload`. `len` includes the seq plus payload bytes, so total on-disk record size is `8 + len`. On replay, any record whose CRC fails or whose `len` runs past EOF marks the tail of the newest segment; truncate there. CRC mismatch mid-stream (older segment) is corruption — bail and require operator intervention.

The `internal/wal` package is type-agnostic: `Append([]byte)` takes an opaque payload. The first byte of the payload is the record-type tag, defined in `internal/store`. This keeps WAL framing free of store-specific concerns and lets the store evolve record formats without touching the WAL.

### Record payloads (defined in `internal/store`)

First byte of payload is the type tag. Remaining bytes are the type-specific encoding below.

- **PUT**: `key_len(2) | key | value_len(4) | value | n(2) | (id_len(2) | id | counter(8))×n`
- **DELETE**: `key_len(2) | key | tombstone_at_ns(8) | n(2) | (id_len(2) | id | counter(8))×n`
- **EVICT**: `key_len(2) | key` — local-only cleanup for keys outside this node's primary range
- **GC**: `n(4) | (key_len(2) | key)×n` — purged tombstones; emitted by the GC pass so replay across a GC boundary doesn't resurrect
- **CHECKPOINT**: `snapshot_seq(8)` — written after a snapshot lands; lets replay skip ahead

Notes:
- `tombstone_at_ns` is the original wall time when the Delete was first accepted. Never `time.Now()` at replay — that would reset TTL and break GC safety.
- Concurrent siblings: each sibling is its own logical record. Replay re-runs `applySibling` and they re-converge.

## Snapshot format

```
header:  node_id | snapshot_epoch | sequence_at | entry_count | crc32(header)
body:    (key | siblings[] | tombstone_at?)×entry_count
```

The body encoding can use `gob` or a simple length-prefixed layout — snapshots are rare and don't need to be fast. The `sequence_at` field is the WAL sequence captured at snapshot time; everything ≤ it can be discarded.

## Write path

```
acquire s.mu
seq := next()
rec := encode(PUT, seq, key, value, clocks)
err := wal.Append(rec)
err := wal.Sync()           // or hand to group-commit goroutine and wait
if err != nil { return err } // memory not modified; WAL is the truth
applySibling(...)
onUpdate(...)
release s.mu
return nil
```

**Key invariants:**
- Ack to caller only after `Sync()` returns. Reads never see writes that aren't on disk.
- If `Append` or `Sync` fails, memory is **not** modified. The WAL is the source of truth; memory catches up on replay. A partial fsync that left a record on disk but didn't ack is fine — replay will apply it.
- `Put`, `Delete`, `Evict` now return `error`. Callers (handler, anti-entropy apply path) propagate or log.

## Recovery flow

1. Read `meta.json`. Refuse to start if `node_id != SELF_ID` (prevents accidental data-dir reuse across nodes).
2. **Downtime gate**: if `now - last_clean_shutdown > gcTTL` OR meta is missing → skip steps 3–6. Clear data files, return an empty store, let the existing `Bootstrap()` flow refill primary ranges from current replicas. A crash does not update `last_clean_shutdown`, so a crashed node recovers normally as long as its last clean shutdown is within `gcTTL`. See "Tombstone GC safety" below.
3. Clean up any `*.snap.tmp` left from a crashed snapshot.
4. Load the highest valid `snap/NNNNNNNN.snap`. Verify header CRC + identity. Set `applied_seq = sequence_at`.
5. Walk `wal/` segments in sequence order. For each record:
   - CRC mismatch or short read in the newest segment's tail → truncate to last good offset.
   - CRC mismatch mid-stream (older segment) → corruption, bail.
   - `seq ≤ applied_seq` → already covered by snapshot, skip.
   - `PUT`/`DELETE` → call store apply path with `onUpdate` suppressed.
   - `EVICT` → remove key + tombstone age, suppress callbacks.
   - `GC` → remove keys + `tombstoneAges` directly.
   - `CHECKPOINT` → advance `applied_seq` (skip-ahead optimization).
6. After replay: `ae.rebuild()` populates Merkle trees from final `store.KeyHashes()`.
7. Open the next WAL segment for writes. Only now signal `main` that startup may proceed — gossip stays in bootstrapping until recovery is done. (`meta.json` is rewritten only at the next clean shutdown.)

## Shutdown flow

Appended to the existing shutdown sequence in `main.go`, after `FlushHints` and before `g.Stop()`:

1. Under `s.mu`, copy `data` + `tombstoneAges` + `seq` into a snapshot struct. Release lock.
2. Write `snap/NNNNNNNN.snap.tmp`, fsync, rename to `.snap`, fsync the directory.
3. Append a `CHECKPOINT` record to the current WAL.
4. Delete WAL segments whose end_seq ≤ NNNNNNNN.
5. Write meta with `last_clean_shutdown = now`, fsync.

## Tombstone GC safety

Persistence breaks the implicit assumption behind the current 1-hour `gcTTL`: that no node is down longer than the anti-entropy propagation window. Once data survives restart, a node can come back days later carrying a write that was tombstoned + GC'd elsewhere — and resurrect it.

### Mechanism: self-enforcing downtime gate

Rather than rely on an operator invariant (Cassandra's `gc_grace_seconds` approach), the recovery flow enforces it in code:

- On graceful shutdown, write `last_clean_shutdown` into `meta.json`.
- On startup, compare against `gcTTL`. If exceeded (or meta is missing), refuse to load local data. The node clears its data files, re-enters bootstrapping, and pulls fresh primary ranges from current replicas via the existing `Bootstrap()` path.
- Bump `gcTTL` from 1 h → 24 h. Covers realistic outages without letting tombstones accumulate indefinitely.

Trade-off: a node down longer than `gcTTL` loses any writes it accepted that hadn't reached quorum. Same risk Cassandra has when `nodetool repair` is forgotten — we just fail closed automatically instead of resurrecting.

### Alternatives considered

- **Documented max-downtime, no enforcement.** Simpler but operationally fragile.
- **Per-replica tombstone acks.** Cleaner in theory, but a real protocol change: acks need to flow through anti-entropy, replica-set membership changes complicate it, permanently-dead replicas need handling. Probably not worth it.

## Edge cases considered

**Framing / WAL:**
- Torn write at tail of newest segment → CRC catches it, truncate to last good offset.
- CRC mismatch mid-stream → corruption, bail. Don't attempt to skip; record framing depends on length fields.
- Disk full → `Append` returns error → `Put` returns error → handler returns 503. No fan-out to replicas because the local write failed.

**Snapshot:**
- Crash mid-snapshot → `.snap.tmp` left behind; cleaned up at startup.
- Snapshot retention rule: keep the last successful snapshot **and** the WAL segments needed to recover from it until the next snapshot has been fsynced and meta updated. Forgetting either fsync leaves a broken recovery target.
- Identity check: two nodes pointed at the same `DATA_DIR` would silently clobber each other. Header carries `node_id`; mismatch → refuse to start.

**Replay:**
- `Delete` records must replay with their original `tombstone_at_ns`, not `time.Now()`.
- `GC` records emitted into the WAL prevent a replay-after-GC from resurrecting purged keys.
- `onUpdate` (Merkle hook) is suppressed during replay; `ae.rebuild()` runs once at the end.

**Startup ordering:**
- WAL replay must complete before gossip allows peers to see this node as alive. Otherwise other coordinators fan reads/writes here while the store is half-loaded. The node stays in bootstrapping until `recover()` returns.

**Ring topology shifted while down:**
- Already handled by `Bootstrap()` and `CleanupStaleKeys()`. With persistence, very long downtime triggers the bootstrap-fresh path; shorter downtime trusts the snapshot and lets anti-entropy + stale-key cleanup converge.

## v1 PR scope

### In

1. New package `internal/wal` — `Writer` (Append, Sync, Rotate, Close), `Reader` (Next iterator with tail truncation), record types, framing, tests for round-trip / torn tail / multi-segment replay / mid-stream corruption.
2. Snapshot read+write in `internal/store/snapshot.go`. Identity check. Header CRC.
3. `Store` gains optional `wal *wal.Writer` and `seq uint64`. `Put`/`Delete`/`Evict` append before applying and return `error`. `GCTombstones` emits a `GC` record. Replay path bypasses WAL and suppresses callbacks.
4. `meta.json` read/write.
5. `recover()` and `finalize()` drivers in `cmd/continuum/persist.go`. Wired into `main.go` ordering: recover → AE → gossip start → ListenAndServe → bootstrap (if seeds) → mark alive. Shutdown adds `finalize()` after `FlushHints`.
6. Downtime gate enforced in `recover()`. Bump `gcTTL` to 24 h with updated safety comment.
7. New env var `DATA_DIR`. Empty → memory-only mode (preserves current test behavior).
8. API change: `Put`/`Delete`/`Evict` return `error`. Handler returns 503 on local store failure without fanning out. AE apply path logs + continues per key.
9. Tests:
   - `internal/wal/*_test.go` (framing, torn tail, rotation, replay, corruption).
   - `internal/store/snapshot_test.go` (round-trip, identity refusal, header CRC).
   - `internal/store/durability_test.go` (write → close → reopen → verify, including tombstone ages).
   - `tests/persistence_test.go` (e2e: single node restart, cluster-of-3 restart).
   - Unit test for the downtime gate (stale `last_clean_shutdown` → empty store).
10. Docs: this file + touch `docs/architecture.md` (WAL+snapshot box) and `docs/operations.md` (DATA_DIR).

### Deferred

- **Group commit.** Per-write fsync in v1. Add a `wal_fsync_seconds` histogram so the next PR has a baseline.
- **Hint store persistence.** AE backstops it for quorum-acked writes; document the gap.
- **Sharded store.** Single mutex; works for the current benchmark. Sharding is a separate PR — touches every store call site.
- **Periodic / threshold-based snapshots.** Shutdown-only in v1. WAL grows unbounded until the next restart. Add a 10 min / 64 MB trigger in a follow-up.
- **Bounded write queue / backpressure.** Doesn't exist today; not regressing.
- **Parallel replay.** Unlocked by the sharded-store PR.

## Scale considerations (for future PRs, not v1)

Quick reference of what bites as the cluster grows, roughly in order:

- **fsync ceiling**: ~500-1k writes/sec/node without group commit. Group commit pushes to 10k-50k.
- **Single store lock**: holding `s.mu` across fsync serializes all writers. Group commit pattern: append under lock, release lock, group fsync outside, re-acquire to apply in seq order.
- **Vector clock bloat**: every node that ever wrote a key leaves an entry in its clock forever. At 100-node clusters with hot keys, real GB of clock overhead. Dotted version vectors (Riak) or timestamp-bounded pruning fix it.
- **Map sharding**: a single `map[string]Entry` under one RWMutex tops out around a few hundred-k QPS. 256 shards by hash; also unlocks parallel snapshot iteration and parallel WAL replay.
- **Snapshot copy-under-lock**: works fine at 100k keys; freezes writes for seconds at 100M keys. Sharding solves it (snapshot iterates shards one at a time under per-shard locks).
- **WAL replay time**: grows unbounded between snapshots. Periodic snapshot trigger is the lever.
- **Anti-entropy bucket depth**: 16 buckets/vnode is too coarse at high key counts. Cassandra uses depth-15. Easy parameter to expose.
- **WAL+snapshot IO contention**: token-bucket throttle the snapshot writer so it doesn't steal IOPS from the WAL fsync.
- **Bootstrap of a new node**: sequential vnode fetch is hours at 100 GB/node. Parallelize across vnodes and source nodes.
- **Backpressure**: bounded write queue in front of WAL appender; return 503 instead of OOMing when fsync stalls.

### Observability to add alongside the WAL code

`wal_fsync_seconds` (histogram), `wal_bytes_total`, `wal_segment_count`, `snapshot_duration_seconds`, `snapshot_bytes`, `replay_duration_seconds`, `replay_records_total`, `store_keys`, `store_tombstones`, `clock_avg_size` (early warning for clock bloat).
