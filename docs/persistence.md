# Persistence

> **Status: implemented (v2, LSM).** WAL + SSTable persistence on the store, gated by the `DATA_DIR` environment variable. v1's shutdown-only snapshot has been replaced by memtable flushes to immutable SSTables ([docs/sstable.md](sstable.md)); legacy snapshot dirs migrate automatically. WAL fsyncs on the `Put`/`Delete` hot path are batched across concurrent writers by group commit, and buffered hints persist to their own append-only log ([docs/hinted-handoff.md](hinted-handoff.md)).

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
  meta.json                  node_id, last_clean_shutdown, latest_seq
  incarnation                gossip epoch (decimal uint64); advanced each restart
  tables/NNNNNNNN.sst        immutable SSTable covering WAL sequences ≤ NNNNNNNN
  tables/NNNNNNNN.sst.tmp    in-flight flush; cleaned up at startup
  wal/NNNNNNNN.wal           segment starting at sequence NNNNNNNN

  snap/NNNNNNNN.snap         legacy v1 snapshot; migrated to an SSTable on
                             first startup, then removed
```

Segmented WAL: rotate at 64 MB. When the memtable exceeds `MEMTABLE_MAX_BYTES` (default 16 MiB) it is flushed to a new SSTable; segments fully covered by the flush get deleted. Tables are named by the WAL sequence they cover, so the set of live tables is derivable from a directory listing — no manifest file until compaction (which must atomically swap N tables for one) requires it. Because the table rename is atomic and sibling-set merging is idempotent, a crash anywhere in the flush sequence is safe: either the WAL still covers the data, or the table and WAL briefly overlap and replay re-merges harmlessly.

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

## SSTable contents

Tables store `(key, value)` byte pairs in the format described in
[docs/sstable.md](sstable.md). The value encoding is owned by the store:

```
ENTRY (0x00): sib_count(2) | siblings | has_tomb(1) | tomb_at_ns(8)?
EVICT (0x01): nothing — the key was migrated away; shadows older tables
```

`tomb_at` rides along so compaction can apply tombstone GC later. The legacy
v1 snapshot format (same sibling layout, single file, written at shutdown)
remains readable for one-time migration only.

## Write path

```
acquire s.mu
merged := mergeSibling(key, incoming)   // folds in frozen/table state; one
                                        // bloom-guarded table probe
if dropped (dominated/idempotent) { release; return nil }  // no WAL append
err := wal.Append(rec); err = wal.Sync()
if err != nil { return err } // memory not modified; WAL is the truth
commit merged entry to memtable; onUpdate(merged hash)
release s.mu
maybe flush (threshold crossed → freeze memtable, write table outside lock)
return nil
```

**Key invariants:**
- Ack to caller only after `Sync()` returns. Reads never see writes that aren't on disk.
- If `Append` or `Sync` fails, memory is **not** modified. The WAL is the source of truth; memory catches up on replay. A partial fsync that left a record on disk but didn't ack is fine — replay will apply it.
- Dominated and idempotent writes are dropped **before** the WAL append, so the log only contains writes that changed state.
- **Read-merge-write invariant**: a write merges any older-generation (frozen/table) state for its key into the memtable. A generation that holds a key therefore holds its complete merged sibling set, and reads stop at the first generation hit — no read-time multi-table merging.
- `Put`, `Delete`, `Evict` return `error`; `Get` and `KeyHashes` do too, since reads can now touch disk. Callers (handler, anti-entropy apply path) propagate or log.

## Recovery flow

1. Read `meta.json`. Refuse to start if `node_id != SELF_ID` (prevents accidental data-dir reuse across nodes).
2. **Downtime gate**: if `now - last_clean_shutdown > gcTTL` OR meta is missing → skip steps 3–6. Clear data files, return an empty store, let the existing `Bootstrap()` flow refill primary ranges from current replicas. A crash does not update `last_clean_shutdown`, so a crashed node recovers normally as long as its last clean shutdown is within `gcTTL`. See "Tombstone GC safety" below.
3. Clean up any `*.sst.tmp` (and legacy `*.snap.tmp`) left from a crashed flush.
4. Open every `tables/*.sst`, newest first. Set `applied_seq` to the highest table name. **Legacy migration**: if there are no tables but a v1 snapshot exists, load it as memtable contents and use its `sequence_at` instead; after step 6 it is flushed out as the first SSTable and the snap files are removed.
5. Walk `wal/` segments in sequence order. For each record:
   - CRC mismatch or short read in the newest segment's tail → truncate to last good offset.
   - CRC mismatch mid-stream (older segment) → corruption, bail.
   - `seq ≤ applied_seq` → already covered by a table, skip.
   - `PUT`/`DELETE` → store apply path with `onUpdate` suppressed; merges against table-resident state (tables are attached first, so the read-merge-write invariant holds after replay).
   - `EVICT` → eviction path: leaves an evict marker if the key is still visible in a table.
   - `GC` → remove keys + `tombstoneAges` directly.
   - `CHECKPOINT` → legacy v1 record, decoded and ignored.
6. Open the next WAL segment for writes; install the flush policy. After recovery: `ae.rebuild()` populates Merkle trees from `store.KeyHashes()` (a merged scan across memtable + tables). Only now does startup proceed — gossip stays in bootstrapping until recovery is done. (`meta.json` is rewritten only at the next clean shutdown.)

## Flush flow

Triggered by the write that pushes the memtable past `MEMTABLE_MAX_BYTES`
(and forced at shutdown):

1. Under `s.mu`: move the active memtable (entries + evict markers + tombstone ages) to the *frozen* slot and install a fresh one. Reads consult memtable → frozen → tables, so nothing disappears.
2. Outside the lock: sort the frozen keys, stream them through the SSTable writer to `tables/NNNNNNNN.sst.tmp` (named by the highest WAL seq the memtable covers), fsync, rename, fsync the directory.
3. Under `s.mu`: attach the new table reader at the front, clear the frozen slot.
4. `TruncateThrough(seq)` deletes WAL segments the table now covers.

A failed flush leaves the frozen memtable in place; the next write retries.
Only one flush runs at a time. The triggering writer pays the flush latency
inline — a deliberate simplification over a background flush thread; the
group-commit PR is the natural place to move it off the write path.

## Shutdown flow

After `FlushHints` and HTTP drain, `finalize()`:

1. `Flush()` the memtable to a final SSTable (truncates covered WAL segments).
2. Close table readers and the WAL.
3. Write meta with `last_clean_shutdown = now`, fsync.

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

**Flush / tables:**
- Crash mid-flush → `.sst.tmp` left behind; cleaned up at startup. The WAL still covers everything in the frozen memtable, so no data is lost.
- Crash after rename but before WAL truncation → the table and WAL overlap; replay re-merges the covered records idempotently.
- WAL retention rule: segments are only deleted **after** the covering table is fsynced and renamed. The currently-open segment is never deleted.
- Identity check: two nodes pointed at the same `DATA_DIR` would silently clobber each other. `meta.json` carries `node_id`; mismatch → refuse to start.
- A tombstone GC pass skips tombstones whose key is still visible in a table — purging only the memtable copy would resurrect the older table value on read. Table-resident tombstones are reclaimed at compaction.

**Replay:**
- `Delete` records must replay with their original `tombstone_at_ns`, not `time.Now()`.
- `GC` records emitted into the WAL prevent a replay-after-GC from resurrecting purged keys.
- Tables must be attached **before** replay so replayed records merge against table state, preserving the read-merge-write invariant.
- `onUpdate` (Merkle hook) is suppressed during replay; `ae.rebuild()` runs once at the end.

**Startup ordering:**
- WAL replay must complete before gossip allows peers to see this node as alive. Otherwise other coordinators fan reads/writes here while the store is half-loaded. The node stays in bootstrapping until `recover()` returns.

**Ring topology shifted while down:**
- Already handled by `Bootstrap()` and `CleanupStaleKeys()`. With persistence, very long downtime triggers the bootstrap-fresh path; shorter downtime trusts the local tables and lets anti-entropy + stale-key cleanup converge. `CleanupStaleKeys` uses the eviction path, so keys already flushed to tables get shadowed by evict markers until compaction drops them.

## v1 PR scope (historical — superseded by the LSM phases in docs/sstable.md)

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
- **Periodic / threshold-based snapshots.** Shutdown-only in v1. ~~WAL grows unbounded until the next restart.~~ **Done in v2**: memtable flushes to SSTables on a size threshold.
- **Bounded write queue / backpressure.** Doesn't exist today; not regressing.
- **Parallel replay.** Unlocked by the sharded-store PR.

## Scale considerations (for future PRs, not v1)

Quick reference of what bites as the cluster grows, roughly in order:

- **fsync ceiling**: ~500-1k writes/sec/node with a per-write fsync. **Done**: the `Put`/`Delete` hot path now appends under `s.mu`, releases, then batches the fsync via `wal.SyncUpTo` (group commit), so concurrent writers collapse into one flush. Trades a small visibility-before-durability window (the write is acknowledged only after its batched fsync) for far fewer syscalls; `wal.FsyncCount` exposes the batching ratio.
- **Single store lock**: a single `map[string]Entry` under one `RWMutex` still serializes the in-memory merge; the sharded-store PR splits it into 256 shards.
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
