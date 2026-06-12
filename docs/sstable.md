# SSTable

> **Status: wired in (phases 1–3).** `internal/sstable` is the on-disk table
> format; the store flushes its memtable to tables on a size threshold and
> serves merged reads across memtable + tables (see
> [docs/persistence.md](persistence.md) for the flush/recovery flows).
> Compaction (phase 4) is next. See [Roadmap](#roadmap).

## Why

The v1 store was an in-memory map made durable by a WAL and a shutdown-only
snapshot. That caps the dataset at RAM, makes restart cost proportional to
total WAL history since the last snapshot, and leaves no path to range
scans. The LSM (log-structured merge) architecture — the one Cassandra,
RocksDB, and LevelDB use — fixes all three with the same move: never update
data in place, only append.

The three LSM components map onto Continuum like this:

| Component | LSM role | Continuum |
| --------- | -------- | --------- |
| Memtable | In-memory table absorbing all writes | `store.data`, bounded by `MEMTABLE_MAX_BYTES` |
| WAL | Crash durability for the memtable | `internal/wal`, truncated on flush |
| SSTable | Immutable sorted on-disk table, written when the memtable fills | **this package** |

When the memtable reaches a size threshold it is written out as one SSTable
and replaced with a fresh memtable; WAL segments covering the flushed data
are deleted. Reads check the memtable first, then tables newest-to-oldest.
A background compaction process merges tables to bound read amplification
and reclaim superseded data.

## File format

One immutable file per table:

```
[data block]...  [index block]  [bloom block]  [footer: 50 bytes]
```

**Data block** — `(key_len:2 | key | val_len:4 | val)...` followed by a
CRC32 trailer. Entries are sorted by key. Blocks target `BlockSize`
(default 4 KiB) and close at the first entry that pushes them past it, so a
single oversized entry gets its own block.

**Index block** — `count:4 | (first_key_len:2 | first_key | offset:8 |
len:4)×count | largest_key_len:2 | largest_key | crc32:4`. One entry per
data block: its first key, file offset, and length. Readers hold the whole
index in memory.

**Bloom block** — `k:1 | bits | crc32:4`. A standard Bloom filter sized at
`BitsPerKey` (default 10, ~1% false positives) with Kirsch–Mitzenmacher
double hashing over murmur3-128: `bit_i = (h1 + i·h2) mod m`. False
negatives are impossible, so a miss skips the table with zero disk reads.

**Footer** — fixed 50 bytes at the end of the file:
`index_off:8 | index_len:8 | bloom_off:8 | bloom_len:8 | entry_count:8 |
version:2 | crc32:4 | magic:4 ("CSST")`. Readers locate it from the file
size, so tables are self-describing.

## Read path

`Open`/`NewReader` reads the footer, index, and bloom filter once and
verifies their CRCs; data block extents are also validated against the
footer before the reader is usable. A `Get` is then:

1. Bloom filter check — most tables that don't hold the key stop here.
2. Binary search the in-memory index for the one block whose key range can
   contain the key.
3. One `ReadAt` for that block, CRC verification, linear scan within it
   (early exit once a larger key is seen).

So a point lookup costs at most one disk read per table. `Iter` walks
blocks in order for full scans — this is what compaction, Merkle rebuild,
and future range scans build on. Readers are immutable and safe for
concurrent use.

## Write path

`Writer.Add` requires strictly increasing keys and streams completed data
blocks to the underlying writer; only the current block, the index, and the
bloom hashes are buffered. `Finish` flushes the last block and appends
index, bloom, and footer. The package does not touch the filesystem on the
write path — the caller owns durability with the same contract as the
snapshot writer: write to a temp file, `Finish`, fsync, rename, fsync the
directory.

## Design decisions

**Opaque values.** Like `internal/wal`, the package stores `(key, value)`
byte pairs and never interprets values. The sibling-set encoding stays in
`internal/store`, which also avoids an import cycle once the store consumes
this package.

**Sibling merge is the LSM merge operator.** Textbook LSMs resolve
multiple versions of a key by sequence number — newest wins. Continuum
can't: a key's value is a set of vector-clocked siblings. Entries are
merged with the same sibling-union logic used everywhere else: union the
sibling sets, drop dominated ones. Because that operation is commutative,
associative, and idempotent, tables can be merged in any order, any number
of times, without losing a causally-newest write — which is exactly the
property compaction needs.

**Merging happens at write time, not read time.** A write folds any
older-generation state for its key into the memtable first (one
bloom-guarded table probe). The resulting invariant — a generation that
holds a key holds its complete merged sibling set — means reads stop at the
first generation hit, newest-first: memtable, frozen memtable, tables. No
read assembles siblings from multiple tables, and compaction reduces to
"keep the newest version of each key".

**Tombstone GC moves into compaction.** A tombstone in an old immutable
table can't be deleted in place. Phase 4 handles it the way Cassandra does:
a tombstone older than `gcTTL` that reaches the bottom of a merge is simply
not written to the output table. The existing GC-record WAL machinery then
only covers the memtable.

**CRC everywhere, verified at the right time.** Footer, index, and bloom
CRCs are checked once at open; data block CRCs on every read, since blocks
are read lazily. Same corruption philosophy as the WAL: detect, don't
guess.

## Limits

| Limit | Value | Enforced by |
| ----- | ----- | ----------- |
| Key size | 64 KiB − 1 | `Add` |
| Value size | 1 GiB | `Add` |
| Keys per table | bounded by `entry_count:8` | practically unbounded |

## Roadmap

1. **SSTable format** *(done)* — writer, reader, iterator, bloom filter,
   corruption handling.
2. **Memtable flush** *(done)* — memtable bounded by `MEMTABLE_MAX_BYTES`,
   freeze-and-flush to seq-named tables (no manifest needed until
   compaction), WAL truncation keyed to flush instead of shutdown snapshot,
   legacy snapshot migration.
3. **Merged read path** *(done — landed with phase 2, since clearing the
   memtable without table-aware reads would lose data)* — `Get` walks
   generations newest-first; `KeyHashes` and Merkle rebuild scan tables;
   evict markers shadow migrated-away keys.
4. **Compaction** — size-tiered (Cassandra's default), folding in
   tombstone GC and evict-marker purge; introduces a manifest for the
   atomic N-tables-to-one swap.
