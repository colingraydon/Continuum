# Backup and Restore

Continuum's on-disk state is a set of **immutable** SSTables plus a `MANIFEST`
naming the live set (see [Persistence](persistence.md) and [SSTable](sstable.md)).
Immutability makes a point-in-time backup nearly free: the tables never change
after they are written, so a backup only has to capture *which* tables are live
and preserve those files. `Store.Backup` does exactly that with hard links.

## What a backup is

`Store.Backup(destDir)` produces a directory that is itself a valid tables
directory — a `MANIFEST` plus one hard link per live SSTable:

1. **Flush.** The active memtable is flushed to an SSTable first, so writes that
   were still in memory are captured on disk. (Backup therefore requires
   flushing to be enabled via `SetFlushPolicy`; a memory-only store has nothing
   on disk to snapshot.)
2. **Snapshot the live set.** Under the store read lock, the current manifest's
   table list is captured and each `.sst` file is **hard-linked** into `destDir`.
   Holding the read lock across the links serializes against compaction: a
   retired table is unlinked only after a new manifest excluding it is committed
   under the store lock, which cannot happen while the read lock is held. Once
   the links exist they share the tables' inodes, so later compaction of the
   live copy cannot destroy the backup.
3. **Commit the manifest.** The captured manifest is written into `destDir` with
   the same atomic temp-file + rename + directory-fsync used everywhere else, so
   the backup becomes durable as a unit.

Because the copy is by hard link, a backup is near-instant and consumes no
extra space until one side is compacted away. It returns a `BackupInfo` with the
table count and the highest WAL sequence the backup covers.

### Constraints

- `destDir` must be on the **same filesystem** as the data directory — hard
  links do not cross devices.
- `destDir` must not already hold a backup; `Backup` refuses to write over an
  existing `MANIFEST` so two backups never mix their table sets in one directory.

## Restore

Restore is the ordinary recovery path — no special code. A backup directory has
the same shape as `DATA_DIR/tables`, so either:

- point a fresh store's `OpenTables` at the backup directory, or
- copy the backup's contents into a data directory's `tables/` folder and start
  the node normally; startup attaches the tables through `OpenTables` and
  replays any WAL tail on top.

Either way the restored store reads the same immutable table data the backup
captured, including its recency order and covered sequence.

## See Also

- [Persistence](persistence.md) — the recovery path a restore reuses
- [SSTable](sstable.md) — the immutable table format backups link
