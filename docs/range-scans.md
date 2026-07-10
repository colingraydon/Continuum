# Range Scans

> Ordered prefix enumeration: a merged iterator over the LSM generations on
> each node, and a scatter-gather `GET /keys?prefix=` across the cluster.

## Overview

Point reads answer "what is key K"; range scans answer "what keys start with
P". Two layers cooperate:

1. **Node-local merged scan** (`store.Scan`) - one node's ordered, visible
   view of a prefix range across its memtable, frozen memtable, and SSTables.
2. **Scatter-gather coordinator** (`GET /keys?prefix=`) - fans the local scan
   to every alive node and merges per-key sibling sets with the same vector
   clock dominance rules as point reads.

## Node-Local Merged Scan

`Store.Scan(prefix, after, limit)` returns up to `limit` visible keys in
ascending order, starting strictly after the cursor `after`.

The LSM's **read-merge-write invariant** does the heavy lifting: every write
folds older-generation state for its key into the memtable, so the newest
generation holding a key has its complete merged sibling set. A scan therefore
never merges siblings across generations - it only tracks which generation is
newest per key:

1. **Tables, oldest → newest**: each table is seeked to the scan start with
   `sstable.IterFrom` (an index binary search, not a full walk) and iterated
   until the first key that no longer matches the prefix. Newer tables
   overwrite older entries; an evict marker deletes the key from the result.
   Tables whose largest key precedes the range are skipped entirely.
2. **Frozen memtable, then active memtable**: map passes filtered by the
   range; entries replace table state wholesale, evictions hide keys.

Locking matches `KeyHashes`: the table-reader guard (`tablesRW`) is held
across table IO so compaction cannot close a reader mid-scan, and the store
mutex is only held for the in-memory overlays.

Tombstone-only entries are **included** in `Scan` results deliberately - see
the coordinator merge below for why.

## Scatter-Gather Coordinator

Keys are placed by hash, so a prefix has no home node - matching keys are
spread across every node's ranges. `GET /keys?prefix=` therefore fans out to
**every alive, non-bootstrapping member** (the local store for self,
`X-Proxied-From`-marked local scans for peers), then merges:

- Per key, sibling sets from all nodes merge under vector clock dominance
  (the same `mergeResponses` used by point reads): dominated versions drop,
  concurrent versions surface as siblings.
- A key whose lone surviving sibling is a tombstone is omitted - this is why
  local scans must return tombstones. If they didn't, a node still holding a
  stale live value would resurrect a deleted key in scan results; the
  tombstone must be present at the merge to dominate it.
- Results are sorted and cut at `limit`, with a `next` cursor for resumption.

### The pagination horizon

Each node returns its smallest `limit` matches. A key can only be trusted as
fully merged if **every** node had the chance to report it - so the trusted
range ends at the smallest last-key among nodes whose page filled (a full page
may have cut off later keys; a partial page means the node exhausted its
range). Keys beyond that horizon are dropped from the current page and
revisited after the cursor advances. `next` is the last emitted key when the
page filled, otherwise the horizon; an empty `next` means the scan is done.
Without this, a node with many small keys could shadow another node's keys out
of a page entirely, and the client would silently skip them.

## API

```
GET /keys?prefix=<p>&limit=<n>&after=<cursor>
```

| Param | Default | Meaning |
| ----- | ------- | ------- |
| `prefix` | (required) | Key prefix to enumerate; empty is rejected |
| `limit` | 100 (max 1000) | Maximum items per page |
| `after` | (none) | Exclusive resume cursor from a previous page's `next` |

```json
{
  "items": [
    { "key": "user:1", "value": "alice" },
    { "key": "user:2", "siblings": [ {"value": "bob", "clocks": {"n1": 2}},
                                     {"value": "carol", "clocks": {"n2": 1}} ] }
  ],
  "next": "user:2"
}
```

With `X-Proxied-From` set the endpoint serves the node-local scan instead
(raw sibling sets, tombstones included) - the internal wire format the
coordinator consumes.

## Consistency Contract

- A scan consults **all alive nodes**, a superset of every key's replica set,
  and merges with the same dominance rules as reads - so it reflects at least
  everything any single healthy replica knows.
- If any alive node fails to respond the scan returns **503** rather than a
  partial result: silently missing a failed node's keys is worse than an
  error the client can retry.
- Scans are not snapshots: concurrent writes may or may not appear, and
  pagination pages are independent reads. Scans do not trigger read repair
  (a possible future refinement).

## Design Decisions

### Fan Out to All Nodes, Fail Closed

A prefix maps to no particular vnode, so any correct scan must consult every
node that might hold matching keys - all of them. The alternatives are worse:
per-vnode scatter (RF requests per vnode, hundreds of requests at
`REPLICAS=150`) or a global index (a second write path to keep consistent).
One local scan per node is the cheapest complete coverage. Failing closed on
any node error keeps the contract crisp: a 200 means the result reflects the
whole live cluster.

**Tradeoff:** scan availability is the intersection of all nodes'
availability, and each scan costs O(cluster size) requests regardless of how
few keys match. Both are acceptable for an operation that is inherently
cluster-wide.

### Reuse Point-Read Merge Semantics

Scans reuse `mergeResponses` verbatim, so a key surfaces in a scan exactly as
it would in a `GET /keys/{key}`: same dominance, same sibling surfacing, same
tombstone handling. There is one behavioral difference - point reads repair
stale replicas they discover; scans do not (repair fan-out for up to `limit`
keys per page would turn a read into a write storm; anti-entropy covers it).

### Ordered Skiplist Memtable

The memtable is an ordered skiplist (`internal/store/skiplist.go`), so a scan
seeks to the range start and walks only the matching keys - the same bounded
walk the table scan uses, since keys with a given prefix form a contiguous
range. Point writes stay O(log m); the overlay no longer sweeps and sorts the
whole memtable on every scan. A narrow-prefix page over a 10k-key memtable
drops from a full O(m) sweep to ~100 touched keys (see
`BenchmarkStoreScanMemtablePrefix`).

## See Also

- [SSTable](sstable.md) - `IterFrom` seek support
- [Replication](replication.md) - the dominance merge scans reuse
- [Anti-Entropy](antientropy.md) - the repair layer scans lean on
- [API Reference](api.md) - endpoint reference
