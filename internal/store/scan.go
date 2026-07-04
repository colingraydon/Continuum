package store

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
)

// KeyItem is one key's complete visible entry, as returned by Scan.
type KeyItem struct {
	Key   string
	Entry Entry
}

// Scan returns up to limit visible keys matching prefix, in ascending key
// order, starting strictly after the cursor `after` (pass "" to start at the
// beginning). Entries whose siblings are all tombstones ARE included: callers
// that surface results to clients filter them, while the scatter-gather
// coordinator needs them so a tombstone on one node can dominate a stale live
// value on another.
//
// The merge follows the same generation rules as Get: the newest generation
// holding a key has its complete sibling set (read-merge-write invariant), and
// evict markers hide the key. Table IO is bounded by the prefix range - each
// table is seeked to the scan start and iteration stops at the first
// non-matching key - while the memtable overlays are full map passes, bounded
// by the memtable size cap.
func (s *Store) Scan(prefix, after string, limit int) ([]KeyItem, error) {
	if limit <= 0 {
		return nil, nil
	}
	start := prefix
	if after >= prefix {
		start = after + "\x00" // smallest key strictly greater than after
	}

	s.mu.RLock()
	tables := s.tables
	frozen := s.frozen
	// Hold the table-reader guard across the scan (acquired before releasing
	// s.mu) so compaction cannot close a reader mid-iteration. Same pattern as
	// KeyHashes.
	s.tablesRW.RLock()
	s.mu.RUnlock()

	merged, err := scanTables(tables, prefix, start)
	s.tablesRW.RUnlock()
	if err != nil {
		return nil, err
	}
	if frozen != nil {
		overlayEntries(merged, frozen.data, frozen.evicted, prefix, start)
	}
	s.mu.RLock()
	overlayEntries(merged, s.data, s.evicted, prefix, start)
	s.mu.RUnlock()

	keys := make([]string, 0, len(merged))
	for key := range merged {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) > limit {
		keys = keys[:limit]
	}
	out := make([]KeyItem, len(keys))
	for i, k := range keys {
		out[i] = KeyItem{Key: k, Entry: merged[k]}
	}
	return out, nil
}

// scanTables folds every table's prefix-matching entries into a key→entry
// map, oldest to newest so later generations overwrite earlier ones and evict
// markers remove the key entirely. Each table is seeked to start and left as
// soon as keys stop matching (keys are sorted, and start >= prefix, so the
// first non-matching key ends the range). The caller holds s.tablesRW for the
// duration.
func scanTables(tables []liveTable, prefix, start string) (map[string]Entry, error) {
	out := make(map[string]Entry)
	startB := []byte(start)
	for i := len(tables) - 1; i >= 0; i-- {
		r := tables[i].r
		if lg := r.Largest(); lg == nil || bytes.Compare(lg, startB) < 0 {
			continue // empty table, or entirely before the scan range
		}
		it := r.IterFrom(startB)
		for it.Next() {
			key := string(it.Key())
			if !strings.HasPrefix(key, prefix) {
				break
			}
			e, evicted, err := decodeTableEntry(it.Value())
			if err != nil {
				return nil, fmt.Errorf("store: decode table entry %q: %w", key, err)
			}
			if evicted {
				delete(out, key)
				continue
			}
			out[key] = e
		}
		if err := it.Err(); err != nil {
			return nil, fmt.Errorf("store: table scan: %w", err)
		}
	}
	return out, nil
}

// overlayEntries applies one memtable generation on top of the accumulated
// scan state: entries replace older ones wholesale (each generation holds a
// key's complete sibling set) and evictions hide the key.
func overlayEntries(out map[string]Entry, data map[string]Entry, evicted map[string]struct{}, prefix, start string) {
	for key, e := range data {
		if strings.HasPrefix(key, prefix) && key >= start {
			out[key] = e
		}
	}
	for key := range evicted {
		delete(out, key)
	}
}
