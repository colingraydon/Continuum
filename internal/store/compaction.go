package store

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/colingraydon/continuum/internal/sstable"
)

// compactionPolicy parameterizes size-tiered compaction. A contiguous run of
// at least minThreshold tables whose sizes lie within sizeRatio of one another
// is merged into a single table, at most maxThreshold tables per round.
//
// Compaction sets must be contiguous in recency order. Reads return the value
// from the newest table holding a key (the read-merge-write invariant keeps
// that copy complete); merging a non-contiguous set could move a stale copy
// above a newer one and surface it. Restricting selection to adjacent runs —
// which freshly flushed, similarly sized tables naturally form — preserves the
// ordering reads depend on.
type compactionPolicy struct {
	minThreshold int
	maxThreshold int
	sizeRatio    float64
}

func defaultCompactionPolicy() compactionPolicy {
	return compactionPolicy{minThreshold: 4, maxThreshold: 32, sizeRatio: 2.0}
}

// SetCompactionPolicy tunes size-tiered compaction: a run of at least
// minThreshold tables within sizeRatio of one another is merged, at most
// maxThreshold at a time. Non-positive counts and ratios below 1 are ignored,
// leaving the current value.
func (s *Store) SetCompactionPolicy(minThreshold, maxThreshold int, sizeRatio float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if minThreshold > 0 {
		s.compaction.minThreshold = minThreshold
	}
	if maxThreshold > 0 {
		s.compaction.maxThreshold = maxThreshold
	}
	if sizeRatio >= 1 {
		s.compaction.sizeRatio = sizeRatio
	}
}

// Compact runs at most one round of size-tiered compaction: it selects a
// contiguous run of similarly sized tables, merges them into one table keeping
// the newest value per key, and atomically swaps the run for the merged table
// via the manifest. Returns true if a merge happened. Safe to call repeatedly;
// only one compaction runs at a time and it never blocks writers (the merge
// reads source tables outside s.mu). tombstoneMaxAge matches the anti-entropy
// GC window: at the bottom of the LSM, tombstones older than it and evict
// markers are dropped entirely.
func (s *Store) Compact(tombstoneMaxAge time.Duration) (bool, error) {
	s.mu.Lock()
	if s.flushDir == "" || s.compacting || s.flushing {
		s.mu.Unlock()
		return false, nil
	}
	s.compacting = true
	tables := append([]liveTable(nil), s.tables...)
	dir := s.flushDir
	policy := s.compaction
	s.mu.Unlock()
	defer s.clearCompacting()

	if len(tables) < policy.minThreshold {
		return false, nil
	}
	sizes, err := tableSizes(dir, tables)
	if err != nil {
		return false, err
	}
	start, count := selectCompaction(tables, sizes, policy)
	if count < 2 {
		return false, nil
	}
	sources := tables[start : start+count]
	bottom := start+count == len(tables)

	s.mu.Lock()
	fileNum := s.nextFileNum
	s.nextFileNum++
	s.mu.Unlock()

	res, err := mergeTables(dir, fileNum, sources, bottom, time.Now().Add(-tombstoneMaxAge))
	if err != nil {
		return false, err
	}
	if err := s.commitCompaction(dir, sources, res.table, res.created); err != nil {
		return false, err
	}
	// Reconcile derived state: keys dropped at the bottom that are now gone
	// from the store must be removed from the anti-entropy Merkle trees, which
	// are maintained incrementally and would otherwise keep stale entries.
	s.notifyCompacted(res.dropped)
	return true, nil
}

// notifyCompacted fires the evict callback for each dropped key that is no
// longer visible anywhere in the store. A dropped tombstone shadowed by a live
// value in a newer table above the merge is intentionally skipped — the tree
// already holds that live key's hash and must keep it. Mirrors the
// GCTombstones → RemoveFromTrees reconciliation for in-memtable tombstones.
func (s *Store) notifyCompacted(dropped []string) {
	if len(dropped) == 0 {
		return
	}
	s.mu.RLock()
	onEvict := s.onEvict
	var gone []string
	if onEvict != nil {
		for _, k := range dropped {
			if _, found, err := s.lookupLocked(k); err == nil && !found {
				gone = append(gone, k)
			}
		}
	}
	s.mu.RUnlock()
	for _, k := range gone {
		onEvict(k)
	}
}

func (s *Store) clearCompacting() {
	s.mu.Lock()
	s.compacting = false
	s.mu.Unlock()
}

// selectCompaction picks the newest contiguous run of tables whose sizes lie
// within policy.sizeRatio of each other and that is at least minThreshold
// long, capped at maxThreshold. Returns the run's start index and length, or
// (0, 0) when no run qualifies. tables and sizes are newest first and aligned.
func selectCompaction(tables []liveTable, sizes []int64, policy compactionPolicy) (int, int) {
	for i := 0; i < len(sizes); {
		runLen := growRun(sizes, i, policy.sizeRatio)
		if runLen >= policy.minThreshold {
			if runLen > policy.maxThreshold {
				runLen = policy.maxThreshold
			}
			return i, runLen
		}
		i += runLen
	}
	return 0, 0
}

// growRun returns the length of the run starting at index start in which every
// table's size stays within ratio of the run's running min and max.
func growRun(sizes []int64, start int, ratio float64) int {
	mn, mx := sizes[start], sizes[start]
	j := start + 1
	for j < len(sizes) {
		cmn, cmx := mn, mx
		if sizes[j] < cmn {
			cmn = sizes[j]
		}
		if sizes[j] > cmx {
			cmx = sizes[j]
		}
		if float64(cmx) > float64(cmn)*ratio {
			break
		}
		mn, mx = cmn, cmx
		j++
	}
	return j - start
}

func tableSizes(dir string, tables []liveTable) ([]int64, error) {
	sizes := make([]int64, len(tables))
	for i, t := range tables {
		fi, err := os.Stat(filepath.Join(dir, t.file))
		if err != nil {
			return nil, fmt.Errorf("store: stat table %s: %w", t.file, err)
		}
		sizes[i] = fi.Size()
	}
	return sizes, nil
}

// mergeResult is the outcome of a merge: the new table (when created), and the
// keys dropped at the bottom so the caller can reconcile derived state (e.g.
// the anti-entropy Merkle trees).
type mergeResult struct {
	table   liveTable
	created bool
	dropped []string
}

// mergeTables merges sources (newest first) into a single SSTable named by
// fileNum, written durably (temp, fsync, rename, fsync dir). created is false
// when the merge produced no entries — every key was dropped as a bottom-level
// evict marker or expired tombstone — in which case the sources are simply
// retired with no replacement.
func mergeTables(dir string, fileNum uint64, sources []liveTable, bottom bool, gcCutoff time.Time) (mergeResult, error) {
	name := tableName(fileNum)
	tmpPath := filepath.Join(dir, name+".tmp")
	f, err := os.Create(tmpPath)
	if err != nil {
		return mergeResult{}, fmt.Errorf("store: create compaction tmp: %w", err)
	}
	count, dropped, err := mergeInto(sstable.NewWriter(f, sstable.Options{}), sources, bottom, gcCutoff)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return mergeResult{}, err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return mergeResult{}, fmt.Errorf("store: sync compaction: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return mergeResult{}, fmt.Errorf("store: close compaction: %w", err)
	}
	if count == 0 {
		_ = os.Remove(tmpPath)
		return mergeResult{dropped: dropped}, nil
	}
	finalPath := filepath.Join(dir, name)
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return mergeResult{}, fmt.Errorf("store: rename compaction: %w", err)
	}
	if err := fsyncDir(dir); err != nil {
		return mergeResult{}, fmt.Errorf("store: fsync compaction dir: %w", err)
	}
	r, err := sstable.Open(finalPath)
	if err != nil {
		return mergeResult{}, err
	}
	return mergeResult{table: liveTable{r: r, file: name}, created: true, dropped: dropped}, nil
}

// mergeInto performs a k-way merge across sources (newest first), writing each
// distinct key once with the value from the newest source holding it. Returns
// the number of entries written and the keys dropped at the bottom (evict
// markers and aged tombstones) so the caller can reconcile derived state.
func mergeInto(w *sstable.Writer, sources []liveTable, bottom bool, gcCutoff time.Time) (int, []string, error) {
	m := newKwayMerge(sources)
	count := 0
	var dropped []string
	for {
		key, val, ok := m.next()
		if !ok {
			break
		}
		keep, err := keepAtBottom(val, bottom, gcCutoff)
		if err != nil {
			return 0, nil, err
		}
		if !keep {
			dropped = append(dropped, string(key))
			continue
		}
		if err := w.Add(key, val); err != nil {
			return 0, nil, fmt.Errorf("store: compaction add %q: %w", key, err)
		}
		count++
	}
	if err := m.err(); err != nil {
		return 0, nil, err
	}
	if err := w.Finish(); err != nil {
		return 0, nil, fmt.Errorf("store: compaction finish: %w", err)
	}
	return count, dropped, nil
}

// kwayMerge merges several sorted table iterators into one ascending key
// stream. Sources are ordered newest first, so when the same key appears in
// more than one, the lowest-index (newest) value wins.
type kwayMerge struct {
	its  []*sstable.Iterator
	live []bool
}

func newKwayMerge(sources []liveTable) *kwayMerge {
	m := &kwayMerge{
		its:  make([]*sstable.Iterator, len(sources)),
		live: make([]bool, len(sources)),
	}
	for i, src := range sources {
		m.its[i] = src.r.Iter()
		m.live[i] = m.its[i].Next()
	}
	return m
}

// next returns the smallest remaining key and its winning value, then advances
// every iterator positioned at that key. ok is false once all are exhausted.
// The returned slices are owned by the caller (copied off the iterator buffers,
// which advancing mutates).
func (m *kwayMerge) next() (key, value []byte, ok bool) {
	mi := m.minIndex()
	if mi < 0 {
		return nil, nil, false
	}
	// minIndex returns the lowest index among ties, i.e. the newest source
	// holding this key, so its value is the one that wins.
	key = append([]byte(nil), m.its[mi].Key()...)
	value = append([]byte(nil), m.its[mi].Value()...)
	m.advancePast(key)
	return key, value, true
}

func (m *kwayMerge) minIndex() int {
	mi := -1
	for i := range m.its {
		if !m.live[i] {
			continue
		}
		if mi == -1 || bytes.Compare(m.its[i].Key(), m.its[mi].Key()) < 0 {
			mi = i
		}
	}
	return mi
}

func (m *kwayMerge) advancePast(key []byte) {
	for i := range m.its {
		if m.live[i] && bytes.Equal(m.its[i].Key(), key) {
			m.live[i] = m.its[i].Next()
		}
	}
}

func (m *kwayMerge) err() error {
	for i := range m.its {
		if err := m.its[i].Err(); err != nil {
			return fmt.Errorf("store: compaction scan: %w", err)
		}
	}
	return nil
}

// keepAtBottom reports whether an entry survives the merge. Off the bottom of
// the LSM everything is kept; at the bottom, evict markers and aged tombstones
// are dropped (see droppableAtBottom).
func keepAtBottom(val []byte, bottom bool, gcCutoff time.Time) (bool, error) {
	if !bottom {
		return true, nil
	}
	drop, err := droppableAtBottom(val, gcCutoff)
	return !drop, err
}

// droppableAtBottom reports whether a value can be discarded when its key
// reaches the bottom of the LSM (no older table below). Evict markers and
// uncontested tombstones past the GC window are dropped; everything else,
// including unexpired or contested tombstones, is kept so reads and
// anti-entropy still observe the delete.
func droppableAtBottom(val []byte, gcCutoff time.Time) (bool, error) {
	if len(val) > 0 && val[0] == tableKindEvict {
		return true, nil
	}
	e, evicted, tombAt, hasTomb, err := decodeTableEntryFull(val)
	if err != nil {
		return false, err
	}
	if evicted {
		return true, nil
	}
	if len(e.Siblings) == 1 && e.Siblings[0].Deleted && hasTomb && tombAt.Before(gcCutoff) {
		return true, nil
	}
	return false, nil
}

// commitCompaction installs the merged table in place of its sources: it
// rewrites the manifest under s.mu (the durable commit point), swaps the
// in-memory table set, then closes and unlinks the retired sources. Closing
// waits on s.tablesRW so a lock-free reader holding the old slice never reads
// a closed reader.
func (s *Store) commitCompaction(dir string, sources []liveTable, merged liveTable, created bool) error {
	s.mu.Lock()
	newTables, retired, ok := spliceTables(s.tables, sources, merged, created)
	if !ok {
		s.mu.Unlock()
		discardMerged(dir, merged, created)
		return errors.New("store: compaction sources no longer attached")
	}
	if err := writeManifest(dir, manifest{
		Tables:      tableNames(newTables),
		MaxSeq:      s.maxTableSeq,
		NextFileNum: s.nextFileNum,
	}); err != nil {
		s.mu.Unlock()
		discardMerged(dir, merged, created)
		return fmt.Errorf("store: write manifest after compaction: %w", err)
	}
	s.tables = newTables
	s.mu.Unlock()

	s.tablesRW.Lock()
	closeAll(retired)
	s.tablesRW.Unlock()
	for _, lt := range retired {
		_ = os.Remove(filepath.Join(dir, lt.file))
	}
	return nil
}

func discardMerged(dir string, merged liveTable, created bool) {
	if created {
		_ = merged.r.Close()
		_ = os.Remove(filepath.Join(dir, merged.file))
	}
}

// spliceTables replaces the contiguous run matching sources (by reader
// identity) in cur with merged, returning the new set and the retired
// sources. ok is false if the run is not found contiguous — a flush only ever
// prepends, so the run stays intact, but this guards against the unexpected.
func spliceTables(cur, sources []liveTable, merged liveTable, created bool) (newTables, retired []liveTable, ok bool) {
	if len(sources) == 0 {
		return nil, nil, false
	}
	start := -1
	for i := range cur {
		if cur[i].r == sources[0].r {
			start = i
			break
		}
	}
	if start < 0 || start+len(sources) > len(cur) {
		return nil, nil, false
	}
	for k := range sources {
		if cur[start+k].r != sources[k].r {
			return nil, nil, false
		}
	}
	newTables = make([]liveTable, 0, len(cur)-len(sources)+1)
	newTables = append(newTables, cur[:start]...)
	if created {
		newTables = append(newTables, merged)
	}
	newTables = append(newTables, cur[start+len(sources):]...)
	retired = append([]liveTable(nil), sources...)
	return newTables, retired, true
}
