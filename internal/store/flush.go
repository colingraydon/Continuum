package store

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/colingraydon/continuum/internal/sstable"
)

const (
	tableSuffix    = ".sst"
	tableTmpSuffix = ".sst.tmp"
)

// liveTable pairs an open table reader with its base filename so the manifest
// can be rewritten from the in-memory table set and so compaction can unlink
// the files it retires.
type liveTable struct {
	r    *sstable.Reader
	file string
}

// SetFlushPolicy enables memtable flushing: once the memtable's estimated
// size exceeds thresholdBytes, the next write flushes it to an SSTable in
// dir and truncates the WAL segments it covers. thresholdBytes <= 0 leaves
// flushing manual (Flush only). Call after OpenTables and SetWAL.
func (s *Store) SetFlushPolicy(dir string, thresholdBytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushDir = dir
	s.flushBytes = thresholdBytes
}

// Flush forces the current memtable out to an SSTable regardless of size.
// No-op when the memtable is empty or no flush directory is configured.
// Called on graceful shutdown in place of the legacy snapshot. Runs twice so
// that a frozen memtable left by an earlier failed flush and the active
// memtable both reach disk.
func (s *Store) Flush() error {
	if err := s.flush(true); err != nil {
		return err
	}
	return s.flush(true)
}

// flushIfNeeded runs a threshold-triggered flush after a write. Flush
// failures are logged, not returned: the write itself is already durable in
// the WAL, and the frozen memtable is retained so the next write retries.
func (s *Store) flushIfNeeded() {
	if err := s.flush(false); err != nil {
		log.Printf("store: flush failed (will retry): %v", err)
	}
}

// flush freezes the active memtable if warranted and writes the frozen one
// to an SSTable. The freeze happens under the lock; encoding and file IO do
// not — concurrent writes go to the fresh memtable and reads consult the
// frozen one until the table is attached. Only one flush runs at a time; a
// failed flush leaves the frozen memtable in place for retry, and the WAL is
// only truncated after the table is durably on disk and named in the manifest.
func (s *Store) flush(force bool) error {
	frozen, dir, fileNum, ok := s.beginFlush(force)
	if !ok {
		return nil
	}
	s.mu.RLock()
	cache := s.blockCache
	s.mu.RUnlock()
	reader, writeErr := writeTable(dir, fileNum, frozen, cache)
	w, err := s.finishFlush(reader, writeErr, fileNum, frozen, dir)
	if err != nil {
		return err
	}
	if w != nil {
		if err := w.TruncateThrough(frozen.seq); err != nil {
			// The table is durable; stale WAL segments only cost replay time.
			return fmt.Errorf("store: truncate wal after flush: %w", err)
		}
	}
	return nil
}

// beginFlush decides under the lock whether a flush should proceed, freezing
// the active memtable if needed and reserving a file number. ok is false when
// there is nothing to flush or a flush is already running.
func (s *Store) beginFlush(force bool) (frozen *memtable, dir string, fileNum uint64, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.flushDir == "" || s.flushing {
		return nil, "", 0, false
	}
	if s.frozen == nil {
		if !force && (s.flushBytes <= 0 || s.memBytes < s.flushBytes) {
			return nil, "", 0, false
		}
		if len(s.data) == 0 && len(s.evicted) == 0 {
			return nil, "", 0, false
		}
		s.frozen = &memtable{
			data:    s.data,
			evicted: s.evicted,
			ages:    s.tombstoneAges,
			seq:     s.lastSeq,
		}
		s.data = make(map[string]Entry)
		s.evicted = make(map[string]struct{})
		s.tombstoneAges = make(map[string]time.Time)
		s.memBytes = 0
	}
	fileNum = s.nextFileNum
	s.nextFileNum++
	s.flushing = true
	return s.frozen, s.flushDir, fileNum, true
}

// finishFlush installs a freshly written table: it clears the flushing flag,
// commits the new table to the manifest (the durable point), then swaps it
// into the in-memory set. Returns the WAL so the caller can truncate covered
// segments after the lock is released. On a write or manifest error the frozen
// memtable is retained for retry and the unreferenced file is removed.
func (s *Store) finishFlush(reader *sstable.Reader, writeErr error, fileNum uint64, frozen *memtable, dir string) (WAL, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushing = false
	if writeErr != nil {
		return nil, writeErr
	}
	newTables := append([]liveTable{{r: reader, file: tableName(fileNum)}}, s.tables...)
	newMaxSeq := s.maxTableSeq
	if frozen.seq > newMaxSeq {
		newMaxSeq = frozen.seq
	}
	if err := writeManifest(dir, manifest{
		Tables:      tableNames(newTables),
		MaxSeq:      newMaxSeq,
		NextFileNum: s.nextFileNum,
	}); err != nil {
		// The manifest is the source of truth: a table not named in it is an
		// orphan cleaned at the next startup. Drop the unreferenced file and
		// keep the frozen memtable for retry. Leave maxTableSeq untouched so
		// in-memory state never claims coverage that isn't durable.
		_ = reader.Close()
		_ = os.Remove(filepath.Join(dir, tableName(fileNum)))
		return nil, fmt.Errorf("store: write manifest after flush: %w", err)
	}
	s.tables = newTables
	s.maxTableSeq = newMaxSeq
	s.frozen = nil
	return s.wal, nil
}

// openTableWithCache opens a table reader and attaches the shared block
// cache before the reader is published anywhere.
func openTableWithCache(path string, cache *sstable.Cache) (*sstable.Reader, error) {
	r, err := sstable.Open(path)
	if err != nil {
		return nil, err
	}
	r.SetCache(cache)
	return r, nil
}

// writeTable encodes a frozen memtable as an SSTable: temp file, fsync,
// rename, fsync directory. Returns an open reader for the finished table.
func writeTable(dir string, fileNum uint64, m *memtable, cache *sstable.Cache) (*sstable.Reader, error) {
	keys := make([]string, 0, len(m.data)+len(m.evicted))
	for k := range m.data {
		keys = append(keys, k)
	}
	for k := range m.evicted {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	name := tableName(fileNum)
	tmpPath := filepath.Join(dir, name+".tmp")
	f, err := os.Create(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("store: create table tmp: %w", err)
	}
	if err := writeTableEntries(f, keys, m); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return nil, err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("store: sync table: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("store: close table: %w", err)
	}
	finalPath := filepath.Join(dir, name)
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("store: rename table: %w", err)
	}
	if err := fsyncDir(dir); err != nil {
		return nil, fmt.Errorf("store: fsync table dir: %w", err)
	}
	return openTableWithCache(finalPath, cache)
}

func writeTableEntries(f *os.File, keys []string, m *memtable) error {
	w := sstable.NewWriter(f, sstable.Options{})
	for _, k := range keys {
		var val []byte
		if _, ok := m.evicted[k]; ok {
			val = tableEvictValue
		} else {
			var err error
			val, err = encodeTableEntry(k, m.data[k], m.ages)
			if err != nil {
				return err
			}
		}
		if err := w.Add([]byte(k), val); err != nil {
			return fmt.Errorf("store: table add %q: %w", k, err)
		}
	}
	if err := w.Finish(); err != nil {
		return fmt.Errorf("store: table finish: %w", err)
	}
	return nil
}

// OpenTables attaches the live table set recorded in dir's manifest, newest
// first, and removes orphaned .sst/.sst.tmp files left by crashed flushes or
// compactions. A pre-compaction data dir has tables but no manifest; one is
// synthesized from the .sst listing (whose names are the WAL sequences they
// cover) and written out. Returns the highest WAL sequence covered, for use
// as the replay skip threshold. Call before Replay so replayed records merge
// against table-resident state.
func (s *Store) OpenTables(dir string) (uint64, error) {
	m, hasManifest, err := readManifest(dir)
	if err != nil {
		return 0, err
	}
	if !hasManifest {
		m, err = migrateLegacyManifest(dir)
		if err != nil {
			return 0, err
		}
	}
	if err := cleanOrphanTables(dir, m.Tables); err != nil {
		return 0, err
	}

	s.mu.RLock()
	cache := s.blockCache
	s.mu.RUnlock()
	live := make([]liveTable, 0, len(m.Tables))
	for _, name := range m.Tables {
		r, err := openTableWithCache(filepath.Join(dir, name), cache)
		if err != nil {
			closeAll(live)
			return 0, fmt.Errorf("store: open table %s: %w", name, err)
		}
		live = append(live, liveTable{r: r, file: name})
	}
	// Persist the synthesized manifest so later startups take the fast path.
	// A brand-new empty dir writes nothing — the first flush creates it — so
	// startup stays I/O-free when there are no tables to record.
	if !hasManifest && len(m.Tables) > 0 {
		if err := writeManifest(dir, m); err != nil {
			closeAll(live)
			return 0, err
		}
	}

	s.mu.Lock()
	s.tables = live
	s.maxTableSeq = m.MaxSeq
	s.nextFileNum = m.NextFileNum
	if m.MaxSeq > s.lastSeq {
		s.lastSeq = m.MaxSeq
	}
	s.mu.Unlock()
	return m.MaxSeq, nil
}

// migrateLegacyManifest builds a manifest from a pre-compaction tables dir
// where files are named by the WAL sequence they cover. File numbers are
// seeded past the largest existing name so freshly allocated tables never
// collide with a migrated one.
func migrateLegacyManifest(dir string) (manifest, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return manifest{}, fmt.Errorf("store: read table dir: %w", err)
	}
	var nums []uint64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), tableSuffix) {
			continue
		}
		n, err := tableNumFromName(e.Name())
		if err != nil {
			return manifest{}, err
		}
		nums = append(nums, n)
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] > nums[j] }) // newest first
	m := manifest{NextFileNum: 1}
	for _, n := range nums {
		m.Tables = append(m.Tables, tableName(n))
		if n > m.MaxSeq {
			m.MaxSeq = n // a legacy filename is the WAL sequence it covers
		}
		if n+1 > m.NextFileNum {
			m.NextFileNum = n + 1
		}
	}
	return m, nil
}

// cleanOrphanTables removes every .sst.tmp file and every .sst file not named
// in keep. Orphans arise when a crash interrupts a flush or compaction after
// the file is written but before the manifest naming it is committed.
func cleanOrphanTables(dir string, keep []string) error {
	want := make(map[string]struct{}, len(keep))
	for _, n := range keep {
		want[n] = struct{}{}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("store: read table dir: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		switch {
		case strings.HasSuffix(name, tableTmpSuffix):
			_ = os.Remove(filepath.Join(dir, name))
		case strings.HasSuffix(name, tableSuffix):
			if _, ok := want[name]; !ok {
				_ = os.Remove(filepath.Join(dir, name))
			}
		}
	}
	return nil
}

// TableCount returns the number of attached SSTables.
func (s *Store) TableCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.tables)
}

// CloseTables closes all attached table readers. Call on shutdown after the
// final Flush; the store must not serve reads afterwards.
func (s *Store) CloseTables() error {
	s.mu.Lock()
	tables := s.tables
	s.tables = nil
	s.mu.Unlock()
	var firstErr error
	for _, t := range tables {
		if err := t.r.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func closeAll(tables []liveTable) {
	for _, t := range tables {
		_ = t.r.Close()
	}
}

func tableNames(tables []liveTable) []string {
	names := make([]string, len(tables))
	for i, t := range tables {
		names[i] = t.file
	}
	return names
}

func tableName(fileNum uint64) string {
	return fmt.Sprintf("%020d%s", fileNum, tableSuffix)
}

func tableNumFromName(name string) (uint64, error) {
	base := strings.TrimSuffix(name, tableSuffix)
	num, err := strconv.ParseUint(base, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("store: bad table name %q: %w", name, err)
	}
	return num, nil
}

func fsyncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return f.Sync()
}

var errBadTableEntry = errors.New("store: bad table entry")
