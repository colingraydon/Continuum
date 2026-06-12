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
// only truncated after the table is durably on disk.
func (s *Store) flush(force bool) error {
	s.mu.Lock()
	if s.flushDir == "" || s.flushing {
		s.mu.Unlock()
		return nil
	}
	if s.frozen == nil {
		if !force && (s.flushBytes <= 0 || s.memBytes < s.flushBytes) {
			s.mu.Unlock()
			return nil
		}
		if len(s.data) == 0 && len(s.evicted) == 0 {
			s.mu.Unlock()
			return nil
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
	frozen := s.frozen
	dir := s.flushDir
	s.flushing = true
	s.mu.Unlock()

	reader, err := writeTable(dir, frozen)

	s.mu.Lock()
	s.flushing = false
	if err != nil {
		s.mu.Unlock()
		return err
	}
	tables := make([]*sstable.Reader, 0, len(s.tables)+1)
	tables = append(tables, reader)
	s.tables = append(tables, s.tables...)
	s.frozen = nil
	w := s.wal
	s.mu.Unlock()

	if w != nil {
		if err := w.TruncateThrough(frozen.seq); err != nil {
			// The table is durable; stale WAL segments only cost replay time.
			return fmt.Errorf("store: truncate wal after flush: %w", err)
		}
	}
	return nil
}

// writeTable encodes a frozen memtable as an SSTable: temp file, fsync,
// rename, fsync directory. Returns an open reader for the finished table.
func writeTable(dir string, m *memtable) (*sstable.Reader, error) {
	keys := make([]string, 0, len(m.data)+len(m.evicted))
	for k := range m.data {
		keys = append(keys, k)
	}
	for k := range m.evicted {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	name := tableName(m.seq)
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
	return sstable.Open(finalPath)
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

// OpenTables attaches every finished table in dir, newest first, and removes
// abandoned .sst.tmp files from crashed flushes. Returns the highest WAL
// sequence covered, for use as the replay skip threshold. Call before Replay
// so replayed records merge against table-resident state.
func (s *Store) OpenTables(dir string) (uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("store: read table dir: %w", err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), tableTmpSuffix) {
			_ = os.Remove(filepath.Join(dir, e.Name()))
			continue
		}
		if strings.HasSuffix(e.Name(), tableSuffix) {
			names = append(names, e.Name())
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names))) // newest (highest seq) first

	var maxSeq uint64
	tables := make([]*sstable.Reader, 0, len(names))
	for _, name := range names {
		seq, err := tableSeqFromName(name)
		if err != nil {
			closeAll(tables)
			return 0, err
		}
		r, err := sstable.Open(filepath.Join(dir, name))
		if err != nil {
			closeAll(tables)
			return 0, fmt.Errorf("store: open table %s: %w", name, err)
		}
		tables = append(tables, r)
		if seq > maxSeq {
			maxSeq = seq
		}
	}

	s.mu.Lock()
	s.tables = tables
	if maxSeq > s.lastSeq {
		s.lastSeq = maxSeq
	}
	s.mu.Unlock()
	return maxSeq, nil
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
		if err := t.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func closeAll(tables []*sstable.Reader) {
	for _, t := range tables {
		_ = t.Close()
	}
}

func tableName(seq uint64) string {
	return fmt.Sprintf("%020d%s", seq, tableSuffix)
}

func tableSeqFromName(name string) (uint64, error) {
	base := strings.TrimSuffix(name, tableSuffix)
	seq, err := strconv.ParseUint(base, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("store: bad table name %q: %w", name, err)
	}
	return seq, nil
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
