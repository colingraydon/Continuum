package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/wal"
)

// newFlushStore returns a store with a real WAL and flush policy pointing at
// temp directories. thresholdBytes <= 0 means manual Flush only.
func newFlushStore(t *testing.T, thresholdBytes int64) (*Store, string, string) {
	t.Helper()
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	tablesDir := filepath.Join(dir, "tables")
	if err := os.MkdirAll(tablesDir, 0o755); err != nil {
		t.Fatalf("mkdir tables: %v", err)
	}
	w, err := wal.Open(walDir)
	if err != nil {
		t.Fatalf("wal open: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	s := New()
	s.SetWAL(w)
	s.SetFlushPolicy(tablesDir, thresholdBytes)
	t.Cleanup(func() { _ = s.CloseTables() })
	return s, walDir, tablesDir
}

func mustGet(t *testing.T, s *Store, key string) (Entry, bool) {
	t.Helper()
	e, ok, err := s.Get(key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	return e, ok
}

func TestFlushAndGetFromTable(t *testing.T) {
	s, _, _ := newFlushStore(t, 0)
	keys := []string{"alpha", "beta", "gamma"}
	for i, k := range keys {
		if err := s.Put(k, "v-"+k, vclock("a", uint64(i+1))); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if n := s.TableCount(); n != 1 {
		t.Fatalf("TableCount = %d, want 1", n)
	}
	for _, k := range keys {
		e, ok := mustGet(t, s, k)
		if !ok || e.Siblings[0].Value != "v-"+k {
			t.Fatalf("Get(%q) after flush = (%v, %v)", k, e, ok)
		}
	}
	if _, ok := mustGet(t, s, "absent"); ok {
		t.Fatal("absent key found after flush")
	}
}

func TestAutoFlushOnThreshold(t *testing.T) {
	s, _, tablesDir := newFlushStore(t, 1) // every write crosses the threshold
	for i, k := range []string{"a", "b", "c"} {
		if err := s.Put(k, "v", vclock("n", uint64(i+1))); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}
	if n := s.TableCount(); n == 0 {
		t.Fatal("expected auto-flush to produce at least one table")
	}
	entries, err := os.ReadDir(tablesDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	sst := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sst") {
			sst++
		}
	}
	if sst != s.TableCount() {
		t.Fatalf("%d .sst files on disk, %d attached", sst, s.TableCount())
	}
	for _, k := range []string{"a", "b", "c"} {
		if _, ok := mustGet(t, s, k); !ok {
			t.Fatalf("key %q lost after auto-flush", k)
		}
	}
}

func TestConcurrentWriteAfterFlushMergesSiblings(t *testing.T) {
	s, _, _ := newFlushStore(t, 0)
	if err := s.Put("k", "v1", vclock("a", 1)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Concurrent with {a:1}: neither clock dominates.
	if err := s.Put("k", "v2", vclock("b", 1)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	e, ok := mustGet(t, s, "k")
	if !ok || len(e.Siblings) != 2 {
		t.Fatalf("expected 2 siblings after cross-generation conflict, got %v (ok=%v)", e, ok)
	}
}

func TestDominanceAcrossGenerations(t *testing.T) {
	s, _, _ := newFlushStore(t, 0)
	if err := s.Put("k", "v1", vclock("a", 1)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Dominates the table-resident sibling: replaces it.
	if err := s.Put("k", "v2", vclock("a", 2)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	e, ok := mustGet(t, s, "k")
	if !ok || len(e.Siblings) != 1 || e.Siblings[0].Value != "v2" {
		t.Fatalf("expected single v2 sibling, got %v", e)
	}
	// Dominated by the memtable state now: dropped, nothing changes.
	if err := s.Put("k", "stale", vclock("a", 1)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	e, _ = mustGet(t, s, "k")
	if len(e.Siblings) != 1 || e.Siblings[0].Value != "v2" {
		t.Fatalf("stale write must be dropped, got %v", e)
	}
}

func TestTombstoneAcrossFlushAndReopen(t *testing.T) {
	s, _, tablesDir := newFlushStore(t, 0)
	if err := s.Put("k", "v", vclock("a", 1)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := s.Delete("k", vclock("a", 2)); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	e, ok := mustGet(t, s, "k")
	if !ok || len(e.Siblings) != 1 || !e.Siblings[0].Deleted {
		t.Fatalf("expected lone tombstone, got %v (ok=%v)", e, ok)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("second Flush: %v", err)
	}

	// Reopen from tables alone: the newest table's entry must win.
	s2 := New()
	if _, err := s2.OpenTables(tablesDir); err != nil {
		t.Fatalf("OpenTables: %v", err)
	}
	defer func() { _ = s2.CloseTables() }()
	e, ok = mustGet(t, s2, "k")
	if !ok || len(e.Siblings) != 1 || !e.Siblings[0].Deleted {
		t.Fatalf("tombstone lost across reopen, got %v (ok=%v)", e, ok)
	}
}

func TestEvictShadowsTableData(t *testing.T) {
	s, _, tablesDir := newFlushStore(t, 0)
	if err := s.Put("k", "v", vclock("a", 1)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := s.Evict("k"); err != nil {
		t.Fatalf("Evict: %v", err)
	}
	if _, ok := mustGet(t, s, "k"); ok {
		t.Fatal("evicted key still visible")
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush marker: %v", err)
	}

	// The flushed marker must shadow the older table after reopen.
	s2 := New()
	if _, err := s2.OpenTables(tablesDir); err != nil {
		t.Fatalf("OpenTables: %v", err)
	}
	defer func() { _ = s2.CloseTables() }()
	if _, ok := mustGet(t, s2, "k"); ok {
		t.Fatal("evict marker did not shadow table data after reopen")
	}
}

func TestEvictWithoutTablesLeavesNoMarker(t *testing.T) {
	s, _, _ := newFlushStore(t, 0)
	if err := s.Put("k", "v", vclock("a", 1)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Evict("k"); err != nil {
		t.Fatalf("Evict: %v", err)
	}
	// Nothing visible below the memtable, so no marker — flushing should be
	// a no-op on an effectively-empty memtable... except the eviction left
	// the memtable truly empty only if no marker was recorded.
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if n := s.TableCount(); n != 0 {
		t.Fatalf("expected no table for marker-free evict, got %d", n)
	}
}

func TestGCSkipsTombstoneShadowingTableValue(t *testing.T) {
	s, _, _ := newFlushStore(t, 0)
	if err := s.Put("k", "v", vclock("a", 1)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := s.Delete("k", vclock("a", 2)); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// A never-flushed key can still be purged.
	if err := s.Delete("k2", vclock("b", 1)); err != nil {
		t.Fatalf("Delete k2: %v", err)
	}
	past := time.Now().Add(-2 * time.Hour)
	s.SetTombstoneAge("k", past)
	s.SetTombstoneAge("k2", past)

	purged, err := s.GCTombstones(time.Hour)
	if err != nil {
		t.Fatalf("GCTombstones: %v", err)
	}
	if len(purged) != 1 || purged[0] != "k2" {
		t.Fatalf("purged = %v, want [k2] only", purged)
	}
	// Purging k would have resurrected the table-resident value.
	e, ok := mustGet(t, s, "k")
	if !ok || len(e.Siblings) != 1 || !e.Siblings[0].Deleted {
		t.Fatalf("shadowing tombstone must survive GC, got %v (ok=%v)", e, ok)
	}
}

func TestReplayMergesAgainstTables(t *testing.T) {
	s, walDir, tablesDir := newFlushStore(t, 0)
	if err := s.Put("k", "v1", vclock("a", 1)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Flush(); err != nil { // truncates WAL through this write
		t.Fatalf("Flush: %v", err)
	}
	flushedSeq := s.LastSeq()
	// Concurrent sibling lands in the WAL only; simulate a crash before the
	// next flush by recovering into a fresh store.
	if err := s.Put("k", "v2", vclock("b", 1)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	s2 := New()
	if _, err := s2.OpenTables(tablesDir); err != nil {
		t.Fatalf("OpenTables: %v", err)
	}
	defer func() { _ = s2.CloseTables() }()
	r, err := wal.NewReader(walDir)
	if err != nil {
		t.Fatalf("wal reader: %v", err)
	}
	defer func() { _ = r.Close() }()
	if _, err := s2.Replay(r, flushedSeq); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	e, ok := mustGet(t, s2, "k")
	if !ok || len(e.Siblings) != 2 {
		t.Fatalf("replay must merge WAL record with table sibling, got %v (ok=%v)", e, ok)
	}
}

func TestKeyHashesMergesGenerations(t *testing.T) {
	s, _, _ := newFlushStore(t, 0)
	if err := s.Put("a", "v", vclock("n", 1)); err != nil {
		t.Fatalf("Put a: %v", err)
	}
	if err := s.Put("b", "v1", vclock("n", 2)); err != nil {
		t.Fatalf("Put b: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := s.Put("b", "v2", vclock("n", 3)); err != nil { // dominates table version
		t.Fatalf("Put b v2: %v", err)
	}
	if err := s.Evict("a"); err != nil { // marker shadows table version
		t.Fatalf("Evict a: %v", err)
	}

	hashes, err := s.KeyHashes()
	if err != nil {
		t.Fatalf("KeyHashes: %v", err)
	}
	if _, ok := hashes["a"]; ok {
		t.Fatal("evicted key must not appear in KeyHashes")
	}
	e, _ := mustGet(t, s, "b")
	if hashes["b"] != entryHash(e) {
		t.Fatalf("hash for b = %d, want %d (merged entry)", hashes["b"], entryHash(e))
	}
	if len(hashes) != 1 {
		t.Fatalf("KeyHashes = %v, want exactly {b}", hashes)
	}
}

func TestFlushEmptyMemtableNoop(t *testing.T) {
	s, _, _ := newFlushStore(t, 0)
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if n := s.TableCount(); n != 0 {
		t.Fatalf("TableCount = %d, want 0", n)
	}
}

func TestFlushTruncatesCoveredWAL(t *testing.T) {
	s, walDir, _ := newFlushStore(t, 0)
	for i := range 5 {
		if err := s.Put("k", "v", vclock("a", uint64(i+1))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Records covered by the table are skipped on the next replay.
	s2 := New()
	r, err := wal.NewReader(walDir)
	if err != nil {
		t.Fatalf("wal reader: %v", err)
	}
	defer func() { _ = r.Close() }()
	last, err := s2.Replay(r, s.LastSeq())
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if last > s.LastSeq() {
		t.Fatalf("unexpected uncovered records: last=%d flushed=%d", last, s.LastSeq())
	}
	if _, ok, _ := s2.Get("k"); ok {
		t.Fatal("replay past flush threshold must not re-apply covered writes")
	}
}

func vclock(id string, c uint64) VectorClockVersion {
	return VectorClockVersion{Clocks: map[string]uint64{id: c}}
}
