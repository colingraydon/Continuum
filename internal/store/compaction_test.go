package store

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// putFlush writes one key at the given clock counter and flushes, producing a
// fresh single-key table on top of the stack.
func putFlush(t *testing.T, s *Store, key, value string, counter uint64) {
	t.Helper()
	if err := s.Put(key, value, vclock("c", counter)); err != nil {
		t.Fatalf("Put(%q): %v", key, err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func countSST(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	n := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), tableSuffix) && !strings.HasSuffix(e.Name(), tableTmpSuffix) {
			n++
		}
	}
	return n
}

func TestSelectCompaction(t *testing.T) {
	p := compactionPolicy{minThreshold: 4, maxThreshold: 32, sizeRatio: 2.0}
	equal := func(n int) []int64 {
		s := make([]int64, n)
		for i := range s {
			s[i] = 10
		}
		return s
	}

	tests := []struct {
		name             string
		sizes            []int64
		wantStart, wantN int
	}{
		{"too few tables", equal(3), 0, 0},
		{"four equal merge all", equal(4), 0, 4},
		{"big oldest excluded", []int64{10, 10, 10, 10, 1000}, 0, 4},
		{"big newest excluded", []int64{1000, 10, 10, 10, 10}, 1, 4},
		{"no qualifying run", []int64{1, 4, 16, 64, 256}, 0, 0},
		{"cap at maxThreshold", equal(40), 0, 32},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			start, n := selectCompaction(make([]liveTable, len(tc.sizes)), tc.sizes, p)
			if start != tc.wantStart || n != tc.wantN {
				t.Fatalf("selectCompaction = (%d, %d), want (%d, %d)", start, n, tc.wantStart, tc.wantN)
			}
		})
	}
}

func TestCompactionKeepNewestAndReopen(t *testing.T) {
	s, _, tablesDir := newFlushStore(t, 0)
	s.SetCompactionPolicy(2, 32, 1e9) // bucket everything together

	putFlush(t, s, "a", "1", 1)
	putFlush(t, s, "a", "2", 2) // dominates "1"; newest table wins
	putFlush(t, s, "b", "1", 1)
	putFlush(t, s, "c", "1", 1)
	if n := s.TableCount(); n != 4 {
		t.Fatalf("TableCount = %d, want 4", n)
	}

	merged, err := s.Compact(time.Hour)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if !merged {
		t.Fatal("Compact reported no merge")
	}
	if n := s.TableCount(); n != 1 {
		t.Fatalf("TableCount after compaction = %d, want 1", n)
	}
	if n := countSST(t, tablesDir); n != 1 {
		t.Fatalf("%d .sst files on disk after compaction, want 1", n)
	}

	want := map[string]string{"a": "2", "b": "1", "c": "1"}
	assertValues(t, s, want)

	// A fresh store loading the manifest must see the merged table and data.
	s2 := New()
	if _, err := s2.OpenTables(tablesDir); err != nil {
		t.Fatalf("OpenTables: %v", err)
	}
	t.Cleanup(func() { _ = s2.CloseTables() })
	if n := s2.TableCount(); n != 1 {
		t.Fatalf("reopened TableCount = %d, want 1", n)
	}
	assertValues(t, s2, want)
}

func assertValues(t *testing.T, s *Store, want map[string]string) {
	t.Helper()
	for k, v := range want {
		e, ok := mustGet(t, s, k)
		if !ok || e.Siblings[0].Value != v {
			t.Fatalf("Get(%q) = (%v, %v), want value %q", k, e, ok, v)
		}
	}
}

func TestCompactionBottomDropsAgedTombstone(t *testing.T) {
	s, _, _ := newFlushStore(t, 0)
	s.SetCompactionPolicy(2, 32, 1e9)

	// "a" gets a value, then an aged tombstone in a newer table.
	putFlush(t, s, "a", "1", 1)
	if err := s.Delete("a", vclock("c", 2)); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	s.SetTombstoneAge("a", time.Now().Add(-48*time.Hour))
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	putFlush(t, s, "b", "1", 1)
	putFlush(t, s, "c", "1", 1)

	merged, err := s.Compact(24 * time.Hour)
	if err != nil || !merged {
		t.Fatalf("Compact = (%v, %v)", merged, err)
	}
	if _, ok := mustGet(t, s, "a"); ok {
		t.Fatal("aged tombstone for \"a\" should be dropped at the bottom")
	}
	for _, k := range []string{"b", "c"} {
		if _, ok := mustGet(t, s, k); !ok {
			t.Fatalf("live key %q lost during compaction", k)
		}
	}
}

func TestCompactionBottomKeepsUnagedTombstone(t *testing.T) {
	s, _, _ := newFlushStore(t, 0)
	s.SetCompactionPolicy(2, 32, 1e9)

	putFlush(t, s, "a", "1", 1)
	if err := s.Delete("a", vclock("c", 2)); err != nil { // recent tombstone
		t.Fatalf("Delete: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	putFlush(t, s, "b", "1", 1)
	putFlush(t, s, "c", "1", 1)

	if _, err := s.Compact(24 * time.Hour); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	e, ok := mustGet(t, s, "a")
	if !ok || len(e.Siblings) != 1 || !e.Siblings[0].Deleted {
		t.Fatalf("recent tombstone for \"a\" should survive compaction, got (%v, %v)", e, ok)
	}
}

func TestCompactionBottomDropsEvictMarker(t *testing.T) {
	s, _, _ := newFlushStore(t, 0)
	s.SetCompactionPolicy(2, 32, 1e9)

	putFlush(t, s, "a", "1", 1)
	if err := s.Evict("a"); err != nil { // evict marker shadows the table copy
		t.Fatalf("Evict: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	putFlush(t, s, "b", "1", 1)
	putFlush(t, s, "c", "1", 1)

	if _, err := s.Compact(24 * time.Hour); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if _, ok := mustGet(t, s, "a"); ok {
		t.Fatal("evict marker for \"a\" should be dropped at the bottom")
	}
	if n := s.TableCount(); n != 1 {
		t.Fatalf("TableCount = %d, want 1", n)
	}
}

func TestCompactionNonBottomRetainsTombstone(t *testing.T) {
	s, _, _ := newFlushStore(t, 0)
	s.SetCompactionPolicy(2, 2, 1e9) // merge only the newest two tables

	putFlush(t, s, "x", "1", 1) // oldest; stays below the compaction set
	putFlush(t, s, "f1", "1", 1)
	putFlush(t, s, "f2", "1", 1)
	if err := s.Delete("x", vclock("c", 2)); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	s.SetTombstoneAge("x", time.Now().Add(-48*time.Hour)) // aged
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	putFlush(t, s, "g", "1", 1) // newest
	if n := s.TableCount(); n != 5 {
		t.Fatalf("TableCount = %d, want 5", n)
	}

	// Newest two are [g, xTomb]; this run does not reach the oldest table, so
	// it is not the bottom and the aged tombstone must be retained.
	merged, err := s.Compact(24 * time.Hour)
	if err != nil || !merged {
		t.Fatalf("Compact = (%v, %v)", merged, err)
	}
	if n := s.TableCount(); n != 4 {
		t.Fatalf("TableCount after non-bottom compaction = %d, want 4", n)
	}
	e, ok := mustGet(t, s, "x")
	if !ok || len(e.Siblings) != 1 || !e.Siblings[0].Deleted {
		t.Fatalf("aged tombstone must survive non-bottom compaction, got (%v, %v)", e, ok)
	}

	// A full (bottom-reaching) compaction now drops it.
	s.SetCompactionPolicy(2, 32, 1e9)
	if _, err := s.Compact(24 * time.Hour); err != nil {
		t.Fatalf("Compact (full): %v", err)
	}
	if _, ok := mustGet(t, s, "x"); ok {
		t.Fatal("aged tombstone should be dropped once compaction reaches the bottom")
	}
}

func TestCompactionNotifiesEvictForDroppedKeys(t *testing.T) {
	s, _, _ := newFlushStore(t, 0)
	s.SetCompactionPolicy(2, 32, 1e9)

	var mu sync.Mutex
	evicted := map[string]bool{}
	s.SetOnEvict(func(key string) {
		mu.Lock()
		evicted[key] = true
		mu.Unlock()
	})

	putFlush(t, s, "a", "1", 1)
	if err := s.Delete("a", vclock("c", 2)); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	s.SetTombstoneAge("a", time.Now().Add(-48*time.Hour))
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	putFlush(t, s, "b", "1", 1)
	putFlush(t, s, "c", "1", 1)

	if _, err := s.Compact(24 * time.Hour); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !evicted["a"] {
		t.Fatal("dropped tombstone \"a\" should fire onEvict so AE drops it from the Merkle tree")
	}
	for _, k := range []string{"b", "c"} {
		if evicted[k] {
			t.Fatalf("live key %q must not fire onEvict", k)
		}
	}
}

func TestCompactionDoesNotNotifyShadowedKey(t *testing.T) {
	s, _, _ := newFlushStore(t, 0)
	s.SetCompactionPolicy(2, 32, 2.0) // real size tiers: the large table is its own bucket

	var mu sync.Mutex
	evicted := map[string]bool{}
	s.SetOnEvict(func(key string) {
		mu.Lock()
		evicted[key] = true
		mu.Unlock()
	})

	// Oldest two tables hold x's value then an aged tombstone; both are small.
	putFlush(t, s, "x", "1", 1)
	if err := s.Delete("x", vclock("c", 2)); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	s.SetTombstoneAge("x", time.Now().Add(-48*time.Hour))
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// A larger, newer table revives x as a live value and shadows the tombstone.
	for i := 0; i < 40; i++ {
		if err := s.Put(key(i), "padding-value", vclock("c", 3)); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := s.Put("x", "2", vclock("c", 3)); err != nil {
		t.Fatalf("Put x: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if n := s.TableCount(); n != 3 {
		t.Fatalf("TableCount = %d, want 3", n)
	}

	// Compaction selects the two small oldest tables (the large newest one is a
	// separate size bucket). The merge reaches the bottom and drops x's aged
	// tombstone, but x is still live in the table above, so onEvict must NOT fire.
	merged, err := s.Compact(24 * time.Hour)
	if err != nil || !merged {
		t.Fatalf("Compact = (%v, %v)", merged, err)
	}
	e, ok := mustGet(t, s, "x")
	if !ok || e.Siblings[0].Value != "2" {
		t.Fatalf("x should remain live = \"2\", got (%v, %v)", e, ok)
	}
	mu.Lock()
	defer mu.Unlock()
	if evicted["x"] {
		t.Fatal("x is still live above the merge; onEvict must not fire for it")
	}
}

func TestCompactionNoopBelowThreshold(t *testing.T) {
	s, _, _ := newFlushStore(t, 0)
	// Default minThreshold is 4; three tables must not compact.
	putFlush(t, s, "a", "1", 1)
	putFlush(t, s, "b", "1", 1)
	putFlush(t, s, "c", "1", 1)

	merged, err := s.Compact(time.Hour)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if merged {
		t.Fatal("Compact should be a no-op below minThreshold")
	}
	if n := s.TableCount(); n != 3 {
		t.Fatalf("TableCount = %d, want 3", n)
	}
}

func TestOpenTablesRemovesOrphanSST(t *testing.T) {
	s, _, tablesDir := newFlushStore(t, 0)
	putFlush(t, s, "a", "1", 1)
	if n := countSST(t, tablesDir); n != 1 {
		t.Fatalf("%d .sst files, want 1", n)
	}

	// Simulate a crashed compaction: an .sst file on disk not named in the
	// manifest. OpenTables must delete it.
	orphan := filepath.Join(tablesDir, tableName(99))
	if err := os.WriteFile(orphan, []byte("garbage"), 0o644); err != nil {
		t.Fatalf("write orphan: %v", err)
	}

	s2 := New()
	if _, err := s2.OpenTables(tablesDir); err != nil {
		t.Fatalf("OpenTables: %v", err)
	}
	t.Cleanup(func() { _ = s2.CloseTables() })
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("orphan .sst still present (stat err=%v)", err)
	}
	if n := s2.TableCount(); n != 1 {
		t.Fatalf("reopened TableCount = %d, want 1", n)
	}
	if _, ok := mustGet(t, s2, "a"); !ok {
		t.Fatal("key \"a\" lost after orphan cleanup")
	}
}

func TestOpenTablesMigratesLegacyNoManifest(t *testing.T) {
	s, _, tablesDir := newFlushStore(t, 0)
	putFlush(t, s, "a", "1", 1)
	putFlush(t, s, "b", "1", 1)

	// Remove the manifest to mimic a pre-compaction data dir.
	if err := os.Remove(filepath.Join(tablesDir, manifestFile)); err != nil {
		t.Fatalf("remove manifest: %v", err)
	}

	s2 := New()
	if _, err := s2.OpenTables(tablesDir); err != nil {
		t.Fatalf("OpenTables (legacy): %v", err)
	}
	t.Cleanup(func() { _ = s2.CloseTables() })
	if n := s2.TableCount(); n != 2 {
		t.Fatalf("migrated TableCount = %d, want 2", n)
	}
	if _, _, err := readManifestFromDir(tablesDir); err != nil {
		t.Fatalf("manifest not recreated: %v", err)
	}
	for _, k := range []string{"a", "b"} {
		if _, ok := mustGet(t, s2, k); !ok {
			t.Fatalf("key %q lost after legacy migration", k)
		}
	}
}

// TestCompactionConcurrentReadsWrites exercises the table-reader retirement
// barrier: Get and KeyHashes read table files outside s.mu, so a compaction
// closing the readers it retires must not race them. Run with -race.
func TestCompactionConcurrentReadsWrites(t *testing.T) {
	s, _, _ := newFlushStore(t, 0)
	s.SetCompactionPolicy(2, 8, 1e9)

	const keys = 40
	for i := 0; i < keys; i++ {
		putFlush(t, s, key(i), "v0", 1)
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	spawn := func(label string, step func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := spin(done, step); err != nil {
				t.Errorf("%s: %v", label, err)
			}
		}()
	}

	for r := 0; r < 4; r++ {
		spawn("read", func() error { return readStep(s, keys) })
	}
	var counter atomic.Uint64
	spawn("write", func() error { return writeStep(s, keys, &counter) })
	spawn("compact", func() error { _, err := s.Compact(time.Hour); return err })

	time.Sleep(300 * time.Millisecond)
	close(done)
	wg.Wait()

	for i := 0; i < keys; i++ {
		if _, ok := mustGet(t, s, key(i)); !ok {
			t.Fatalf("key %q lost after concurrent compaction", key(i))
		}
	}
}

// spin runs step repeatedly until done is closed or step returns an error.
func spin(done <-chan struct{}, step func() error) error {
	for {
		select {
		case <-done:
			return nil
		default:
		}
		if err := step(); err != nil {
			return err
		}
	}
}

func readStep(s *Store, keys int) error {
	if _, _, err := s.Get(key(int(time.Now().UnixNano()) % keys)); err != nil {
		return err
	}
	_, err := s.KeyHashes()
	return err
}

func writeStep(s *Store, keys int, counter *atomic.Uint64) error {
	n := counter.Add(1)
	if err := s.Put(key(int(n)%keys), "v", vclock("c", n+1)); err != nil {
		return err
	}
	return s.Flush()
}

func key(i int) string { return "k" + strconv.Itoa(i) }

// readManifestFromDir is a thin test wrapper that fails if the manifest is
// absent, used to assert OpenTables recreated it.
func readManifestFromDir(dir string) (manifest, bool, error) {
	m, ok, err := readManifest(dir)
	if err == nil && !ok {
		return m, ok, os.ErrNotExist
	}
	return m, ok, err
}
