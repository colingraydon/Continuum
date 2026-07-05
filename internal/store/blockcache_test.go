package store

import (
	"fmt"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/sstable"
)

// TestBlockCacheAttachedAcrossTableLifecycles proves the shared cache is
// wired into every path that opens a table reader: memtable flush,
// compaction output, and OpenTables on restart.
func TestBlockCacheAttachedAcrossTableLifecycles(t *testing.T) {
	s, _, tablesDir := newFlushStore(t, 0)
	cache := sstable.NewCache(1 << 20)
	s.SetBlockCache(cache)

	// Flush-created table: the second Get of the same key must hit.
	for i := 0; i < 50; i++ {
		putFlush(t, s, fmt.Sprintf("flush-%03d", i), "v1", uint64(i+1))
	}
	if _, ok := mustGet(t, s, "flush-007"); !ok {
		t.Fatal("flush-007 missing after flush")
	}
	before := s.BlockCacheStats()
	if _, ok := mustGet(t, s, "flush-007"); !ok {
		t.Fatal("flush-007 missing on second read")
	}
	if after := s.BlockCacheStats(); after.Hits <= before.Hits {
		t.Fatalf("no cache hit reading a flushed table: hits %d -> %d", before.Hits, after.Hits)
	}

	// Compaction output: merge the tables and confirm reads from the merged
	// table still populate and hit the cache.
	for i := 0; i < 50; i++ {
		putFlush(t, s, fmt.Sprintf("compact-%03d", i), "v1", uint64(100+i))
	}
	s.SetCompactionPolicy(2, 32, 100)
	if ok, err := s.Compact(time.Hour); err != nil || !ok {
		t.Fatalf("Compact: ok=%v err=%v", ok, err)
	}
	if _, ok := mustGet(t, s, "compact-007"); !ok {
		t.Fatal("compact-007 missing after compaction")
	}
	before = s.BlockCacheStats()
	if _, ok := mustGet(t, s, "compact-007"); !ok {
		t.Fatal("compact-007 missing on second read")
	}
	if after := s.BlockCacheStats(); after.Hits <= before.Hits {
		t.Fatalf("no cache hit reading a compacted table: hits %d -> %d", before.Hits, after.Hits)
	}

	// Restart: a fresh store with the cache set before OpenTables serves the
	// same data and caches it.
	if err := s.CloseTables(); err != nil {
		t.Fatalf("CloseTables: %v", err)
	}
	s2 := New()
	cache2 := sstable.NewCache(1 << 20)
	s2.SetBlockCache(cache2)
	if _, err := s2.OpenTables(tablesDir); err != nil {
		t.Fatalf("OpenTables: %v", err)
	}
	t.Cleanup(func() { _ = s2.CloseTables() })
	if _, ok := mustGet(t, s2, "flush-007"); !ok {
		t.Fatal("flush-007 missing after reopen")
	}
	if _, ok := mustGet(t, s2, "flush-007"); !ok {
		t.Fatal("flush-007 missing on second read after reopen")
	}
	if st := cache2.Stats(); st.Hits == 0 {
		t.Fatalf("no cache hit reading a reopened table: %+v", st)
	}
}
