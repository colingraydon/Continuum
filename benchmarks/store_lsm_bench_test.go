package benchmarks

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/sstable"
	"github.com/colingraydon/continuum/internal/store"
	"github.com/colingraydon/continuum/internal/wal"
)

// newDurableStore returns a WAL-backed store flushing to a temp dir.
// thresholdBytes <= 0 leaves flushing manual.
func newDurableStore(b *testing.B, thresholdBytes int64) *store.Store {
	b.Helper()
	dir := b.TempDir()
	tablesDir := filepath.Join(dir, "tables")
	if err := os.MkdirAll(tablesDir, 0o755); err != nil {
		b.Fatalf("mkdir: %v", err)
	}
	w, err := wal.Open(filepath.Join(dir, "wal"))
	if err != nil {
		b.Fatalf("wal open: %v", err)
	}
	b.Cleanup(func() { _ = w.Close() })
	s := store.New()
	s.SetWAL(w)
	s.SetFlushPolicy(tablesDir, thresholdBytes)
	b.Cleanup(func() { _ = s.CloseTables() })
	return s
}

func benchClock(n uint64) store.VectorClockVersion {
	return store.VectorClockVersion{Clocks: map[string]uint64{"bench": n}}
}

// BenchmarkStorePutDurableSequential: one writer, so every Put pays a full
// fsync with no batching opportunity - the group-commit "before" shape.
func BenchmarkStorePutDurableSequential(b *testing.B) {
	s := newDurableStore(b, 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("seq-%08d", i)
		if err := s.Put(key, "value-payload-of-plausible-size", benchClock(uint64(i+1))); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
}

// BenchmarkStorePutDurableParallel: concurrent writers share fsyncs via group
// commit (SyncUpTo batches all appends up to a sequence in one fsync) - the
// group-commit "after" shape. Compare per-op time against Sequential.
func BenchmarkStorePutDurableParallel(b *testing.B) {
	s := newDurableStore(b, 0)
	var n atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := n.Add(1)
			key := fmt.Sprintf("par-%08d", i)
			if err := s.Put(key, "value-payload-of-plausible-size", benchClock(i)); err != nil {
				b.Fatalf("Put: %v", err)
			}
		}
	})
}

// populateAndFlush seeds n keys and flushes them into one SSTable.
func populateAndFlush(b *testing.B, s *store.Store, prefix string, n int) {
	b.Helper()
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%s-%08d", prefix, i)
		if err := s.Put(key, "value-payload-of-plausible-size", benchClock(uint64(i+1))); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
	if err := s.Flush(); err != nil {
		b.Fatalf("Flush: %v", err)
	}
}

// BenchmarkStoreGetFromTable: point reads served from a single SSTable
// (bloom filter -> index binary search -> one block read).
func BenchmarkStoreGetFromTable(b *testing.B) {
	s := newDurableStore(b, 0)
	const keys = 10_000
	populateAndFlush(b, s, "tbl", keys)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("tbl-%08d", i%keys)
		if _, ok, err := s.Get(key); err != nil || !ok {
			b.Fatalf("Get(%s): ok=%v err=%v", key, ok, err)
		}
	}
}

// BenchmarkStoreGetFromTableCached: the same reads with a 16 MiB shared block
// cache, so after the first pass every block is served from memory — no disk
// read, CRC check, or decompression. Compare against GetFromTable for the
// cache's effect on the steady-state hot read path.
func BenchmarkStoreGetFromTableCached(b *testing.B) {
	s := newDurableStore(b, 0)
	s.SetBlockCache(sstable.NewCache(16 << 20))
	const keys = 10_000
	populateAndFlush(b, s, "tbl", keys)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("tbl-%08d", i%keys)
		if _, ok, err := s.Get(key); err != nil || !ok {
			b.Fatalf("Get(%s): ok=%v err=%v", key, ok, err)
		}
	}
}

// BenchmarkStoreGetFromTables4: the same reads with the keyspace spread over
// four tables, so misses on newer generations pay bloom probes before the hit.
func BenchmarkStoreGetFromTables4(b *testing.B) {
	s := newDurableStore(b, 0)
	const keys, generations = 10_000, 4
	per := keys / generations
	for g := 0; g < generations; g++ {
		for i := g * per; i < (g+1)*per; i++ {
			key := fmt.Sprintf("tbl-%08d", i)
			if err := s.Put(key, "value-payload-of-plausible-size", benchClock(uint64(i+1))); err != nil {
				b.Fatalf("Put: %v", err)
			}
		}
		if err := s.Flush(); err != nil {
			b.Fatalf("Flush: %v", err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("tbl-%08d", i%keys)
		if _, ok, err := s.Get(key); err != nil || !ok {
			b.Fatalf("Get(%s): ok=%v err=%v", key, ok, err)
		}
	}
}

// BenchmarkStoreScan100: a 100-key prefix page out of a 10k-key table plus a
// warm memtable overlay - the node-local cost of one scan page.
func BenchmarkStoreScan100(b *testing.B) {
	s := newDurableStore(b, 0)
	populateAndFlush(b, s, "scn", 10_000)
	// A few memtable-resident overwrites so the overlay path has work to do.
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("scn-%08d", i*20)
		if err := s.Put(key, "overwritten", benchClock(uint64(100_000+i))); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		items, err := s.Scan("scn-", "", 100)
		if err != nil || len(items) != 100 {
			b.Fatalf("Scan: len=%d err=%v", len(items), err)
		}
	}
}

// BenchmarkStoreScanMemtablePrefix: a narrow 100-key prefix page served
// entirely from a large in-memory memtable holding 10k keys across 100
// prefixes. With the ordered skiplist the overlay seeks to the range start and
// stops at the prefix end, touching ~100 keys instead of sweeping all 10k -
// the memtable-overlay cost the hash-map memtable paid on every scan.
func BenchmarkStoreScanMemtablePrefix(b *testing.B) {
	s := store.New() // memory-only: every key stays in the active memtable
	seq := uint64(1)
	for p := 0; p < 100; p++ {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("p%03d-%05d", p, i)
			if err := s.Put(key, "value-payload", benchClock(seq)); err != nil {
				b.Fatalf("Put: %v", err)
			}
			seq++
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		items, err := s.Scan("p042-", "", 100)
		if err != nil || len(items) != 100 {
			b.Fatalf("Scan: len=%d err=%v", len(items), err)
		}
	}
}

// BenchmarkStoreTombstoneGC: one GC pass over a store holding 10k aged
// tombstones - the pause a GC tick can introduce.
func BenchmarkStoreTombstoneGC(b *testing.B) {
	const tombs = 10_000
	aged := time.Now().Add(-48 * time.Hour)
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s := store.New()
		for j := 0; j < tombs; j++ {
			key := fmt.Sprintf("gc-%08d", j)
			if err := s.Delete(key, benchClock(uint64(j+1))); err != nil {
				b.Fatalf("Delete: %v", err)
			}
			s.SetTombstoneAge(key, aged)
		}
		b.StartTimer()
		purged, err := s.GCTombstones(24 * time.Hour)
		if err != nil || len(purged) != tombs {
			b.Fatalf("GCTombstones: purged=%d err=%v", len(purged), err)
		}
	}
}
