package benchmarks

import (
	"fmt"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/antientropy"
	"github.com/colingraydon/continuum/internal/merkle"
	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/store"
)

// BenchmarkMerkleTreeUpdate: the incremental cost every write pays to keep
// its vnode tree current (the onUpdate callback path).
func BenchmarkMerkleTreeUpdate(b *testing.B) {
	t := merkle.New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t.Update(fmt.Sprintf("mk-%08d", i%100_000), uint32(i))
	}
}

// BenchmarkMerkleBucketHash1k: recomputing one bucket's aggregate hash over
// 1k entries - the unit of work the scan fallback repeats per bucket.
func BenchmarkMerkleBucketHash1k(b *testing.B) {
	entries := make(map[string]uint32, 1000)
	for i := 0; i < 1000; i++ {
		entries[fmt.Sprintf("bh-%08d", i)] = uint32(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		merkle.ComputeBucketHash(entries)
	}
}

// newSyncManager returns a manager over a single-node ring with n keys in the
// store, mirroring the startup rebuild, plus the vnode end of a range that
// holds keys.
func newSyncManager(b *testing.B, n int) (*antientropy.Manager, *store.Store, uint32) {
	b.Helper()
	r := ring.NewRing(8)
	r.AddNode("self", "127.0.0.1:1")
	s := store.New()
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("sy-%08d", i)
		if err := s.Put(key, "value-payload-of-plausible-size", benchClock(uint64(i+1))); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
	m := antientropy.New(r, s, "self", 2, time.Second)
	ranges := r.GetPrimaryVnodeRanges("self")
	if len(ranges) == 0 {
		b.Fatal("no vnode ranges")
	}
	return m, s, ranges[0].End
}

// BenchmarkSyncStateFromTree: serving GET /sync from the incrementally
// maintained tree - O(buckets), independent of key count.
func BenchmarkSyncStateFromTree(b *testing.B) {
	m, _, vnode := newSyncManager(b, 10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, ok := m.SyncState(vnode); !ok {
			b.Fatal("no tree for vnode")
		}
	}
}

// BenchmarkSyncStateScanFallback10k: the pre-maintained-trees cost of the same
// request - a full KeyHashes pass plus per-bucket hashing over 10k keys, paid
// on every sync request before replicas kept trees. Compare per-op time
// against BenchmarkSyncStateFromTree.
func BenchmarkSyncStateScanFallback10k(b *testing.B) {
	_, s, _ := newSyncManager(b, 10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hashes, err := s.KeyHashes()
		if err != nil {
			b.Fatalf("KeyHashes: %v", err)
		}
		buckets := make([]map[string]uint32, merkle.BucketCount)
		for j := range buckets {
			buckets[j] = make(map[string]uint32)
		}
		for key, hash := range hashes {
			buckets[merkle.BucketIndex(key)][key] = hash
		}
		bucketHashes := make([]uint32, merkle.BucketCount)
		for j, entries := range buckets {
			bucketHashes[j] = merkle.ComputeBucketHash(entries)
		}
		merkle.ComputeRootHash(bucketHashes)
	}
}

// BenchmarkManagerRebuild10k: the full tree rebuild a membership change
// triggers - one store scan routed into per-vnode trees.
func BenchmarkManagerRebuild10k(b *testing.B) {
	r := ring.NewRing(8)
	r.AddNode("self", "127.0.0.1:1")
	s := store.New()
	for i := 0; i < 10_000; i++ {
		key := fmt.Sprintf("rb-%08d", i)
		if err := s.Put(key, "value-payload-of-plausible-size", benchClock(uint64(i+1))); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		antientropy.New(r, s, "self", 2, time.Second)
	}
}
