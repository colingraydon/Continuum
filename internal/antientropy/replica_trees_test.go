package antientropy

import (
	"fmt"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/merkle"
	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/store"
)

// newReplicaManager builds a 3-node ring with n keys already in the store and a
// manager for "self" at the given replication factor. New scans the store into
// the trees, mirroring the startup path.
func newReplicaManager(t *testing.T, vnodes, n, factor int) (*Manager, *ring.Ring, *store.Store) {
	t.Helper()
	r := ring.NewRing(vnodes)
	r.AddNode("self", "127.0.0.1:1")
	r.AddNode("node2", "127.0.0.1:2")
	r.AddNode("node3", "127.0.0.1:3")
	s := store.New()
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("rep-k%03d", i)
		if err := s.Put(key, fmt.Sprintf("v%03d", i), store.VectorClockVersion{Clocks: map[string]uint64{"w": 1}}); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	return New(r, s, "self", factor, time.Second), r, s
}

// TestReplicaTreesCoverFullReplicaSet proves the manager keeps a tree for every
// vnode it replicates — a strict superset of the vnodes it is primary for.
func TestReplicaTreesCoverFullReplicaSet(t *testing.T) {
	m, r, _ := newReplicaManager(t, 16, 40, 2)

	replicaEnds := endsOfRanges(r.GetReplicaVnodeRanges("self", 2))
	m.mu.RLock()
	got := make(map[uint32]struct{}, len(m.trees))
	for end := range m.trees {
		got[end] = struct{}{}
	}
	m.mu.RUnlock()

	if len(got) != len(replicaEnds) {
		t.Fatalf("manager holds %d trees, want %d (one per replicated vnode)", len(got), len(replicaEnds))
	}
	for end := range replicaEnds {
		if _, ok := got[end]; !ok {
			t.Errorf("no tree for replicated vnode %d", end)
		}
	}

	primaryEnds := endsOfRanges(r.GetPrimaryVnodeRanges("self"))
	if len(replicaEnds) <= len(primaryEnds) {
		t.Fatalf("test setup: replica set (%d) is not a strict superset of primary set (%d)", len(replicaEnds), len(primaryEnds))
	}
}

// TestSyncStateMatchesScan is the core correctness check: for every vnode the
// node replicates, the tree-served root and bucket hashes are identical to what
// the on-the-fly store scan would produce.
func TestSyncStateMatchesScan(t *testing.T) {
	m, r, s := newReplicaManager(t, 16, 60, 2)

	replicaRanges := r.GetReplicaVnodeRanges("self", 2)
	if len(replicaRanges) == 0 {
		t.Fatal("expected self to replicate some vnodes")
	}
	for _, vr := range replicaRanges {
		wantRoot, wantBuckets := scanSyncState(t, s, vr)
		gotRoot, gotBuckets, ok := m.SyncState(vr.End)
		if !ok {
			t.Errorf("vnode %d: no tree despite being replicated", vr.End)
			continue
		}
		if gotRoot != wantRoot {
			t.Errorf("vnode %d: tree root %d != scan root %d", vr.End, gotRoot, wantRoot)
		}
		if !slices.Equal(gotBuckets, wantBuckets) {
			t.Errorf("vnode %d: tree buckets %v != scan buckets %v", vr.End, gotBuckets, wantBuckets)
		}
	}
}

// TestBucketKeysMatchScan checks the pull-side discovery path: tree-served
// bucket keys match the scan for every replicated vnode and bucket.
func TestBucketKeysMatchScan(t *testing.T) {
	m, r, s := newReplicaManager(t, 16, 60, 2)

	for _, vr := range r.GetReplicaVnodeRanges("self", 2) {
		for b := 0; b < merkle.BucketCount; b++ {
			want := scanBucketKeys(t, s, vr, b)
			got, ok := m.BucketKeys(vr.End, b)
			if !ok {
				t.Fatalf("vnode %d bucket %d: no tree", vr.End, b)
			}
			if !slices.Equal(got, want) {
				t.Errorf("vnode %d bucket %d: tree keys %v != scan keys %v", vr.End, b, got, want)
			}
		}
	}
}

// TestSyncAccessorsUnknownVnode: a vnode the node does not replicate has no
// tree, so both accessors report ok=false (the handler then falls back to a
// scan / 404).
func TestSyncAccessorsUnknownVnode(t *testing.T) {
	m, r, _ := newReplicaManager(t, 16, 20, 1) // factor 1: self replicates only its own vnodes

	replicaEnds := endsOfRanges(r.GetReplicaVnodeRanges("self", 1))
	var foreign uint32
	found := false
	for _, id := range []string{"node2", "node3"} {
		for end := range endsOfRanges(r.GetReplicaVnodeRanges(id, 1)) {
			if _, ok := replicaEnds[end]; !ok {
				foreign, found = end, true
				break
			}
		}
		if found {
			break
		}
	}
	if !found {
		t.Fatal("test setup: could not find a vnode self does not replicate")
	}

	if _, _, ok := m.SyncState(foreign); ok {
		t.Errorf("SyncState returned ok for non-replicated vnode %d", foreign)
	}
	if _, ok := m.BucketKeys(foreign, 0); ok {
		t.Errorf("BucketKeys returned ok for non-replicated vnode %d", foreign)
	}
}

func endsOfRanges(ranges []ring.VnodeRange) map[uint32]struct{} {
	ends := make(map[uint32]struct{}, len(ranges))
	for _, vr := range ranges {
		ends[vr.End] = struct{}{}
	}
	return ends
}

// scanSyncState reproduces the handler's on-the-fly computation of the root and
// bucket hashes for a vnode range, straight from the store.
func scanSyncState(t *testing.T, s *store.Store, vr ring.VnodeRange) (uint32, []uint32) {
	t.Helper()
	hashes, err := s.KeyHashes()
	if err != nil {
		t.Fatalf("KeyHashes: %v", err)
	}
	bmaps := make([]map[string]uint32, merkle.BucketCount)
	for i := range bmaps {
		bmaps[i] = make(map[string]uint32)
	}
	for k, h := range hashes {
		if vr.Contains(merkle.HashKey(k)) {
			bmaps[merkle.BucketIndex(k)][k] = h
		}
	}
	buckets := make([]uint32, merkle.BucketCount)
	for i, e := range bmaps {
		buckets[i] = merkle.ComputeBucketHash(e)
	}
	return merkle.ComputeRootHash(buckets), buckets
}

func scanBucketKeys(t *testing.T, s *store.Store, vr ring.VnodeRange, bucket int) []string {
	t.Helper()
	hashes, err := s.KeyHashes()
	if err != nil {
		t.Fatalf("KeyHashes: %v", err)
	}
	keys := []string{}
	for k := range hashes {
		if vr.Contains(merkle.HashKey(k)) && merkle.BucketIndex(k) == bucket {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}
