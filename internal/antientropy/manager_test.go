package antientropy

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/colingraydon/continuum/api"
	"github.com/colingraydon/continuum/internal/gossip"
	"github.com/colingraydon/continuum/internal/merkle"
	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/store"
)

// newSyncNode creates a ring, store, and HTTP test server for the given node.
// The ring starts empty; callers register nodes explicitly so addresses are
// known before registration.
func newSyncNode(t *testing.T, nodeID string) (*ring.Ring, *store.Store, *httptest.Server) {
	t.Helper()
	r := ring.NewRing(50)
	ml := gossip.NewMemberList(nodeID, "", func(m *gossip.Member, status gossip.MemberStatus) {
		switch status {
		case gossip.MemberAlive:
			r.AddNode(m.ID, m.Address)
		case gossip.MemberDead:
			r.RemoveNode(m.ID)
		}
	})
	transport, err := gossip.NewTransport("0")
	if err != nil {
		t.Fatalf("failed to create transport: %v", err)
	}
	g := gossip.NewGossiper(nodeID, "0", ml, transport)
	s := store.New()
	srv := httptest.NewServer(api.NewServer(r, ml, g, s, nodeID, 2, 1, 1, time.Second))
	t.Cleanup(func() {
		srv.Close()
		transport.Stop()
	})
	return r, s, srv
}

func aeServerAddr(srv *httptest.Server) string {
	return strings.TrimPrefix(srv.URL, "http://")
}

// firstPrimaryKey returns the first key (from a deterministic search) for which
// nodeID is the primary owner (first clockwise node on the ring). With two
// nodes and 50 vnodes each, roughly half of all keys map to each node so this
// converges within a few iterations.
func firstPrimaryKey(r *ring.Ring, nodeID string) string {
	for i := 0; ; i++ {
		key := fmt.Sprintf("ae-key-%d", i)
		nodes := r.GetReplicationNodes(key, 1)
		if len(nodes) > 0 && nodes[0].ID == nodeID {
			return key
		}
	}
}

// syncAll syncs every primary vnode owned by mgr against its replicas. This is
// equivalent to running syncRound once per primary vnode rather than picking one
// at random. Used in tests so the vnode holding the key under test is always
// covered, regardless of which of the 50 primary vnodes it happens to fall in.
func syncAll(t *testing.T, mgr *Manager) {
	t.Helper()
	mgr.mu.RLock()
	ends := make([]uint32, 0, len(mgr.trees))
	for end := range mgr.trees {
		ends = append(ends, end)
	}
	mgr.mu.RUnlock()

	for _, end := range ends {
		mgr.mu.RLock()
		tree := mgr.trees[end]
		mgr.mu.RUnlock()

		nodes := mgr.r.GetReplicationNodesForHash(end, mgr.replicationFactor)
		for _, node := range nodes {
			if node.ID == mgr.selfID {
				continue
			}
			if err := mgr.syncWithReplica(node.Address, end, tree); err != nil {
				t.Logf("syncAll: vnode %d replica %s: %v", end, node.ID, err)
			}
		}
	}
}

// TestAntiEntropyRepairsPrimaryFromReplica verifies that when a replica holds a
// causally newer version of a key, the anti-entropy sync pulls it and applies it
// to the primary's store.
func TestAntiEntropyRepairsPrimaryFromReplica(t *testing.T) {
	r1, s1, _ := newSyncNode(t, "node1")
	r2, s2, srv2 := newSyncNode(t, "node2")

	addr2 := aeServerAddr(srv2)
	// node1's address is never called by the manager (it skips self), so a
	// placeholder is fine. Both rings need node1 registered so GetVnodeRange
	// computes the correct topology on both sides.
	r1.AddNode("node1", "127.0.0.1:0")
	r1.AddNode("node2", addr2)
	r2.AddNode("node1", "127.0.0.1:0")
	r2.AddNode("node2", addr2)

	key := firstPrimaryKey(r1, "node1")

	// Primary has an older version.
	oldClock := store.VectorClockVersion{Clocks: map[string]uint64{"node1": 1}}
	s1.Put(key, "old-value", oldClock)

	// Replica has a causally newer version.
	newClock := store.VectorClockVersion{Clocks: map[string]uint64{"node1": 1, "node2": 1}}
	s2.Put(key, "new-value", newClock)

	// Create the manager after seeding s1 so rebuild populates the trees from
	// the current store state.
	mgr := New(r1, s1, "node1", 2, time.Second)

	syncAll(t, mgr)

	entry, ok := s1.Get(key)
	if !ok {
		t.Fatal("key not found in primary store after sync")
	}
	if len(entry.Siblings) != 1 {
		t.Fatalf("expected 1 sibling after repair, got %d: %+v", len(entry.Siblings), entry.Siblings)
	}
	if entry.Siblings[0].Value != "new-value" {
		t.Errorf("expected 'new-value' after repair, got %q", entry.Siblings[0].Value)
	}
}

// TestAntiEntropySkipsWhenInSync verifies that the sync exits early without
// modifying the primary's store when the Merkle roots already match.
func TestAntiEntropySkipsWhenInSync(t *testing.T) {
	r1, s1, _ := newSyncNode(t, "node1")
	r2, s2, srv2 := newSyncNode(t, "node2")

	addr2 := aeServerAddr(srv2)
	r1.AddNode("node1", "127.0.0.1:0")
	r1.AddNode("node2", addr2)
	r2.AddNode("node1", "127.0.0.1:0")
	r2.AddNode("node2", addr2)

	key := firstPrimaryKey(r1, "node1")

	// Both nodes have the same data.
	clock := store.VectorClockVersion{Clocks: map[string]uint64{"node1": 1}}
	s1.Put(key, "same-value", clock)
	s2.Put(key, "same-value", clock)

	mgr := New(r1, s1, "node1", 2, time.Second)

	// Register a callback after the manager is built so any sync-triggered write
	// to s1 is visible.
	updated := false
	s1.SetOnUpdate(func(k string, _ uint32) {
		if k == key {
			updated = true
		}
	})

	syncAll(t, mgr)

	if updated {
		t.Error("sync modified primary store when roots already matched")
	}
}

// TestAntiEntropyPushesToReplica verifies the push direction: when the primary
// holds a key that the replica is missing, syncAll pushes it to the replica.
func TestAntiEntropyPushesToReplica(t *testing.T) {
	r1, s1, _ := newSyncNode(t, "node1")
	r2, s2, srv2 := newSyncNode(t, "node2")

	addr2 := aeServerAddr(srv2)
	r1.AddNode("node1", "127.0.0.1:0")
	r1.AddNode("node2", addr2)
	r2.AddNode("node1", "127.0.0.1:0")
	r2.AddNode("node2", addr2)

	key := firstPrimaryKey(r1, "node1")

	// Primary has the key; replica has never seen it.
	clock := store.VectorClockVersion{Clocks: map[string]uint64{"node1": 1}}
	s1.Put(key, "primary-only", clock)

	mgr := New(r1, s1, "node1", 2, time.Second)
	syncAll(t, mgr)

	entry, ok := s2.Get(key)
	if !ok {
		t.Fatal("replica did not receive pushed entry")
	}
	if len(entry.Siblings) != 1 || entry.Siblings[0].Value != "primary-only" {
		t.Errorf("replica has wrong value after push: %+v", entry.Siblings)
	}
}

// TestAntiEntropyGCPurgesTombstone verifies that GCTombstones removes an
// uncontested tombstone and that removeFromTrees evicts it from the Merkle tree.
func TestAntiEntropyGCPurgesTombstone(t *testing.T) {
	r1, s1, _ := newSyncNode(t, "node1")
	r1.AddNode("node1", "127.0.0.1:0")

	key := firstPrimaryKey(r1, "node1")

	s1.Put(key, "value", store.VectorClockVersion{Clocks: map[string]uint64{"node1": 1}})
	s1.Delete(key, store.VectorClockVersion{Clocks: map[string]uint64{"node1": 2}})

	mgr := New(r1, s1, "node1", 1, time.Second)

	// A negative TTL makes every tombstone immediately eligible for GC.
	purged := s1.GCTombstones(-1)
	if len(purged) != 1 || purged[0] != key {
		t.Fatalf("expected [%s] purged, got %v", key, purged)
	}
	mgr.removeFromTrees(key)

	if _, ok := s1.Get(key); ok {
		t.Error("key still present in store after GC")
	}

	// Verify the key is no longer tracked in any of the manager's Merkle trees.
	keyHash := merkle.HashKey(key)
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	for end, vr := range mgr.ranges {
		if !vr.Contains(keyHash) {
			continue
		}
		for _, k := range mgr.trees[end].BucketKeys(merkle.BucketIndex(key)) {
			if k == key {
				t.Errorf("key %q still in Merkle tree after removeFromTrees", key)
			}
		}
	}
}

// TestAntiEntropyRepairsTombstone verifies that a tombstone on a replica is
// propagated to the primary by the sync loop.
func TestAntiEntropyRepairsTombstone(t *testing.T) {
	r1, s1, _ := newSyncNode(t, "node1")
	r2, s2, srv2 := newSyncNode(t, "node2")

	addr2 := aeServerAddr(srv2)
	r1.AddNode("node1", "127.0.0.1:0")
	r1.AddNode("node2", addr2)
	r2.AddNode("node1", "127.0.0.1:0")
	r2.AddNode("node2", addr2)

	key := firstPrimaryKey(r1, "node1")

	// Primary has a live value.
	liveClock := store.VectorClockVersion{Clocks: map[string]uint64{"node1": 1}}
	s1.Put(key, "live-value", liveClock)

	// Replica has a tombstone that causally dominates the live value (simulates a
	// delete that reached the replica before the primary could be updated).
	tombClock := store.VectorClockVersion{Clocks: map[string]uint64{"node1": 1, "node2": 1}}
	s2.Delete(key, tombClock)

	mgr := New(r1, s1, "node1", 2, time.Second)

	syncAll(t, mgr)

	entry, ok := s1.Get(key)
	if !ok {
		t.Fatal("key not found in primary store after tombstone sync")
	}
	if len(entry.Siblings) != 1 {
		t.Fatalf("expected 1 sibling after tombstone sync, got %d", len(entry.Siblings))
	}
	if !entry.Siblings[0].Deleted {
		t.Errorf("expected tombstone in primary after sync, got value=%q", entry.Siblings[0].Value)
	}
}

func TestUnion(t *testing.T) {
	tests := []struct {
		name string
		a, b []string
		want []string
	}{
		{"both empty", nil, nil, nil},
		{"a empty", nil, []string{"x", "y"}, []string{"x", "y"}},
		{"b empty", []string{"x", "y"}, nil, []string{"x", "y"}},
		{"no overlap", []string{"a", "b"}, []string{"c", "d"}, []string{"a", "b", "c", "d"}},
		{"full overlap", []string{"a", "b"}, []string{"a", "b"}, []string{"a", "b"}},
		{"partial overlap", []string{"a", "b"}, []string{"b", "c"}, []string{"a", "b", "c"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := union(tc.a, tc.b)
			if len(got) != len(tc.want) {
				t.Fatalf("union(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
			seen := make(map[string]bool, len(got))
			for _, k := range got {
				if seen[k] {
					t.Errorf("duplicate key %q in union result", k)
				}
				seen[k] = true
			}
			for _, k := range tc.want {
				if !seen[k] {
					t.Errorf("expected key %q missing from union result %v", k, got)
				}
			}
		})
	}
}
