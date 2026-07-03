package antientropy

import (
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/store"
)

// TestAntiEntropyRepairsClockOnlyDivergence reproduces the fault-harness
// quorum-loss finding: the primary and a replica hold the SAME value but the
// replica's clock is causally older (its copy arrived via a hint for an
// earlier write attempt). Value-only Merkle hashes made the roots match, so
// sync skipped and the replicas never converged; with clocks folded into the
// entry hash the divergence is visible and one sync pushes the newer clock.
func TestAntiEntropyRepairsClockOnlyDivergence(t *testing.T) {
	r1, s1, _ := newSyncNode(t, "node1")
	r2, s2, srv2 := newSyncNode(t, "node2")

	addr2 := aeServerAddr(srv2)
	r1.AddNode("node1", "127.0.0.1:0")
	r1.AddNode("node2", addr2)
	r2.AddNode("node1", "127.0.0.1:0")
	r2.AddNode("node2", addr2)

	key := firstPrimaryKey(r1, "node1")

	// Same value on both sides; the primary's clock dominates the replica's.
	s1.Put(key, "same-value", store.VectorClockVersion{Clocks: map[string]uint64{"node1": 17}})
	s2.Put(key, "same-value", store.VectorClockVersion{Clocks: map[string]uint64{"node1": 16}})

	mgr := New(r1, s1, "node1", 2, time.Second)
	syncAll(t, mgr)

	entry, ok, _ := s2.Get(key)
	if !ok {
		t.Fatal("key missing on replica after sync")
	}
	if len(entry.Siblings) != 1 {
		t.Fatalf("expected 1 sibling after repair, got %d: %+v", len(entry.Siblings), entry.Siblings)
	}
	if got := entry.Siblings[0].Version.Clocks["node1"]; got != 17 {
		t.Errorf("replica clock not repaired: node1=%d, want 17", got)
	}
}
