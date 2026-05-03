package store

import (
	"testing"
	"time"
)

func clock(counters map[string]uint64) VectorClockVersion {
	return VectorClockVersion{Clocks: counters}
}

func TestPutAndGet(t *testing.T) {
	s := New()
	v := NewClock().Increment("node1")
	s.Put("k", "v", v)

	e, ok := s.Get("k")
	if !ok {
		t.Fatal("expected entry to exist")
	}
	if len(e.Siblings) != 1 {
		t.Fatalf("expected 1 sibling, got %d", len(e.Siblings))
	}
	if e.Siblings[0].Value != "v" {
		t.Errorf("expected 'v', got %q", e.Siblings[0].Value)
	}
	if e.Siblings[0].Hash == 0 {
		t.Error("expected non-zero hash")
	}
}

func TestGetMissing(t *testing.T) {
	s := New()
	_, ok := s.Get("missing")
	if ok {
		t.Error("expected miss for unknown key")
	}
}

func TestNewerClockWins(t *testing.T) {
	s := New()
	s.Put("k", "old", clock(map[string]uint64{"node1": 1}))
	s.Put("k", "new", clock(map[string]uint64{"node1": 2}))

	e, _ := s.Get("k")
	if len(e.Siblings) != 1 {
		t.Fatalf("expected 1 sibling, got %d", len(e.Siblings))
	}
	if e.Siblings[0].Value != "new" {
		t.Errorf("expected 'new', got %q", e.Siblings[0].Value)
	}
}

func TestOlderClockDropped(t *testing.T) {
	s := New()
	s.Put("k", "new", clock(map[string]uint64{"node1": 2}))
	s.Put("k", "old", clock(map[string]uint64{"node1": 1}))

	e, _ := s.Get("k")
	if len(e.Siblings) != 1 {
		t.Fatalf("expected 1 sibling, got %d", len(e.Siblings))
	}
	if e.Siblings[0].Value != "new" {
		t.Errorf("expected 'new' to win, got %q", e.Siblings[0].Value)
	}
}

func TestConcurrentClocksAreSiblings(t *testing.T) {
	s := New()
	// node1 and node2 each wrote independently - neither happens-before the other.
	s.Put("k", "first", clock(map[string]uint64{"node1": 1}))
	s.Put("k", "second", clock(map[string]uint64{"node2": 1}))

	e, _ := s.Get("k")
	if len(e.Siblings) != 2 {
		t.Fatalf("expected 2 siblings for concurrent writes, got %d", len(e.Siblings))
	}
	values := map[string]bool{e.Siblings[0].Value: true, e.Siblings[1].Value: true}
	if !values["first"] || !values["second"] {
		t.Errorf("expected siblings to contain both 'first' and 'second', got %v", values)
	}
}

func TestDominatingWriteResolvesSiblings(t *testing.T) {
	s := New()
	// Create siblings via concurrent writes.
	s.Put("k", "first", clock(map[string]uint64{"node1": 1}))
	s.Put("k", "second", clock(map[string]uint64{"node2": 1}))
	// A write whose clock dominates both siblings resolves the conflict.
	s.Put("k", "resolved", clock(map[string]uint64{"node1": 1, "node2": 1}))

	e, _ := s.Get("k")
	if len(e.Siblings) != 1 {
		t.Fatalf("expected 1 sibling after resolution, got %d", len(e.Siblings))
	}
	if e.Siblings[0].Value != "resolved" {
		t.Errorf("expected 'resolved', got %q", e.Siblings[0].Value)
	}
}

func TestEqualClocksAreIdempotent(t *testing.T) {
	s := New()
	s.Put("k", "first", clock(map[string]uint64{"node1": 1}))
	s.Put("k", "second", clock(map[string]uint64{"node1": 1}))

	e, _ := s.Get("k")
	if len(e.Siblings) != 1 {
		t.Fatalf("expected 1 sibling for equal clocks, got %d", len(e.Siblings))
	}
	if e.Siblings[0].Value != "first" {
		t.Errorf("expected 'first' to be unchanged, got %q", e.Siblings[0].Value)
	}
}

func TestHashDeterministic(t *testing.T) {
	s := New()
	s.Put("k", "v", NewClock().Increment("node1"))
	e1, _ := s.Get("k")

	s2 := New()
	s2.Put("k", "v", NewClock().Increment("node1").Increment("node1"))
	e2, _ := s2.Get("k")

	if e1.Siblings[0].Hash != e2.Siblings[0].Hash {
		t.Errorf("hash should depend only on value, got %d vs %d", e1.Siblings[0].Hash, e2.Siblings[0].Hash)
	}
}

func TestHashDiffersForDifferentValues(t *testing.T) {
	s := New()
	v := NewClock().Increment("node1")
	s.Put("a", "foo", v)
	s.Put("b", "bar", v)

	ea, _ := s.Get("a")
	eb, _ := s.Get("b")
	if ea.Siblings[0].Hash == eb.Siblings[0].Hash {
		t.Error("expected different hashes for different values")
	}
}

func TestHappensBefore(t *testing.T) {
	cases := []struct {
		name     string
		a, b     VectorClockVersion
		aBeforeB bool
		bBeforeA bool
	}{
		{
			name:     "strictly before",
			a:        clock(map[string]uint64{"n1": 1}),
			b:        clock(map[string]uint64{"n1": 2}),
			aBeforeB: true,
			bBeforeA: false,
		},
		{
			name:     "equal",
			a:        clock(map[string]uint64{"n1": 1}),
			b:        clock(map[string]uint64{"n1": 1}),
			aBeforeB: false,
			bBeforeA: false,
		},
		{
			name:     "concurrent",
			a:        clock(map[string]uint64{"n1": 1}),
			b:        clock(map[string]uint64{"n2": 1}),
			aBeforeB: false,
			bBeforeA: false,
		},
		{
			name:     "empty happens-before non-empty",
			a:        NewClock(),
			b:        clock(map[string]uint64{"n1": 1}),
			aBeforeB: true,
			bBeforeA: false,
		},
		{
			name:     "multi-node before",
			a:        clock(map[string]uint64{"n1": 1, "n2": 1}),
			b:        clock(map[string]uint64{"n1": 2, "n2": 2}),
			aBeforeB: true,
			bBeforeA: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.HappensBefore(tc.b); got != tc.aBeforeB {
				t.Errorf("a.HappensBefore(b): expected %v, got %v", tc.aBeforeB, got)
			}
			if got := tc.b.HappensBefore(tc.a); got != tc.bBeforeA {
				t.Errorf("b.HappensBefore(a): expected %v, got %v", tc.bBeforeA, got)
			}
		})
	}
}

func TestEqual(t *testing.T) {
	cases := []struct {
		name string
		a, b VectorClockVersion
		want bool
	}{
		{"identical", clock(map[string]uint64{"n1": 1}), clock(map[string]uint64{"n1": 1}), true},
		{"different value", clock(map[string]uint64{"n1": 1}), clock(map[string]uint64{"n1": 2}), false},
		{"different keys", clock(map[string]uint64{"n1": 1}), clock(map[string]uint64{"n2": 1}), false},
		{"both empty", NewClock(), NewClock(), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.Equal(tc.b); got != tc.want {
				t.Errorf("Equal: expected %v, got %v", tc.want, got)
			}
		})
	}
}

func TestIncrement(t *testing.T) {
	v := NewClock().Increment("node1").Increment("node1").Increment("node2")
	if v.Clocks["node1"] != 2 {
		t.Errorf("expected node1=2, got %d", v.Clocks["node1"])
	}
	if v.Clocks["node2"] != 1 {
		t.Errorf("expected node2=1, got %d", v.Clocks["node2"])
	}
}

func TestIncrementDoesNotMutateReceiver(t *testing.T) {
	original := NewClock().Increment("node1")
	_ = original.Increment("node1")
	if original.Clocks["node1"] != 1 {
		t.Errorf("Increment mutated receiver: node1=%d", original.Clocks["node1"])
	}
}

func TestDeleteWritesTombstone(t *testing.T) {
	s := New()
	s.Put("k", "v", clock(map[string]uint64{"node1": 1}))
	s.Delete("k", clock(map[string]uint64{"node1": 2}))

	e, ok := s.Get("k")
	if !ok {
		t.Fatal("expected entry to exist after delete")
	}
	if len(e.Siblings) != 1 {
		t.Fatalf("expected 1 sibling, got %d", len(e.Siblings))
	}
	if !e.Siblings[0].Deleted {
		t.Error("expected tombstone sibling")
	}
}

func TestDeleteOnMissingKeyCreatesTombstone(t *testing.T) {
	s := New()
	s.Delete("k", clock(map[string]uint64{"node1": 1}))

	e, ok := s.Get("k")
	if !ok {
		t.Fatal("expected entry to exist")
	}
	if !e.Siblings[0].Deleted {
		t.Error("expected tombstone for delete on missing key")
	}
}

func TestOlderDeleteDropped(t *testing.T) {
	s := New()
	s.Put("k", "v", clock(map[string]uint64{"node1": 2}))
	s.Delete("k", clock(map[string]uint64{"node1": 1}))

	e, _ := s.Get("k")
	if len(e.Siblings) != 1 {
		t.Fatalf("expected 1 sibling, got %d", len(e.Siblings))
	}
	if e.Siblings[0].Deleted {
		t.Error("stale delete should be dropped; value should survive")
	}
	if e.Siblings[0].Value != "v" {
		t.Errorf("expected 'v', got %q", e.Siblings[0].Value)
	}
}

func TestConcurrentWriteAndDeleteAreSiblings(t *testing.T) {
	s := New()
	s.Put("k", "v", clock(map[string]uint64{"node1": 1}))
	s.Delete("k", clock(map[string]uint64{"node2": 1}))

	e, _ := s.Get("k")
	if len(e.Siblings) != 2 {
		t.Fatalf("expected 2 siblings for concurrent write/delete, got %d", len(e.Siblings))
	}
	var hasValue, hasTombstone bool
	for _, sib := range e.Siblings {
		if sib.Deleted {
			hasTombstone = true
		} else {
			hasValue = true
		}
	}
	if !hasValue || !hasTombstone {
		t.Error("expected one value sibling and one tombstone sibling")
	}
}

func TestDominatingDeleteResolvesSiblings(t *testing.T) {
	s := New()
	s.Put("k", "first", clock(map[string]uint64{"node1": 1}))
	s.Put("k", "second", clock(map[string]uint64{"node2": 1}))
	s.Delete("k", clock(map[string]uint64{"node1": 1, "node2": 1}))

	e, _ := s.Get("k")
	if len(e.Siblings) != 1 {
		t.Fatalf("expected 1 sibling after dominating delete, got %d", len(e.Siblings))
	}
	if !e.Siblings[0].Deleted {
		t.Error("expected tombstone to win over dominated siblings")
	}
}

// --- onUpdate callback tests ---

func TestOnUpdateFiredOnPut(t *testing.T) {
	s := New()
	var gotKey string
	var gotHash uint32
	var calls int
	s.SetOnUpdate(func(key string, hash uint32) {
		gotKey = key
		gotHash = hash
		calls++
	})

	v := clock(map[string]uint64{"node1": 1})
	s.Put("k", "v", v)

	if calls != 1 {
		t.Fatalf("expected 1 callback call, got %d", calls)
	}
	if gotKey != "k" {
		t.Errorf("expected key 'k', got %q", gotKey)
	}
	e, _ := s.Get("k")
	if gotHash != entryHash(e) {
		t.Errorf("callback hash %d does not match entryHash %d", gotHash, entryHash(e))
	}
}

func TestOnUpdateFiredOnDelete(t *testing.T) {
	s := New()
	var gotHash uint32
	s.SetOnUpdate(func(_ string, hash uint32) { gotHash = hash })

	s.Delete("k", clock(map[string]uint64{"node1": 1}))

	if gotHash != tombstoneSentinel {
		t.Errorf("expected tombstone sentinel %#x, got %#x", tombstoneSentinel, gotHash)
	}
}

func TestOnUpdateNotFiredOnDominatedPut(t *testing.T) {
	s := New()
	var calls int
	s.SetOnUpdate(func(_ string, _ uint32) { calls++ })

	s.Put("k", "new", clock(map[string]uint64{"node1": 2}))
	calls = 0 // reset after initial write

	s.Put("k", "old", clock(map[string]uint64{"node1": 1}))

	if calls != 0 {
		t.Errorf("callback should not fire for a dominated Put, got %d calls", calls)
	}
}

func TestOnUpdateNotFiredOnEqualClockPut(t *testing.T) {
	s := New()
	var calls int
	s.SetOnUpdate(func(_ string, _ uint32) { calls++ })

	v := clock(map[string]uint64{"node1": 1})
	s.Put("k", "v", v)
	calls = 0

	s.Put("k", "v", v)

	if calls != 0 {
		t.Errorf("callback should not fire for an idempotent Put, got %d calls", calls)
	}
}

func TestOnUpdateNotFiredOnDominatedDelete(t *testing.T) {
	s := New()
	var calls int
	s.SetOnUpdate(func(_ string, _ uint32) { calls++ })

	s.Put("k", "v", clock(map[string]uint64{"node1": 2}))
	calls = 0

	s.Delete("k", clock(map[string]uint64{"node1": 1}))

	if calls != 0 {
		t.Errorf("callback should not fire for a dominated Delete, got %d calls", calls)
	}
}

func TestOnUpdateHashForSiblings(t *testing.T) {
	s := New()
	var lastHash uint32
	s.SetOnUpdate(func(_ string, hash uint32) { lastHash = hash })

	s.Put("k", "first", clock(map[string]uint64{"node1": 1}))
	s.Put("k", "second", clock(map[string]uint64{"node2": 1}))

	e, _ := s.Get("k")
	if len(e.Siblings) != 2 {
		t.Fatalf("expected 2 siblings, got %d", len(e.Siblings))
	}
	if lastHash != entryHash(e) {
		t.Errorf("callback hash %#x does not match entryHash %#x", lastHash, entryHash(e))
	}
}

func TestOnUpdateNoPanicWithoutCallback(t *testing.T) {
	s := New()
	// No SetOnUpdate call — Put and Delete must not panic.
	s.Put("k", "v", clock(map[string]uint64{"node1": 1}))
	s.Delete("k", clock(map[string]uint64{"node1": 2}))
}

// --- GCTombstones tests ---

func TestGCTombstonesRemovesEligibleTombstone(t *testing.T) {
	s := New()
	s.Delete("k", clock(map[string]uint64{"node1": 1}))

	// Negative TTL makes every tombstone immediately eligible.
	purged := s.GCTombstones(-1)

	if len(purged) != 1 || purged[0] != "k" {
		t.Fatalf("expected [k] purged, got %v", purged)
	}
	if _, ok := s.Get("k"); ok {
		t.Error("key should be gone from store after GC")
	}
}

func TestGCTombstonesPreservesLiveEntry(t *testing.T) {
	s := New()
	s.Put("k", "v", clock(map[string]uint64{"node1": 1}))

	purged := s.GCTombstones(-1)

	if len(purged) != 0 {
		t.Errorf("live entry must not be GC'd, got %v", purged)
	}
}

func TestGCTombstonesPreservesFreshTombstone(t *testing.T) {
	s := New()
	s.Delete("k", clock(map[string]uint64{"node1": 1}))

	// TTL of 1 hour: tombstone is only milliseconds old, well within the window.
	purged := s.GCTombstones(time.Hour)

	if len(purged) != 0 {
		t.Errorf("fresh tombstone must not be GC'd, got %v", purged)
	}
	if _, ok := s.Get("k"); !ok {
		t.Error("tombstone should still be present")
	}
}

func TestGCTombstonesPreservesContestedEntry(t *testing.T) {
	s := New()
	// Concurrent write and delete produce two siblings — GC must not touch it.
	s.Put("k", "v", clock(map[string]uint64{"node1": 1}))
	s.Delete("k", clock(map[string]uint64{"node2": 1}))

	e, _ := s.Get("k")
	if len(e.Siblings) != 2 {
		t.Fatalf("expected 2 siblings (contested), got %d", len(e.Siblings))
	}

	purged := s.GCTombstones(-1)

	if len(purged) != 0 {
		t.Errorf("contested entry must not be GC'd, got %v", purged)
	}
}

func TestGCTombstonesEmptyStore(t *testing.T) {
	s := New()
	purged := s.GCTombstones(-1)
	if len(purged) != 0 {
		t.Errorf("expected no purges on empty store, got %v", purged)
	}
}

func TestGCTombstonesMultipleKeys(t *testing.T) {
	s := New()
	s.Delete("a", clock(map[string]uint64{"node1": 1}))
	s.Delete("b", clock(map[string]uint64{"node1": 1}))
	s.Put("c", "v", clock(map[string]uint64{"node1": 1}))

	purged := s.GCTombstones(-1)

	if len(purged) != 2 {
		t.Fatalf("expected 2 purged, got %d: %v", len(purged), purged)
	}
	purgedSet := map[string]bool{purged[0]: true, purged[1]: true}
	if !purgedSet["a"] || !purgedSet["b"] {
		t.Errorf("expected a and b purged, got %v", purged)
	}
	if _, ok := s.Get("c"); !ok {
		t.Error("live key c should remain after GC")
	}
}

func TestTombstoneAgeResetAfterWriteDeleteCycle(t *testing.T) {
	s := New()

	// First delete: tombstone age set to T0.
	s.Delete("k", clock(map[string]uint64{"node1": 1}))

	// Live write replaces the tombstone: age must be cleared.
	s.Put("k", "v", clock(map[string]uint64{"node1": 2}))

	// Second delete at a newer clock: should get a fresh age, not T0.
	s.Delete("k", clock(map[string]uint64{"node1": 3}))

	// GC with a positive TTL: tombstone is milliseconds old, must NOT be purged.
	purged := s.GCTombstones(time.Hour)
	if len(purged) != 0 {
		t.Error("second tombstone should not be prematurely GC'd due to stale first timestamp")
	}
}

func TestTombstoneAgeNotResetByEqualClockReapplication(t *testing.T) {
	s := New()
	s.Delete("k", clock(map[string]uint64{"node1": 1}))

	// Re-applying the same tombstone clock is idempotent and must not count as
	// a new deletion event (applySibling returns false, timestamp unchanged).
	s.Delete("k", clock(map[string]uint64{"node1": 1}))

	// Both deletes were no-ops after the first; the age should be old enough
	// that a negative-TTL GC removes the tombstone (not blocked by a reset age).
	purged := s.GCTombstones(-1)
	if len(purged) != 1 {
		t.Errorf("expected tombstone GC'd, got %v", purged)
	}
}

func TestTombstoneAgeUpdatedByNewerDeletion(t *testing.T) {
	s := New()
	// First deletion creates tombstone {node1:1}.
	s.Delete("k", clock(map[string]uint64{"node1": 1}))

	// A second deletion at a newer clock arrives (e.g. from a remote node).
	// This replaces the first tombstone and must reset the age.
	s.Delete("k", clock(map[string]uint64{"node1": 2}))

	// Fresh tombstone must not be GC'd within a positive TTL.
	purged := s.GCTombstones(time.Hour)
	if len(purged) != 0 {
		t.Error("tombstone from newer deletion should not be prematurely GC'd")
	}
}
