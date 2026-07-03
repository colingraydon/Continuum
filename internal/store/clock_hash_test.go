package store

import "testing"

// TestEntryHashDistinguishesClocks pins the property anti-entropy depends on:
// replicas holding the SAME value under DIFFERENT vector clocks must produce
// different entry hashes, or Merkle comparison can never see the divergence
// and the replicas stay permanently out of sync (found by the fault harness's
// quorum-loss scenario).
func TestEntryHashDistinguishesClocks(t *testing.T) {
	at := func(clocks map[string]uint64) Entry {
		return Entry{Siblings: []Sibling{{Value: "same-value", Hash: 42, Version: VectorClockVersion{Clocks: clocks}}}}
	}

	a := entryHash(at(map[string]uint64{"node1": 16}))
	b := entryHash(at(map[string]uint64{"node1": 17}))
	if a == b {
		t.Error("same value at different clocks produced identical entry hashes")
	}

	c := entryHash(at(map[string]uint64{"node1": 16}))
	if a != c {
		t.Error("identical entries produced different hashes")
	}
}

func TestEntryHashDistinguishesTombstoneClocks(t *testing.T) {
	at := func(n uint64) Entry {
		return Entry{Siblings: []Sibling{{Deleted: true, Version: VectorClockVersion{Clocks: map[string]uint64{"node1": n}}}}}
	}
	if entryHash(at(1)) == entryHash(at(2)) {
		t.Error("tombstones at different clocks produced identical entry hashes")
	}
}

func TestClockHashCanonical(t *testing.T) {
	if got := clockHash(VectorClockVersion{}); got != 0 {
		t.Errorf("empty clock should hash to 0, got %#x", got)
	}

	// Same logical clock built in different insertion orders must hash equal.
	x := map[string]uint64{"a": 1, "b": 2, "c": 3}
	y := map[string]uint64{"c": 3, "a": 1, "b": 2}
	if clockHash(VectorClockVersion{Clocks: x}) != clockHash(VectorClockVersion{Clocks: y}) {
		t.Error("clock hash depends on map insertion order")
	}

	// Moving a counter between nodes must change the hash (id participates).
	p := clockHash(VectorClockVersion{Clocks: map[string]uint64{"a": 1}})
	q := clockHash(VectorClockVersion{Clocks: map[string]uint64{"b": 1}})
	if p == q {
		t.Error("different node ids with equal counters hashed identically")
	}
}
