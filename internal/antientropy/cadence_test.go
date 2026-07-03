package antientropy

import (
	"fmt"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/merkle"
	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/store"
)

func newCadenceManager(t *testing.T, vnodes int) (*Manager, *ring.Ring, *store.Store) {
	t.Helper()
	r := ring.NewRing(vnodes)
	r.AddNode("self", "127.0.0.1:1")
	s := store.New()
	return New(r, s, "self", 2, time.Second), r, s
}

// TestNextVnodeCyclesAllVnodes proves the round-robin guarantee that random
// selection lacked: every primary vnode is synced exactly once per
// len(order) rounds, in a stable order, so full-keyspace repair time is
// bounded instead of coupon-collector.
func TestNextVnodeCyclesAllVnodes(t *testing.T) {
	m, _, _ := newCadenceManager(t, 8)
	n := len(m.order)
	if n == 0 {
		t.Fatal("expected primary vnodes for the only ring member")
	}

	seen := make(map[uint32]int, n)
	var firstPass []uint32
	for i := 0; i < 2*n; i++ {
		end, ok := m.nextVnode()
		if !ok {
			t.Fatalf("nextVnode returned no vnode at step %d", i)
		}
		seen[end]++
		if i < n {
			firstPass = append(firstPass, end)
		} else if firstPass[i-n] != end {
			t.Fatalf("second pass diverged at step %d: %d vs %d", i, firstPass[i-n], end)
		}
	}
	if len(seen) != n {
		t.Fatalf("expected %d distinct vnodes over one full cycle, got %d", n, len(seen))
	}
	for end, count := range seen {
		if count != 2 {
			t.Errorf("vnode %d synced %d times over two cycles, want exactly 2", end, count)
		}
	}
}

func TestNextVnodeEmptyRing(t *testing.T) {
	m := New(ring.NewRing(4), store.New(), "self", 2, time.Second)
	if _, ok := m.nextVnode(); ok {
		t.Error("expected no vnode from an empty ring")
	}
}

func TestRangesEqual(t *testing.T) {
	a := ring.VnodeRange{Start: 10, End: 20}
	b := ring.VnodeRange{Start: 20, End: 30}
	cur := map[uint32]ring.VnodeRange{a.End: a, b.End: b}

	if !rangesEqual(cur, []ring.VnodeRange{a, b}) {
		t.Error("identical range sets reported unequal")
	}
	if rangesEqual(cur, []ring.VnodeRange{a}) {
		t.Error("shorter range set reported equal")
	}
	if rangesEqual(cur, []ring.VnodeRange{a, {Start: 21, End: 30}}) {
		t.Error("range with shifted bounds reported equal")
	}
	if rangesEqual(cur, []ring.VnodeRange{a, {Start: 20, End: 31}}) {
		t.Error("range with unknown end reported equal")
	}
}

// TestMaybeRebuildOnMembershipChange proves the manager adapts to ring
// changes: primary ranges computed at startup are replaced when a node
// joins, and the rebuilt trees track exactly the keys in the new ranges.
func TestMaybeRebuildOnMembershipChange(t *testing.T) {
	m, r, s := newCadenceManager(t, 8)

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("cadence-k%02d", i)
		if err := s.Put(key, "v", store.VectorClockVersion{Clocks: map[string]uint64{"w": 1}}); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		m.Update(key, 1)
	}

	// Alone in the ring, self is primary for everything.
	if got := treeKeyCount(m); got != 20 {
		t.Fatalf("expected all 20 keys tracked before membership change, got %d", got)
	}

	r.AddNode("other", "127.0.0.1:2")
	fresh := r.GetPrimaryVnodeRanges("self")
	if rangesEqual(m.ranges, fresh) {
		t.Fatal("test setup: adding a node did not change self's primary ranges")
	}

	m.maybeRebuild()

	if !rangesEqual(m.ranges, fresh) {
		t.Error("ranges not rebuilt after membership change")
	}
	if m.cursor != 0 {
		t.Errorf("cursor not reset on rebuild, got %d", m.cursor)
	}
	want := keysInRanges(fresh, 20)
	if got := treeKeyCount(m); got != want {
		t.Errorf("rebuilt trees track %d keys, want %d (keys inside the new primary ranges)", got, want)
	}

	// A second call with an unchanged ring must be a no-op comparison.
	before := m.trees
	m.maybeRebuild()
	for end, tree := range m.trees {
		if before[end] != tree {
			t.Error("maybeRebuild rebuilt trees despite unchanged ranges")
			break
		}
	}
}

func treeKeyCount(m *Manager) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	total := 0
	for _, tree := range m.trees {
		for b := 0; b < merkle.BucketCount; b++ {
			total += len(tree.BucketKeys(b))
		}
	}
	return total
}

func keysInRanges(ranges []ring.VnodeRange, n int) int {
	count := 0
	for i := 0; i < n; i++ {
		h := merkle.HashKey(fmt.Sprintf("cadence-k%02d", i))
		for _, vr := range ranges {
			if vr.Contains(h) {
				count++
				break
			}
		}
	}
	return count
}
