package hintstore

import (
	"sync"
	"testing"
	"time"
)

func makeHint(key string) Hint {
	return Hint{Key: key, Value: "v", Clocks: map[string]uint64{"n1": 1}, At: time.Now()}
}

func TestStore_DrainRoundtrip(t *testing.T) {
	hs := New(100, time.Hour)
	hs.Store("nodeA", makeHint("k1"))
	hs.Store("nodeA", makeHint("k2"))
	hs.Store("nodeA", makeHint("k3"))

	hints := hs.Drain("nodeA")
	if len(hints) != 3 {
		t.Fatalf("want 3 hints, got %d", len(hints))
	}
	if hints[0].Key != "k1" || hints[1].Key != "k2" || hints[2].Key != "k3" {
		t.Fatalf("unexpected hint order: %v", hints)
	}

	// Second drain should return nothing.
	if got := hs.Drain("nodeA"); got != nil {
		t.Fatalf("expected nil after drain, got %v", got)
	}
}

func TestStore_DrainUnknownNode(t *testing.T) {
	hs := New(100, time.Hour)
	if got := hs.Drain("nobody"); got != nil {
		t.Fatalf("expected nil for unknown node, got %v", got)
	}
}

func TestStore_CapEvictsOldest(t *testing.T) {
	const cap = 3
	hs := New(cap, time.Hour)

	// Store cap+2 hints; the first two should be evicted.
	for i := range cap + 2 {
		hs.Store("nodeA", Hint{Key: string(rune('a' + i)), At: time.Now()})
	}

	hints := hs.Drain("nodeA")
	if len(hints) != cap {
		t.Fatalf("want %d hints after cap eviction, got %d", cap, len(hints))
	}
	// Oldest two evicted: remaining should start from index 2.
	if hints[0].Key != string(rune('a'+2)) {
		t.Fatalf("oldest hint not evicted: first key = %q", hints[0].Key)
	}
}

func TestStore_TTLExpiry(t *testing.T) {
	hs := New(100, time.Hour)

	old := makeHint("old")
	old.At = time.Now().Add(-2 * time.Hour)
	hs.Store("nodeA", old)
	hs.Store("nodeA", makeHint("fresh"))

	hs.ExpireOld()

	hints := hs.Drain("nodeA")
	if len(hints) != 1 {
		t.Fatalf("want 1 fresh hint after expiry, got %d", len(hints))
	}
	if hints[0].Key != "fresh" {
		t.Fatalf("wrong hint survived expiry: %q", hints[0].Key)
	}
}

func TestStore_ExpireAllClearsNode(t *testing.T) {
	hs := New(100, time.Hour)

	old := makeHint("k1")
	old.At = time.Now().Add(-2 * time.Hour)
	hs.Store("nodeA", old)

	hs.ExpireOld()

	if nodes := hs.PendingNodes(); len(nodes) != 0 {
		t.Fatalf("expected no pending nodes, got %v", nodes)
	}
}

func TestStore_PendingNodes(t *testing.T) {
	hs := New(100, time.Hour)
	hs.Store("nodeA", makeHint("k1"))
	hs.Store("nodeB", makeHint("k2"))

	nodes := hs.PendingNodes()
	if len(nodes) != 2 {
		t.Fatalf("want 2 pending nodes, got %d", len(nodes))
	}

	hs.Drain("nodeA")
	nodes = hs.PendingNodes()
	if len(nodes) != 1 || nodes[0] != "nodeB" {
		t.Fatalf("want [nodeB] after drain, got %v", nodes)
	}
}

func TestStore_ConcurrentAccess(t *testing.T) {
	hs := New(1000, time.Hour)
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			hs.Store("nodeA", makeHint(string(rune('a'+i%26))))
		}(i)
	}
	wg.Wait()
	hints := hs.Drain("nodeA")
	// Cap is 1000 so all 50 should survive.
	if len(hints) != 50 {
		t.Fatalf("want 50 hints, got %d", len(hints))
	}
}
