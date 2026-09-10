package hintstore

import (
	"sync"
	"testing"
	"time"
)

// recordLosses returns a handler plus an accessor for what it saw.
func recordLosses() (func(string, int), func() map[string]int) {
	var mu sync.Mutex
	seen := make(map[string]int)
	return func(nodeID string, dropped int) {
			mu.Lock()
			defer mu.Unlock()
			seen[nodeID] += dropped
		}, func() map[string]int {
			mu.Lock()
			defer mu.Unlock()
			out := make(map[string]int, len(seen))
			for k, v := range seen {
				out[k] = v
			}
			return out
		}
}

// TestLossHandlerFiresOnCapEviction is the whole point of the escalation path.
// Before this, overrunning the per-node cap silently dropped a write from every
// fast path - the hint was gone, and only anti-entropy's background cycle would
// ever notice, whenever it happened upon that key's vnode. The drop is now an
// event.
func TestLossHandlerFiresOnCapEviction(t *testing.T) {
	hs := New(2, time.Hour)
	handler, seen := recordLosses()
	hs.SetLossHandler(handler)

	for i := range 5 {
		hs.Store("node-a", Hint{Key: string(rune('a' + i)), At: time.Now()})
	}

	// Cap 2, five stores: three evictions.
	if got := seen()["node-a"]; got != 3 {
		t.Errorf("loss handler saw %d dropped hints, want 3", got)
	}
	if got := hs.Lost()["node-a"]; got != 3 {
		t.Errorf("Lost() reported %d dropped hints, want 3", got)
	}
}

// TestLossHandlerFiresOnTTLExpiry covers the other way a hint dies undelivered.
// A prolonged outage hits this one rather than the cap: the target never comes
// back before the hints age out.
func TestLossHandlerFiresOnTTLExpiry(t *testing.T) {
	hs := New(100, time.Hour)
	handler, seen := recordLosses()
	hs.SetLossHandler(handler)

	old := time.Now().Add(-2 * time.Hour)
	hs.Store("node-a", Hint{Key: "k1", At: old})
	hs.Store("node-a", Hint{Key: "k2", At: old})
	hs.Store("node-b", Hint{Key: "k3", At: time.Now()}) // fresh, must survive

	hs.ExpireOld()

	got := seen()
	if got["node-a"] != 2 {
		t.Errorf("loss handler saw %d expired hints for node-a, want 2", got["node-a"])
	}
	if got["node-b"] != 0 {
		t.Errorf("loss handler fired for node-b with a fresh hint (%d)", got["node-b"])
	}
}

// TestNoLossOnDeliveredHints pins the negative: draining hints for delivery is
// not loss. Firing there would escalate a full anti-entropy sweep on every
// successful handoff - the common case - and swamp the WAN it is meant to save.
func TestNoLossOnDeliveredHints(t *testing.T) {
	hs := New(10, time.Hour)
	handler, seen := recordLosses()
	hs.SetLossHandler(handler)

	hs.Store("node-a", Hint{Key: "k1", At: time.Now()})
	hs.Store("node-a", Hint{Key: "k2", At: time.Now()})
	if drained := hs.Drain("node-a"); len(drained) != 2 {
		t.Fatalf("drained %d hints, want 2", len(drained))
	}

	if got := seen()["node-a"]; got != 0 {
		t.Errorf("loss handler fired %d times for delivered hints, want 0", got)
	}
	if got := hs.Lost()["node-a"]; got != 0 {
		t.Errorf("Lost() counted %d for delivered hints, want 0", got)
	}
}

// TestLossHandlerCanCallBackIntoStore pins the lock discipline. The real
// handler consults membership and may touch the store again; firing it under
// hs.mu would deadlock. This calls straight back in.
func TestLossHandlerCanCallBackIntoStore(t *testing.T) {
	hs := New(1, time.Hour)
	done := make(chan struct{})
	hs.SetLossHandler(func(nodeID string, dropped int) {
		_ = hs.PendingNodes()
		_ = hs.Lost()
		select {
		case <-done:
		default:
			close(done)
		}
	})

	hs.Store("node-a", Hint{Key: "k1", At: time.Now()})
	hs.Store("node-a", Hint{Key: "k2", At: time.Now()}) // evicts k1

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("loss handler deadlocked calling back into the store")
	}
}

// TestNoLossHandlerStillCounts keeps the metric independent of the escalation:
// an operator view of dropped hints must not depend on a handler being wired.
func TestNoLossHandlerStillCounts(t *testing.T) {
	hs := New(1, time.Hour)
	hs.Store("node-a", Hint{Key: "k1", At: time.Now()})
	hs.Store("node-a", Hint{Key: "k2", At: time.Now()})

	if got := hs.Lost()["node-a"]; got != 1 {
		t.Errorf("Lost() = %d with no handler registered, want 1", got)
	}
}
