package antientropy

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/store"
)

// countingPeer stands in for a replica, counting the sync requests it receives.
// The body it returns does not matter: syncRound only needs the request to have
// been attempted, and a decode failure is logged rather than retried.
func countingPeer(t *testing.T) (addr string, hits *atomic.Int64) {
	t.Helper()
	hits = &atomic.Int64{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"root":0,"buckets":[]}`))
	}))
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().String(), hits
}

// newCrossDCManager builds a two-DC ring where self and one peer sit in
// us-east and another peer sits in eu-west, with RF high enough that every
// node replicates every vnode.
func newCrossDCManager(t *testing.T) (m *Manager, localHits, remoteHits *atomic.Int64) {
	t.Helper()
	localAddr, localHits := countingPeer(t)
	remoteAddr, remoteHits := countingPeer(t)

	r := ring.NewRing(4)
	r.AddZonedNodeDC("self", "127.0.0.1:1", "us-east", "rack1", 1.0)
	r.AddZonedNodeDC("local-peer", localAddr, "us-east", "rack2", 1.0)
	r.AddZonedNodeDC("remote-peer", remoteAddr, "eu-west", "rack1", 1.0)

	m = New(r, store.New(), "self", 3, time.Second)
	m.SetSelfDC("us-east")
	return m, localHits, remoteHits
}

// TestCrossDCSyncRunsOnEveryNthRound pins the WAN cadence: a same-DC replica is
// compared every round, a replica across the WAN only every crossDCEvery
// rounds. Anti-entropy pays a round trip per vnode per round and the vast
// majority report "identical", so an unpaced loop spends WAN bandwidth to learn
// nothing.
func TestCrossDCSyncRunsOnEveryNthRound(t *testing.T) {
	m, localHits, remoteHits := newCrossDCManager(t)
	m.SetCrossDCSyncEvery(4)

	const rounds = 12
	for range rounds {
		m.syncRound()
	}

	if got := localHits.Load(); got != rounds {
		t.Errorf("local-DC replica synced %d times over %d rounds, want %d", got, rounds, rounds)
	}
	// Rounds 1, 5, 9 cross the WAN: the cadence is offset so the first round
	// after start syncs rather than waiting out a full multiple.
	if got, want := remoteHits.Load(), int64(rounds/4); got != want {
		t.Errorf("remote-DC replica synced %d times over %d rounds, want %d", got, rounds, want)
	}
}

// TestCrossDCSyncFirstRoundCrossesWAN pins the offset. A node that just started
// is exactly when remote divergence is most likely, so the WAN pass must not
// wait for the first full multiple of the cadence to elapse.
func TestCrossDCSyncFirstRoundCrossesWAN(t *testing.T) {
	m, _, remoteHits := newCrossDCManager(t)
	m.SetCrossDCSyncEvery(8)

	m.syncRound()

	if got := remoteHits.Load(); got != 1 {
		t.Errorf("remote-DC replica synced %d times on the first round, want 1", got)
	}
}

// TestUnlabeledNodeSyncsEveryReplicaEveryRound pins the compatibility rule: a
// cluster with no DC labels must behave exactly as it did before multi-DC, so
// the cadence never engages when either side of the comparison is unlabeled.
func TestUnlabeledNodeSyncsEveryReplicaEveryRound(t *testing.T) {
	peerAddr, peerHits := countingPeer(t)
	r := ring.NewRing(4)
	r.AddNode("self", "127.0.0.1:1")
	r.AddNode("peer", peerAddr)

	m := New(r, store.New(), "self", 2, time.Second)
	m.SetCrossDCSyncEvery(4) // set, but must not engage without labels

	const rounds = 6
	for range rounds {
		m.syncRound()
	}

	if got := peerHits.Load(); got != rounds {
		t.Errorf("unlabeled peer synced %d times over %d rounds, want %d", got, rounds, rounds)
	}
}

func TestIsRemoteDC(t *testing.T) {
	cases := []struct {
		selfDC, nodeDC string
		want           bool
	}{
		{"us-east", "eu-west", true},
		{"us-east", "us-east", false},
		{"", "eu-west", false}, // unlabeled self: cannot tell, treat as local
		{"us-east", "", false}, // unlabeled peer: same
		{"", "", false},        // pre-multi-DC cluster
	}
	for _, c := range cases {
		if got := isRemoteDC(c.selfDC, c.nodeDC); got != c.want {
			t.Errorf("isRemoteDC(%q, %q) = %v, want %v", c.selfDC, c.nodeDC, got, c.want)
		}
	}
}

func TestSetCrossDCSyncEveryClampsBelowOne(t *testing.T) {
	m := New(ring.NewRing(4), store.New(), "self", 2, time.Second)
	for _, n := range []int{0, -1} {
		m.SetCrossDCSyncEvery(n)
		m.mu.RLock()
		got := m.crossDCEvery
		m.mu.RUnlock()
		if got != 1 {
			t.Errorf("SetCrossDCSyncEvery(%d) = %d, want clamp to 1", n, got)
		}
	}
}

// TestResyncWithNodeCoversEverySharedVnode pins the escalation path's contract:
// a bulk resync must touch every vnode this node is primary for that the target
// also replicates, not just the one the round-robin cursor happens to be on.
// That is the whole point of escalating - a dropped hint's key can live in any
// vnode, and waiting for the cursor to reach it is what escalation avoids.
func TestResyncWithNodeCoversEverySharedVnode(t *testing.T) {
	peerAddr, peerHits := countingPeer(t)
	r := ring.NewRing(4)
	r.AddNode("self", "127.0.0.1:1")
	r.AddNode("peer", peerAddr)

	m := New(r, store.New(), "self", 2, time.Second)
	m.mu.RLock()
	// The full replica set, not the primary subset: a coordinator is primary
	// for only a fraction of the keys it accepts, so a resync scoped to its
	// primary ranges would leave most dropped writes unrepaired.
	vnodes := int64(len(m.trees))
	m.mu.RUnlock()
	if vnodes == 0 {
		t.Fatal("expected replicated vnodes")
	}

	m.ResyncWithNode("peer", peerAddr)

	if got := peerHits.Load(); got != vnodes {
		t.Errorf("resync touched %d vnodes, want all %d shared vnodes", got, vnodes)
	}
}

// TestResyncWithNodeSkipsNonReplica proves the resync is scoped: a node that
// replicates none of our vnodes is never contacted, so escalating for an
// unrelated node cannot generate a full sweep of pointless requests.
func TestResyncWithNodeSkipsNonReplica(t *testing.T) {
	peerAddr, peerHits := countingPeer(t)
	r := ring.NewRing(4)
	r.AddNode("self", "127.0.0.1:1")
	r.AddNode("peer", peerAddr)

	m := New(r, store.New(), "self", 1, time.Second) // RF=1: peer replicates nothing of ours

	m.ResyncWithNode("peer", peerAddr)

	if got := peerHits.Load(); got != 0 {
		t.Errorf("resync contacted a non-replica %d times, want 0", got)
	}
}

// TestSyncRoundConcurrentWithSetters is a race-detector target: the cadence
// reads selfDC and crossDCEvery under the manager lock while setters mutate
// them, and rounds is incremented from the sync loop.
func TestSyncRoundConcurrentWithSetters(t *testing.T) {
	m, _, _ := newCrossDCManager(t)

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for range 50 {
			m.syncRound()
		}
	}()
	go func() {
		defer wg.Done()
		for range 50 {
			m.SetCrossDCSyncEvery(2)
		}
	}()
	go func() {
		defer wg.Done()
		for range 50 {
			m.SetSelfDC("us-east")
		}
	}()
	wg.Wait()
}
