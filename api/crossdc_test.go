package api

import (
	"sync"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/store"
)

// fakeResync records the escalations the handler drives.
type fakeResync struct {
	mu    sync.Mutex
	calls []string
	done  chan struct{}
}

func newFakeResync() *fakeResync {
	return &fakeResync{done: make(chan struct{}, 16)}
}

func (f *fakeResync) ResyncWithNode(nodeID, address string) {
	f.mu.Lock()
	f.calls = append(f.calls, nodeID)
	f.mu.Unlock()
	select {
	case f.done <- struct{}{}:
	default:
	}
}

func (f *fakeResync) seen() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func newCrossDCHandler(t *testing.T, selfDC string, crossTimeout time.Duration) *Handler {
	t.Helper()
	r := ring.NewRing(10)
	ml := newTestMemberList(r)
	return NewHandler(r, ml, store.New(), HandlerConfig{
		SelfID:                "self",
		SelfDC:                selfDC,
		ReplicationFactor:     3,
		WriteQuorum:           1,
		ReadQuorum:            1,
		ReplicaTimeout:        500 * time.Millisecond,
		CrossDCReplicaTimeout: crossTimeout,
	}, nil)
}

// TestClientForDCUsesWANBudgetAcrossDCs is the fix for the quiet failure that a
// single replica client caused: one 500ms budget served both a rack neighbour
// and a cross-continent hop, so a healthy remote replica timed out routinely
// and its write was hinted instead of delivered.
func TestClientForDCUsesWANBudgetAcrossDCs(t *testing.T) {
	h := newCrossDCHandler(t, "us-east", 2*time.Second)

	if got := h.clientForDC("eu-west").Timeout; got != 2*time.Second {
		t.Errorf("remote-DC hop got a %v timeout, want the 2s WAN budget", got)
	}
	if got := h.clientForDC("us-east").Timeout; got != 500*time.Millisecond {
		t.Errorf("same-DC hop got a %v timeout, want the 500ms local budget", got)
	}
}

// TestClientForDCTreatsUnlabeledAsLocal pins the compatibility rule: without
// labels on both sides the WAN budget never engages, so a single-DC cluster is
// bit-for-bit unchanged.
func TestClientForDCTreatsUnlabeledAsLocal(t *testing.T) {
	labeled := newCrossDCHandler(t, "us-east", 2*time.Second)
	if got := labeled.clientForDC("").Timeout; got != 500*time.Millisecond {
		t.Errorf("unlabeled peer got %v, want the local budget", got)
	}

	unlabeled := newCrossDCHandler(t, "", 2*time.Second)
	if got := unlabeled.clientForDC("eu-west").Timeout; got != 500*time.Millisecond {
		t.Errorf("unlabeled coordinator got %v, want the local budget", got)
	}
}

// TestCrossDCTimeoutDefaultsToReplicaTimeout keeps an unconfigured deployment
// on exactly its old behavior rather than silently inheriting a longer budget.
func TestCrossDCTimeoutDefaultsToReplicaTimeout(t *testing.T) {
	h := newCrossDCHandler(t, "us-east", 0)
	if got := h.clientForDC("eu-west").Timeout; got != 500*time.Millisecond {
		t.Errorf("unset CrossDCReplicaTimeout gave %v, want the replica timeout", got)
	}
}

// TestOnHintLossEscalatesForLiveNode covers the reachable case: hints dropped
// for a node that is up should start a repair pass immediately rather than wait
// for the anti-entropy cursor to reach those vnodes.
func TestOnHintLossEscalatesForLiveNode(t *testing.T) {
	h := newCrossDCHandler(t, "us-east", time.Second)
	fr := newFakeResync()
	h.SetResyncTrigger(fr)
	h.memberList.Add("peer", "10.0.0.1:8080")

	h.OnHintLoss("peer", 5)

	select {
	case <-fr.done:
	case <-time.After(5 * time.Second):
		t.Fatal("no resync escalated for a live node")
	}
	if got := fr.seen(); len(got) != 1 || got[0] != "peer" {
		t.Errorf("escalated %v, want exactly [peer]", got)
	}
}

// TestOnHintLossDefersUnreachableNode is the case that actually happens: hints
// pile up and overflow precisely because the target is down. Escalating then
// would burn a full vnode sweep on connection timeouts, so the node is
// remembered and escalated later.
func TestOnHintLossDefersUnreachableNode(t *testing.T) {
	h := newCrossDCHandler(t, "us-east", time.Second)
	fr := newFakeResync()
	h.SetResyncTrigger(fr)
	h.memberList.Add("peer", "10.0.0.1:8080")
	h.memberList.MarkDead("peer")

	h.OnHintLoss("peer", 5)

	if got := fr.seen(); len(got) != 0 {
		t.Fatalf("escalated %v against a dead node, want none", got)
	}
	// Nothing to run yet - still dead.
	h.RunPendingResyncs()
	if got := fr.seen(); len(got) != 0 {
		t.Fatalf("escalated %v while still dead, want none", got)
	}

	// Back up: the deferred escalation must now fire.
	h.memberList.Add("peer", "10.0.0.1:8080")
	h.RunPendingResyncs()

	if got := fr.seen(); len(got) != 1 || got[0] != "peer" {
		t.Errorf("after recovery escalated %v, want exactly [peer]", got)
	}
}

// TestRunPendingResyncsIsOneShot proves the pending set is cleared, so a single
// loss does not re-trigger a full vnode sweep on every sweep tick thereafter.
func TestRunPendingResyncsIsOneShot(t *testing.T) {
	h := newCrossDCHandler(t, "us-east", time.Second)
	fr := newFakeResync()
	h.SetResyncTrigger(fr)
	h.memberList.Add("peer", "10.0.0.1:8080")
	h.memberList.MarkDead("peer")
	h.OnHintLoss("peer", 1)
	h.memberList.Add("peer", "10.0.0.1:8080")

	for range 3 {
		h.RunPendingResyncs()
	}

	if got := fr.seen(); len(got) != 1 {
		t.Errorf("escalated %d times across three sweeps, want exactly 1", len(got))
	}
}

// TestOnHintLossWithoutTriggerIsSafe covers the unwired configuration (tests,
// and any node built without an anti-entropy manager).
func TestOnHintLossWithoutTriggerIsSafe(t *testing.T) {
	h := newCrossDCHandler(t, "us-east", time.Second)
	h.memberList.Add("peer", "10.0.0.1:8080")
	h.OnHintLoss("peer", 3)
	h.RunPendingResyncs()
}
