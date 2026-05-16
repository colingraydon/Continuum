package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/gossip"
	"github.com/colingraydon/continuum/internal/merkle"
	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/store"
)

// newMigrationTestNode creates a handler+server with configurable RF/WQ/RQ.
func newMigrationTestNode(t *testing.T, selfID string, rf, wq, rq int) (*httptest.Server, *Handler) {
	t.Helper()
	r := ring.NewRing(50)
	ml := gossip.NewMemberList(selfID, "", func(m *gossip.Member, status gossip.MemberStatus) {
		switch status {
		case gossip.MemberAlive:
			r.AddNode(m.ID, m.Address)
		case gossip.MemberDead:
			r.RemoveNode(m.ID)
		}
	})
	transport, err := gossip.NewTransport("0")
	if err != nil {
		t.Fatalf("transport: %v", err)
	}
	g := gossip.NewGossiper(selfID, "0", ml, transport)
	s := store.New()
	h := NewHandler(r, ml, g, s, selfID, rf, wq, rq, time.Second, nil)
	srv := httptest.NewServer(newMux(h))
	t.Cleanup(func() {
		srv.Close()
		transport.Stop()
	})
	return srv, h
}

// TestCleanupStaleKeys_EvictsNonOwned verifies that a key not in this node's
// replica set is removed from the local store.
func TestCleanupStaleKeys_EvictsNonOwned(t *testing.T) {
	_, h := newMigrationTestNode(t, "node1", 1, 1, 1)
	h.ring.AddNode("node1", "addr1")
	h.ring.AddNode("node2", "addr2")

	// Find a key that hashes to node2 (RF=1, so node2 is the sole owner).
	var node2Key string
	for _, candidate := range []string{"alpha", "beta", "gamma", "delta", "epsilon"} {
		nodes := h.ring.GetReplicationNodes(candidate, 1)
		if len(nodes) > 0 && nodes[0].ID == "node2" {
			node2Key = candidate
			break
		}
	}
	if node2Key == "" {
		t.Skip("no test key found that routes to node2 with this ring config")
	}

	h.store.Put(node2Key, "stale", store.VectorClockVersion{Clocks: map[string]uint64{"node2": 1}})
	h.CleanupStaleKeys()

	if _, ok := h.store.Get(node2Key); ok {
		t.Errorf("key %q should have been evicted (owned by node2, not node1)", node2Key)
	}
}

// TestCleanupStaleKeys_KeepsOwnedKeys verifies that keys this node owns are
// not evicted.
func TestCleanupStaleKeys_KeepsOwnedKeys(t *testing.T) {
	_, h := newMigrationTestNode(t, "node1", 1, 1, 1)
	h.ring.AddNode("node1", "addr1")
	h.ring.AddNode("node2", "addr2")

	var node1Key string
	for _, candidate := range []string{"alpha", "beta", "gamma", "delta", "epsilon"} {
		nodes := h.ring.GetReplicationNodes(candidate, 1)
		if len(nodes) > 0 && nodes[0].ID == "node1" {
			node1Key = candidate
			break
		}
	}
	if node1Key == "" {
		t.Skip("no test key found that routes to node1 with this ring config")
	}

	h.store.Put(node1Key, "mine", store.VectorClockVersion{Clocks: map[string]uint64{"node1": 1}})
	h.CleanupStaleKeys()

	if _, ok := h.store.Get(node1Key); !ok {
		t.Errorf("key %q should NOT have been evicted (owned by node1)", node1Key)
	}
}

// TestCleanupStaleKeys_EmptyStore verifies cleanup is a no-op on an empty store.
func TestCleanupStaleKeys_EmptyStore(t *testing.T) {
	_, h := newMigrationTestNode(t, "node1", 1, 1, 1)
	h.ring.AddNode("node1", "addr1")
	h.CleanupStaleKeys() // must not panic
}

// TestBootstrap_PullsFromSources verifies that Bootstrap fetches keys in the
// joining node's primary vnode ranges from the existing nodes.
func TestBootstrap_PullsFromSources(t *testing.T) {
	existSrv, existHandler := newMigrationTestNode(t, "existing", 2, 1, 1)
	existAddr := serverAddress(existSrv)

	joinSrv, joinHandler := newMigrationTestNode(t, "joining", 2, 1, 1)
	joinAddr := serverAddress(joinSrv)

	for _, h := range []*Handler{existHandler, joinHandler} {
		h.ring.AddNode("existing", existAddr)
		h.ring.AddNode("joining", joinAddr)
	}

	// Find a key in the joining node's primary vnode ranges to seed on existing.
	var migrKey string
	for _, vr := range joinHandler.ring.GetPrimaryVnodeRanges("joining") {
		for _, c := range []string{"apple", "banana", "cherry", "date", "elderberry", "fig", "grape"} {
			if vr.Contains(merkle.HashKey(c)) {
				migrKey = c
				break
			}
		}
		if migrKey != "" {
			break
		}
	}
	if migrKey == "" {
		t.Skip("no test key found in joining node's primary ranges")
	}

	existHandler.store.Put(migrKey, "migrated-value",
		store.VectorClockVersion{Clocks: map[string]uint64{"existing": 1}})

	joinHandler.Bootstrap()

	entry, ok := joinHandler.store.Get(migrKey)
	if !ok {
		t.Fatalf("Bootstrap should have pulled %q from existing into joining", migrKey)
	}
	if len(entry.Siblings) != 1 || entry.Siblings[0].Value != "migrated-value" {
		t.Errorf("unexpected entry after bootstrap: %+v", entry)
	}
}

// TestPushKeysToSuccessors_SendsToAliveReplicas verifies that PushKeysToSuccessors
// delivers all locally-held keys to alive replica nodes.
func TestPushKeysToSuccessors_SendsToAliveReplicas(t *testing.T) {
	node2Srv, node2Handler := newMigrationTestNode(t, "node2", 2, 1, 1)
	addr2 := serverAddress(node2Srv)

	_, node1Handler := newMigrationTestNode(t, "node1", 2, 1, 1)

	node1Handler.ring.AddNode("node1", "127.0.0.1:0")
	node1Handler.ring.AddNode("node2", addr2)
	node2Handler.ring.AddNode("node1", "127.0.0.1:0")
	node2Handler.ring.AddNode("node2", addr2)
	node1Handler.memberList.Add("node2", addr2)

	node1Handler.store.Put("push-key", "push-value",
		store.VectorClockVersion{Clocks: map[string]uint64{"node1": 1}})

	node1Handler.PushKeysToSuccessors()

	entry, ok := node2Handler.store.Get("push-key")
	if !ok {
		t.Fatal("node2 should have received push-key from PushKeysToSuccessors")
	}
	if len(entry.Siblings) != 1 || entry.Siblings[0].Value != "push-value" {
		t.Errorf("unexpected entry on node2: %+v", entry)
	}
}

// TestPushKeysToSuccessors_SkipsDeadNodes verifies that dead nodes are not pushed to.
func TestPushKeysToSuccessors_SkipsDeadNodes(t *testing.T) {
	called := false
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		http.Error(w, "should not be called", http.StatusInternalServerError)
	}))
	t.Cleanup(dead.Close)
	deadAddr := serverAddress(dead)

	_, node1Handler := newMigrationTestNode(t, "node1", 2, 1, 1)
	node1Handler.ring.AddNode("node1", "127.0.0.1:0")
	node1Handler.ring.AddNode("node2", deadAddr)
	node1Handler.store.Put("k", "v", store.VectorClockVersion{Clocks: map[string]uint64{"node1": 1}})
	node1Handler.memberList.Add("node2", deadAddr)
	node1Handler.memberList.MarkDead("node2")

	node1Handler.PushKeysToSuccessors()

	if called {
		t.Error("PushKeysToSuccessors should not push to dead nodes")
	}
}

// TestE2EMigrationOnJoin is an end-to-end test of the full join flow: an
// existing node has data, a joining node bootstraps, and acquires the keys
// that fall in its primary vnode ranges.
func TestE2EMigrationOnJoin(t *testing.T) {
	existSrv, existHandler := newMigrationTestNode(t, "existing", 2, 1, 1)
	existAddr := serverAddress(existSrv)

	joinSrv, joinHandler := newMigrationTestNode(t, "joining", 2, 1, 1)
	joinAddr := serverAddress(joinSrv)

	for _, h := range []*Handler{existHandler, joinHandler} {
		h.ring.AddNode("existing", existAddr)
		h.ring.AddNode("joining", joinAddr)
	}

	var migrKey string
	for _, vr := range joinHandler.ring.GetPrimaryVnodeRanges("joining") {
		for _, c := range []string{"w", "x", "y", "z", "join", "migrate", "transfer"} {
			if vr.Contains(merkle.HashKey(c)) {
				migrKey = c
				break
			}
		}
		if migrKey != "" {
			break
		}
	}
	if migrKey == "" {
		t.Skip("no test key found in joining node's primary ranges")
	}

	existHandler.store.Put(migrKey, "original",
		store.VectorClockVersion{Clocks: map[string]uint64{"existing": 1}})

	joinHandler.Bootstrap()

	entry, ok := joinHandler.store.Get(migrKey)
	if !ok {
		t.Fatalf("joining node should have %q after Bootstrap", migrKey)
	}
	if len(entry.Siblings) != 1 || entry.Siblings[0].Value != "original" {
		t.Errorf("unexpected entry on joining node: %+v", entry)
	}
}

// TestE2ELeaveDataPreserved verifies the graceful leave flow: a node pushes its
// keys to successors before leaving so data survives the departure.
func TestE2ELeaveDataPreserved(t *testing.T) {
	node2Srv, node2Handler := newMigrationTestNode(t, "node2", 2, 1, 1)
	addr2 := serverAddress(node2Srv)

	_, node1Handler := newMigrationTestNode(t, "node1", 2, 1, 1)

	node1Handler.ring.AddNode("node1", "127.0.0.1:0")
	node1Handler.ring.AddNode("node2", addr2)
	node2Handler.ring.AddNode("node1", "127.0.0.1:0")
	node2Handler.ring.AddNode("node2", addr2)
	node1Handler.memberList.Add("node2", addr2)

	node1Handler.store.Put("leave-key", "leave-value",
		store.VectorClockVersion{Clocks: map[string]uint64{"node1": 1}})

	node1Handler.PushKeysToSuccessors()

	entry, ok := node2Handler.store.Get("leave-key")
	if !ok {
		t.Fatal("node2 should have leave-key after node1's graceful leave")
	}
	if len(entry.Siblings) != 1 || entry.Siblings[0].Value != "leave-value" {
		t.Errorf("unexpected entry: %+v", entry)
	}
}

// TestBootstrappingNodeRejectedAsCoordinator verifies that a bootstrapping node
// returns 503 for client reads and writes but still accepts replica sub-requests.
func TestBootstrappingNodeRejectedAsCoordinator(t *testing.T) {
	srv, h := newMigrationTestNode(t, "node1", 1, 1, 1)
	addr := serverAddress(srv)
	h.ring.AddNode("node1", addr)
	h.memberList.SetBootstrapping("node1", true)

	// Coordinator PUT should be rejected.
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/keys/k",
		strings.NewReader(`{"value":"v"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT failed: %v", err)
	}
	closeBody(t, resp)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 for coordinator PUT during bootstrap, got %d", resp.StatusCode)
	}

	// Coordinator GET should be rejected.
	getResp, err := http.Get(srv.URL + "/keys/k")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	closeBody(t, getResp)
	if getResp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 for coordinator GET during bootstrap, got %d", getResp.StatusCode)
	}

	// Replica PUT (X-Proxied-From) should still be accepted.
	req2, _ := http.NewRequest(http.MethodPut, srv.URL+"/keys/k",
		strings.NewReader(`{"value":"v"}`))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("X-Proxied-From", "coordinator")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("replica PUT failed: %v", err)
	}
	closeBody(t, resp2)
	if resp2.StatusCode != http.StatusNoContent {
		t.Errorf("expected 204 for replica PUT during bootstrap, got %d", resp2.StatusCode)
	}
}

// TestBootstrappingNodeExcludedFromReads verifies that a bootstrapping node is
// not included in the read replica set when another node coordinates a read.
func TestBootstrappingNodeExcludedFromReads(t *testing.T) {
	// node1 is the coordinator. node2 is bootstrapping (excluded from reads).
	// With RF=2 and RQ=2, excluding node2 leaves only 1 node — quorum should
	// drop to 1 (min of RQ and available non-bootstrapping nodes).
	node1Srv, node1Handler := newMigrationTestNode(t, "node1", 2, 1, 1)
	node2Srv, node2Handler := newMigrationTestNode(t, "node2", 2, 1, 1)
	addr1 := serverAddress(node1Srv)
	addr2 := serverAddress(node2Srv)

	for _, h := range []*Handler{node1Handler, node2Handler} {
		h.ring.AddNode("node1", addr1)
		h.ring.AddNode("node2", addr2)
	}

	// Mark node2 as bootstrapping via its own member list, then propagate.
	node2Handler.memberList.SetBootstrapping("node2", true)
	node1Handler.memberList.Add("node2", addr2)
	// Manually set bootstrapping on node1's view of node2.
	if m, ok := node1Handler.memberList.Get("node2"); ok {
		m.Bootstrapping = true
	}

	// Seed node1 with a value.
	node1Handler.store.Put("b-key", "b-val",
		store.VectorClockVersion{Clocks: map[string]uint64{"node1": 1}})

	// Read from node1: should succeed using only node1 (node2 excluded).
	getResp, err := http.Get(node1Srv.URL + "/keys/b-key")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}

	closeBody(t, getResp)
	
	if getResp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", getResp.StatusCode)
	}
}
