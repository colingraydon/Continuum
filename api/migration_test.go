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
	s := store.New()
	h := NewHandler(r, ml, s, HandlerConfig{SelfID: selfID, ReplicationFactor: rf, WriteQuorum: wq, ReadQuorum: rq, ReplicaTimeout: time.Second}, nil)
	srv := httptest.NewServer(newMux(h))
	t.Cleanup(func() { srv.Close() })
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

	if _, ok, _ := h.store.Get(node2Key); ok {
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

	if _, ok, _ := h.store.Get(node1Key); !ok {
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

	entry, ok, _ := joinHandler.store.Get(migrKey)
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

	entry, ok, _ := node2Handler.store.Get("push-key")
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

	entry, ok, _ := joinHandler.store.Get(migrKey)
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

	entry, ok, _ := node2Handler.store.Get("leave-key")
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
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 for coordinator PUT during bootstrap, got %d", resp.StatusCode)
	}

	// Coordinator GET should be rejected.
	getResp, err := http.Get(srv.URL + "/keys/k")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer getResp.Body.Close()
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
	defer resp2.Body.Close()
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
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", getResp.StatusCode)
	}
}

func TestBootstrap_PullsTombstoneFromSource(t *testing.T) {
	existSrv, existHandler := newMigrationTestNode(t, "existing", 2, 1, 1)
	existAddr := serverAddress(existSrv)

	_, joinHandler := newMigrationTestNode(t, "joining", 2, 1, 1)

	for _, h := range []*Handler{existHandler, joinHandler} {
		h.ring.AddNode("existing", existAddr)
		h.ring.AddNode("joining", "127.0.0.1:0")
	}

	var tombKey string
	for _, vr := range joinHandler.ring.GetPrimaryVnodeRanges("joining") {
		for _, c := range []string{"del-alpha", "del-beta", "del-gamma", "del-delta",
			"del-epsilon", "del-zeta", "del-eta", "del-theta", "del-iota", "del-kappa"} {
			if vr.Contains(merkle.HashKey(c)) {
				tombKey = c
				break
			}
		}
		if tombKey != "" {
			break
		}
	}
	if tombKey == "" {
		t.Skip("no test key found in joining node's primary ranges")
	}

	existHandler.store.Delete(tombKey, store.VectorClockVersion{Clocks: map[string]uint64{"existing": 1}})
	joinHandler.Bootstrap()

	entry, ok, _ := joinHandler.store.Get(tombKey)
	if !ok {
		t.Fatal("Bootstrap should have pulled tombstone from existing into joining")
	}
	if len(entry.Siblings) != 1 || !entry.Siblings[0].Deleted {
		t.Errorf("expected tombstone on joining, got %+v", entry.Siblings)
	}
}

func TestBootstrap_PullError(t *testing.T) {
	deadSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadAddr := strings.TrimPrefix(deadSrv.URL, "http://")
	deadSrv.Close()

	r := ring.NewRing(10)
	ml := gossip.NewMemberList("joining", "localhost", nil)
	s := store.New()
	h := NewHandler(r, ml, s, HandlerConfig{SelfID: "joining", ReplicationFactor: 2, WriteQuorum: 1, ReadQuorum: 1, ReplicaTimeout: time.Second}, nil)
	r.AddNode("existing", deadAddr)
	r.AddNode("joining", "127.0.0.1:0")

	h.Bootstrap() // logs errors, must not panic
}

func TestMigFetchBucketKeys_HTTPError(t *testing.T) {
	_, h := newMigrationTestNode(t, "node1", 1, 1, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	addr := strings.TrimPrefix(srv.URL, "http://")
	srv.Close()

	_, err := h.migFetchBucketKeys(addr, 1, 0)
	if err == nil {
		t.Error("expected error from migFetchBucketKeys when server is unreachable")
	}
}

func TestMigFetchBucketKeys_InvalidJSON(t *testing.T) {
	_, h := newMigrationTestNode(t, "node1", 1, 1, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not-json"))
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	_, err := h.migFetchBucketKeys(addr, 1, 0)
	if err == nil {
		t.Error("expected error from migFetchBucketKeys on invalid JSON response")
	}
}

func TestMigFetchSyncKeys_HTTPError(t *testing.T) {
	_, h := newMigrationTestNode(t, "node1", 1, 1, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	addr := strings.TrimPrefix(srv.URL, "http://")
	srv.Close()

	_, err := h.migFetchSyncKeys(addr, []string{"k"})
	if err == nil {
		t.Error("expected error from migFetchSyncKeys when server is unreachable")
	}
}

func TestMigFetchSyncKeys_InvalidJSON(t *testing.T) {
	_, h := newMigrationTestNode(t, "node1", 1, 1, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not-json"))
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	_, err := h.migFetchSyncKeys(addr, []string{"k"})
	if err == nil {
		t.Error("expected error from migFetchSyncKeys on invalid JSON response")
	}
}

func TestMigPushSyncEntries_NonOKStatus(t *testing.T) {
	badSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer badSrv.Close()

	_, h := newMigrationTestNode(t, "node1", 1, 1, 1)
	addr := strings.TrimPrefix(badSrv.URL, "http://")
	err := h.migPushSyncEntries(addr, map[string][]SyncSibling{
		"k": {{Value: "v", Clocks: map[string]uint64{"n1": 1}}},
	})
	if err == nil {
		t.Error("expected error when server returns non-204")
	}
}

func TestMigPushSyncEntries_HTTPError(t *testing.T) {
	_, h := newMigrationTestNode(t, "node1", 1, 1, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	addr := strings.TrimPrefix(srv.URL, "http://")
	srv.Close()

	err := h.migPushSyncEntries(addr, map[string][]SyncSibling{
		"k": {{Value: "v", Clocks: map[string]uint64{"n1": 1}}},
	})
	if err == nil {
		t.Error("expected error from migPushSyncEntries when server is unreachable")
	}
}

func TestPullVnodeRange_FetchKeysError(t *testing.T) {
	_, h := newMigrationTestNode(t, "node1", 1, 1, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	addr := strings.TrimPrefix(srv.URL, "http://")
	srv.Close()

	err := h.pullVnodeRange(addr, 1)
	if err == nil {
		t.Error("expected error from pullVnodeRange when bucket-keys fetch fails")
	}
}

func TestPullVnodeRange_FetchEntriesError(t *testing.T) {
	_, h := newMigrationTestNode(t, "node1", 1, 1, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sync/bucket-keys" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"keys":["some-key"]}`))
		} else {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("not-json"))
		}
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	err := h.pullVnodeRange(addr, 1)
	if err == nil {
		t.Error("expected error from pullVnodeRange when sync/keys fetch fails")
	}
}

func TestPushKeysToSuccessors_NonAliveMemberInRing(t *testing.T) {
	r := ring.NewRing(10)
	ml := gossip.NewMemberList("node1", "localhost", nil)
	s := store.New()
	h := NewHandler(r, ml, s, HandlerConfig{SelfID: "node1", ReplicationFactor: 2, WriteQuorum: 1, ReadQuorum: 1, ReplicaTimeout: time.Second}, nil)

	r.AddNode("node1", "127.0.0.1:0")
	r.AddNode("node2", "127.0.0.2:0")
	ml.Add("node2", "127.0.0.2:0")
	ml.MarkDead("node2")

	s.Put("some-key", "v", store.VectorClockVersion{Clocks: map[string]uint64{"node1": 1}})

	h.PushKeysToSuccessors() // must not push to dead node
}

func TestPushKeysToSuccessors_PushFails(t *testing.T) {
	failSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server error", http.StatusInternalServerError)
	}))
	defer failSrv.Close()
	failAddr := strings.TrimPrefix(failSrv.URL, "http://")

	r := ring.NewRing(10)
	ml := gossip.NewMemberList("node1", "localhost", nil)
	s := store.New()
	h := NewHandler(r, ml, s, HandlerConfig{SelfID: "node1", ReplicationFactor: 2, WriteQuorum: 1, ReadQuorum: 1, ReplicaTimeout: time.Second}, nil)

	r.AddNode("node1", "127.0.0.1:0")
	r.AddNode("node2", failAddr)
	ml.Add("node2", failAddr)

	s.Put("some-key", "v", store.VectorClockVersion{Clocks: map[string]uint64{"node1": 1}})

	h.PushKeysToSuccessors() // push fails, must log and not panic
}
