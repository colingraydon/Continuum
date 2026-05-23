package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/gossip"
	"github.com/colingraydon/continuum/internal/hintstore"
	"github.com/colingraydon/continuum/internal/merkle"
	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/store"
)

func TestBuildMux(t *testing.T) {
	h := newTestHandler(t)
	mux := BuildMux(h)
	if mux == nil {
		t.Fatal("expected non-nil mux from BuildMux")
	}
	req := httptest.NewRequest(http.MethodGet, "/nodes", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 on GET /nodes, got %d", w.Code)
	}
}

func TestFlushHints_NilHintStore(t *testing.T) {
	h := newTestHandler(t) // hintStore is nil
	h.FlushHints()         // must not panic
}

func TestFlushHints_NodeNotInMemberList(t *testing.T) {
	r := ring.NewRing(10)
	ml := newTestMemberList(r)
	s := store.New()
	hs := hintstore.New(100, time.Hour)
	h := NewHandler(r, ml, newTestGossiper(t, ml), s, "self", 3, 1, 1, time.Second, hs)

	// Store a hint for a node that is not in the member list.
	hs.Store("unknown-node", hintstore.Hint{
		Key:    "k",
		Value:  "v",
		Clocks: map[string]uint64{"n1": 1},
		At:     time.Now(),
	})

	h.FlushHints() // should skip unknown-node (not in member list)

	// Hint should remain since it was not delivered.
	if nodes := hs.PendingNodes(); len(nodes) != 1 || nodes[0] != "unknown-node" {
		t.Errorf("hint for unknown-node should remain, pending=%v", nodes)
	}
}

func TestFlushHints_NodeDeadInMemberList(t *testing.T) {
	r := ring.NewRing(10)
	ml := newTestMemberList(r)
	s := store.New()
	hs := hintstore.New(100, time.Hour)
	h := NewHandler(r, ml, newTestGossiper(t, ml), s, "self", 3, 1, 1, time.Second, hs)

	// Add a dead node to the member list.
	ml.Add("dead-node", "10.0.0.99")
	ml.MarkDead("dead-node")

	hs.Store("dead-node", hintstore.Hint{
		Key:    "k",
		Value:  "v",
		Clocks: map[string]uint64{"n1": 1},
		At:     time.Now(),
	})

	h.FlushHints() // should skip dead-node

	// Hint should remain since delivery was skipped.
	if nodes := hs.PendingNodes(); len(nodes) != 1 || nodes[0] != "dead-node" {
		t.Errorf("hint for dead-node should remain, pending=%v", nodes)
	}
}

func TestNodeStatus_Unknown(t *testing.T) {
	h := newTestHandler(t)
	// Add a node directly to the ring without going through the member list.
	h.ring.AddNode("ring-only-node", "10.0.0.99")

	req := httptest.NewRequest(http.MethodGet, "/nodes", nil)
	w := httptest.NewRecorder()
	h.GetNodes(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp []NodeResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, n := range resp {
		if n.ID == "ring-only-node" && n.Status != "unknown" {
			t.Errorf("expected status=unknown for ring-only node, got %q", n.Status)
		}
	}
}

func TestEntryToResponse_DeletedSibling(t *testing.T) {
	h := newTestHandler(t)
	v := store.VectorClockVersion{Clocks: map[string]uint64{"n1": 1}}
	h.store.Put("tomb-key", "v", v)
	h.store.Delete("tomb-key", store.VectorClockVersion{Clocks: map[string]uint64{"n1": 2}})

	req := httptest.NewRequest(http.MethodGet, "/keys/tomb-key", nil)
	req.Header.Set("X-Proxied-From", "coord")
	w := httptest.NewRecorder()
	h.GetNode(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp NodeResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !resp.Deleted {
		t.Error("expected Deleted=true for tombstone entry")
	}
}

func TestEntryToResponse_MultipleSiblings(t *testing.T) {
	h := newTestHandler(t)
	// Create concurrent siblings (neither clock dominates the other).
	h.store.Put("sibling-key", "first", store.VectorClockVersion{Clocks: map[string]uint64{"n1": 1}})
	h.store.Put("sibling-key", "second", store.VectorClockVersion{Clocks: map[string]uint64{"n2": 1}})

	req := httptest.NewRequest(http.MethodGet, "/keys/sibling-key", nil)
	req.Header.Set("X-Proxied-From", "coord")
	w := httptest.NewRecorder()
	h.GetNode(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp NodeResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Siblings) != 2 {
		t.Errorf("expected 2 siblings, got %d", len(resp.Siblings))
	}
}

func TestGetSyncState_UnknownVnode(t *testing.T) {
	h := newTestHandler(t)
	h.memberList.Add("node1", "10.0.0.1") // adds node to ring via callback

	// Find a hash that's not a vnode position.
	var missing uint32
	known := make(map[uint32]bool)
	for _, vr := range h.ring.GetPrimaryVnodeRanges("node1") {
		known[vr.End] = true
	}
	for hv := uint32(1); ; hv++ {
		if !known[hv] {
			missing = hv
			break
		}
	}

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/sync?vnode=%d", missing), nil)
	w := httptest.NewRecorder()
	h.GetSyncState(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404 for unknown vnode, got %d", w.Code)
	}
}

func TestGetSyncState_ValidVnode(t *testing.T) {
	h := newTestHandler(t)
	h.memberList.Add("node1", "10.0.0.1")

	ranges := h.ring.GetPrimaryVnodeRanges("node1")
	if len(ranges) == 0 {
		t.Skip("no primary ranges")
	}

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/sync?vnode=%d", ranges[0].End), nil)
	w := httptest.NewRecorder()
	h.GetSyncState(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestGetSyncState_InvalidVnodeParam(t *testing.T) {
	h := newTestHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/sync?vnode=notanumber", nil)
	w := httptest.NewRecorder()
	h.GetSyncState(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetSyncKeys_MissingKeys(t *testing.T) {
	h := newTestHandler(t)
	body := `{"keys":["missing-key-1","missing-key-2"]}`
	req := httptest.NewRequest(http.MethodPost, "/sync/keys", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.GetSyncKeys(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var resp SyncKeysResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Entries) != 0 {
		t.Errorf("expected empty entries for missing keys, got %v", resp.Entries)
	}
}

func TestMigPushSyncEntries_NonOKStatus(t *testing.T) {
	// Create a server that returns a non-204 status to exercise the error path.
	badSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer badSrv.Close()

	h := newTestHandler(t)
	addr := strings.TrimPrefix(badSrv.URL, "http://")
	err := h.migPushSyncEntries(addr, map[string][]SyncSibling{
		"k": {{Value: "v", Clocks: map[string]uint64{"n1": 1}}},
	})
	if err == nil {
		t.Error("expected error when server returns non-204")
	}
}

func TestBootstrap_PullsTombstoneFromSource(t *testing.T) {
	// Verify Bootstrap handles tombstone entries (exercises pullVnodeRange deleted path).
	existSrv, existHandler := newMigrationTestNode(t, "existing", 2, 1, 1)
	existAddr := serverAddress(existSrv)

	_, joinHandler := newMigrationTestNode(t, "joining", 2, 1, 1)
	joinAddr := serverAddress(existSrv) // placeholder for joining's ring
	_ = joinAddr

	for _, h := range []*Handler{existHandler, joinHandler} {
		h.ring.AddNode("existing", existAddr)
		h.ring.AddNode("joining", "127.0.0.1:0")
	}

	// Find a key in the joining node's primary vnode ranges to store as tombstone on existing.
	var tombKey string
	for _, vr := range joinHandler.ring.GetPrimaryVnodeRanges("joining") {
		candidates := []string{"del-alpha", "del-beta", "del-gamma", "del-delta",
			"del-epsilon", "del-zeta", "del-eta", "del-theta", "del-iota", "del-kappa"}
		for _, c := range candidates {
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

	// Store tombstone on existing.
	existHandler.store.Delete(tombKey, store.VectorClockVersion{Clocks: map[string]uint64{"existing": 1}})

	joinHandler.Bootstrap()

	entry, ok := joinHandler.store.Get(tombKey)
	if !ok {
		t.Fatal("Bootstrap should have pulled tombstone from existing into joining")
	}
	if len(entry.Siblings) != 1 || !entry.Siblings[0].Deleted {
		t.Errorf("expected tombstone on joining, got %+v", entry.Siblings)
	}
}

func TestGetNode_EmptyKey(t *testing.T) {
	h := newTestHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/keys/", nil)
	w := httptest.NewRecorder()
	h.GetNode(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for empty key, got %d", w.Code)
	}
}

func TestPutKey_MissingValue(t *testing.T) {
	h := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPut, "/keys/somekey", strings.NewReader(`{"value":""}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.PutKey(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for missing value, got %d", w.Code)
	}
}

func TestGetNode_Bootstrapping(t *testing.T) {
	h := newTestHandler(t)
	h.memberList.Add(h.selfID, "127.0.0.1:0")
	h.memberList.SetBootstrapping(h.selfID, true)
	req := httptest.NewRequest(http.MethodGet, "/keys/somekey", nil)
	w := httptest.NewRecorder()
	h.GetNode(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 while bootstrapping, got %d", w.Code)
	}
}

func TestMergeResponses_WithSiblings(t *testing.T) {
	responses := []NodeResponse{
		{
			ID:     "node1",
			Status: "alive",
			Siblings: []SiblingResponse{
				{Value: "v1", Clocks: map[string]uint64{"n1": 1}},
				{Value: "v2", Clocks: map[string]uint64{"n2": 1}},
			},
		},
	}
	survivors := mergeResponses(responses)
	if len(survivors) != 2 {
		t.Errorf("expected 2 survivors from sibling responses, got %d", len(survivors))
	}
}

func TestDeliverHints_DeletedHint(t *testing.T) {
	targetRing := ring.NewRing(10)
	targetML := gossip.NewMemberList("target", "localhost", nil)
	targetStore := store.New()
	targetTransport, err := gossip.NewTransport("0")
	if err != nil {
		t.Fatalf("failed to create transport: %v", err)
	}
	defer targetTransport.Stop()
	targetGossiper := gossip.NewGossiper("target", "0", targetML, targetTransport)
	targetSrv := httptest.NewServer(NewServer(targetRing, targetML, targetGossiper, targetStore, "target", 1, 1, 1, time.Second, nil))
	defer targetSrv.Close()

	r := ring.NewRing(10)
	ml := newTestMemberList(r)
	s := store.New()
	hs := hintstore.New(100, time.Hour)
	h := NewHandler(r, ml, newTestGossiper(t, ml), s, "self", 3, 1, 1, time.Second, hs)

	targetAddr := targetSrv.Listener.Addr().String()
	ml.Add("target-node", targetAddr)

	hs.Store("target-node", hintstore.Hint{
		Key:     "hint-key",
		Deleted: true,
		Clocks:  map[string]uint64{"self": 1},
		At:      time.Now(),
	})

	h.FlushHints()

	if nodes := hs.PendingNodes(); len(nodes) != 0 {
		t.Errorf("deleted hint should be drained after delivery, pending=%v", nodes)
	}
}

func TestFlushHints_AliveNodeDeliversHints(t *testing.T) {
	// Start a real HTTP server to receive the hint delivery.
	targetRing := ring.NewRing(10)
	targetML := gossip.NewMemberList("target", "localhost", nil)
	targetStore := store.New()
	targetTransport, err := gossip.NewTransport("0")
	if err != nil {
		t.Fatalf("failed to create transport: %v", err)
	}
	defer targetTransport.Stop()
	targetGossiper := gossip.NewGossiper("target", "0", targetML, targetTransport)
	targetSrv := httptest.NewServer(NewServer(targetRing, targetML, targetGossiper, targetStore, "target", 1, 1, 1, time.Second, nil))
	defer targetSrv.Close()

	// Build the sender handler with a live peer.
	r := ring.NewRing(10)
	ml := newTestMemberList(r)
	s := store.New()
	hs := hintstore.New(100, time.Hour)
	h := NewHandler(r, ml, newTestGossiper(t, ml), s, "self", 3, 1, 1, time.Second, hs)

	targetAddr := targetSrv.Listener.Addr().String()
	ml.Add("target-node", targetAddr)

	hs.Store("target-node", hintstore.Hint{
		Key:    "hint-key",
		Value:  "hint-value",
		Clocks: map[string]uint64{"self": 1},
		At:     time.Now(),
	})

	h.FlushHints()

	// After delivery, the hint should have been drained.
	if nodes := hs.PendingNodes(); len(nodes) != 0 {
		t.Errorf("hints should be drained after successful delivery, pending=%v", nodes)
	}
}
