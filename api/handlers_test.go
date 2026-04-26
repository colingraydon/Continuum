package api

import (
	"bytes"
	"encoding/json"
	"fmt"
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

func newTestMemberList(r *ring.Ring) *gossip.MemberList {
	return gossip.NewMemberList("self", "localhost", func(m *gossip.Member, status gossip.MemberStatus) {
		switch status {
		case gossip.MemberAlive:
			r.AddNode(m.ID, m.Address)
		case gossip.MemberDead:
			r.RemoveNode(m.ID)
		}
	})
}

func newTestGossiper(t *testing.T, ml *gossip.MemberList) *gossip.Gossiper {
	t.Helper()
	transport, err := gossip.NewTransport("0")
	if err != nil {
		t.Fatalf("failed to create transport: %v", err)
	}
	t.Cleanup(func() { transport.Stop() })
	return gossip.NewGossiper("self", "0", ml, transport)
}

func newTestHandler(t *testing.T) *Handler {
	r := ring.NewRing(10)
	ml := newTestMemberList(r)
	s := store.New()
	return NewHandler(r, ml, newTestGossiper(t, ml), s, "self", 3, 1, 1, time.Second)
}

func TestAddNode(t *testing.T) {
	// Arrange
	h := newTestHandler(t)
	body := `{"id": "node1", "address": "10.0.0.1"}`
	req := httptest.NewRequest(http.MethodPost, "/nodes", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	// Act
	h.AddNode(w, req)

	// Assert
	if w.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d", w.Code)
	}
	var resp NodeResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.ID != "node1" || resp.Address != "10.0.0.1" {
		t.Errorf("unexpected response: %+v", resp)
	}
}

func TestAddNodeInvalidBody(t *testing.T) {
	// Arrange
	h := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPost, "/nodes", bytes.NewBufferString("not json"))
	w := httptest.NewRecorder()

	// Act
	h.AddNode(w, req)

	// Assert
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestAddNodeMissingID(t *testing.T) {
	// Arrange
	h := newTestHandler(t)
	body := `{"address": "10.0.0.1"}`
	req := httptest.NewRequest(http.MethodPost, "/nodes", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	// Act
	h.AddNode(w, req)

	// Assert
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestAddNodeMissingAddress(t *testing.T) {
	// Arrange
	h := newTestHandler(t)
	body := `{"id": "node1"}`
	req := httptest.NewRequest(http.MethodPost, "/nodes", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	// Act
	h.AddNode(w, req)

	// Assert
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestRemoveNode(t *testing.T) {
	// Arrange
	h := newTestHandler(t)
	h.memberList.Add("node1", "10.0.0.1")
	req := httptest.NewRequest(http.MethodDelete, "/nodes/node1", nil)
	w := httptest.NewRecorder()

	// Act
	h.RemoveNode(w, req)

	// Assert
	if w.Code != http.StatusNoContent {
		t.Errorf("expected 204, got %d", w.Code)
	}
	if h.ring.NodeCount() != 0 {
		t.Errorf("expected 0 nodes, got %d", h.ring.NodeCount())
	}
}

func TestRemoveNodeMissingID(t *testing.T) {
	// Arrange
	h := newTestHandler(t)
	req := httptest.NewRequest(http.MethodDelete, "/nodes/", nil)
	w := httptest.NewRecorder()

	// Act
	h.RemoveNode(w, req)

	// Assert
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetNodes(t *testing.T) {
	// Arrange
	h := newTestHandler(t)
	h.memberList.Add("node1", "10.0.0.1")
	h.memberList.Add("node2", "10.0.0.2")
	req := httptest.NewRequest(http.MethodGet, "/nodes", nil)
	w := httptest.NewRecorder()

	// Act
	h.GetNodes(w, req)

	// Assert
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var resp []NodeResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(resp) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(resp))
	}
}

func TestGetNodesEmpty(t *testing.T) {
	// Arrange
	h := newTestHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/nodes", nil)
	w := httptest.NewRecorder()

	// Act
	h.GetNodes(w, req)

	// Assert
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var resp []NodeResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(resp) != 0 {
		t.Errorf("expected 0 nodes, got %d", len(resp))
	}
}

func TestGetNode(t *testing.T) {
	// Arrange
	h := newTestHandler(t)
	h.memberList.Add("node1", "10.0.0.1")
	h.selfID = "node1"
	req := httptest.NewRequest(http.MethodGet, "/keys/mykey", nil)
	w := httptest.NewRecorder()

	// Act
	h.GetNode(w, req)

	// Assert
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var resp NodeResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.ID != "node1" {
		t.Errorf("expected node1, got %s", resp.ID)
	}
}

func TestGetNodeEmptyRing(t *testing.T) {
	// Arrange
	h := newTestHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/keys/mykey", nil)
	w := httptest.NewRecorder()

	// Act
	h.GetNode(w, req)

	// Assert
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}
}

func TestGetNodeMissingKey(t *testing.T) {
	// Arrange
	h := newTestHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/keys/", nil)
	w := httptest.NewRecorder()

	// Act
	h.GetNode(w, req)

	// Assert
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetStats(t *testing.T) {
	// Arrange
	h := newTestHandler(t)
	h.memberList.Add("node1", "10.0.0.1")
	h.memberList.Add("node2", "10.0.0.2")
	h.memberList.Add("node3", "10.0.0.3")
	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	w := httptest.NewRecorder()

	// Act
	h.GetStats(w, req)

	// Assert
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var stats ring.RingStats
	if err := json.NewDecoder(w.Body).Decode(&stats); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if stats.TotalNodes != 3 {
		t.Errorf("expected 3 nodes, got %d", stats.TotalNodes)
	}
	if stats.TotalVNodes != 30 {
		t.Errorf("expected 30 vnodes, got %d", stats.TotalVNodes)
	}
	if stats.MostLoaded == "" {
		t.Error("expected most loaded to be set")
	}
	if stats.LeastLoaded == "" {
		t.Error("expected least loaded to be set")
	}
}

func TestGetStatsEmptyRing(t *testing.T) {
	// Arrange
	h := newTestHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	w := httptest.NewRecorder()

	// Act
	h.GetStats(w, req)

	// Assert
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var stats ring.RingStats
	if err := json.NewDecoder(w.Body).Decode(&stats); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if stats.TotalNodes != 0 {
		t.Errorf("expected 0 nodes, got %d", stats.TotalNodes)
	}
	if stats.Variance != 0 {
		t.Errorf("expected 0 variance, got %f", stats.Variance)
	}
}

func TestGetReplicationNodes(t *testing.T) {
	// Arrange
	h := newTestHandler(t)
	h.memberList.Add("node1", "10.0.0.1")
	h.memberList.Add("node2", "10.0.0.2")
	h.memberList.Add("node3", "10.0.0.3")
	body := `{"key": "somekey", "factor": 3}`
	req := httptest.NewRequest(http.MethodPost, "/replicate", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	// Act
	h.GetReplicationNodes(w, req)

	// Assert
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var resp ReplicateResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Key != "somekey" {
		t.Errorf("expected somekey, got %s", resp.Key)
	}
	if len(resp.Nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(resp.Nodes))
	}
}

func TestGetReplicationNodesInvalidBody(t *testing.T) {
	// Arrange
	h := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPost, "/replicate", bytes.NewBufferString("not json"))
	w := httptest.NewRecorder()

	// Act
	h.GetReplicationNodes(w, req)

	// Assert
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetReplicationNodesMissingKey(t *testing.T) {
	// Arrange
	h := newTestHandler(t)
	body := `{"factor": 3}`
	req := httptest.NewRequest(http.MethodPost, "/replicate", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	// Act
	h.GetReplicationNodes(w, req)

	// Assert
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetReplicationNodesInvalidFactor(t *testing.T) {
	// Arrange
	h := newTestHandler(t)
	body := `{"key": "somekey", "factor": 0}`
	req := httptest.NewRequest(http.MethodPost, "/replicate", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	// Act
	h.GetReplicationNodes(w, req)

	// Assert
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetReplicationNodesEmptyRing(t *testing.T) {
	// Arrange
	h := newTestHandler(t)
	body := `{"key": "somekey", "factor": 3}`
	req := httptest.NewRequest(http.MethodPost, "/replicate", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	// Act
	h.GetReplicationNodes(w, req)

	// Assert
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}
}

func TestGetReplicationNodesDistinct(t *testing.T) {
	// Arrange
	h := newTestHandler(t)
	h.memberList.Add("node1", "10.0.0.1")
	h.memberList.Add("node2", "10.0.0.2")
	h.memberList.Add("node3", "10.0.0.3")
	body := `{"key": "somekey", "factor": 3}`
	req := httptest.NewRequest(http.MethodPost, "/replicate", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	// Act
	h.GetReplicationNodes(w, req)

	// Assert
	var resp ReplicateResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	seen := make(map[string]bool)
	for _, node := range resp.Nodes {
		if seen[node.ID] {
			t.Errorf("duplicate node %s in replication set", node.ID)
		}
		seen[node.ID] = true
	}
}

func TestHealth(t *testing.T) {
	// Arrange
	h := newTestHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	// Act
	h.Health(w, req)

	// Assert
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp["status"] != "ok" {
		t.Errorf("expected status ok, got %s", resp["status"])
	}
	if _, ok := resp["total_nodes"]; !ok {
		t.Error("expected total_nodes in response")
	}
	if _, ok := resp["healthy_nodes"]; !ok {
		t.Error("expected healthy_nodes in response")
	}
	if _, ok := resp["uptime"]; !ok {
		t.Error("expected uptime in response")
	}
}

func TestGossip(t *testing.T) {
	// Arrange
	h := newTestHandler(t)
	body := `{"members": [{"ID": "node1", "Address": "10.0.0.1", "Heartbeat": 1, "Status": 0}]}`
	req := httptest.NewRequest(http.MethodPost, "/gossip", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	// Act
	h.Gossip(w, req)

	// Assert
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var resp []*gossip.Member
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(resp) < 1 {
		t.Errorf("expected at least 1 member in response, got %d", len(resp))
	}
}

func TestGossipInvalidBody(t *testing.T) {
	// Arrange
	h := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPost, "/gossip", bytes.NewBufferString("not json"))
	w := httptest.NewRecorder()

	// Act
	h.Gossip(w, req)

	// Assert
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

// newHandlerWithSlowReplica starts an httptest.Server that hangs for hangFor
// before responding, registers it in the returned handler's ring, and returns
// both. The handler is configured with replicaTimeout so the slow replica will
// always exceed it. Callers must defer slow.Close().
func newHandlerWithSlowReplica(t *testing.T, replicaTimeout, hangFor time.Duration) (*Handler, *httptest.Server) {
	t.Helper()
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(hangFor):
			w.WriteHeader(http.StatusNoContent)
		case <-r.Context().Done():
		}
	}))

	r := ring.NewRing(10)
	ml := gossip.NewMemberList("self", "localhost", func(m *gossip.Member, status gossip.MemberStatus) {
		switch status {
		case gossip.MemberAlive:
			r.AddNode(m.ID, m.Address)
		case gossip.MemberDead:
			r.RemoveNode(m.ID)
		}
	})
	transport, err := gossip.NewTransport("0")
	if err != nil {
		t.Fatalf("failed to create transport: %v", err)
	}
	t.Cleanup(func() { transport.Stop() })
	g := gossip.NewGossiper("self", "0", ml, transport)
	// writeQuorum=2, readQuorum=2: self counts as one, slow replica must ack for quorum.
	s := store.New()
	h := NewHandler(r, ml, g, s, "self", 3, 2, 2, replicaTimeout)

	replicaAddr := strings.TrimPrefix(slow.URL, "http://")
	ml.Add("self", "localhost:8080")
	ml.Add("replica1", replicaAddr)

	return h, slow
}

func TestPutKeyReplicaTimeout(t *testing.T) {
	// Arrange: replica hangs for 10x the client timeout.
	h, slow := newHandlerWithSlowReplica(t, 50*time.Millisecond, 150*time.Millisecond)
	defer slow.Close()

	body := `{"value": "testval"}`
	req := httptest.NewRequest(http.MethodPut, "/keys/testkey", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	// Act
	h.PutKey(w, req)

	// Assert: self ack=1 < writeQuorum=2 because replica timed out.
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 when replica times out, got %d", w.Code)
	}
}

func TestGetNodeReplicaTimeout(t *testing.T) {
	// Arrange: replica hangs for 3x the client timeout.
	h, slow := newHandlerWithSlowReplica(t, 50*time.Millisecond, 150*time.Millisecond)
	defer slow.Close()

	req := httptest.NewRequest(http.MethodGet, "/keys/testkey", nil)
	w := httptest.NewRecorder()

	// Act
	h.GetNode(w, req)

	// Assert: self response=1 < readQuorum=2 because replica timed out.
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 when replica times out, got %d", w.Code)
	}
}

func TestDeleteKeyMissingKey(t *testing.T) {
	h := newTestHandler(t)
	req := httptest.NewRequest(http.MethodDelete, "/keys/", bytes.NewBufferString("{}"))
	w := httptest.NewRecorder()

	h.DeleteKey(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestDeleteKeyInvalidBody(t *testing.T) {
	h := newTestHandler(t)
	h.memberList.Add("self", "localhost:8080")
	req := httptest.NewRequest(http.MethodDelete, "/keys/k", bytes.NewBufferString("not json"))
	w := httptest.NewRecorder()

	h.DeleteKey(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestDeleteKeyLocalAndReadBack(t *testing.T) {
	// Arrange: single-node cluster, WQ=1, RQ=1.
	h := newTestHandler(t)
	h.memberList.Add("self", "localhost:8080")
	h.selfID = "self"

	// Write a value.
	putReq := httptest.NewRequest(http.MethodPut, "/keys/k", bytes.NewBufferString(`{"value":"v"}`))
	putReq.Header.Set("Content-Type", "application/json")
	putW := httptest.NewRecorder()
	h.PutKey(putW, putReq)
	if putW.Code != http.StatusNoContent {
		t.Fatalf("put: expected 204, got %d", putW.Code)
	}

	// Delete it.
	delReq := httptest.NewRequest(http.MethodDelete, "/keys/k", bytes.NewBufferString("{}"))
	delW := httptest.NewRecorder()
	h.DeleteKey(delW, delReq)
	if delW.Code != http.StatusNoContent {
		t.Fatalf("delete: expected 204, got %d", delW.Code)
	}

	// Read should return 404.
	getReq := httptest.NewRequest(http.MethodGet, "/keys/k", nil)
	getW := httptest.NewRecorder()
	h.GetNode(getW, getReq)
	if getW.Code != http.StatusNotFound {
		t.Errorf("expected 404 after delete, got %d", getW.Code)
	}
}

func TestDeleteKeyReplicaPassthrough(t *testing.T) {
	// A replica delete (X-Proxied-From set) stores tombstone without fan-out.
	h := newTestHandler(t)
	h.selfID = "self"

	req := httptest.NewRequest(http.MethodDelete, "/keys/k", bytes.NewBufferString(`{"clocks":{"node1":1}}`))
	req.Header.Set("X-Proxied-From", "node1")
	w := httptest.NewRecorder()

	h.DeleteKey(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	e, ok := h.store.Get("k")
	if !ok || !e.Siblings[0].Deleted {
		t.Error("expected tombstone in local store after replica delete")
	}
}

func TestDeleteKeyClockBootstrapping(t *testing.T) {
	// Without bootstrapping, the delete increments from an empty clock and
	// produces {self:1}, which equals the value's clock and is dropped as
	// idempotent. Bootstrapping reads the current entry first so the tombstone
	// gets {self:2}, which dominates {self:1} and wins.
	h := newTestHandler(t)
	h.memberList.Add("self", "localhost:8080")
	h.selfID = "self"

	putReq := httptest.NewRequest(http.MethodPut, "/keys/k", bytes.NewBufferString(`{"value":"v"}`))
	putReq.Header.Set("Content-Type", "application/json")
	h.PutKey(httptest.NewRecorder(), putReq)

	// Confirm the value is stored at {self:1}.
	entry, ok := h.store.Get("k")
	if !ok || entry.Siblings[0].Version.Clocks["self"] != 1 {
		t.Fatalf("expected value at clock {self:1}, got %+v", entry)
	}

	// Delete with no clocks provided — bootstrapping must kick in.
	delReq := httptest.NewRequest(http.MethodDelete, "/keys/k", bytes.NewBufferString("{}"))
	delW := httptest.NewRecorder()
	h.DeleteKey(delW, delReq)
	if delW.Code != http.StatusNoContent {
		t.Fatalf("delete: expected 204, got %d", delW.Code)
	}

	// Tombstone must have been written (not silently dropped).
	entry, ok = h.store.Get("k")
	if !ok {
		t.Fatal("expected entry to exist after delete")
	}
	if len(entry.Siblings) != 1 || !entry.Siblings[0].Deleted {
		t.Errorf("expected single tombstone sibling, got %+v", entry.Siblings)
	}
	if entry.Siblings[0].Version.Clocks["self"] != 2 {
		t.Errorf("expected tombstone clock {self:2}, got %v", entry.Siblings[0].Version.Clocks)
	}
}

func TestDeleteKeyReplicaTimeout(t *testing.T) {
	h, slow := newHandlerWithSlowReplica(t, 50*time.Millisecond, 150*time.Millisecond)
	defer slow.Close()

	req := httptest.NewRequest(http.MethodDelete, "/keys/testkey", bytes.NewBufferString("{}"))
	w := httptest.NewRecorder()

	h.DeleteKey(w, req)

	// self ack=1 < writeQuorum=2 because replica timed out.
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 when replica times out, got %d", w.Code)
	}
}

func TestGetSyncStateMissingParam(t *testing.T) {
	h := newTestHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/sync", nil)
	w := httptest.NewRecorder()
	h.GetSyncState(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing vnode param, got %d", w.Code)
	}
}

func TestGetSyncStateUnknownVnode(t *testing.T) {
	h := newTestHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/sync?vnode=9999999", nil)
	w := httptest.NewRecorder()
	h.GetSyncState(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown vnode, got %d", w.Code)
	}
}

func TestGetSyncStateEmpty(t *testing.T) {
	h := newTestHandler(t)
	h.memberList.Add("self", "localhost:8080")

	ranges := h.ring.GetPrimaryVnodeRanges("self")
	if len(ranges) == 0 {
		t.Fatal("expected primary vnode ranges for self")
	}
	url := fmt.Sprintf("/sync?vnode=%d", ranges[0].End)

	req := httptest.NewRequest(http.MethodGet, url, nil)
	w := httptest.NewRecorder()
	h.GetSyncState(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp SyncStateResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Buckets) != merkle.BucketCount {
		t.Errorf("expected %d buckets, got %d", merkle.BucketCount, len(resp.Buckets))
	}
}

func TestGetSyncStateChangesAfterWrite(t *testing.T) {
	h := newTestHandler(t)
	h.memberList.Add("self", "localhost:8080")
	h.selfID = "self"

	// Find which vnode range key "k" falls in so we can query that vnode.
	keyHash := merkle.HashKey("k")
	var vnodeHash uint32
	var found bool
	for _, vr := range h.ring.GetPrimaryVnodeRanges("self") {
		if vr.Contains(keyHash) {
			vnodeHash = vr.End
			found = true
			break
		}
	}
	if !found {
		t.Fatal("key 'k' not in any primary vnode range")
	}

	syncURL := fmt.Sprintf("/sync?vnode=%d", vnodeHash)

	reqBefore := httptest.NewRequest(http.MethodGet, syncURL, nil)
	wBefore := httptest.NewRecorder()
	h.GetSyncState(wBefore, reqBefore)
	var before SyncStateResponse
	if err := json.NewDecoder(wBefore.Body).Decode(&before); err != nil {
		t.Fatalf("decode before: %v", err)
	}

	putReq := httptest.NewRequest(http.MethodPut, "/keys/k", bytes.NewBufferString(`{"value":"v"}`))
	putReq.Header.Set("Content-Type", "application/json")
	h.PutKey(httptest.NewRecorder(), putReq)

	reqAfter := httptest.NewRequest(http.MethodGet, syncURL, nil)
	wAfter := httptest.NewRecorder()
	h.GetSyncState(wAfter, reqAfter)
	var after SyncStateResponse
	if err := json.NewDecoder(wAfter.Body).Decode(&after); err != nil {
		t.Fatalf("decode after: %v", err)
	}

	if after.Root == before.Root {
		t.Error("root hash should change after a write to a key in this vnode range")
	}
}

func TestGetSyncKeysReturnsEntries(t *testing.T) {
	h := newTestHandler(t)
	h.memberList.Add("self", "localhost:8080")
	h.selfID = "self"

	putReq := httptest.NewRequest(http.MethodPut, "/keys/fruit", bytes.NewBufferString(`{"value":"apple"}`))
	putReq.Header.Set("Content-Type", "application/json")
	h.PutKey(httptest.NewRecorder(), putReq)

	body := `{"keys":["fruit","missing"]}`
	req := httptest.NewRequest(http.MethodPost, "/sync/keys", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	h.GetSyncKeys(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp SyncKeysResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := resp.Entries["fruit"]; !ok {
		t.Error("expected 'fruit' in response")
	}
	if _, ok := resp.Entries["missing"]; ok {
		t.Error("expected 'missing' to be absent from response")
	}
	sibs := resp.Entries["fruit"]
	if len(sibs) != 1 || sibs[0].Value != "apple" {
		t.Errorf("unexpected siblings for 'fruit': %+v", sibs)
	}
}

func TestGetSyncKeysReturnsTombstone(t *testing.T) {
	h := newTestHandler(t)
	h.memberList.Add("self", "localhost:8080")
	h.selfID = "self"

	putReq := httptest.NewRequest(http.MethodPut, "/keys/k", bytes.NewBufferString(`{"value":"v"}`))
	putReq.Header.Set("Content-Type", "application/json")
	h.PutKey(httptest.NewRecorder(), putReq)

	delReq := httptest.NewRequest(http.MethodDelete, "/keys/k", bytes.NewBufferString("{}"))
	h.DeleteKey(httptest.NewRecorder(), delReq)

	req := httptest.NewRequest(http.MethodPost, "/sync/keys", bytes.NewBufferString(`{"keys":["k"]}`))
	w := httptest.NewRecorder()
	h.GetSyncKeys(w, req)

	var resp SyncKeysResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	sibs := resp.Entries["k"]
	if len(sibs) != 1 || !sibs[0].Deleted {
		t.Errorf("expected tombstone sibling, got %+v", sibs)
	}
}

func TestGetSyncKeysInvalidBody(t *testing.T) {
	h := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPost, "/sync/keys", bytes.NewBufferString("not json"))
	w := httptest.NewRecorder()

	h.GetSyncKeys(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}