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

	"github.com/colingraydon/continuum/internal/antientropy"
	"github.com/colingraydon/continuum/internal/gossip"
	"github.com/colingraydon/continuum/internal/hintstore"
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

func newTestHandler(t *testing.T) *Handler {
	t.Helper()
	r := ring.NewRing(10)
	ml := newTestMemberList(r)
	s := store.New()
	return NewHandler(r, ml, s, HandlerConfig{SelfID: "self", ReplicationFactor: 3, WriteQuorum: 1, ReadQuorum: 1, ReplicaTimeout: time.Second}, nil)
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

func TestGetReplicationNodesFactorAboveCap(t *testing.T) {
	// Arrange
	h := newTestHandler(t)
	body := `{"key": "somekey", "factor": 1025}`
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
	// writeQuorum=2, readQuorum=2: self counts as one, slow replica must ack for quorum.
	s := store.New()
	h := NewHandler(r, ml, s, HandlerConfig{SelfID: "self", ReplicationFactor: 3, WriteQuorum: 2, ReadQuorum: 2, ReplicaTimeout: replicaTimeout}, nil)

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
	e, ok, _ := h.store.Get("k")
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
	entry, ok, _ := h.store.Get("k")
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
	entry, ok, _ = h.store.Get("k")
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

// --- Per-request consistency tests ---

func TestRequestedQuorumLevels(t *testing.T) {
	h := newTestHandler(t) // ReplicationFactor: 3
	cases := []struct {
		param   string
		want    int
		wantErr bool
	}{
		{"", 2, false}, // absent -> caller's default
		{"one", 1, false},
		{"quorum", 2, false}, // RF/2+1
		{"all", 3, false},    // RF
		{"ONE", 0, true},     // levels are case-sensitive
		{"bogus", 0, true},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodGet, "/keys/k?consistency="+tc.param, nil)
		got, err := h.requestedQuorum(req, 2)
		if tc.wantErr {
			if err == nil {
				t.Errorf("consistency=%q: expected error, got quorum %+v", tc.param, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("consistency=%q: unexpected error: %v", tc.param, err)
			continue
		}
		if got.size != tc.want {
			t.Errorf("consistency=%q: got quorum %d, want %d", tc.param, got.size, tc.want)
		}
		if got.localOnly {
			t.Errorf("consistency=%q: cluster-wide level marked localOnly", tc.param)
		}
	}
}

// newDegradedClusterHandler returns a coordinator whose ring holds self plus
// two replicas that always fail (an httptest server answering 500), so quorum
// outcomes depend entirely on the requested level: only self can ack.
func newDegradedClusterHandler(t *testing.T) *Handler {
	t.Helper()
	failSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(failSrv.Close)

	r := ring.NewRing(10)
	ml := newTestMemberList(r)
	s := store.New()
	h := NewHandler(r, ml, s, HandlerConfig{SelfID: "self", ReplicationFactor: 3, WriteQuorum: 1, ReadQuorum: 1, ReplicaTimeout: time.Second}, nil)

	r.AddNode("self", "localhost:8080")
	failAddr := failSrv.Listener.Addr().String()
	r.AddNode("down1", failAddr)
	r.AddNode("down2", failAddr)
	return h
}

func TestPutKeyPerRequestConsistency(t *testing.T) {
	h := newDegradedClusterHandler(t)

	put := func(consistency string) int {
		url := "/keys/pc-key"
		if consistency != "" {
			url += "?consistency=" + consistency
		}
		req := httptest.NewRequest(http.MethodPut, url, bytes.NewBufferString(`{"value":"v"}`))
		w := httptest.NewRecorder()
		h.PutKey(w, req)
		return w.Code
	}

	// one: self's ack alone meets quorum despite both replicas being down.
	if code := put("one"); code != http.StatusNoContent {
		t.Errorf("consistency=one: got %d, want 204", code)
	}
	// all: requires 3 acks; only self can ack -> quorum failure.
	if code := put("all"); code != http.StatusServiceUnavailable {
		t.Errorf("consistency=all: got %d, want 503", code)
	}
	// absent: process default (W=1) still applies.
	if code := put(""); code != http.StatusNoContent {
		t.Errorf("default consistency: got %d, want 204", code)
	}
}

func TestPutKeyInvalidConsistencyRejectedBeforeWrite(t *testing.T) {
	h := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPut, "/keys/reject-key?consistency=bogus", bytes.NewBufferString(`{"value":"v"}`))
	w := httptest.NewRecorder()
	h.PutKey(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400", w.Code)
	}
	if _, ok, err := h.store.Get("reject-key"); err != nil || ok {
		t.Errorf("invalid consistency must not write locally (ok=%v err=%v)", ok, err)
	}
}

func TestDeleteKeyInvalidConsistencyRejectedBeforeWrite(t *testing.T) {
	h := newTestHandler(t)
	req := httptest.NewRequest(http.MethodDelete, "/keys/reject-del?consistency=bogus", bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	h.DeleteKey(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("got %d, want 400", w.Code)
	}
	if _, ok, err := h.store.Get("reject-del"); err != nil || ok {
		t.Errorf("invalid consistency must not store a tombstone (ok=%v err=%v)", ok, err)
	}
}

func TestGetKeyPerRequestConsistency(t *testing.T) {
	h := newDegradedClusterHandler(t)
	if err := h.store.Put("pc-read", "v", store.VectorClockVersion{Clocks: map[string]uint64{"self": 1}}); err != nil {
		t.Fatalf("seed put: %v", err)
	}

	get := func(consistency string) int {
		req := httptest.NewRequest(http.MethodGet, "/keys/pc-read?consistency="+consistency, nil)
		w := httptest.NewRecorder()
		h.GetNode(w, req)
		return w.Code
	}

	if code := get("one"); code != http.StatusOK {
		t.Errorf("consistency=one: got %d, want 200", code)
	}
	if code := get("all"); code != http.StatusServiceUnavailable {
		t.Errorf("consistency=all: got %d, want 503", code)
	}
	if code := get("bogus"); code != http.StatusBadRequest {
		t.Errorf("consistency=bogus: got %d, want 400", code)
	}
}

// --- Sloppy quorum tests ---

// TestPutKeySloppyQuorumSkipsUnhealthyAndHints proves the always-writable
// property: with one strict-set replica unhealthy, the write meets W via the
// next healthy node on the ring, and the skipped intended owner gets a hint
// for later replay.
func TestPutKeySloppyQuorumSkipsUnhealthyAndHints(t *testing.T) {
	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer okSrv.Close()
	okAddr := okSrv.Listener.Addr().String()

	r := ring.NewRing(10)
	ml := newTestMemberList(r)
	s := store.New()
	hs := hintstore.New(100, time.Hour)
	h := NewHandler(r, ml, s, HandlerConfig{SelfID: "self", ReplicationFactor: 3, WriteQuorum: 3, ReadQuorum: 1, ReplicaTimeout: time.Second}, hs)

	r.AddNode("self", "localhost:8080")
	r.AddNode("healthy1", okAddr)
	r.AddNode("healthy2", okAddr)
	r.AddNode("down", "10.255.255.1:1") // never contacted: skipped by the health walk

	// Find a key whose strict replica set includes the down node, so the
	// healthy walk must actually skip it and pull in a substitute.
	var key string
	for i := 0; i < 1000 && key == ""; i++ {
		candidate := fmt.Sprintf("sloppy-%d", i)
		for _, n := range r.GetReplicationNodes(candidate, 3) {
			if n.ID == "down" {
				key = candidate
				break
			}
		}
	}
	if key == "" {
		t.Fatal("no key found with down in its replica set")
	}
	r.SetHealthFilter(func(id string) bool { return id != "down" })

	req := httptest.NewRequest(http.MethodPut, "/keys/"+key, bytes.NewBufferString(`{"value":"v"}`))
	w := httptest.NewRecorder()
	h.PutKey(w, req)

	// W=3 must be met by healthy nodes alone despite a down strict replica.
	if w.Code != http.StatusNoContent {
		t.Fatalf("sloppy write got %d, want 204", w.Code)
	}

	// The skipped owner must be hinted (may be buffered asynchronously).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range hs.PendingNodes() {
			if id == "down" {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("no hint buffered for skipped owner; pending=%v", hs.PendingNodes())
}

// TestSyncHandlersProviderMatchesScanFallback proves the tree-served fast path
// and the store-scan fallback return byte-identical JSON, so installing the
// provider changes only cost, not behavior.
func TestSyncHandlersProviderMatchesScanFallback(t *testing.T) {
	r := ring.NewRing(16)
	r.AddNode("self", "localhost:8080")
	r.AddNode("node2", "10.0.0.2")
	r.AddNode("node3", "10.0.0.3")
	s := store.New()
	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("sk%03d", i)
		if err := s.Put(key, fmt.Sprintf("v%03d", i), store.VectorClockVersion{Clocks: map[string]uint64{"w": 1}}); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	ml := gossip.NewMemberList("self", "localhost", nil)
	h := NewHandler(r, ml, s, HandlerConfig{SelfID: "self", ReplicationFactor: 2, WriteQuorum: 1, ReadQuorum: 1, ReplicaTimeout: time.Second}, nil)

	ranges := r.GetReplicaVnodeRanges("self", 2)
	if len(ranges) == 0 {
		t.Fatal("expected self to replicate vnodes")
	}
	vnode := ranges[0].End
	stateURL := fmt.Sprintf("/sync?vnode=%d", vnode)
	bucketURL := fmt.Sprintf("/sync/bucket-keys?vnode=%d&bucket=3", vnode)

	// Scan fallback (no provider installed).
	scanState := recordGet(t, h.GetSyncState, stateURL)
	scanBucket := recordGet(t, h.GetSyncBucketKeys, bucketURL)

	// Fast path via the real manager's maintained trees.
	h.SetSyncTreeProvider(antientropy.New(r, s, "self", 2, time.Second))
	provState := recordGet(t, h.GetSyncState, stateURL)
	provBucket := recordGet(t, h.GetSyncBucketKeys, bucketURL)

	if provState != scanState {
		t.Errorf("sync-state JSON differs:\n provider: %s scan:     %s", provState, scanState)
	}
	if provBucket != scanBucket {
		t.Errorf("bucket-keys JSON differs:\n provider: %s scan:     %s", provBucket, scanBucket)
	}
}

func recordGet(t *testing.T, handler func(http.ResponseWriter, *http.Request), url string) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, url, nil)
	w := httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET %s: got %d: %s", url, w.Code, w.Body.String())
	}
	return w.Body.String()
}

// --- GetSyncBucketKeys tests ---

func TestGetSyncBucketKeysMissingParams(t *testing.T) {
	h := newTestHandler(t)
	h.memberList.Add("self", "localhost:8080")

	cases := []struct {
		url  string
		desc string
	}{
		{"/sync/bucket-keys", "both missing"},
		{"/sync/bucket-keys?vnode=123", "bucket missing"},
		{"/sync/bucket-keys?bucket=0", "vnode missing"},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.url, nil)
			w := httptest.NewRecorder()
			h.GetSyncBucketKeys(w, req)
			if w.Code != http.StatusBadRequest {
				t.Errorf("expected 400, got %d", w.Code)
			}
		})
	}
}

func TestGetSyncBucketKeysInvalidParams(t *testing.T) {
	h := newTestHandler(t)
	h.memberList.Add("self", "localhost:8080")

	cases := []struct {
		url  string
		desc string
	}{
		{"/sync/bucket-keys?vnode=notanumber&bucket=0", "non-numeric vnode"},
		{"/sync/bucket-keys?vnode=123&bucket=notanumber", "non-numeric bucket"},
		{fmt.Sprintf("/sync/bucket-keys?vnode=123&bucket=%d", merkle.BucketCount), "bucket == BucketCount"},
		{"/sync/bucket-keys?vnode=123&bucket=-1", "negative bucket"},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.url, nil)
			w := httptest.NewRecorder()
			h.GetSyncBucketKeys(w, req)
			if w.Code != http.StatusBadRequest {
				t.Errorf("%s: expected 400, got %d", tc.desc, w.Code)
			}
		})
	}
}

func TestGetSyncBucketKeysUnknownVnode(t *testing.T) {
	h := newTestHandler(t)
	h.memberList.Add("self", "localhost:8080")

	req := httptest.NewRequest(http.MethodGet, "/sync/bucket-keys?vnode=9999999&bucket=0", nil)
	w := httptest.NewRecorder()
	h.GetSyncBucketKeys(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404 for unknown vnode, got %d", w.Code)
	}
}

func TestGetSyncBucketKeysEmptyBucket(t *testing.T) {
	h := newTestHandler(t)
	h.memberList.Add("self", "localhost:8080")

	ranges := h.ring.GetPrimaryVnodeRanges("self")
	if len(ranges) == 0 {
		t.Fatal("expected primary vnode ranges")
	}
	url := fmt.Sprintf("/sync/bucket-keys?vnode=%d&bucket=0", ranges[0].End)

	req := httptest.NewRequest(http.MethodGet, url, nil)
	w := httptest.NewRecorder()
	h.GetSyncBucketKeys(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp SyncBucketKeysResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Keys == nil {
		t.Error("expected non-nil empty slice, got nil")
	}
	if len(resp.Keys) != 0 {
		t.Errorf("expected 0 keys in empty bucket, got %d: %v", len(resp.Keys), resp.Keys)
	}
}

func TestGetSyncBucketKeysReturnsKeysInBucket(t *testing.T) {
	h := newTestHandler(t)
	h.memberList.Add("self", "localhost:8080")
	h.selfID = "self"

	// Find a vnode range and a key that falls in it.
	ranges := h.ring.GetPrimaryVnodeRanges("self")
	if len(ranges) == 0 {
		t.Fatal("expected primary vnode ranges")
	}
	vr := ranges[0]
	var testKey string
	for i := 0; ; i++ {
		k := fmt.Sprintf("bk-%d", i)
		if vr.Contains(merkle.HashKey(k)) {
			testKey = k
			break
		}
	}
	targetBucket := merkle.BucketIndex(testKey)

	h.store.Put(testKey, "val", store.VectorClockVersion{Clocks: map[string]uint64{"self": 1}})

	url := fmt.Sprintf("/sync/bucket-keys?vnode=%d&bucket=%d", vr.End, targetBucket)
	req := httptest.NewRequest(http.MethodGet, url, nil)
	w := httptest.NewRecorder()
	h.GetSyncBucketKeys(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp SyncBucketKeysResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	found := false
	for _, k := range resp.Keys {
		if k == testKey {
			found = true
		}
	}
	if !found {
		t.Errorf("expected %q in response, got %v", testKey, resp.Keys)
	}
}

func TestGetSyncBucketKeysExcludesWrongBucket(t *testing.T) {
	h := newTestHandler(t)
	h.memberList.Add("self", "localhost:8080")
	h.selfID = "self"

	ranges := h.ring.GetPrimaryVnodeRanges("self")
	if len(ranges) == 0 {
		t.Fatal("expected primary vnode ranges")
	}
	vr := ranges[0]

	// Find two keys in the same vnode range but different buckets.
	var keyA, keyB string
	var bucketA int
	for i := 0; keyA == "" || keyB == ""; i++ {
		k := fmt.Sprintf("xbk-%d", i)
		if !vr.Contains(merkle.HashKey(k)) {
			continue
		}
		b := merkle.BucketIndex(k)
		if keyA == "" {
			keyA = k
			bucketA = b
		} else if b != bucketA {
			keyB = k
		}
	}

	h.store.Put(keyA, "a", store.VectorClockVersion{Clocks: map[string]uint64{"self": 1}})
	h.store.Put(keyB, "b", store.VectorClockVersion{Clocks: map[string]uint64{"self": 1}})

	// Query only bucket A; keyB must not appear.
	url := fmt.Sprintf("/sync/bucket-keys?vnode=%d&bucket=%d", vr.End, bucketA)
	req := httptest.NewRequest(http.MethodGet, url, nil)
	w := httptest.NewRecorder()
	h.GetSyncBucketKeys(w, req)

	var resp SyncBucketKeysResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, k := range resp.Keys {
		if k == keyB {
			t.Errorf("key %q from wrong bucket should not appear in bucket %d response", keyB, bucketA)
		}
	}
}

func TestGetSyncBucketKeysExcludesWrongVnodeRange(t *testing.T) {
	h := newTestHandler(t)
	h.memberList.Add("self", "localhost:8080")
	h.memberList.Add("other", "10.0.0.2:8080")
	h.selfID = "self"

	selfRanges := h.ring.GetPrimaryVnodeRanges("self")
	otherRanges := h.ring.GetPrimaryVnodeRanges("other")
	if len(selfRanges) == 0 || len(otherRanges) == 0 {
		t.Skip("need ranges for both nodes")
	}

	// Find a key that falls in other's range (not self's).
	outsideKey := findKeyInOtherRange(otherRanges, selfRanges)
	h.store.Put(outsideKey, "v", store.VectorClockVersion{Clocks: map[string]uint64{"self": 1}})

	// Query self's first vnode range — the outside key must not appear.
	vr := selfRanges[0]
	url := fmt.Sprintf("/sync/bucket-keys?vnode=%d&bucket=%d", vr.End, merkle.BucketIndex(outsideKey))
	req := httptest.NewRequest(http.MethodGet, url, nil)
	w := httptest.NewRecorder()
	h.GetSyncBucketKeys(w, req)

	var resp SyncBucketKeysResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, k := range resp.Keys {
		if k == outsideKey {
			t.Errorf("key from a different vnode range must not appear in this bucket response")
		}
	}
}

// --- PushSyncEntries tests ---

func TestPushSyncEntriesInvalidBody(t *testing.T) {
	h := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPost, "/sync/push", bytes.NewBufferString("not json"))
	w := httptest.NewRecorder()

	h.PushSyncEntries(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestPushSyncEntriesAppliesValue(t *testing.T) {
	h := newTestHandler(t)
	body := `{"entries":{"fruit":[{"value":"apple","clocks":{"node1":1}}]}}`
	req := httptest.NewRequest(http.MethodPost, "/sync/push", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	h.PushSyncEntries(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	e, ok, _ := h.store.Get("fruit")
	if !ok {
		t.Fatal("pushed entry not found in store")
	}
	if len(e.Siblings) != 1 || e.Siblings[0].Value != "apple" {
		t.Errorf("unexpected store contents: %+v", e.Siblings)
	}
}

func TestPushSyncEntriesAppliesTombstone(t *testing.T) {
	h := newTestHandler(t)
	body := `{"entries":{"k":[{"deleted":true,"clocks":{"node1":1}}]}}`
	req := httptest.NewRequest(http.MethodPost, "/sync/push", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	h.PushSyncEntries(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	e, ok, _ := h.store.Get("k")
	if !ok || !e.Siblings[0].Deleted {
		t.Error("expected tombstone in store after push")
	}
}

func TestPushSyncEntriesMultipleKeys(t *testing.T) {
	h := newTestHandler(t)
	body := `{"entries":{
		"k1":[{"value":"v1","clocks":{"n":1}}],
		"k2":[{"value":"v2","clocks":{"n":1}}]
	}}`
	req := httptest.NewRequest(http.MethodPost, "/sync/push", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	h.PushSyncEntries(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	for _, key := range []string{"k1", "k2"} {
		if _, ok, _ := h.store.Get(key); !ok {
			t.Errorf("expected %q in store after push", key)
		}
	}
}

func TestPushSyncEntriesDominatedEntryDropped(t *testing.T) {
	h := newTestHandler(t)
	// Seed a newer value first.
	h.store.Put("k", "new", store.VectorClockVersion{Clocks: map[string]uint64{"n": 2}})

	// Push an older version — must be silently ignored.
	body := `{"entries":{"k":[{"value":"old","clocks":{"n":1}}]}}`
	req := httptest.NewRequest(http.MethodPost, "/sync/push", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	h.PushSyncEntries(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	e, _, _ := h.store.Get("k")
	if len(e.Siblings) != 1 || e.Siblings[0].Value != "new" {
		t.Errorf("dominated push should not overwrite newer local value: %+v", e.Siblings)
	}
}

func TestPushSyncEntriesEmptyBody(t *testing.T) {
	h := newTestHandler(t)
	body := `{"entries":{}}`
	req := httptest.NewRequest(http.MethodPost, "/sync/push", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	h.PushSyncEntries(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("expected 204 for empty push, got %d", w.Code)
	}
}

// ---------------------------------------------------------------------------
// mergeResponses
// ---------------------------------------------------------------------------

func TestMergeResponses_Empty(t *testing.T) {
	if got := mergeResponses(nil); got != nil {
		t.Errorf("expected nil for empty input, got %v", got)
	}
}

func TestMergeResponses_NoValue(t *testing.T) {
	// Responses that carry no value (key not found on replica).
	responses := []NodeResponse{{ID: "n1"}, {ID: "n2"}}
	if got := mergeResponses(responses); got != nil {
		t.Errorf("expected nil when no replica has value, got %v", got)
	}
}

func TestMergeResponses_SingleWinner(t *testing.T) {
	responses := []NodeResponse{
		{ID: "n1", Value: "v1", Clocks: map[string]uint64{"n1": 2}},
		{ID: "n2", Value: "v1", Clocks: map[string]uint64{"n1": 1}}, // dominated
	}
	got := mergeResponses(responses)
	if len(got) != 1 {
		t.Fatalf("expected 1 survivor, got %d", len(got))
	}
	if got[0].Value != "v1" || got[0].Clocks["n1"] != 2 {
		t.Errorf("unexpected winner: %+v", got[0])
	}
}

func TestMergeResponses_Dedup(t *testing.T) {
	// Same clock from two replicas → deduplicated to one survivor.
	clocks := map[string]uint64{"n1": 1}
	responses := []NodeResponse{
		{ID: "n1", Value: "v", Clocks: clocks},
		{ID: "n2", Value: "v", Clocks: clocks},
	}
	got := mergeResponses(responses)
	if len(got) != 1 {
		t.Fatalf("expected 1 survivor after dedup, got %d", len(got))
	}
}

func TestMergeResponses_Siblings(t *testing.T) {
	// Concurrent clocks: neither dominates the other.
	responses := []NodeResponse{
		{ID: "n1", Value: "alice", Clocks: map[string]uint64{"n1": 1}},
		{ID: "n2", Value: "bob", Clocks: map[string]uint64{"n2": 1}},
	}
	got := mergeResponses(responses)
	if len(got) != 2 {
		t.Fatalf("expected 2 siblings, got %d: %v", len(got), got)
	}
}

func TestMergeResponses_Tombstone(t *testing.T) {
	responses := []NodeResponse{
		{ID: "n1", Deleted: true, Clocks: map[string]uint64{"n1": 2}},
		{ID: "n2", Value: "v", Clocks: map[string]uint64{"n1": 1}}, // dominated
	}
	got := mergeResponses(responses)
	if len(got) != 1 || !got[0].Deleted {
		t.Errorf("expected single tombstone survivor, got %v", got)
	}
}

// ---------------------------------------------------------------------------
// staleReplicas
// ---------------------------------------------------------------------------

func TestStaleReplicas_NoneStale(t *testing.T) {
	clocks := map[string]uint64{"n1": 1}
	responses := []NodeResponse{
		{ID: "n1", Value: "v", Clocks: clocks},
		{ID: "n2", Value: "v", Clocks: clocks},
	}
	survivors := []SiblingResponse{{Value: "v", Clocks: clocks}}
	addrByID := map[string]string{"n1": "addr1", "n2": "addr2"}

	stale := staleReplicas(responses, survivors, addrByID)
	if len(stale) != 0 {
		t.Errorf("expected no stale replicas, got %v", stale)
	}
}

func TestStaleReplicas_OneStale(t *testing.T) {
	winner := map[string]uint64{"n1": 2}
	old := map[string]uint64{"n1": 1}
	responses := []NodeResponse{
		{ID: "n1", Value: "new", Clocks: winner},
		{ID: "n2", Value: "old", Clocks: old}, // stale
	}
	survivors := []SiblingResponse{{Value: "new", Clocks: winner}}
	addrByID := map[string]string{"n1": "addr1", "n2": "addr2"}

	stale := staleReplicas(responses, survivors, addrByID)
	if len(stale) != 1 {
		t.Fatalf("expected 1 stale replica, got %v", stale)
	}
	if addr, ok := stale["n2"]; !ok || addr != "addr2" {
		t.Errorf("expected n2 stale at addr2, got %v", stale)
	}
}

func TestStaleReplicas_MissingSibling(t *testing.T) {
	// n2 has one sibling; n1 has both. n2 is stale (missing one concurrent version).
	c1 := map[string]uint64{"n1": 1}
	c2 := map[string]uint64{"n2": 1}
	responses := []NodeResponse{
		{
			ID:       "n1",
			Siblings: []SiblingResponse{{Value: "alice", Clocks: c1}, {Value: "bob", Clocks: c2}},
		},
		{
			ID:    "n2",
			Value: "alice", Clocks: c1, // only has one of the two concurrent writes
		},
	}
	survivors := []SiblingResponse{
		{Value: "alice", Clocks: c1},
		{Value: "bob", Clocks: c2},
	}
	addrByID := map[string]string{"n1": "addr1", "n2": "addr2"}

	stale := staleReplicas(responses, survivors, addrByID)
	if _, ok := stale["n2"]; !ok {
		t.Errorf("expected n2 to be stale (missing sibling), got %v", stale)
	}
	if _, ok := stale["n1"]; ok {
		t.Errorf("n1 should not be stale, got %v", stale)
	}
}

func TestStaleReplicas_EmptyResponseIsStale(t *testing.T) {
	// Replica never had the key at all.
	clocks := map[string]uint64{"n1": 1}
	responses := []NodeResponse{
		{ID: "n1", Value: "v", Clocks: clocks},
		{ID: "n2"}, // key not found on n2
	}
	survivors := []SiblingResponse{{Value: "v", Clocks: clocks}}
	addrByID := map[string]string{"n1": "addr1", "n2": "addr2"}

	stale := staleReplicas(responses, survivors, addrByID)
	if _, ok := stale["n2"]; !ok {
		t.Errorf("expected empty-response replica to be stale, got %v", stale)
	}
}

func TestStaleReplicas_SelfStale(t *testing.T) {
	winner := map[string]uint64{"n2": 1}
	old := map[string]uint64{"n1": 1}
	responses := []NodeResponse{
		{ID: "self", Value: "old", Clocks: old},
		{ID: "n2", Value: "new", Clocks: winner},
	}
	survivors := []SiblingResponse{{Value: "new", Clocks: winner}}
	addrByID := map[string]string{"self": "selfaddr", "n2": "addr2"}

	stale := staleReplicas(responses, survivors, addrByID)
	if addr, ok := stale["self"]; !ok || addr != "selfaddr" {
		t.Errorf("expected self in stale map, got %v", stale)
	}
}

// ---------------------------------------------------------------------------
// repairReplicas
// ---------------------------------------------------------------------------

func TestRepairReplicas_SelfRepair(t *testing.T) {
	h := newTestHandler(t)
	// Seed with old value.
	h.store.Put("k", "old", store.VectorClockVersion{Clocks: map[string]uint64{"n1": 1}})

	winner := map[string]uint64{"n1": 2}
	survivors := []SiblingResponse{{Value: "new", Clocks: winner}}
	stale := map[string]string{h.selfID: "unused-addr"}

	h.repairReplicas("k", survivors, stale)

	entry, ok, _ := h.store.Get("k")
	if !ok {
		t.Fatal("key should exist after self-repair")
	}
	if len(entry.Siblings) != 1 || entry.Siblings[0].Value != "new" {
		t.Errorf("self-repair did not apply winner: %+v", entry)
	}
}

func TestRepairReplicas_SelfRepairTombstone(t *testing.T) {
	h := newTestHandler(t)
	h.store.Put("k", "alive", store.VectorClockVersion{Clocks: map[string]uint64{"n1": 1}})

	tombClock := map[string]uint64{"n1": 2}
	survivors := []SiblingResponse{{Deleted: true, Clocks: tombClock}}
	stale := map[string]string{h.selfID: "unused-addr"}

	h.repairReplicas("k", survivors, stale)

	entry, ok, _ := h.store.Get("k")
	if !ok {
		t.Fatal("key should exist (as tombstone) after self-repair")
	}
	if len(entry.Siblings) != 1 || !entry.Siblings[0].Deleted {
		t.Errorf("self-repair should have applied tombstone: %+v", entry)
	}
}

func TestRepairReplicas_HTTPRepair(t *testing.T) {
	h := newTestHandler(t)

	var received []string
	replicaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received = append(received, r.Method+" "+r.URL.Path)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(replicaSrv.Close)
	replicaAddr := strings.TrimPrefix(replicaSrv.URL, "http://")

	winner := map[string]uint64{"n1": 1}
	survivors := []SiblingResponse{{Value: "v", Clocks: winner}}
	stale := map[string]string{"remote": replicaAddr}

	h.repairReplicas("mykey", survivors, stale)

	if len(received) != 1 || received[0] != "PUT /keys/mykey" {
		t.Errorf("expected one PUT to replica, got %v", received)
	}
}

func TestRepairReplicas_HTTPRepairSiblings(t *testing.T) {
	// Two surviving siblings should both be pushed to the stale replica.
	h := newTestHandler(t)

	var received []string
	replicaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received = append(received, r.Method)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(replicaSrv.Close)
	replicaAddr := strings.TrimPrefix(replicaSrv.URL, "http://")

	survivors := []SiblingResponse{
		{Value: "alice", Clocks: map[string]uint64{"n1": 1}},
		{Value: "bob", Clocks: map[string]uint64{"n2": 1}},
	}
	stale := map[string]string{"remote": replicaAddr}

	h.repairReplicas("k", survivors, stale)

	if len(received) != 2 {
		t.Errorf("expected 2 repair writes for 2 siblings, got %d: %v", len(received), received)
	}
}

func TestDeliverPendingHints_NilHintStore(t *testing.T) {
	h := newTestHandler(t)  // hintStore is nil
	h.DeliverPendingHints() // must not panic
}

func TestDeliverPendingHints_NodeNotInMemberList(t *testing.T) {
	r := ring.NewRing(10)
	ml := newTestMemberList(r)
	s := store.New()
	hs := hintstore.New(100, time.Hour)
	h := NewHandler(r, ml, s, HandlerConfig{SelfID: "self", ReplicationFactor: 3, WriteQuorum: 1, ReadQuorum: 1, ReplicaTimeout: time.Second}, hs)

	hs.Store("unknown-node", hintstore.Hint{
		Key:    "k",
		Value:  "v",
		Clocks: map[string]uint64{"n1": 1},
		At:     time.Now(),
	})

	h.DeliverPendingHints()

	if nodes := hs.PendingNodes(); len(nodes) != 1 || nodes[0] != "unknown-node" {
		t.Errorf("hint for unknown-node should remain, pending=%v", nodes)
	}
}

func TestDeliverPendingHints_NodeDeadInMemberList(t *testing.T) {
	r := ring.NewRing(10)
	ml := newTestMemberList(r)
	s := store.New()
	hs := hintstore.New(100, time.Hour)
	h := NewHandler(r, ml, s, HandlerConfig{SelfID: "self", ReplicationFactor: 3, WriteQuorum: 1, ReadQuorum: 1, ReplicaTimeout: time.Second}, hs)

	ml.Add("dead-node", "10.0.0.99")
	ml.MarkDead("dead-node")

	hs.Store("dead-node", hintstore.Hint{
		Key:    "k",
		Value:  "v",
		Clocks: map[string]uint64{"n1": 1},
		At:     time.Now(),
	})

	h.DeliverPendingHints()

	if nodes := hs.PendingNodes(); len(nodes) != 1 || nodes[0] != "dead-node" {
		t.Errorf("hint for dead-node should remain, pending=%v", nodes)
	}
}

func TestDeliverHints_DeletedHint(t *testing.T) {
	targetRing := ring.NewRing(10)
	targetML := gossip.NewMemberList("target", "localhost", nil)
	targetStore := store.New()
	targetSrv := httptest.NewServer(NewServer(targetRing, targetML, targetStore, HandlerConfig{SelfID: "target", ReplicationFactor: 1, WriteQuorum: 1, ReadQuorum: 1, ReplicaTimeout: time.Second}, nil))
	defer targetSrv.Close()

	r := ring.NewRing(10)
	ml := newTestMemberList(r)
	s := store.New()
	hs := hintstore.New(100, time.Hour)
	h := NewHandler(r, ml, s, HandlerConfig{SelfID: "self", ReplicationFactor: 3, WriteQuorum: 1, ReadQuorum: 1, ReplicaTimeout: time.Second}, hs)

	targetAddr := targetSrv.Listener.Addr().String()
	ml.Add("target-node", targetAddr)

	hs.Store("target-node", hintstore.Hint{
		Key:     "hint-key",
		Deleted: true,
		Clocks:  map[string]uint64{"self": 1},
		At:      time.Now(),
	})

	h.DeliverPendingHints()

	if nodes := hs.PendingNodes(); len(nodes) != 0 {
		t.Errorf("deleted hint should be drained after delivery, pending=%v", nodes)
	}
}

// TestDeliverHints_ReBuffersUndelivered pins the behavior that makes periodic
// delivery safe against a still-unreachable target (e.g. an asymmetric
// partition): a hint that fails to deliver is put back rather than dropped,
// and its original timestamp is preserved so its TTL is not reset by the retry.
func TestDeliverHints_ReBuffersUndelivered(t *testing.T) {
	// A server that is immediately closed yields connection-refused on delivery.
	deadSrv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadAddr := deadSrv.Listener.Addr().String()
	deadSrv.Close()

	r := ring.NewRing(10)
	ml := newTestMemberList(r)
	s := store.New()
	hs := hintstore.New(100, time.Hour)
	h := NewHandler(r, ml, s, HandlerConfig{SelfID: "self", ReplicationFactor: 3, WriteQuorum: 1, ReadQuorum: 1, ReplicaTimeout: time.Second}, hs)

	ml.Add("target-node", deadAddr)

	originalAt := time.Now().Add(-30 * time.Minute)
	hs.Store("target-node", hintstore.Hint{
		Key:    "hint-key",
		Value:  "hint-value",
		Clocks: map[string]uint64{"self": 1},
		At:     originalAt,
	})

	h.DeliverPendingHints() // delivery fails; hint must be re-buffered

	if nodes := hs.PendingNodes(); len(nodes) != 1 || nodes[0] != "target-node" {
		t.Fatalf("undelivered hint should remain buffered, pending=%v", nodes)
	}
	remaining := hs.Drain("target-node")
	if len(remaining) != 1 {
		t.Fatalf("expected 1 re-buffered hint, got %d", len(remaining))
	}
	if !remaining[0].At.Equal(originalAt) {
		t.Errorf("re-buffered hint timestamp reset: got %v, want %v (TTL must count from original write)", remaining[0].At, originalAt)
	}
}

func TestDeliverPendingHints_AliveNodeDeliversHints(t *testing.T) {
	targetRing := ring.NewRing(10)
	targetML := gossip.NewMemberList("target", "localhost", nil)
	targetStore := store.New()
	targetSrv := httptest.NewServer(NewServer(targetRing, targetML, targetStore, HandlerConfig{SelfID: "target", ReplicationFactor: 1, WriteQuorum: 1, ReadQuorum: 1, ReplicaTimeout: time.Second}, nil))
	defer targetSrv.Close()

	r := ring.NewRing(10)
	ml := newTestMemberList(r)
	s := store.New()
	hs := hintstore.New(100, time.Hour)
	h := NewHandler(r, ml, s, HandlerConfig{SelfID: "self", ReplicationFactor: 3, WriteQuorum: 1, ReadQuorum: 1, ReplicaTimeout: time.Second}, hs)

	targetAddr := targetSrv.Listener.Addr().String()
	ml.Add("target-node", targetAddr)

	hs.Store("target-node", hintstore.Hint{
		Key:    "hint-key",
		Value:  "hint-value",
		Clocks: map[string]uint64{"self": 1},
		At:     time.Now(),
	})

	h.DeliverPendingHints()

	if nodes := hs.PendingNodes(); len(nodes) != 0 {
		t.Errorf("hints should be drained after successful delivery, pending=%v", nodes)
	}
}

func TestNodeStatus_Unknown(t *testing.T) {
	h := newTestHandler(t)
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

func TestGetSyncState_UnknownVnode(t *testing.T) {
	h := newTestHandler(t)
	h.memberList.Add("node1", "10.0.0.1")

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

// findKeyInOtherRange returns the first key that falls in otherRanges but not
// in selfRanges, by iterating candidate keys until one is found.
func findKeyInOtherRange(otherRanges, selfRanges []ring.VnodeRange) string {
	for i := 0; ; i++ {
		k := fmt.Sprintf("out-%d", i)
		kh := merkle.HashKey(k)
		inOther, inSelf := false, false
		for _, vr := range otherRanges {
			if vr.Contains(kh) {
				inOther = true
			}
		}
		for _, vr := range selfRanges {
			if vr.Contains(kh) {
				inSelf = true
			}
		}
		if inOther && !inSelf {
			return k
		}
	}
}

// --- Coverage gap tests ---

func TestPutKey_EmptyKey(t *testing.T) {
	h := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPut, "/keys/", bytes.NewBufferString(`{"value":"v"}`))
	w := httptest.NewRecorder()
	h.PutKey(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for empty key, got %d", w.Code)
	}
}

func TestPutKey_InvalidBody(t *testing.T) {
	h := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPut, "/keys/mykey", bytes.NewBufferString("not-json"))
	w := httptest.NewRecorder()
	h.PutKey(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid body, got %d", w.Code)
	}
}

func TestDeleteKey_InvalidBody(t *testing.T) {
	h := newTestHandler(t)
	req := httptest.NewRequest(http.MethodDelete, "/keys/mykey", bytes.NewBufferString("not-json"))
	w := httptest.NewRecorder()
	h.DeleteKey(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid body, got %d", w.Code)
	}
}

func TestRepairReplicas_HTTPRepairTombstone(t *testing.T) {
	h := newTestHandler(t)
	var method string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	survivors := []SiblingResponse{{Deleted: true, Clocks: map[string]uint64{"n1": 2}}}
	h.repairReplicas("mykey", survivors, map[string]string{"remote": addr})

	if method != http.MethodDelete {
		t.Errorf("expected DELETE for tombstone repair, got %q", method)
	}
}

func TestRepairReplicas_HTTPRepairError(t *testing.T) {
	h := newTestHandler(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	survivors := []SiblingResponse{{Value: "v", Clocks: map[string]uint64{"n1": 1}}}
	h.repairReplicas("mykey", survivors, map[string]string{"remote": addr}) // error is only logged
}

func TestGetNode_AllNodesBootstrapping(t *testing.T) {
	h := newTestHandler(t)
	h.memberList.Add("nodeA", "10.0.0.1:8080")
	h.memberList.Add("nodeB", "10.0.0.2:8080")
	h.memberList.SetBootstrapping("nodeA", true)
	h.memberList.SetBootstrapping("nodeB", true)

	req := httptest.NewRequest(http.MethodGet, "/keys/anykey", nil)
	w := httptest.NewRecorder()
	h.GetNode(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 when all read nodes are bootstrapping, got %d", w.Code)
	}
}

func TestDeliverHints_DirectNilHintStore(t *testing.T) {
	h := newTestHandler(t)                   // hintStore is nil
	h.DeliverHints("node1", "10.0.0.1:8080") // must not panic
}

func TestDeliverHints_DirectEmptyHints(t *testing.T) {
	r := ring.NewRing(10)
	ml := newTestMemberList(r)
	s := store.New()
	hs := hintstore.New(100, time.Hour)
	h := NewHandler(r, ml, s, HandlerConfig{SelfID: "self", ReplicationFactor: 1, WriteQuorum: 1, ReadQuorum: 1, ReplicaTimeout: time.Second}, hs)
	// No hints buffered for this node; Drain returns empty.
	h.DeliverHints("no-hints-node", "10.0.0.1:8080") // must not panic
}

func TestDeliverHints_DirectDeliveryError(t *testing.T) {
	r := ring.NewRing(10)
	ml := newTestMemberList(r)
	s := store.New()
	hs := hintstore.New(100, time.Hour)
	h := NewHandler(r, ml, s, HandlerConfig{SelfID: "self", ReplicationFactor: 1, WriteQuorum: 1, ReadQuorum: 1, ReplicaTimeout: time.Second}, hs)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	hs.Store("fail-node", hintstore.Hint{
		Key:    "k",
		Value:  "v",
		Clocks: map[string]uint64{"self": 1},
		At:     time.Now(),
	})
	h.DeliverHints("fail-node", addr) // error is only logged, must not panic
}

func TestBufferHints_RemainingGoroutine(t *testing.T) {
	r := ring.NewRing(10)
	ml := newTestMemberList(r)
	s := store.New()
	hs := hintstore.New(100, time.Hour)
	h := NewHandler(r, ml, s, HandlerConfig{SelfID: "self", ReplicationFactor: 1, WriteQuorum: 1, ReadQuorum: 1, ReplicaTimeout: time.Second}, hs)

	ch := make(chan replicaResult, 1)
	ch <- replicaResult{nodeID: "late-node", err: fmt.Errorf("timeout")}

	h.bufferHints(
		hintstore.Hint{Key: "k", Value: "v", Clocks: map[string]uint64{"self": 1}},
		nil, 1, ch,
	)

	// Goroutine drains ch and stores the hint asynchronously.
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		if len(hs.PendingNodes()) > 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	if nodes := hs.PendingNodes(); len(nodes) != 1 || nodes[0] != "late-node" {
		t.Errorf("expected buffered hint for late-node, got %v", nodes)
	}
}
