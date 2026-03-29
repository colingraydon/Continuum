package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/colingraydon/continuum/internal/gossip"
	"github.com/colingraydon/continuum/internal/ring"
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
	return NewHandler(r, ml, newTestGossiper(t, ml), "self")
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