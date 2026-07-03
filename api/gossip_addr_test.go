package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestAddNodeWithGossipAddress verifies that POST /nodes forwards the optional
// gossip_address to the member list so gossip reaches nodes on heterogeneous
// ports (e.g. local multi-process clusters).
func TestAddNodeWithGossipAddress(t *testing.T) {
	h := newTestHandler(t)
	body := `{"id": "node1", "address": "10.0.0.1:8080", "gossip_address": "10.0.0.1:9555"}`
	req := httptest.NewRequest(http.MethodPost, "/nodes", bytes.NewBufferString(body))
	w := httptest.NewRecorder()

	h.AddNode(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	m, ok := h.memberList.Get("node1")
	if !ok {
		t.Fatal("node1 not in member list")
	}
	if m.GossipAddr != "10.0.0.1:9555" {
		t.Errorf("expected gossip addr stored on member, got %q", m.GossipAddr)
	}
}
