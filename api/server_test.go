package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/gossip"
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

func TestRoutes(t *testing.T) {
	// Arrange
	r := ring.NewRing(50)
	ml := gossip.NewMemberList("self", "localhost", func(m *gossip.Member, status gossip.MemberStatus) {
		switch status {
		case gossip.MemberAlive:
			r.AddNode(m.ID, m.Address)
		case gossip.MemberDead:
			r.RemoveNode(m.ID)
		}
	})
	s := store.New()
	srv := NewServer(r, ml, s, HandlerConfig{SelfID: "self", ReplicationFactor: 3, WriteQuorum: 1, ReadQuorum: 1, ReplicaTimeout: time.Second}, nil)

	tests := []struct {
		name   string
		method string
		path   string
		status int
	}{
		{"add node", http.MethodPost, "/nodes", http.StatusBadRequest},
		{"get nodes", http.MethodGet, "/nodes", http.StatusOK},
		{"get node by key", http.MethodGet, "/keys/mykey", http.StatusServiceUnavailable},
		{"remove node missing id", http.MethodDelete, "/nodes/", http.StatusBadRequest},
		{"not found", http.MethodGet, "/nonexistent", http.StatusNotFound},
		{"get stats", http.MethodGet, "/stats", http.StatusOK},
		{"replicate", http.MethodPost, "/replicate", http.StatusBadRequest},
		{"health", http.MethodGet, "/health", http.StatusOK},
		{"gossip", http.MethodPost, "/gossip", http.StatusBadRequest},
		{"sync state missing vnode", http.MethodGet, "/sync", http.StatusBadRequest},
		{"sync keys", http.MethodPost, "/sync/keys", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			req := httptest.NewRequest(tt.method, tt.path, nil)
			w := httptest.NewRecorder()

			// Act
			srv.ServeHTTP(w, req)

			// Assert
			if w.Code != tt.status {
				t.Errorf("expected %d, got %d", tt.status, w.Code)
			}
		})
	}
}
