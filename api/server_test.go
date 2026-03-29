package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/colingraydon/continuum/internal/gossip"
	"github.com/colingraydon/continuum/internal/ring"
)

func TestRoutes(t *testing.T) {
	// Arrange
	ml := gossip.NewMemberList("self", "localhost", nil)
	transport, err := gossip.NewTransport("0")
	if err != nil {
		t.Fatalf("failed to create transport: %v", err)
	}
	defer transport.Stop()
	g := gossip.NewGossiper("self", "0", ml, transport)
	srv := NewServer(ring.NewRing(50), ml, g, "self")

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