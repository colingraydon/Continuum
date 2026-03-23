package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/health"
)

func TestRoutes(t *testing.T) {
	// Arrange
	srv := NewServer(ring.NewRing(50), health.NewChecker(health.DefaultConfig(), nil))

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