package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRoutes(t *testing.T) {
	srv := NewServer(); 

	tests := []struct {
		name	string
		method  string
		path	string
		status	int
	} {
		{"hello route", http.MethodGet, "/hello", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			req := httptest.NewRequest(tt.method, tt.path, nil);
			w := httptest.NewRecorder()

			// Act
			srv.ServeHTTP(w, req);

			// Assert
			if w.Code != tt.status {
				t.Errorf("expected %d, got %d", tt.status, w.Code)
			}
		})
	}
}