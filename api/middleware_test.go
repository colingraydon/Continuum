package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestUpdateRingMetrics(t *testing.T) {
	// Arrange
	resetMetrics()

	// Act
	UpdateRingMetrics(3, 450)

	// Assert
	if got := testutil.ToFloat64(ringNodeCount); got != 3 {
		t.Errorf("expected node count 3, got %f", got)
	}
	if got := testutil.ToFloat64(ringVNodeCount); got != 450 {
		t.Errorf("expected vnode count 450, got %f", got)
	}
}

func TestMiddlewareRecordsRequestCount(t *testing.T) {
	// Arrange
	resetMetrics()
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := metricsMiddleware(next)
	req := httptest.NewRequest(http.MethodGet, "/nodes", nil)
	w := httptest.NewRecorder()

	// Act
	handler.ServeHTTP(w, req)

	// Assert
	count := testutil.ToFloat64(httpRequestsTotal.WithLabelValues("GET", "/nodes", "200"))
	if count != 1 {
		t.Errorf("expected 1 request, got %f", count)
	}
}

func TestMiddlewareRecordsDuration(t *testing.T) {
	// Arrange
	resetMetrics()
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := metricsMiddleware(next)
	req := httptest.NewRequest(http.MethodGet, "/nodes", nil)
	w := httptest.NewRecorder()

	// Act
	handler.ServeHTTP(w, req)

	// Assert
	count := testutil.ToFloat64(httpRequestsTotal.WithLabelValues("GET", "/nodes", "200"))
	if count != 1 {
		t.Errorf("expected duration to be recorded alongside request, got %f", count)
	}
}

func TestMiddlewareRecordsErrorStatus(t *testing.T) {
	// Arrange
	resetMetrics()
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	handler := metricsMiddleware(next)
	req := httptest.NewRequest(http.MethodGet, "/nodes", nil)
	w := httptest.NewRecorder()

	// Act
	handler.ServeHTTP(w, req)

	// Assert
	count := testutil.ToFloat64(httpRequestsTotal.WithLabelValues("GET", "/nodes", "500"))
	if count != 1 {
		t.Errorf("expected 1 error request, got %f", count)
	}
}

func TestMiddlewareDefaultsTo200(t *testing.T) {
	// Arrange
	resetMetrics()
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := fmt.Fprintf(w, "ok"); err != nil {
			t.Errorf("failed to write response: %v", err)
		}
	})
	handler := metricsMiddleware(next)
	req := httptest.NewRequest(http.MethodGet, "/nodes", nil)
	w := httptest.NewRecorder()

	// Act
	handler.ServeHTTP(w, req)

	// Assert
	count := testutil.ToFloat64(httpRequestsTotal.WithLabelValues("GET", "/nodes", "200"))
	if count != 1 {
		t.Errorf("expected default 200 status, got %f", count)
	}
}