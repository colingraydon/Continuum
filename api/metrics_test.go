package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"strings"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/health"
)

func resetMetrics() {
	httpRequestsTotal.Reset()
	httpRequestDuration.Reset()
	ringNodeCount.Set(0)
	ringVNodeCount.Set(0)
	ringKeyLookups.Add(0)
	ringVariance.Set(0)
}

func TestMetricsNodeCountGauge(t *testing.T) {
	// Arrange
	resetMetrics()
	h := newTestHandler()

	// Act
	h.ring.AddNode("node1", "10.0.0.1")
	ringNodeCount.Set(float64(h.ring.NodeCount()))

	// Assert
	if err := testutil.CollectAndCompare(ringNodeCount, strings.NewReader(`
		# HELP continuum_ring_node_count Current number of physical nodes in the ring
		# TYPE continuum_ring_node_count gauge
		continuum_ring_node_count 1
	`)); err != nil {
		t.Errorf("unexpected metric value: %v", err)
	}
}

func TestMetricsHTTPRequestsTotal(t *testing.T) {
	// Arrange
	resetMetrics()
	srv := NewServer(ring.NewRing(10), health.NewChecker(health.DefaultConfig(), nil))
	req := httptest.NewRequest(http.MethodGet, "/nodes", nil)
	w := httptest.NewRecorder()

	// Act
	srv.ServeHTTP(w, req)

	// Assert
	count := testutil.ToFloat64(httpRequestsTotal.With(prometheus.Labels{
		"method": "GET",
		"path":   "/nodes",
		"status": "200",
	}))
	if count != 1 {
		t.Errorf("expected 1 request, got %f", count)
	}
}

func TestMetricsRequestDurationRecorded(t *testing.T) {
	// Arrange
	resetMetrics()
	srv := NewServer(ring.NewRing(10), health.NewChecker(health.DefaultConfig(), nil))
	req := httptest.NewRequest(http.MethodGet, "/nodes", nil)
	w := httptest.NewRecorder()

	// Act
	srv.ServeHTTP(w, req)

	// Assert
	count := testutil.ToFloat64(httpRequestsTotal.With(prometheus.Labels{
		"method": "GET",
		"path":   "/nodes",
		"status": "200",
	}))
	if count != 1 {
		t.Errorf("expected 1 request recorded, got %f", count)
	}
}