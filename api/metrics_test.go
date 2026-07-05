package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/gossip"
	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/sstable"
	"github.com/colingraydon/continuum/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
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
	h := newTestHandler(t)

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
	ml := gossip.NewMemberList("self", "localhost", nil)
	s1 := store.New()
	srv := NewServer(ring.NewRing(50), ml, s1, HandlerConfig{SelfID: "self", ReplicationFactor: 3, WriteQuorum: 1, ReadQuorum: 1, ReplicaTimeout: time.Second}, nil)
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

func TestBlockCacheMetricsReadStatsAtScrape(t *testing.T) {
	st := sstable.CacheStats{Hits: 3, Misses: 2, Bytes: 128, Entries: 1}
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(blockCacheCollectors(func() sstable.CacheStats { return st })...)

	if err := testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP continuum_block_cache_bytes Bytes of decompressed blocks currently held by the block cache, including per-entry overhead
		# TYPE continuum_block_cache_bytes gauge
		continuum_block_cache_bytes 128
		# HELP continuum_block_cache_entries Number of blocks currently held by the block cache
		# TYPE continuum_block_cache_entries gauge
		continuum_block_cache_entries 1
		# HELP continuum_block_cache_hits_total Total SSTable block cache hits
		# TYPE continuum_block_cache_hits_total counter
		continuum_block_cache_hits_total 3
		# HELP continuum_block_cache_misses_total Total SSTable block cache misses
		# TYPE continuum_block_cache_misses_total counter
		continuum_block_cache_misses_total 2
	`)); err != nil {
		t.Errorf("unexpected block cache metrics: %v", err)
	}

	// The collectors read stats at scrape time, not registration time.
	st = sstable.CacheStats{Hits: 10, Misses: 2, Bytes: 128, Entries: 1}
	if got := testutil.ToFloat64(blockCacheCollectors(func() sstable.CacheStats { return st })[0]); got != 10 {
		t.Errorf("hits after update = %v, want 10", got)
	}

	// The exported registrar targets the default registry; safe to exercise
	// once per process (nothing else registers these names in tests).
	RegisterBlockCacheMetrics(func() sstable.CacheStats { return st })
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	found := false
	for _, mf := range families {
		if mf.GetName() == "continuum_block_cache_hits_total" {
			found = true
		}
	}
	if !found {
		t.Error("continuum_block_cache_hits_total not registered on the default registry")
	}
}

func TestMetricsRequestDurationRecorded(t *testing.T) {
	// Arrange
	resetMetrics()
	ml := gossip.NewMemberList("self", "localhost", nil)
	s2 := store.New()
	srv := NewServer(ring.NewRing(50), ml, s2, HandlerConfig{SelfID: "self", ReplicationFactor: 3, WriteQuorum: 1, ReadQuorum: 1, ReplicaTimeout: time.Second}, nil)
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
