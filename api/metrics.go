package api

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	httpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "continuum_http_requests_total",
			Help: "Total number of HTTP requests by method, path, and status code",
		},
		[]string{"method", "path", "status"},
	)

	httpRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "continuum_http_request_duration_seconds",
			Help:    "HTTP request latency by method and path",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)

	ringNodeCount = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "continuum_ring_node_count",
			Help: "Current number of physical nodes in the ring",
		},
	)

	ringVNodeCount = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "continuum_ring_vnode_count",
			Help: "Current number of virtual nodes in the ring",
		},
	)

	ringKeyLookups = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "continuum_ring_key_lookups_total",
			Help: "Total number of key lookups performed",
		},
	)

	ringVariance = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "continuum_ring_distribution_variance",
			Help: "Current variance of key distribution across nodes",
		},
	)
)

func UpdateRingMetrics(nodeCount, vnodeCount int) {
	ringNodeCount.Set(float64(nodeCount))
	ringVNodeCount.Set(float64(vnodeCount))
}

func RecordKeyLookup() {
	ringKeyLookups.Inc()
}

func RecordVariance(variance float64) {
	ringVariance.Set(variance)
}