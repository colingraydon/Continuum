package api

import (
	"net/http"

	"github.com/colingraydon/continuum/internal/ring"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func NewServer(r *ring.Ring) http.Handler {
	h := NewHandler(r)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /nodes", h.AddNode)
	mux.HandleFunc("DELETE /nodes/", h.RemoveNode)
	mux.HandleFunc("GET /nodes", h.GetNodes)
	mux.HandleFunc("GET /keys/", h.GetNode)
	mux.HandleFunc("GET /stats", h.GetStats)
	mux.HandleFunc("POST /replicate", h.GetReplicationNodes)
	mux.HandleFunc("GET /health", h.Health)
	mux.Handle("GET /metrics", promhttp.Handler())
	return metricsMiddleware(mux)
}