package api

import (
	"net/http"

	"github.com/colingraydon/continuum/internal/gossip"
	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/store"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func NewServer(r *ring.Ring, ml *gossip.MemberList, g *gossip.Gossiper, s *store.Store, selfID string, replicationFactor int) http.Handler {
	h := NewHandler(r, ml, g, s, selfID, replicationFactor)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /nodes", h.AddNode)
	mux.HandleFunc("DELETE /nodes/", h.RemoveNode)
	mux.HandleFunc("GET /nodes", h.GetNodes)
	mux.HandleFunc("GET /keys/", h.GetNode)
	mux.HandleFunc("PUT /keys/", h.PutKey)
	mux.HandleFunc("GET /stats", h.GetStats)
	mux.HandleFunc("POST /replicate", h.GetReplicationNodes)
	mux.HandleFunc("GET /health", h.Health)
	mux.HandleFunc("POST /gossip", h.Gossip)
	mux.Handle("GET /metrics", promhttp.Handler())
	return metricsMiddleware(mux)
}