package api

import (
	"net/http"

	"github.com/colingraydon/continuum/internal/gossip"
	"github.com/colingraydon/continuum/internal/hintstore"
	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/store"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// BuildMux registers all routes for h and wraps them in metrics middleware.
// Use this when you need a reference to the Handler separately from the mux
// (e.g. to call DeliverHints from a gossip callback).
func BuildMux(h *Handler) http.Handler {
	return newMux(h)
}

func newMux(h *Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /nodes", h.AddNode)
	mux.HandleFunc("DELETE /nodes/", h.RemoveNode)
	mux.HandleFunc("GET /nodes", h.GetNodes)
	mux.HandleFunc("GET /keys/", h.GetNode)
	mux.HandleFunc("PUT /keys/", h.PutKey)
	mux.HandleFunc("DELETE /keys/", h.DeleteKey)
	mux.HandleFunc("GET /stats", h.GetStats)
	mux.HandleFunc("POST /replicate", h.GetReplicationNodes)
	mux.HandleFunc("GET /health", h.Health)
	mux.HandleFunc("POST /gossip", h.Gossip)
	mux.HandleFunc("GET /sync", h.GetSyncState)
	mux.HandleFunc("GET /sync/bucket-keys", h.GetSyncBucketKeys)
	mux.HandleFunc("POST /sync/keys", h.GetSyncKeys)
	mux.HandleFunc("POST /sync/push", h.PushSyncEntries)
	mux.Handle("GET /metrics", promhttp.Handler())
	return metricsMiddleware(mux)
}

func NewServer(r *ring.Ring, ml *gossip.MemberList, s *store.Store, cfg HandlerConfig, hs *hintstore.HintStore) http.Handler {
	return newMux(NewHandler(r, ml, s, cfg, hs))
}
