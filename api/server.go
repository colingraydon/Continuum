package api

import (
	"net/http"

	"github.com/colingraydon/continuum/internal/ring"
)

func NewServer(r *ring.Ring) *http.ServeMux {
	h := NewHandler(r)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /nodes", h.AddNode)
	mux.HandleFunc("DELETE /nodes/", h.RemoveNode)
	mux.HandleFunc("GET /nodes", h.GetNodes)
	mux.HandleFunc("GET /keys/", h.GetNode)
	return mux
}