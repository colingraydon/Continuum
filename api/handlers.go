package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/colingraydon/continuum/internal/ring"
)

type Handler struct {
	ring *ring.Ring
}

func NewHandler(r *ring.Ring) *Handler {
	return &Handler{ring: r}
}

type AddNodeRequest struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

type NodeResponse struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

func (h *Handler) AddNode(w http.ResponseWriter, req *http.Request) {
	var body AddNodeRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if body.ID == "" || body.Address == "" {
		http.Error(w, "id and address are required", http.StatusBadRequest)
		return
	}

	h.ring.AddNode(body.ID, body.Address)

	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(NodeResponse(body)); err != nil {
		http.Error(w, "failed to write response", http.StatusInternalServerError)
	}
}

func (h *Handler) RemoveNode(w http.ResponseWriter, req *http.Request) {
	id := strings.TrimPrefix(req.URL.Path, "/nodes/")
	if id == "" {
		http.Error(w, "node id is required", http.StatusBadRequest)
		return
	}

	h.ring.RemoveNode(id)
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) GetNodes(w http.ResponseWriter, req *http.Request) {
	nodes := h.ring.GetNodes()
	resp := make([]NodeResponse, 0, len(nodes))
	for _, n := range nodes {
		resp = append(resp, NodeResponse{ID: n.ID, Address: n.Address})
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, "failed to write response", http.StatusInternalServerError)
	}
}

func (h *Handler) GetNode(w http.ResponseWriter, req *http.Request) {
	key := strings.TrimPrefix(req.URL.Path, "/keys/")
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	node, found := h.ring.GetNode(key)
	if !found {
		http.Error(w, "no nodes available", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(NodeResponse{ID: node.ID, Address: node.Address}); err != nil {
		http.Error(w, "failed to write response", http.StatusInternalServerError)
	}
}
