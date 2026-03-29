package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
	"io"

	"github.com/colingraydon/continuum/internal/gossip"
	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/stats"
)

type Handler struct {
	ring       *ring.Ring
	aggregator *stats.Aggregator
	memberList *gossip.MemberList
	gossiper   *gossip.Gossiper
	selfID     string
	startTime  time.Time
}

func NewHandler(r *ring.Ring, ml *gossip.MemberList, g *gossip.Gossiper, selfID string) *Handler {
	return &Handler{
		ring:       r,
		aggregator: stats.NewAggregator(r, ml),
		memberList: ml,
		gossiper:   g,
		selfID:     selfID,
		startTime:  time.Now(),
	}
}

type AddNodeRequest struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

type NodeResponse struct {
	ID      string `json:"id"`
	Address string `json:"address"`
	Status  string `json:"status"`
}

type ReplicateRequest struct {
	Key    string `json:"key"`
	Factor int    `json:"factor"`
}

type ReplicateResponse struct {
	Key   string         `json:"key"`
	Nodes []NodeResponse `json:"nodes"`
}

type GossipRequest struct {
	Members []*gossip.Member `json:"members"`
}

func (h *Handler) nodeStatus(id string) string {
	m, ok := h.memberList.Get(id)
	if !ok {
		return "unknown"
	}
	return m.Status.String()
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
	h.memberList.Add(body.ID, body.Address)
	w.WriteHeader(http.StatusCreated)
	node := NodeResponse{ID: body.ID, Address: body.Address, Status: "alive"}
	if err := json.NewEncoder(w).Encode(node); err != nil {
		http.Error(w, "failed to write response", http.StatusInternalServerError)
	}
}

func (h *Handler) RemoveNode(w http.ResponseWriter, req *http.Request) {
	id := strings.TrimPrefix(req.URL.Path, "/nodes/")
	if id == "" {
		http.Error(w, "node id is required", http.StatusBadRequest)
		return
	}
	h.memberList.MarkDead(id)
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) GetNodes(w http.ResponseWriter, req *http.Request) {
	nodes := h.ring.GetNodes()
	resp := make([]NodeResponse, 0, len(nodes))
	for _, n := range nodes {
		resp = append(resp, NodeResponse{
			ID:      n.ID,
			Address: n.Address,
			Status:  h.nodeStatus(n.ID),
		})
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
	RecordKeyLookup()

	// peer routing — if responsible node is not self, proxy the request
	// skip if already proxied to avoid infinite loops
	if node.ID != h.selfID && req.Header.Get("X-Proxied-From") == "" {
		h.proxyRequest(w, req, node.Address, key)
		return
	}

	resp := NodeResponse{ID: node.ID, Address: node.Address, Status: h.nodeStatus(node.ID)}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, "failed to write response", http.StatusInternalServerError)
	}
}

func (h *Handler) proxyRequest(w http.ResponseWriter, req *http.Request, address, key string) {
	url := "http://" + address + "/keys/" + key

	proxyReq, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		http.Error(w, "failed to create proxy request", http.StatusInternalServerError)
		return
	}
	proxyReq.Header.Set("X-Proxied-From", h.selfID)

	resp, err := http.DefaultClient.Do(proxyReq)
	if err != nil {
		http.Error(w, "failed to proxy request to peer", http.StatusBadGateway)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)

	if _, err := io.Copy(w, resp.Body); err != nil {
		return
	}
}

func (h *Handler) GetStats(w http.ResponseWriter, req *http.Request) {
	s := h.aggregator.GetStats()
	RecordVariance(s.Variance)
	RecordHealthStats(s.HealthyNodes, s.SuspectNodes, s.DeadNodes)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(s); err != nil {
		http.Error(w, "failed to write response", http.StatusInternalServerError)
	}
}

func (h *Handler) GetReplicationNodes(w http.ResponseWriter, req *http.Request) {
	var body ReplicateRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if body.Key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}
	if body.Factor < 1 {
		http.Error(w, "factor must be at least 1", http.StatusBadRequest)
		return
	}
	nodes := h.ring.GetReplicationNodes(body.Key, body.Factor)
	if len(nodes) == 0 {
		http.Error(w, "no nodes available", http.StatusServiceUnavailable)
		return
	}
	resp := ReplicateResponse{
		Key:   body.Key,
		Nodes: make([]NodeResponse, 0, len(nodes)),
	}
	for _, n := range nodes {
		resp.Nodes = append(resp.Nodes, NodeResponse{ID: n.ID, Address: n.Address, Status: h.nodeStatus(n.ID)})
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, "failed to write response", http.StatusInternalServerError)
	}
}

func (h *Handler) Health(w http.ResponseWriter, req *http.Request) {
	s := h.aggregator.GetStats()
	resp := map[string]any{
		"status":        "ok",
		"total_nodes":   s.TotalNodes,
		"healthy_nodes": s.HealthyNodes,
		"suspect_nodes": s.SuspectNodes,
		"dead_nodes":    s.DeadNodes,
		"uptime":        time.Since(h.startTime).String(),
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, "failed to write response", http.StatusInternalServerError)
	}
}

func (h *Handler) Gossip(w http.ResponseWriter, req *http.Request) {
	var body GossipRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	h.memberList.Merge(body.Members)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(h.memberList.GetAll()); err != nil {
		http.Error(w, "failed to write response", http.StatusInternalServerError)
	}
}
