package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/colingraydon/continuum/internal/gossip"
	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/stats"
	"github.com/colingraydon/continuum/internal/store"
)

type Handler struct {
	ring              *ring.Ring
	aggregator        *stats.Aggregator
	memberList        *gossip.MemberList
	gossiper          *gossip.Gossiper
	store             *store.Store
	selfID            string
	replicationFactor int
	startTime         time.Time
}

func NewHandler(r *ring.Ring, ml *gossip.MemberList, g *gossip.Gossiper, s *store.Store, selfID string, replicationFactor int) *Handler {
	return &Handler{
		ring:              r,
		aggregator:        stats.NewAggregator(r, ml),
		memberList:        ml,
		gossiper:          g,
		store:             s,
		selfID:            selfID,
		replicationFactor: replicationFactor,
		startTime:         time.Now(),
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
	Value   string `json:"value,omitempty"`
}

type PutKeyRequest struct {
	Value  string            `json:"value"`
	Clocks map[string]uint64 `json:"clocks,omitempty"`
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
	if node.ID == h.selfID {
		if entry, ok := h.store.Get(key); ok {
			resp.Value = entry.Value
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, "failed to write response", http.StatusInternalServerError)
	}
}

func (h *Handler) PutKey(w http.ResponseWriter, req *http.Request) {
	key := strings.TrimPrefix(req.URL.Path, "/keys/")
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}
	var body PutKeyRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if body.Value == "" {
		http.Error(w, "value is required", http.StatusBadRequest)
		return
	}

	incoming := store.VectorClockVersion{Clocks: body.Clocks}
	if incoming.Clocks == nil {
		incoming.Clocks = make(map[string]uint64)
	}

	// Primary write: increment self's counter. Replica write: use clock as-is.
	var version store.VectorClockVersion
	if req.Header.Get("X-Proxied-From") == "" {
		version = incoming.Increment(h.selfID)
	} else {
		version = incoming
	}

	h.store.Put(key, body.Value, version)

	// Fan out to replica nodes. Skip if this is already a replicated write to
	// avoid chained replication.
	if req.Header.Get("X-Proxied-From") == "" {
		nodes := h.ring.GetReplicationNodes(key, h.replicationFactor)
		for _, n := range nodes {
			if n.ID != h.selfID {
				go h.replicateTo(n.Address, key, body.Value, version.Clocks)
			}
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) replicateTo(address, key, value string, clocks map[string]uint64) {
	body, err := json.Marshal(PutKeyRequest{Value: value, Clocks: clocks})
	if err != nil {
		return
	}
	req, err := http.NewRequest(http.MethodPut, "http://"+address+"/keys/"+key, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Proxied-From", h.selfID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	_ = resp.Body.Close()
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
