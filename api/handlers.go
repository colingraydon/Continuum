package api

import (
	"bytes"
	"encoding/json"
	"fmt"
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
	writeQuorum       int
	readQuorum        int
	startTime         time.Time
}

func NewHandler(r *ring.Ring, ml *gossip.MemberList, g *gossip.Gossiper, s *store.Store, selfID string, replicationFactor, writeQuorum, readQuorum int) *Handler {
	return &Handler{
		ring:              r,
		aggregator:        stats.NewAggregator(r, ml),
		memberList:        ml,
		gossiper:          g,
		store:             s,
		selfID:            selfID,
		replicationFactor: replicationFactor,
		writeQuorum:       writeQuorum,
		readQuorum:        readQuorum,
		startTime:         time.Now(),
	}
}

type AddNodeRequest struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

type NodeResponse struct {
	ID      string            `json:"id"`
	Address string            `json:"address"`
	Status  string            `json:"status"`
	Value   string            `json:"value,omitempty"`
	Clocks  map[string]uint64 `json:"clocks,omitempty"`
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

	// Replica sub-read: return local entry with vector clock so the coordinator
	// can pick the entry with the highest clock across R replicas.
	if req.Header.Get("X-Proxied-From") != "" {
		resp := NodeResponse{ID: h.selfID, Status: h.nodeStatus(h.selfID)}
		if entry, ok := h.store.Get(key); ok {
			resp.Value = entry.Value
			if vc, ok := entry.Version.(store.VectorClockVersion); ok {
				resp.Clocks = vc.Clocks
			}
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			http.Error(w, "failed to write response", http.StatusInternalServerError)
		}
		return
	}

	// Consistent read: fan out to the replica set and return the entry with the
	// highest vector clock across R responses.
	nodes := h.ring.GetReplicationNodes(key, h.replicationFactor)
	if len(nodes) == 0 {
		http.Error(w, "no nodes available", http.StatusServiceUnavailable)
		return
	}
	RecordKeyLookup()

	quorum := h.readQuorum
	if quorum > len(nodes) {
		quorum = len(nodes)
	}

	type replicaResult struct {
		resp NodeResponse
		err  error
	}
	results := make(chan replicaResult, len(nodes))

	for _, n := range nodes {
		go func(node *ring.Node) {
			if node.ID == h.selfID {
				r := NodeResponse{ID: h.selfID, Status: h.nodeStatus(h.selfID)}
				if entry, ok := h.store.Get(key); ok {
					r.Value = entry.Value
					if vc, ok2 := entry.Version.(store.VectorClockVersion); ok2 {
						r.Clocks = vc.Clocks
					}
				}
				results <- replicaResult{resp: r}
			} else {
				r, err := h.readFromReplica(node.Address, key)
				results <- replicaResult{resp: r, err: err}
			}
		}(n)
	}

	var responses []NodeResponse
	for i := 0; i < len(nodes); i++ {
		r := <-results
		if r.err == nil {
			responses = append(responses, r.resp)
		}
		if len(responses) >= quorum {
			break
		}
	}

	if len(responses) < quorum {
		http.Error(w, "read quorum not met", http.StatusServiceUnavailable)
		return
	}

	best := responses[0]
	for _, r := range responses[1:] {
		best = latestEntry(best, r)
	}

	// Return primary replica metadata with the best value found across replicas.
	primary := nodes[0]
	resp := NodeResponse{
		ID:      primary.ID,
		Address: primary.Address,
		Status:  h.nodeStatus(primary.ID),
		Value:   best.Value,
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

	// Replica write: store as-is without fan-out or quorum tracking.
	if req.Header.Get("X-Proxied-From") != "" {
		h.store.Put(key, body.Value, incoming)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Primary write: increment self's counter and store locally.
	version := incoming.Increment(h.selfID)
	h.store.Put(key, body.Value, version)

	// Quorum write: fan out to all replica nodes and wait for W acks (self
	// already counts as one). Return 503 if quorum cannot be reached.
	nodes := h.ring.GetReplicationNodes(key, h.replicationFactor)
	quorum := min(h.writeQuorum, len(nodes))

	acks := 1 // self
	if acks >= quorum {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	type result struct{ err error }
	pending := 0
	results := make(chan result, len(nodes))
	for _, n := range nodes {
		if n.ID == h.selfID {
			continue
		}
		pending++
		go func(addr string) {
			results <- result{h.replicateToSync(addr, key, body.Value, version.Clocks)}
		}(n.Address)
	}

	for i := 0; i < pending; i++ {
		r := <-results
		if r.err == nil {
			acks++
		}
		if acks >= quorum {
			break
		}
	}

	if acks < quorum {
		http.Error(w, "write quorum not met", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// replicateToSync sends a replica write to addr and returns an error if the
// write fails or the replica responds with a non-204 status.
func (h *Handler) replicateToSync(address, key, value string, clocks map[string]uint64) error {
	body, err := json.Marshal(PutKeyRequest{Value: value, Clocks: clocks})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPut, "http://"+address+"/keys/"+key, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Proxied-From", h.selfID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("replica returned %d", resp.StatusCode)
	}
	return nil
}

// readFromReplica fetches the local entry for key from a replica node. The
// response includes the vector clock so the coordinator can compare versions.
func (h *Handler) readFromReplica(address, key string) (NodeResponse, error) {
	req, err := http.NewRequest(http.MethodGet, "http://"+address+"/keys/"+key, nil)
	if err != nil {
		return NodeResponse{}, err
	}
	req.Header.Set("X-Proxied-From", h.selfID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return NodeResponse{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	var nr NodeResponse
	if err := json.NewDecoder(resp.Body).Decode(&nr); err != nil {
		return NodeResponse{}, err
	}
	return nr, nil
}

// latestEntry returns whichever of a or b has the higher vector clock.
// If the clocks are concurrent (neither happens-before the other), the entry
// with the larger clock sum is preferred as a best-effort tiebreak.
func latestEntry(a, b NodeResponse) NodeResponse {
	av := store.VectorClockVersion{Clocks: a.Clocks}
	bv := store.VectorClockVersion{Clocks: b.Clocks}
	if av.HappensBefore(bv) {
		return b
	}
	if bv.HappensBefore(av) {
		return a
	}
	var aSum, bSum uint64
	for _, v := range a.Clocks {
		aSum += v
	}
	for _, v := range b.Clocks {
		bSum += v
	}
	if bSum > aSum {
		return b
	}
	return a
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
