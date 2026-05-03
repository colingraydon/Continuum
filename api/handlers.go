package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/colingraydon/continuum/internal/gossip"
	"github.com/colingraydon/continuum/internal/merkle"
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
	replicaClient     *http.Client
}

func NewHandler(r *ring.Ring, ml *gossip.MemberList, g *gossip.Gossiper, s *store.Store, selfID string, replicationFactor, writeQuorum, readQuorum int, replicaTimeout time.Duration) *Handler {
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
		replicaClient:     &http.Client{Timeout: replicaTimeout},
	}
}

type AddNodeRequest struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

// SiblingResponse is a single causally-distinct value returned when concurrent
// writes exist for a key. Clients should resolve the conflict and write back
// a new value with a clock that dominates all siblings. Deleted=true means
// this sibling is a tombstone (concurrent write/delete conflict).
type SiblingResponse struct {
	Value   string            `json:"value,omitempty"`
	Clocks  map[string]uint64 `json:"clocks"`
	Deleted bool              `json:"deleted,omitempty"`
}

type NodeResponse struct {
	ID       string            `json:"id"`
	Address  string            `json:"address"`
	Status   string            `json:"status"`
	Value    string            `json:"value,omitempty"`
	Siblings []SiblingResponse `json:"siblings,omitempty"`
	Clocks   map[string]uint64 `json:"clocks,omitempty"`
	Deleted  bool              `json:"deleted,omitempty"`
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

// entryToResponse converts a store entry to a NodeResponse, surfacing siblings
// when concurrent writes exist.
func entryToResponse(id, status string, entry store.Entry) NodeResponse {
	r := NodeResponse{ID: id, Status: status}
	switch len(entry.Siblings) {
	case 1:
		sib := entry.Siblings[0]
		r.Clocks = sib.Version.Clocks
		if sib.Deleted {
			r.Deleted = true
		} else {
			r.Value = sib.Value
		}
	default:
		for _, sib := range entry.Siblings {
			r.Siblings = append(r.Siblings, SiblingResponse{
				Value:   sib.Value,
				Clocks:  sib.Version.Clocks,
				Deleted: sib.Deleted,
			})
		}
	}
	return r
}

// mergeResponses merges sibling sets from multiple replica responses into a
// single canonical result. Entries dominated by a higher-clock sibling are
// dropped; genuinely concurrent entries are preserved as siblings.
// deleted=true means the winning result is a tombstone with no siblings.
func mergeResponses(responses []NodeResponse) (value string, siblings []SiblingResponse, deleted bool) {
	type candidate struct {
		value   string
		clocks  map[string]uint64
		deleted bool
	}

	// Flatten all (value, clock) pairs across every replica response,
	// including tombstones.
	var all []candidate
	for _, r := range responses {
		if len(r.Siblings) > 0 {
			for _, s := range r.Siblings {
				all = append(all, candidate{s.Value, s.Clocks, s.Deleted})
			}
		} else if r.Value != "" || r.Deleted {
			all = append(all, candidate{r.Value, r.Clocks, r.Deleted})
		}
	}

	if len(all) == 0 {
		return "", nil, false
	}

	// Retain only non-dominated, deduplicated candidates.
	var survivors []candidate
	for i, c := range all {
		cv := store.VectorClockVersion{Clocks: c.clocks}
		dominated := false
		for j, other := range all {
			if i == j {
				continue
			}
			if cv.HappensBefore(store.VectorClockVersion{Clocks: other.clocks}) {
				dominated = true
				break
			}
		}
		if dominated {
			continue
		}
		dup := false
		for _, s := range survivors {
			if cv.Equal(store.VectorClockVersion{Clocks: s.clocks}) {
				dup = true
				break
			}
		}
		if !dup {
			survivors = append(survivors, c)
		}
	}

	if len(survivors) == 1 {
		return survivors[0].value, nil, survivors[0].deleted
	}
	var sibs []SiblingResponse
	for _, s := range survivors {
		sibs = append(sibs, SiblingResponse{Value: s.value, Clocks: s.clocks, Deleted: s.deleted})
	}
	return "", sibs, false
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

	// Replica sub-read: return local entry (including any siblings) so the
	// coordinator can merge sibling sets across R replicas.
	if req.Header.Get("X-Proxied-From") != "" {
		resp := NodeResponse{ID: h.selfID, Status: h.nodeStatus(h.selfID)}
		if entry, ok := h.store.Get(key); ok {
			resp = entryToResponse(h.selfID, h.nodeStatus(h.selfID), entry)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			http.Error(w, "failed to write response", http.StatusInternalServerError)
		}
		return
	}

	// Consistent read: fan out to the replica set, merge sibling sets, and
	// return the canonical result - either a single value or a siblings list.
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
					r = entryToResponse(h.selfID, h.nodeStatus(h.selfID), entry)
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

	value, siblings, deleted := mergeResponses(responses)
	if deleted && len(siblings) == 0 {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	primary := nodes[0]
	resp := NodeResponse{
		ID:       primary.ID,
		Address:  primary.Address,
		Status:   h.nodeStatus(primary.ID),
		Value:    value,
		Siblings: siblings,
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

	for range pending {
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

type DeleteKeyRequest struct {
	Clocks map[string]uint64 `json:"clocks,omitempty"`
}

func (h *Handler) DeleteKey(w http.ResponseWriter, req *http.Request) {
	key := strings.TrimPrefix(req.URL.Path, "/keys/")
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}
	var body DeleteKeyRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	incoming := store.VectorClockVersion{Clocks: body.Clocks}
	if incoming.Clocks == nil {
		incoming.Clocks = make(map[string]uint64)
	}

	// Replica delete: store tombstone as-is without fan-out.
	if req.Header.Get("X-Proxied-From") != "" {
		h.store.Delete(key, incoming)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Bootstrap clock from current local entry if the client didn't provide one,
	// so the tombstone's clock dominates the existing value rather than equaling it.
	if len(incoming.Clocks) == 0 {
		if entry, ok := h.store.Get(key); ok {
			for _, sib := range entry.Siblings {
				for nodeID, c := range sib.Version.Clocks {
					if incoming.Clocks[nodeID] < c {
						incoming.Clocks[nodeID] = c
					}
				}
			}
		}
	}

	// Primary delete: increment self's counter and store tombstone locally.
	version := incoming.Increment(h.selfID)
	h.store.Delete(key, version)

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
			results <- result{h.replicateDeleteToSync(addr, key, version.Clocks)}
		}(n.Address)
	}

	for range pending {
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
	resp, err := h.replicaClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("replica returned %d", resp.StatusCode)
	}
	return nil
}

// replicateDeleteToSync sends a replica tombstone to addr and returns an error
// if the delete fails or the replica responds with a non-204 status.
func (h *Handler) replicateDeleteToSync(address, key string, clocks map[string]uint64) error {
	body, err := json.Marshal(DeleteKeyRequest{Clocks: clocks})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodDelete, "http://"+address+"/keys/"+key, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Proxied-From", h.selfID)
	resp, err := h.replicaClient.Do(req)
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
// response includes the vector clock so the coordinator can merge versions.
func (h *Handler) readFromReplica(address, key string) (NodeResponse, error) {
	req, err := http.NewRequest(http.MethodGet, "http://"+address+"/keys/"+key, nil)
	if err != nil {
		return NodeResponse{}, err
	}
	req.Header.Set("X-Proxied-From", h.selfID)
	resp, err := h.replicaClient.Do(req)
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

// SyncStateResponse is returned by GET /sync. Root is a hash of all bucket
// hashes; a matching root means the two nodes are in sync. Buckets narrows
// divergence to a specific key range without transferring any keys.
type SyncStateResponse struct {
	Root    uint32   `json:"root"`
	Buckets []uint32 `json:"buckets"`
}

type SyncKeysRequest struct {
	Keys []string `json:"keys"`
}

// SyncSibling carries a single causally-distinct version of a key, including
// the vector clock needed for the receiving node to apply it correctly.
type SyncSibling struct {
	Value   string            `json:"value,omitempty"`
	Deleted bool              `json:"deleted,omitempty"`
	Clocks  map[string]uint64 `json:"clocks"`
}

type SyncKeysResponse struct {
	Entries map[string][]SyncSibling `json:"entries"`
}

// SyncBucketKeysResponse is returned by GET /sync/bucket-keys.
type SyncBucketKeysResponse struct {
	Keys []string `json:"keys"`
}

// GetSyncBucketKeys returns the key names in a specific bucket of a vnode range.
// Used by the primary during bidirectional anti-entropy to discover keys the
// replica holds that the primary does not.
func (h *Handler) GetSyncBucketKeys(w http.ResponseWriter, req *http.Request) {
	vnodeParam := req.URL.Query().Get("vnode")
	bucketParam := req.URL.Query().Get("bucket")
	if vnodeParam == "" || bucketParam == "" {
		http.Error(w, "vnode and bucket params required", http.StatusBadRequest)
		return
	}
	parsedVnode, err := strconv.ParseUint(vnodeParam, 10, 32)
	if err != nil {
		http.Error(w, "invalid vnode param", http.StatusBadRequest)
		return
	}
	parsedBucket, err := strconv.Atoi(bucketParam)
	if err != nil || parsedBucket < 0 || parsedBucket >= merkle.BucketCount {
		http.Error(w, "invalid bucket param", http.StatusBadRequest)
		return
	}
	vnodeHash := uint32(parsedVnode)

	vr, ok := h.ring.GetVnodeRange(vnodeHash)
	if !ok {
		http.Error(w, "unknown vnode", http.StatusNotFound)
		return
	}

	var keys []string
	for key := range h.store.KeyHashes() {
		if vr.Contains(merkle.HashKey(key)) && merkle.BucketIndex(key) == parsedBucket {
			keys = append(keys, key)
		}
	}
	if keys == nil {
		keys = []string{}
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(SyncBucketKeysResponse{Keys: keys}); err != nil {
		http.Error(w, "failed to write response", http.StatusInternalServerError)
	}
}

// PushSyncEntries applies a batch of entries sent by a primary node. The
// request body uses the same format as the SyncKeysResponse so the primary can
// reuse its existing serialization path.
func (h *Handler) PushSyncEntries(w http.ResponseWriter, req *http.Request) {
	var body SyncKeysResponse
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	for key, sibs := range body.Entries {
		for _, sib := range sibs {
			v := store.VectorClockVersion{Clocks: sib.Clocks}
			if sib.Deleted {
				h.store.Delete(key, v)
			} else {
				h.store.Put(key, sib.Value, v)
			}
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

// GetSyncState returns the root hash and per-bucket hashes for the requested
// vnode (?vnode=<endHash>). Computes bucket hashes on-the-fly from the local
// store so that replicas can serve sync state without maintaining their own
// Merkle trees.
func (h *Handler) GetSyncState(w http.ResponseWriter, req *http.Request) {
	param := req.URL.Query().Get("vnode")
	if param == "" {
		http.Error(w, "vnode param required", http.StatusBadRequest)
		return
	}
	parsed, err := strconv.ParseUint(param, 10, 32)
	if err != nil {
		http.Error(w, "invalid vnode param", http.StatusBadRequest)
		return
	}
	vnodeHash := uint32(parsed)

	vr, ok := h.ring.GetVnodeRange(vnodeHash)
	if !ok {
		http.Error(w, "unknown vnode", http.StatusNotFound)
		return
	}

	buckets := make([]map[string]uint32, merkle.BucketCount)
	for i := range buckets {
		buckets[i] = make(map[string]uint32)
	}
	for key, hash := range h.store.KeyHashes() {
		if !vr.Contains(merkle.HashKey(key)) {
			continue
		}
		buckets[merkle.BucketIndex(key)][key] = hash
	}

	bucketHashes := make([]uint32, merkle.BucketCount)
	for i, entries := range buckets {
		bucketHashes[i] = merkle.ComputeBucketHash(entries)
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(SyncStateResponse{
		Root:    merkle.ComputeRootHash(bucketHashes),
		Buckets: bucketHashes,
	}); err != nil {
		http.Error(w, "failed to write response", http.StatusInternalServerError)
	}
}

// GetSyncKeys returns the full entry (all siblings with vector clocks) for
// each requested key. Used by the anti-entropy loop to fetch entries from a
// divergent bucket so the caller can apply repairs via the normal write path.
func (h *Handler) GetSyncKeys(w http.ResponseWriter, req *http.Request) {
	var body SyncKeysRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	entries := make(map[string][]SyncSibling, len(body.Keys))
	for _, key := range body.Keys {
		entry, ok := h.store.Get(key)
		if !ok {
			continue
		}
		sibs := make([]SyncSibling, len(entry.Siblings))
		for i, sib := range entry.Siblings {
			sibs[i] = SyncSibling{Value: sib.Value, Deleted: sib.Deleted, Clocks: sib.Version.Clocks}
		}
		entries[key] = sibs
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(SyncKeysResponse{Entries: entries}); err != nil {
		http.Error(w, "failed to write response", http.StatusInternalServerError)
	}
}
