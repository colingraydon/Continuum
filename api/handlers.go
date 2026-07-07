package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/colingraydon/continuum/internal/gossip"
	"github.com/colingraydon/continuum/internal/hintstore"
	"github.com/colingraydon/continuum/internal/merkle"
	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/stats"
	"github.com/colingraydon/continuum/internal/store"
)

const (
	contentTypeHeader    = "Content-Type"
	contentTypeJSON      = "application/json"
	keysPrefix           = "/keys/"
	headerXProxiedFrom   = "X-Proxied-From"
	headerXSessionClock  = "X-Session-Clock"
	headerXCASForwarded  = "X-CAS-Forwarded-From"
	schemeHTTP           = "http://"
	errKeyRequired       = "key is required"
	errInvalidBody       = "invalid request body"
	errFailedWrite       = "failed to write response"
	errNodeBootstrapping = "node is bootstrapping"
	errNoNodes           = "no nodes available"
	errLocalRead         = "local store read failed"
)

// HandlerConfig holds the scalar settings for a Handler.
type HandlerConfig struct {
	SelfID            string
	ReplicationFactor int
	WriteQuorum       int
	ReadQuorum        int
	ReplicaTimeout    time.Duration
	// Transport, when non-nil, replaces the default transport for all
	// outbound node-to-node HTTP (replica fan-out, CAS forwarding, hint
	// delivery, migration, scan scatter). The simulation harness injects an
	// in-memory network with seeded faults here; production leaves it nil.
	Transport http.RoundTripper
}

// SyncTreeProvider serves anti-entropy sync state for a vnode from a
// pre-maintained Merkle tree, letting the sync endpoints skip a full store
// scan. ok=false means no tree is held for the vnode; the handler then falls
// back to scanning. Implemented by *antientropy.Manager.
type SyncTreeProvider interface {
	SyncState(vnodeHash uint32) (root uint32, buckets []uint32, ok bool)
	BucketKeys(vnodeHash uint32, bucket int) (keys []string, ok bool)
}

type Handler struct {
	ring              *ring.Ring
	aggregator        *stats.Aggregator
	memberList        *gossip.MemberList
	store             *store.Store
	hintStore         *hintstore.HintStore
	syncTrees         SyncTreeProvider
	selfID            string
	replicationFactor int
	writeQuorum       int
	readQuorum        int
	startTime         time.Time
	replicaClient     *http.Client
	// casClient forwards CAS requests to the key's primary. Separate from
	// replicaClient because a forwarded CAS contains the primary's own
	// replica fan-out, so its round trip is bounded by the replica timeout
	// plus the primary's quorum wait, not by one replica hop.
	casClient *http.Client
}

func NewHandler(r *ring.Ring, ml *gossip.MemberList, s *store.Store, cfg HandlerConfig, hs *hintstore.HintStore) *Handler {
	return &Handler{
		ring:              r,
		aggregator:        stats.NewAggregator(r, ml),
		memberList:        ml,
		store:             s,
		hintStore:         hs,
		selfID:            cfg.SelfID,
		replicationFactor: cfg.ReplicationFactor,
		writeQuorum:       cfg.WriteQuorum,
		readQuorum:        cfg.ReadQuorum,
		startTime:         time.Now(),
		replicaClient:     &http.Client{Timeout: cfg.ReplicaTimeout, Transport: cfg.Transport},
		casClient:         &http.Client{Timeout: 2 * cfg.ReplicaTimeout, Transport: cfg.Transport},
	}
}

// SetSyncTreeProvider installs the provider used to serve anti-entropy sync
// state without a store scan. Wired in main after the anti-entropy manager is
// built; when unset (e.g. in tests) the sync endpoints fall back to scanning.
func (h *Handler) SetSyncTreeProvider(p SyncTreeProvider) {
	h.syncTrees = p
}

// Per-request consistency levels accepted by the ?consistency= query param on
// GET/PUT/DELETE /keys/{key}. Each maps to a quorum size against the
// replication factor; an absent param keeps the process-configured default.
const (
	consistencyParam  = "consistency"
	consistencyOne    = "one"
	consistencyQuorum = "quorum"
	consistencyAll    = "all"
)

// requestedQuorum resolves the quorum size for one request: the ?consistency=
// level mapped against the replication factor (one=1, quorum=RF/2+1, all=RF),
// or dflt when the param is absent. An unrecognized level is an error; callers
// must reject the request with 400 before touching the store. "all" means all
// current replicas — like the configured W/R it is later clamped to the
// available replica set, so it tracks membership rather than acting as a hard
// durability floor.
func (h *Handler) requestedQuorum(req *http.Request, dflt int) (int, error) {
	switch level := req.URL.Query().Get(consistencyParam); level {
	case "":
		return dflt, nil
	case consistencyOne:
		return 1, nil
	case consistencyQuorum:
		return h.replicationFactor/2 + 1, nil
	case consistencyAll:
		return h.replicationFactor, nil
	default:
		return 0, fmt.Errorf("unknown consistency level %q (want one, quorum, or all)", level)
	}
}

// casParam enables compare-and-set semantics on PUT/DELETE /keys/{key}:
// the write is applied only if its clock dominates every sibling currently
// held by the coordinator, and rejected with 412 instead of creating a
// sibling otherwise.
const casParam = "cas"

// requestedCAS reports whether the ?cas= query param asks for a conditional
// write. Only "true", "false", or absence are accepted; anything else is an
// error so a typo cannot silently downgrade to a normal sloppy write.
func requestedCAS(req *http.Request) (bool, error) {
	switch v := req.URL.Query().Get(casParam); v {
	case "", "false":
		return false, nil
	case "true":
		return true, nil
	default:
		return false, fmt.Errorf("invalid cas param %q (want true or false)", v)
	}
}

// casRoutedElsewhere serializes CAS writes for a key through its primary
// replica: the first node on the strict ring walk, which every coordinator
// sharing the ring view resolves identically. It returns false when this
// node is the primary and the caller should execute the CAS locally.
// Otherwise it has already written the response: either the primary's reply,
// relayed by forwardCAS, or a fail-closed 503 when the primary cannot be
// reached or the forwarder's ring view disagrees with ours. Failing closed
// keeps the CAS contract honest: a 204 means the precondition was checked
// under the one mutex all CAS writes for the key serialize on, and an
// ambiguous cluster state yields a retryable error, never a sibling.
func (h *Handler) casRoutedElsewhere(w http.ResponseWriter, req *http.Request, key string, body any) bool {
	nodes := h.ring.GetReplicationNodes(key, 1)
	if len(nodes) == 0 {
		http.Error(w, errNoNodes, http.StatusServiceUnavailable)
		return true
	}
	primary := nodes[0]
	if primary.ID == h.selfID {
		return false
	}
	if req.Header.Get(headerXCASForwarded) != "" {
		// The forwarder believed we were the primary but our ring view names
		// someone else: membership is converging. Never forward again (no
		// loops); make the client retry once the views agree.
		http.Error(w, "cas primary mismatch, retry", http.StatusServiceUnavailable)
		return true
	}
	if m, ok := h.memberList.Get(primary.ID); !ok || m.Status != gossip.MemberAlive {
		http.Error(w, "cas primary unavailable", http.StatusServiceUnavailable)
		return true
	}
	h.forwardCAS(w, req, primary.Address, body)
	return true
}

// forwardCAS re-issues the client's CAS request against the primary and
// relays the primary's verdict (status, session clock, and body) back to the
// client unchanged, so 204/412 semantics are identical whichever coordinator
// the client happened to hit.
func (h *Handler) forwardCAS(w http.ResponseWriter, req *http.Request, address string, body any) {
	payload, err := json.Marshal(body)
	if err != nil {
		http.Error(w, errFailedWrite, http.StatusInternalServerError)
		return
	}
	fwd, err := http.NewRequest(req.Method, schemeHTTP+address+req.URL.RequestURI(), bytes.NewReader(payload))
	if err != nil {
		http.Error(w, errFailedWrite, http.StatusInternalServerError)
		return
	}
	fwd.Header.Set(contentTypeHeader, contentTypeJSON)
	fwd.Header.Set(headerXCASForwarded, h.selfID)
	resp, err := h.casClient.Do(fwd)
	if err != nil {
		log.Printf("cas forward to %s: %v", address, err)
		http.Error(w, "cas primary unreachable", http.StatusServiceUnavailable)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if c := resp.Header.Get(headerXSessionClock); c != "" {
		w.Header().Set(headerXSessionClock, c)
	}
	if ct := resp.Header.Get(contentTypeHeader); ct != "" {
		w.Header().Set(contentTypeHeader, ct)
	}
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil {
		log.Printf("cas forward relay body: %v", err)
	}
}

// parseSessionClock decodes the client's session vector clock from the
// X-Session-Clock request header. An absent header means no session guarantee
// was requested and returns a nil map.
func parseSessionClock(req *http.Request) (map[string]uint64, error) {
	raw := req.Header.Get(headerXSessionClock)
	if raw == "" {
		return nil, nil
	}
	var clocks map[string]uint64
	if err := json.Unmarshal([]byte(raw), &clocks); err != nil {
		return nil, fmt.Errorf("invalid %s header (want a JSON clocks object): %v", headerXSessionClock, err)
	}
	return clocks, nil
}

// setSessionClockHeader attaches a clock to the response so the client can
// carry it into later session reads and CAS writes for the same key.
func setSessionClockHeader(w http.ResponseWriter, clocks map[string]uint64) {
	// Marshal cannot fail on a map[string]uint64.
	b, _ := json.Marshal(clocks)
	w.Header().Set(headerXSessionClock, string(b))
}

// survivorsMaxClock folds all surviving sibling clocks into their
// componentwise maximum: the most advanced state this read observed.
func survivorsMaxClock(survivors []SiblingResponse) map[string]uint64 {
	m := make(map[string]uint64)
	for _, s := range survivors {
		for node, c := range s.Clocks {
			if m[node] < c {
				m[node] = c
			}
		}
	}
	return m
}

// clockCovered reports whether observed dominates-or-equals session
// componentwise, i.e. the read result reflects every write the session has
// seen.
func clockCovered(session, observed map[string]uint64) bool {
	for node, c := range session {
		if observed[node] < c {
			return false
		}
	}
	return true
}

// replicaResult carries the outcome of a single replica fan-out attempt.
type replicaResult struct {
	nodeID string
	err    error
}

// nodeIDs extracts the IDs of a node slice.
func nodeIDs(nodes []*ring.Node) []string {
	ids := make([]string, len(nodes))
	for i, n := range nodes {
		ids[i] = n.ID
	}
	return ids
}

func cloneClocks(clocks map[string]uint64) map[string]uint64 {
	c := make(map[string]uint64, len(clocks))
	for k, v := range clocks {
		c[k] = v
	}
	return c
}

// bufferHints stores hints for replicas that failed to ack a write. It handles
// two cases:
//   - All results collected (remaining==0): hints are stored synchronously.
//   - In-flight goroutines remain (remaining>0): a background goroutine drains
//     the channel and stores hints for any failures, then adds the pre-quorum
//     failures too. The caller can return 204 immediately without waiting.
func (h *Handler) bufferHints(template hintstore.Hint, preQuorumFailed []string, remaining int, ch <-chan replicaResult) {
	storeFn := func(nodeIDs []string) {
		for _, id := range nodeIDs {
			hint := template
			hint.Clocks = cloneClocks(template.Clocks)
			hint.At = time.Now()
			h.hintStore.Store(id, hint)
		}
	}
	if remaining > 0 {
		captured := make([]string, len(preQuorumFailed))
		copy(captured, preQuorumFailed)
		go func() {
			for range remaining {
				if r := <-ch; r.err != nil {
					captured = append(captured, r.nodeID)
				}
			}
			storeFn(captured)
		}()
	} else {
		storeFn(preQuorumFailed)
	}
}

// DeliverHints drains all buffered hints for nodeID and replays them to
// address as replica sub-writes. A hint that fails to deliver is re-buffered
// (its original timestamp is preserved, so its TTL keeps counting from the
// original write) to be retried by the next delivery sweep; anti-entropy is
// the backstop for any hint that ages out before its target is reachable.
// This makes DeliverHints safe to call periodically against a target that is
// still unreachable — e.g. an asymmetric partition where the node looks alive
// to gossip but cannot receive inbound writes.
func (h *Handler) DeliverHints(nodeID, address string) {
	if h.hintStore == nil {
		return
	}
	hints := h.hintStore.Drain(nodeID)
	if len(hints) == 0 {
		return
	}
	log.Printf("hinted handoff: delivering %d hints to %s", len(hints), nodeID)
	var requeued int
	for _, hint := range hints {
		var err error
		if hint.Deleted {
			err = h.replicateDeleteToSync(address, hint.Key, hint.Clocks)
		} else {
			err = h.replicateToSync(address, hint.Key, hint.Value, hint.Clocks)
		}
		if err != nil {
			log.Printf("hinted handoff: failed to deliver hint for %s to %s: %v", hint.Key, nodeID, err)
			h.hintStore.Store(nodeID, hint)
			requeued++
		}
	}
	if requeued > 0 {
		log.Printf("hinted handoff: re-buffered %d undelivered hints for %s", requeued, nodeID)
	}
}

// DeliverPendingHints delivers all buffered hints to any currently-alive node.
// It drives both the periodic delivery sweep — the backstop for targets that
// never transition dead→alive, such as an asymmetric partition — and the
// graceful-shutdown flush so buffered hints are not stranded when the
// coordinator exits. Hints for nodes that are not alive, or that fail to
// deliver, remain buffered (see DeliverHints).
func (h *Handler) DeliverPendingHints() {
	if h.hintStore == nil {
		return
	}
	for _, nodeID := range h.hintStore.PendingNodes() {
		m, ok := h.memberList.Get(nodeID)
		if !ok || m.Status != gossip.MemberAlive {
			continue
		}
		h.DeliverHints(nodeID, m.Address)
	}
}

type AddNodeRequest struct {
	ID      string `json:"id"`
	Address string `json:"address"`
	// GossipAddress is the UDP address the node receives gossip on. Optional;
	// when empty, peers assume the node shares their gossip port.
	GossipAddress string `json:"gossip_address,omitempty"`
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

// mergeCandidate is an intermediate type used during sibling merging.
type mergeCandidate struct {
	value   string
	clocks  map[string]uint64
	deleted bool
}

func collectCandidates(responses []NodeResponse) []mergeCandidate {
	var all []mergeCandidate
	for _, r := range responses {
		if len(r.Siblings) > 0 {
			for _, s := range r.Siblings {
				all = append(all, mergeCandidate{s.Value, s.Clocks, s.Deleted})
			}
		} else if r.Value != "" || r.Deleted {
			all = append(all, mergeCandidate{r.Value, r.Clocks, r.Deleted})
		}
	}
	return all
}

func isCandidateDominated(i int, cv store.VectorClockVersion, all []mergeCandidate) bool {
	for j, other := range all {
		if i == j {
			continue
		}
		if cv.HappensBefore(store.VectorClockVersion{Clocks: other.clocks}) {
			return true
		}
	}
	return false
}

func isCandidateDuplicate(cv store.VectorClockVersion, survivors []mergeCandidate) bool {
	for _, s := range survivors {
		if cv.Equal(store.VectorClockVersion{Clocks: s.clocks}) {
			return true
		}
	}
	return false
}

// mergeResponses merges sibling sets from multiple replica responses into the
// canonical set of surviving siblings. Entries dominated by a higher-clock
// sibling are dropped; genuinely concurrent entries are preserved. The caller
// interprets a single Deleted survivor as a tombstone (404) and multiple
// survivors as a conflict (siblings). Nil means no replica held the key.
func mergeResponses(responses []NodeResponse) []SiblingResponse {
	all := collectCandidates(responses)
	if len(all) == 0 {
		return nil
	}

	var survivors []mergeCandidate
	for i, c := range all {
		cv := store.VectorClockVersion{Clocks: c.clocks}
		if isCandidateDominated(i, cv, all) {
			continue
		}
		if !isCandidateDuplicate(cv, survivors) {
			survivors = append(survivors, c)
		}
	}

	out := make([]SiblingResponse, len(survivors))
	for i, s := range survivors {
		out[i] = SiblingResponse{Value: s.value, Clocks: s.clocks, Deleted: s.deleted}
	}
	return out
}

// staleReplicas returns a nodeID→address map for every response that is
// missing at least one surviving sibling. A replica is stale if its sibling
// set is a proper subset of the survivors (matched by equal clocks).
func staleReplicas(responses []NodeResponse, survivors []SiblingResponse, addrByID map[string]string) map[string]string {
	stale := make(map[string]string)
	for _, r := range responses {
		if !responseHasAllSurvivors(r, survivors) {
			if addr, ok := addrByID[r.ID]; ok {
				stale[r.ID] = addr
			}
		}
	}
	return stale
}

func responseHasAllSurvivors(r NodeResponse, survivors []SiblingResponse) bool {
	for _, s := range survivors {
		sv := store.VectorClockVersion{Clocks: s.Clocks}
		found := false
		if len(r.Siblings) > 0 {
			for _, rs := range r.Siblings {
				if sv.Equal(store.VectorClockVersion{Clocks: rs.Clocks}) {
					found = true
					break
				}
			}
		} else {
			found = sv.Equal(store.VectorClockVersion{Clocks: r.Clocks})
		}
		if !found {
			return false
		}
	}
	return true
}

func (h *Handler) repairSurvivor(key, nodeID, addr string, s SiblingResponse) {
	v := store.VectorClockVersion{Clocks: s.Clocks}
	if nodeID == h.selfID {
		var err error
		if s.Deleted {
			err = h.store.Delete(key, v)
		} else {
			err = h.store.Put(key, s.Value, v)
		}
		if err != nil {
			log.Printf("read repair: local store %s for key %s: %v", nodeID, key, err)
		}
		return
	}
	var err error
	if s.Deleted {
		err = h.replicateDeleteToSync(addr, key, s.Clocks)
	} else {
		err = h.replicateToSync(addr, key, s.Value, s.Clocks)
	}
	if err != nil {
		log.Printf("read repair: failed to repair %s for key %s: %v", nodeID, key, err)
	}
}

// repairReplicas pushes all surviving siblings to each stale replica. For the
// coordinator itself the write goes directly to the local store; for remote
// nodes it uses the existing replica HTTP path. Failures are logged; anti-
// entropy covers any keys that could not be repaired.
func (h *Handler) repairReplicas(key string, survivors []SiblingResponse, stale map[string]string) {
	for nodeID, addr := range stale {
		for _, s := range survivors {
			h.repairSurvivor(key, nodeID, addr, s)
		}
	}
}

func (h *Handler) AddNode(w http.ResponseWriter, req *http.Request) {
	var body AddNodeRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, errInvalidBody, http.StatusBadRequest)
		return
	}
	if body.ID == "" || body.Address == "" {
		http.Error(w, "id and address are required", http.StatusBadRequest)
		return
	}
	h.memberList.AddWithGossip(body.ID, body.Address, body.GossipAddress)
	w.WriteHeader(http.StatusCreated)
	node := NodeResponse{ID: body.ID, Address: body.Address, Status: "alive"}
	if err := json.NewEncoder(w).Encode(node); err != nil {
		http.Error(w, errFailedWrite, http.StatusInternalServerError)
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
	w.Header().Set(contentTypeHeader, contentTypeJSON)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, errFailedWrite, http.StatusInternalServerError)
	}
}

// handleReplicaRead serves a sub-read from a coordinator, returning the local
// entry (including any siblings) for clock merging on the coordinator side.
func (h *Handler) handleReplicaRead(w http.ResponseWriter, key string) {
	resp := NodeResponse{ID: h.selfID, Status: h.nodeStatus(h.selfID)}
	entry, ok, err := h.store.Get(key)
	if err != nil {
		log.Printf("replica read %s: %v", key, err)
		http.Error(w, errLocalRead, http.StatusInternalServerError)
		return
	}
	if ok {
		resp = entryToResponse(h.selfID, h.nodeStatus(h.selfID), entry)
	}
	w.Header().Set(contentTypeHeader, contentTypeJSON)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, errFailedWrite, http.StatusInternalServerError)
	}
}

// filterReadNodes returns nodes that are not currently bootstrapping. Bootstrapping
// nodes are excluded because they don't yet hold complete data.
func (h *Handler) filterReadNodes(nodes []*ring.Node) []*ring.Node {
	out := make([]*ring.Node, 0, len(nodes))
	for _, n := range nodes {
		if m, ok := h.memberList.Get(n.ID); !ok || !m.Bootstrapping {
			out = append(out, n)
		}
	}
	return out
}

type readResult struct {
	resp NodeResponse
	err  error
}

// readNode reads key from one node: the local store when it is self, otherwise
// the replica over HTTP. An error counts as a failed replica; quorum can still
// be met from the remaining nodes.
func (h *Handler) readNode(node *ring.Node, key string) readResult {
	if node.ID != h.selfID {
		r, err := h.readFromReplica(node.Address, key)
		return readResult{resp: r, err: err}
	}
	entry, ok, err := h.store.Get(key)
	if err != nil {
		return readResult{err: err}
	}
	r := NodeResponse{ID: h.selfID, Status: h.nodeStatus(h.selfID)}
	if ok {
		r = entryToResponse(h.selfID, h.nodeStatus(h.selfID), entry)
	}
	return readResult{resp: r}
}

// quorumReadFanOut fans out reads to readNodes, collects results until quorum
// is met, and returns the responses plus whether quorum was reached.
func (h *Handler) quorumReadFanOut(readNodes []*ring.Node, key string, quorum int) ([]NodeResponse, bool) {
	results := make(chan readResult, len(readNodes))
	for _, n := range readNodes {
		go func(node *ring.Node) { results <- h.readNode(node, key) }(n)
	}
	var responses []NodeResponse
	for i := 0; i < len(readNodes); i++ {
		r := <-results
		if r.err == nil {
			responses = append(responses, r.resp)
		}
		if len(responses) >= quorum {
			break
		}
	}
	return responses, len(responses) >= quorum
}

// readAllFanOut reads key from every read node and waits for all replies,
// tolerating individual failures. Used to escalate a session read past the
// initial quorum when the quorum result does not cover the session clock.
func (h *Handler) readAllFanOut(readNodes []*ring.Node, key string) []NodeResponse {
	results := make(chan readResult, len(readNodes))
	for _, n := range readNodes {
		go func(node *ring.Node) { results <- h.readNode(node, key) }(n)
	}
	var responses []NodeResponse
	for range readNodes {
		if r := <-results; r.err == nil {
			responses = append(responses, r.resp)
		}
	}
	return responses
}

// writeKeyResponse encodes the final coordinator GET response using the merged
// survivors. A single deleted survivor becomes a 404; multiple survivors are
// returned as a sibling conflict.
func (h *Handler) writeKeyResponse(w http.ResponseWriter, primary *ring.Node, survivors []SiblingResponse) {
	resp := NodeResponse{
		ID:      primary.ID,
		Address: primary.Address,
		Status:  h.nodeStatus(primary.ID),
	}
	// Return the observed clock so the client can advance its session clock;
	// set before any status is written so it also rides the tombstone 404.
	if len(survivors) > 0 {
		setSessionClockHeader(w, survivorsMaxClock(survivors))
	}
	switch len(survivors) {
	case 0:
		// No replica held this key; return node info with no value.
	case 1:
		if survivors[0].Deleted {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		resp.Value = survivors[0].Value
	default:
		resp.Siblings = survivors
	}
	w.Header().Set(contentTypeHeader, contentTypeJSON)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, errFailedWrite, http.StatusInternalServerError)
	}
}

func (h *Handler) GetNode(w http.ResponseWriter, req *http.Request) {
	key := strings.TrimPrefix(req.URL.Path, keysPrefix)
	if key == "" {
		http.Error(w, errKeyRequired, http.StatusBadRequest)
		return
	}

	if req.Header.Get(headerXProxiedFrom) != "" {
		h.handleReplicaRead(w, key)
		return
	}

	if m, ok := h.memberList.Get(h.selfID); ok && m.Bootstrapping {
		http.Error(w, errNodeBootstrapping, http.StatusServiceUnavailable)
		return
	}

	readQuorum, err := h.requestedQuorum(req, h.readQuorum)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	session, err := parseSessionClock(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Consistent read: fan out to the replica set, merge sibling sets, and
	// return the canonical result - either a single value or a siblings list.
	// Reads use the same healthy walk as writes so a fallback node that took a
	// sloppy write is included in the read set.
	nodes, _ := h.ring.GetHealthyReplicationNodes(key, h.replicationFactor)
	if len(nodes) == 0 {
		http.Error(w, errNoNodes, http.StatusServiceUnavailable)
		return
	}
	RecordKeyLookup()

	readNodes := h.filterReadNodes(nodes)
	if len(readNodes) == 0 {
		http.Error(w, errNoNodes, http.StatusServiceUnavailable)
		return
	}

	quorum := min(readQuorum, len(readNodes))

	responses, ok := h.quorumReadFanOut(readNodes, key, quorum)
	if !ok {
		http.Error(w, "read quorum not met", http.StatusServiceUnavailable)
		return
	}

	survivors := mergeResponses(responses)

	responses, survivors, satisfied := h.enforceSessionClock(session, readNodes, key, responses, survivors)
	if !satisfied {
		http.Error(w, "session clock not satisfiable", http.StatusServiceUnavailable)
		return
	}

	addrByID := make(map[string]string, len(readNodes))
	for _, n := range readNodes {
		addrByID[n.ID] = n.Address
	}
	if stale := staleReplicas(responses, survivors, addrByID); len(stale) > 0 {
		go h.repairReplicas(key, survivors, stale)
	}

	h.writeKeyResponse(w, nodes[0], survivors)
}

// enforceSessionClock applies the session guarantee to a completed quorum
// read. If the merged survivors do not cover the client's session clock, the
// quorum read hit only replicas that have not yet seen the session's writes
// (the sloppy-quorum visibility window): escalate once to every read node and
// re-merge. satisfied=false means even the full replica set cannot produce a
// covering result and the read must fail rather than silently violating
// read-your-writes. An empty session clock is always satisfied.
func (h *Handler) enforceSessionClock(session map[string]uint64, readNodes []*ring.Node, key string, responses []NodeResponse, survivors []SiblingResponse) ([]NodeResponse, []SiblingResponse, bool) {
	if len(session) == 0 || clockCovered(session, survivorsMaxClock(survivors)) {
		return responses, survivors, true
	}
	if len(responses) < len(readNodes) {
		if all := h.readAllFanOut(readNodes, key); len(all) > 0 {
			responses = all
			survivors = mergeResponses(responses)
		}
	}
	return responses, survivors, clockCovered(session, survivorsMaxClock(survivors))
}

// quorumFanOut fans out a write operation to all non-self replica nodes,
// collects results until quorum is met, and returns the ack count (including
// self's pre-counted ack), in-flight count, failed node IDs, and the result
// channel so the caller can drain remaining results for hint buffering.
func (h *Handler) quorumFanOut(nodes []*ring.Node, quorum int, op func(*ring.Node) replicaResult) (acks, remaining int, failed []string, ch <-chan replicaResult) {
	acks = 1 // self
	inner := make(chan replicaResult, len(nodes))
	pending := 0
	for _, n := range nodes {
		if n.ID == h.selfID {
			continue
		}
		pending++
		go func(n *ring.Node) { inner <- op(n) }(n)
	}
	// If quorum is already satisfied by self's ack (e.g. W=1), don't wait at
	// all: the fan-out still happens, and the caller's hint-buffering goroutine
	// drains the channel for failures.
	collected := 0
	for collected < pending && acks < quorum {
		r := <-inner
		collected++
		if r.err == nil {
			acks++
		} else {
			failed = append(failed, r.nodeID)
		}
	}
	return acks, pending - collected, failed, inner
}

func (h *Handler) PutKey(w http.ResponseWriter, req *http.Request) {
	key := strings.TrimPrefix(req.URL.Path, keysPrefix)
	if key == "" {
		http.Error(w, errKeyRequired, http.StatusBadRequest)
		return
	}
	var body PutKeyRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, errInvalidBody, http.StatusBadRequest)
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
	if req.Header.Get(headerXProxiedFrom) != "" {
		if err := h.store.Put(key, body.Value, incoming); err != nil {
			http.Error(w, "store write failed", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}

	h.coordinateWrite(w, req, key, incoming, keyWrite{
		value:  body.Value,
		body:   body,
		errMsg: "store write failed",
	})
}

// keyWrite describes one client mutation flowing through the coordinator
// write path: a value write, or a tombstone when deleted is set. body is the
// decoded request payload, re-marshaled when a CAS write is forwarded to the
// key's primary; errMsg is the status text for local store failures.
type keyWrite struct {
	value   string
	deleted bool
	body    any
	errMsg  string
}

// coordinateWrite is the shared coordinator write path behind PutKey and
// DeleteKey (everything after the replica passthrough): quorum and CAS
// resolution, primary routing for CAS, clock bootstrapping, the local store
// mutation, sloppy quorum fan-out with hint buffering, and the session clock
// response.
func (h *Handler) coordinateWrite(w http.ResponseWriter, req *http.Request, key string, incoming store.VectorClockVersion, wr keyWrite) {
	if m, ok := h.memberList.Get(h.selfID); ok && m.Bootstrapping {
		http.Error(w, errNodeBootstrapping, http.StatusServiceUnavailable)
		return
	}

	// Resolve the write quorum and CAS mode before touching the store so an
	// invalid consistency level or cas param rejects the request without side
	// effects.
	writeQuorum, err := h.requestedQuorum(req, h.writeQuorum)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	useCAS, err := requestedCAS(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// CAS mutations serialize through the key's primary replica so writers
	// racing through different coordinators contend on one store mutex
	// instead of forking into siblings.
	if useCAS && h.casRoutedElsewhere(w, req, key, wr.body) {
		return
	}

	// Bootstrap clock from the current local entry if the client didn't
	// provide one, so a blind overwrite dominates the existing value rather
	// than equaling it (an equal clock would be dropped as an idempotent
	// write). Never for CAS: there the clocks field is the client's
	// precondition, and an absent one must mean "expect no current value"
	// rather than silently adopting whatever the coordinator happens to hold.
	if len(incoming.Clocks) == 0 && !useCAS {
		incoming.Clocks = h.bootstrapClock(key)
	}

	// Primary write: increment self's counter and apply locally. In CAS mode
	// the store applies the mutation only if this version dominates every
	// existing sibling; a stale or concurrent precondition clock rejects the
	// request with 412 and no side effects.
	version := incoming.Increment(h.selfID)
	if err := h.applyLocalWrite(key, wr, version, useCAS); err != nil {
		if errors.Is(err, store.ErrCASConflict) {
			http.Error(w, "cas conflict: clocks do not dominate the current value", http.StatusPreconditionFailed)
			return
		}
		http.Error(w, wr.errMsg, http.StatusServiceUnavailable)
		return
	}

	// Sloppy quorum: fan out to the first RF *healthy* nodes on the ring and
	// wait for W acks. Unhealthy replicas are skipped in favor of the next
	// healthy nodes (so the write stays available while any W healthy nodes
	// exist) and each skipped intended owner gets a hint for later replay. The
	// fan-out happens even when self's ack already satisfies quorum (W=1) so
	// replicas still receive the write without waiting on anti-entropy.
	nodes, skipped := h.ring.GetHealthyReplicationNodes(key, h.replicationFactor)
	quorum := min(writeQuorum, len(nodes))
	acks, remaining, failed, resultCh := h.quorumFanOut(nodes, quorum, func(n *ring.Node) replicaResult {
		return replicaResult{n.ID, h.replicateWriteToSync(n.Address, key, wr, version.Clocks)}
	})

	if h.hintStore != nil {
		h.bufferHints(
			hintstore.Hint{Key: key, Value: wr.value, Deleted: wr.deleted, Clocks: version.Clocks},
			append(failed, nodeIDs(skipped)...), remaining, resultCh,
		)
	}

	if acks < quorum {
		http.Error(w, "write quorum not met", http.StatusServiceUnavailable)
		return
	}
	// Return the write's clock so the client can chain CAS writes and demand
	// read-your-writes on later session reads.
	setSessionClockHeader(w, version.Clocks)
	w.WriteHeader(http.StatusNoContent)
}

// applyLocalWrite applies wr to the local store at version v, using the
// compare-and-set variant when cas is set.
func (h *Handler) applyLocalWrite(key string, wr keyWrite, v store.VectorClockVersion, cas bool) error {
	switch {
	case wr.deleted && cas:
		return h.store.DeleteCAS(key, v)
	case wr.deleted:
		return h.store.Delete(key, v)
	case cas:
		return h.store.PutCAS(key, wr.value, v)
	default:
		return h.store.Put(key, wr.value, v)
	}
}

type DeleteKeyRequest struct {
	Clocks map[string]uint64 `json:"clocks,omitempty"`
}

// bootstrapClock returns a vector clock that takes the max of all sibling clocks
// for key. Used to seed delete tombstones so they causally dominate the current value.
func (h *Handler) bootstrapClock(key string) map[string]uint64 {
	clocks := make(map[string]uint64)
	entry, ok, err := h.store.Get(key)
	if err != nil {
		log.Printf("bootstrap clock %s: %v", key, err)
		return clocks
	}
	if ok {
		for _, sib := range entry.Siblings {
			for nodeID, c := range sib.Version.Clocks {
				if clocks[nodeID] < c {
					clocks[nodeID] = c
				}
			}
		}
	}
	return clocks
}

func (h *Handler) DeleteKey(w http.ResponseWriter, req *http.Request) {
	key := strings.TrimPrefix(req.URL.Path, keysPrefix)
	if key == "" {
		http.Error(w, errKeyRequired, http.StatusBadRequest)
		return
	}
	var body DeleteKeyRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, errInvalidBody, http.StatusBadRequest)
		return
	}

	incoming := store.VectorClockVersion{Clocks: body.Clocks}
	if incoming.Clocks == nil {
		incoming.Clocks = make(map[string]uint64)
	}

	// Replica delete: store tombstone as-is without fan-out.
	if req.Header.Get(headerXProxiedFrom) != "" {
		if err := h.store.Delete(key, incoming); err != nil {
			http.Error(w, "store delete failed", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}

	h.coordinateWrite(w, req, key, incoming, keyWrite{
		deleted: true,
		body:    body,
		errMsg:  "store delete failed",
	})
}

// replicateWriteToSync sends a replica sub-write (a value write, or a
// tombstone when wr.deleted is set) to addr and returns an error if the
// request fails or the replica responds with a non-204 status.
func (h *Handler) replicateWriteToSync(address, key string, wr keyWrite, clocks map[string]uint64) error {
	method, payload := http.MethodPut, any(PutKeyRequest{Value: wr.value, Clocks: clocks})
	if wr.deleted {
		method, payload = http.MethodDelete, any(DeleteKeyRequest{Clocks: clocks})
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(method, schemeHTTP+address+keysPrefix+key, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set(contentTypeHeader, contentTypeJSON)
	req.Header.Set(headerXProxiedFrom, h.selfID)
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

// replicateToSync sends a replica value write to addr: see replicateWriteToSync.
func (h *Handler) replicateToSync(address, key, value string, clocks map[string]uint64) error {
	return h.replicateWriteToSync(address, key, keyWrite{value: value}, clocks)
}

// replicateDeleteToSync sends a replica tombstone to addr: see replicateWriteToSync.
func (h *Handler) replicateDeleteToSync(address, key string, clocks map[string]uint64) error {
	return h.replicateWriteToSync(address, key, keyWrite{deleted: true}, clocks)
}

// readFromReplica fetches the local entry for key from a replica node. The
// response includes the vector clock so the coordinator can merge versions.
func (h *Handler) readFromReplica(address, key string) (NodeResponse, error) {
	req, err := http.NewRequest(http.MethodGet, schemeHTTP+address+keysPrefix+key, nil)
	if err != nil {
		return NodeResponse{}, err
	}
	req.Header.Set(headerXProxiedFrom, h.selfID)
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
	w.Header().Set(contentTypeHeader, contentTypeJSON)
	if err := json.NewEncoder(w).Encode(s); err != nil {
		http.Error(w, errFailedWrite, http.StatusInternalServerError)
	}
}

func (h *Handler) GetReplicationNodes(w http.ResponseWriter, req *http.Request) {
	var body ReplicateRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, errInvalidBody, http.StatusBadRequest)
		return
	}
	if body.Key == "" {
		http.Error(w, errKeyRequired, http.StatusBadRequest)
		return
	}
	if body.Factor < 1 {
		http.Error(w, "factor must be at least 1", http.StatusBadRequest)
		return
	}
	nodes := h.ring.GetReplicationNodes(body.Key, body.Factor)
	if len(nodes) == 0 {
		http.Error(w, errNoNodes, http.StatusServiceUnavailable)
		return
	}
	resp := ReplicateResponse{
		Key:   body.Key,
		Nodes: make([]NodeResponse, 0, len(nodes)),
	}
	for _, n := range nodes {
		resp.Nodes = append(resp.Nodes, NodeResponse{ID: n.ID, Address: n.Address, Status: h.nodeStatus(n.ID)})
	}
	w.Header().Set(contentTypeHeader, contentTypeJSON)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, errFailedWrite, http.StatusInternalServerError)
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
	w.Header().Set(contentTypeHeader, contentTypeJSON)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, errFailedWrite, http.StatusInternalServerError)
	}
}

func (h *Handler) Gossip(w http.ResponseWriter, req *http.Request) {
	var body GossipRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, errInvalidBody, http.StatusBadRequest)
		return
	}
	h.memberList.Merge(body.Members)
	w.Header().Set(contentTypeHeader, contentTypeJSON)
	if err := json.NewEncoder(w).Encode(h.memberList.GetAll()); err != nil {
		http.Error(w, errFailedWrite, http.StatusInternalServerError)
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

	// Fast path: serve from the pre-maintained tree without a store scan.
	if h.syncTrees != nil {
		if keys, ok := h.syncTrees.BucketKeys(vnodeHash, parsedBucket); ok {
			writeBucketKeys(w, keys)
			return
		}
	}

	// Fallback: scan the store for keys in the vnode range and bucket.
	vr, ok := h.ring.GetVnodeRange(vnodeHash)
	if !ok {
		http.Error(w, "unknown vnode", http.StatusNotFound)
		return
	}
	hashes, err := h.store.KeyHashes()
	if err != nil {
		log.Printf("sync bucket keys scan: %v", err)
		http.Error(w, errLocalRead, http.StatusInternalServerError)
		return
	}
	var keys []string
	for key := range hashes {
		if vr.Contains(merkle.HashKey(key)) && merkle.BucketIndex(key) == parsedBucket {
			keys = append(keys, key)
		}
	}
	writeBucketKeys(w, keys)
}

// writeBucketKeys encodes a bucket-keys response, normalizing a nil slice to an
// empty array so the JSON is `[]` rather than `null`.
func writeBucketKeys(w http.ResponseWriter, keys []string) {
	if keys == nil {
		keys = []string{}
	}
	w.Header().Set(contentTypeHeader, contentTypeJSON)
	if err := json.NewEncoder(w).Encode(SyncBucketKeysResponse{Keys: keys}); err != nil {
		http.Error(w, errFailedWrite, http.StatusInternalServerError)
	}
}

// PushSyncEntries applies a batch of entries sent by a primary node. The
// request body uses the same format as the SyncKeysResponse so the primary can
// reuse its existing serialization path.
func (h *Handler) PushSyncEntries(w http.ResponseWriter, req *http.Request) {
	var body SyncKeysResponse
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, errInvalidBody, http.StatusBadRequest)
		return
	}
	for key, sibs := range body.Entries {
		for _, sib := range sibs {
			v := store.VectorClockVersion{Clocks: sib.Clocks}
			var err error
			if sib.Deleted {
				err = h.store.Delete(key, v)
			} else {
				err = h.store.Put(key, sib.Value, v)
			}
			if err != nil {
				log.Printf("sync push apply %s: %v", key, err)
			}
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

// GetSyncState returns the root hash and per-bucket hashes for the requested
// vnode (?vnode=<endHash>). It serves from the pre-maintained Merkle tree when
// available, falling back to an on-the-fly scan of the local store. Both paths
// produce identical hashes (the tree and the scan share ComputeBucketHash /
// ComputeRootHash).
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

	// Fast path: serve from the pre-maintained tree without a store scan.
	if h.syncTrees != nil {
		if root, buckets, ok := h.syncTrees.SyncState(vnodeHash); ok {
			writeSyncState(w, root, buckets)
			return
		}
	}

	// Fallback: recompute bucket hashes by scanning the local store.
	vr, ok := h.ring.GetVnodeRange(vnodeHash)
	if !ok {
		http.Error(w, "unknown vnode", http.StatusNotFound)
		return
	}
	hashes, err := h.store.KeyHashes()
	if err != nil {
		log.Printf("sync state scan: %v", err)
		http.Error(w, errLocalRead, http.StatusInternalServerError)
		return
	}
	buckets := make([]map[string]uint32, merkle.BucketCount)
	for i := range buckets {
		buckets[i] = make(map[string]uint32)
	}
	for key, hash := range hashes {
		if !vr.Contains(merkle.HashKey(key)) {
			continue
		}
		buckets[merkle.BucketIndex(key)][key] = hash
	}
	bucketHashes := make([]uint32, merkle.BucketCount)
	for i, entries := range buckets {
		bucketHashes[i] = merkle.ComputeBucketHash(entries)
	}
	writeSyncState(w, merkle.ComputeRootHash(bucketHashes), bucketHashes)
}

// writeSyncState encodes a sync-state response from a root and its bucket hashes.
func writeSyncState(w http.ResponseWriter, root uint32, buckets []uint32) {
	w.Header().Set(contentTypeHeader, contentTypeJSON)
	if err := json.NewEncoder(w).Encode(SyncStateResponse{Root: root, Buckets: buckets}); err != nil {
		http.Error(w, errFailedWrite, http.StatusInternalServerError)
	}
}

// GetSyncKeys returns the full entry (all siblings with vector clocks) for
// each requested key. Used by the anti-entropy loop to fetch entries from a
// divergent bucket so the caller can apply repairs via the normal write path.
func (h *Handler) GetSyncKeys(w http.ResponseWriter, req *http.Request) {
	var body SyncKeysRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, errInvalidBody, http.StatusBadRequest)
		return
	}
	entries := make(map[string][]SyncSibling, len(body.Keys))
	for _, key := range body.Keys {
		entry, ok, err := h.store.Get(key)
		if err != nil {
			log.Printf("sync keys read %s: %v", key, err)
			continue
		}
		if !ok {
			continue
		}
		sibs := make([]SyncSibling, len(entry.Siblings))
		for i, sib := range entry.Siblings {
			sibs[i] = SyncSibling{Value: sib.Value, Deleted: sib.Deleted, Clocks: sib.Version.Clocks}
		}
		entries[key] = sibs
	}
	w.Header().Set(contentTypeHeader, contentTypeJSON)
	if err := json.NewEncoder(w).Encode(SyncKeysResponse{Entries: entries}); err != nil {
		http.Error(w, errFailedWrite, http.StatusInternalServerError)
	}
}
