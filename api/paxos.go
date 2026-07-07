package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/colingraydon/continuum/internal/hintstore"
	"github.com/colingraydon/continuum/internal/paxos"
	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/store"
)

// Conditional writes (?cas=true) run one single-decree Paxos round per
// mutation among the key's strict replica set, Cassandra-LWT style. The
// coordinator prepares a ballot with a majority, finishes any in-flight
// round it learns about, evaluates the client's precondition against the
// majority-merged committed state, proposes, and applies the mutation once a
// majority accepts. Serial reads (?consistency=serial) run the prepare phase
// only. Majority intersection is what closes fault-harness finding #7: no
// two rounds can both decide without sharing an acceptor, no matter how the
// ring view churns, so CAS can no longer fork through two nodes that each
// believe they are the key's primary.

const (
	pathPaxosPrepare = "/paxos/prepare"
	pathPaxosPropose = "/paxos/propose"
	pathPaxosCommit  = "/paxos/commit"

	errCASUnavailable = "cas quorum unavailable, retry"
	errCASContended   = "cas round contended, retry"
	errCASInFlight    = "cas finished an in-flight round, retry"
	errCASConflict    = "cas conflict: clocks do not dominate the current value"
	casAttempts       = 3
)

// SetPaxosAcceptor replaces the handler's acceptor. main wires a persistent
// one (DATA_DIR/paxos) before serving; the default from NewHandler is
// memory-only, which is only safe when the store itself is memory-only.
func (h *Handler) SetPaxosAcceptor(a *paxos.Acceptor) { h.acceptor = a }

// nextBallot mints a ballot strictly above every ballot this node has minted
// or observed. Wall time seeds the counter so fresh coordinators start above
// long-dead rounds; observeBallot folds in rejections so retries leapfrog.
func (h *Handler) nextBallot() paxos.Ballot {
	for {
		prev := h.ballotCounter.Load()
		next := uint64(time.Now().UnixNano())
		if next <= prev {
			next = prev + 1
		}
		if h.ballotCounter.CompareAndSwap(prev, next) {
			return paxos.Ballot{Counter: next, Node: h.selfID}
		}
	}
}

// observeBallot records a ballot seen in a rejection so the next mint
// supersedes it.
func (h *Handler) observeBallot(b paxos.Ballot) {
	for {
		prev := h.ballotCounter.Load()
		if b.Counter <= prev || h.ballotCounter.CompareAndSwap(prev, b.Counter) {
			return
		}
	}
}

// --- replica-side endpoints --------------------------------------------------

type prepareRequest struct {
	Key    string       `json:"key"`
	Ballot paxos.Ballot `json:"ballot"`
}

// prepareResponse is a Promise plus the replica's committed state for the
// key, so the prepare phase doubles as the quorum read the precondition (and
// a serial read) needs.
type prepareResponse struct {
	paxos.Promise
	Entry NodeResponse `json:"entry"`
}

// PaxosPrepare handles the prepare phase on a replica: gate the ballot
// through the acceptor and report local committed state.
func (h *Handler) PaxosPrepare(w http.ResponseWriter, req *http.Request) {
	var pr prepareRequest
	if err := json.NewDecoder(req.Body).Decode(&pr); err != nil || pr.Key == "" {
		http.Error(w, errInvalidBody, http.StatusBadRequest)
		return
	}
	resp, err := h.localPrepare(pr.Key, pr.Ballot)
	if err != nil {
		http.Error(w, "paxos prepare failed", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set(contentTypeHeader, contentTypeJSON)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, errFailedWrite, http.StatusInternalServerError)
	}
}

func (h *Handler) localPrepare(key string, b paxos.Ballot) (prepareResponse, error) {
	p, err := h.acceptor.Prepare(key, b)
	if err != nil {
		return prepareResponse{}, err
	}
	resp := prepareResponse{Promise: p}
	resp.Entry = NodeResponse{ID: h.selfID, Status: h.nodeStatus(h.selfID)}
	entry, ok, err := h.store.Get(key)
	if err != nil {
		return prepareResponse{}, err
	}
	if ok {
		resp.Entry = entryToResponse(h.selfID, h.nodeStatus(h.selfID), entry)
	}
	return resp, nil
}

// PaxosPropose handles the accept phase on a replica.
func (h *Handler) PaxosPropose(w http.ResponseWriter, req *http.Request) {
	var m paxos.Mutation
	if err := json.NewDecoder(req.Body).Decode(&m); err != nil || m.Key == "" {
		http.Error(w, errInvalidBody, http.StatusBadRequest)
		return
	}
	p, err := h.acceptor.Accept(m)
	if err != nil {
		http.Error(w, "paxos accept failed", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set(contentTypeHeader, contentTypeJSON)
	if err := json.NewEncoder(w).Encode(p); err != nil {
		http.Error(w, errFailedWrite, http.StatusInternalServerError)
	}
}

// PaxosCommit applies a decided mutation to the replica's store, then clears
// the round's accepted state. Apply-before-clear: a crash between the two
// re-commits idempotently (an equal clock is dropped as an idempotent write).
func (h *Handler) PaxosCommit(w http.ResponseWriter, req *http.Request) {
	var m paxos.Mutation
	if err := json.NewDecoder(req.Body).Decode(&m); err != nil || m.Key == "" {
		http.Error(w, errInvalidBody, http.StatusBadRequest)
		return
	}
	if err := h.localCommit(m); err != nil {
		http.Error(w, "paxos commit failed", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) localCommit(m paxos.Mutation) error {
	version := store.VectorClockVersion{Clocks: m.Clocks}
	var err error
	if m.Deleted {
		err = h.store.Delete(m.Key, version)
	} else {
		err = h.store.Put(m.Key, m.Value, version)
	}
	if err != nil {
		return err
	}
	return h.acceptor.Commit(m.Key, m.Ballot)
}

// --- coordinator round machinery ----------------------------------------------

// postPaxos runs one phase request against a peer over the casClient.
func (h *Handler) postPaxos(address, path string, body, out any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, schemeHTTP+address+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set(contentTypeHeader, contentTypeJSON)
	resp, err := h.casClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("paxos %s on %s: status %d", path, address, resp.StatusCode)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// preparePhase runs prepare on every replica, returning as soon as a
// majority has promised (or every replica has answered). Early exit at
// majority is safe: any majority of promises intersects any accept
// majority, so a decided (majority-accepted) in-flight mutation is always
// visible in the collected promises; debris on the stragglers never held a
// majority and can be superseded. It also keeps a dead replica's timeout
// off the round's latency — the reason contended rounds piled up before.
// The second return is the highest rejecting ballot seen (zero if none).
func (h *Handler) preparePhase(key string, b paxos.Ballot, replicas []*ring.Node, majority int) (promised []prepareResponse, higher paxos.Ballot) {
	results := make(chan *prepareResponse, len(replicas))
	rejected := make(chan paxos.Ballot, len(replicas))
	for _, n := range replicas {
		go func(n *ring.Node) {
			var resp prepareResponse
			var err error
			if n.ID == h.selfID {
				resp, err = h.localPrepare(key, b)
			} else {
				err = h.postPaxos(n.Address, pathPaxosPrepare, prepareRequest{Key: key, Ballot: b}, &resp)
			}
			if err != nil {
				log.Printf("paxos prepare %s on %s: %v", key, n.ID, err)
				results <- nil
				return
			}
			if !resp.OK {
				rejected <- resp.Promised
				results <- nil
				return
			}
			results <- &resp
		}(n)
	}
	for range replicas {
		if r := <-results; r != nil {
			promised = append(promised, *r)
			if len(promised) >= majority {
				break
			}
		}
	}
	for {
		select {
		case b := <-rejected:
			if higher.Less(b) {
				higher = b
			}
		default:
			return promised, higher
		}
	}
}

// proposePhase runs accept on every replica, returning at majority acks (or
// when every replica has answered); returns the ack count and the highest
// rejecting ballot.
func (h *Handler) proposePhase(m paxos.Mutation, replicas []*ring.Node, majority int) (acks int, higher paxos.Ballot) {
	results := make(chan *paxos.Promise, len(replicas))
	for _, n := range replicas {
		go func(n *ring.Node) {
			var p paxos.Promise
			var err error
			if n.ID == h.selfID {
				p, err = h.acceptor.Accept(m)
			} else {
				err = h.postPaxos(n.Address, pathPaxosPropose, m, &p)
			}
			if err != nil {
				log.Printf("paxos propose %s on %s: %v", m.Key, n.ID, err)
				results <- nil
				return
			}
			results <- &p
		}(n)
	}
	for range replicas {
		p := <-results
		if p == nil {
			continue
		}
		if p.OK {
			acks++
			if acks >= majority {
				return acks, higher
			}
		} else if higher.Less(p.Promised) {
			higher = p.Promised
		}
	}
	return acks, higher
}

// commitPhase applies the decided mutation on every replica, returning at
// majority acks; a background goroutine drains the stragglers and hints any
// failures for replay (anti-entropy remains the backstop). The decision
// itself is already durable in the accept quorum, so a missed commit can
// always be finished by a later round's prepare.
func (h *Handler) commitPhase(m paxos.Mutation, replicas []*ring.Node, majority int) (acks int) {
	results := make(chan replicaResult, len(replicas))
	for _, n := range replicas {
		go func(n *ring.Node) {
			var err error
			if n.ID == h.selfID {
				err = h.localCommit(m)
			} else {
				err = h.postPaxos(n.Address, pathPaxosCommit, m, nil)
			}
			results <- replicaResult{n.ID, err}
		}(n)
	}
	var failed []string
	received := 0
	for received < len(replicas) {
		r := <-results
		received++
		if r.err == nil {
			acks++
			if acks >= majority {
				break
			}
		} else {
			log.Printf("paxos commit %s on %s: %v", m.Key, r.nodeID, r.err)
			failed = append(failed, r.nodeID)
		}
	}
	if h.hintStore != nil {
		h.bufferHints(hintstore.Hint{Key: m.Key, Value: m.Value, Deleted: m.Deleted, Clocks: m.Clocks},
			failed, len(replicas)-received, results)
	}
	return acks
}

// pendingAccepted returns the accepted mutation that may still be an
// undecided in-flight round: the highest-ballot accepted among the promises
// whose ballot lies above every committed ballot the promises report.
//
// The commit filter is load-bearing. A proposal accepted by a sub-majority
// (say, only its own coordinator) whose round then lost to a higher ballot
// is dead debris — the winner's round committed without ever being obliged
// to see it. Treating that debris as in-flight and "finishing" it would
// re-propose a superseded write over the newer commit; the history checker
// caught precisely this as forked CAS generations. Debris above every
// visible commit is different: if it was decided, its accept majority
// intersects this prepare majority, so it shows up here and must be
// finished before anything newer is proposed.
func pendingAccepted(promises []prepareResponse) *paxos.Mutation {
	var maxCommitted paxos.Ballot
	for _, p := range promises {
		if maxCommitted.Less(p.Committed) {
			maxCommitted = p.Committed
		}
	}
	var best *paxos.Mutation
	for _, p := range promises {
		if p.Accepted != nil && maxCommitted.Less(p.Accepted.Ballot) &&
			(best == nil || best.Ballot.Less(p.Accepted.Ballot)) {
			best = p.Accepted
		}
	}
	return best
}

// finishRound re-proposes and commits a previously-accepted mutation under
// the caller's ballot, completing a round whose original coordinator
// vanished between accept and commit. Idempotent from the store's view.
func (h *Handler) finishRound(acc paxos.Mutation, b paxos.Ballot, replicas []*ring.Node, majority int) bool {
	m := acc
	m.Ballot = b
	acks, _ := h.proposePhase(m, replicas, majority)
	if acks < majority {
		return false
	}
	return h.commitPhase(m, replicas, majority) >= majority
}

// committedSurvivors dominance-merges the committed entries carried by the
// promises: the freshest committed state any majority can prove.
func committedSurvivors(promises []prepareResponse) []SiblingResponse {
	responses := make([]NodeResponse, len(promises))
	for i, p := range promises {
		responses[i] = p.Entry
	}
	return mergeResponses(responses)
}

// casReplicas resolves the strict replica set (the health-ignoring ring
// walk) and the round's majority size. Paxos quorums must come from a stable
// set, so two rules apply. First, the walk ignores health verdicts: the
// healthy walk resizes with suspicion and quorums over a shifting set lose
// the intersection guarantee. Second, the majority denominator is the
// replication factor capped by *total known membership including dead
// members* — never by the locally resolvable replica set. A partitioned
// node declares its unreachable peers dead and drops them from its ring; if
// that shrank the denominator, the node would become a "majority of one"
// and decide rounds disjointly from the real majority on the other side
// (this forked histories in the asymmetric-partition fault scenario).
// A dead replica still counts toward the quorum size; it just cannot vote.
func (h *Handler) casReplicas(key string) ([]*ring.Node, int, bool) {
	replicas := h.ring.GetReplicationNodes(key, h.replicationFactor)
	if len(replicas) == 0 {
		return nil, 0, false
	}
	expected := h.replicationFactor
	if n := h.memberList.Size(); n < expected {
		expected = n
	}
	return replicas, expected/2 + 1, true
}

// paxosCAS is the coordinator side of a conditional write: the full round,
// with bounded retries under contention. incoming is the client's
// precondition clock context ("expect no current value" when empty).
func (h *Handler) paxosCAS(w http.ResponseWriter, key string, incoming store.VectorClockVersion, wr keyWrite) {
	replicas, majority, ok := h.casReplicas(key)
	if !ok {
		http.Error(w, errNoNodes, http.StatusServiceUnavailable)
		return
	}

	// The write's version is deterministic across attempts (client clocks
	// plus this coordinator's increment), which is what lets a retry
	// recognize its own mutation in the survivors after a rival round
	// resurrected and committed it.
	version := incoming.Increment(h.selfID)

	// proposed tracks whether any attempt has sent an accept request. From
	// that point on this mutation may commit through someone else's
	// resurrection at any time, so a failed precondition can no longer
	// prove "no side effects" and must degrade from 412 to a retryable 503.
	proposed := false

	for attempt := 0; attempt < casAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(10+attempt*20) * time.Millisecond)
		}
		ballot := h.nextBallot()
		promises, higher := h.preparePhase(key, ballot, replicas, majority)
		if len(promises) < majority {
			if !higher.IsZero() {
				h.observeBallot(higher)
				continue // outpaced by a concurrent round, not unavailable
			}
			http.Error(w, errCASUnavailable, http.StatusServiceUnavailable)
			return
		}

		// Finish an in-flight round first: its mutation may already hold a
		// majority of accepts, i.e. be decided without having been applied.
		// If it is this request's own mutation from an earlier attempt,
		// finishing it *is* success; otherwise the client retries against
		// the settled state.
		if acc := pendingAccepted(promises); acc != nil {
			ok := h.finishRound(*acc, ballot, replicas, majority)
			if ok && mutationMatches(*acc, version, wr) {
				setSessionClockHeader(w, version.Clocks)
				w.WriteHeader(http.StatusNoContent)
				return
			}
			http.Error(w, errCASInFlight, http.StatusServiceUnavailable)
			return
		}

		survivors := committedSurvivors(promises)

		// An earlier attempt's proposal may have been resurrected and
		// committed by a rival round while this coordinator saw only
		// timeouts. The committed sibling carries this request's exact
		// version and value, so it is provably this write: report success.
		if casCommittedHere(survivors, version, wr) {
			setSessionClockHeader(w, version.Clocks)
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// Precondition: the write's version must strictly dominate every
		// committed sibling the majority can prove — same client-facing
		// semantics as before, but against quorum-merged state instead of
		// one primary's local view.
		if !casPreconditionHolds(survivors, version) {
			if proposed {
				// The mutation left this coordinator in an earlier attempt
				// and may have committed and been superseded since; 412
				// would promise "no side effects" that cannot be proven.
				http.Error(w, errCASUnavailable, http.StatusServiceUnavailable)
				return
			}
			http.Error(w, errCASConflict, http.StatusPreconditionFailed)
			return
		}

		m := paxos.Mutation{Key: key, Value: wr.value, Deleted: wr.deleted, Clocks: version.Clocks, Ballot: ballot}
		proposed = true
		acks, higher := h.proposePhase(m, replicas, majority)
		if acks < majority {
			if !higher.IsZero() {
				h.observeBallot(higher)
				continue
			}
			http.Error(w, errCASUnavailable, http.StatusServiceUnavailable)
			return
		}

		if h.commitPhase(m, replicas, majority) < majority {
			// Decided but under-applied: a later round will finish it, but
			// this client cannot be told it committed durably everywhere.
			http.Error(w, errFailedWrite, http.StatusServiceUnavailable)
			return
		}
		setSessionClockHeader(w, version.Clocks)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	http.Error(w, errCASContended, http.StatusServiceUnavailable)
}

// mutationMatches reports whether an accepted mutation is this request's
// own: same version clock, value, and tombstone flag. The clock alone
// identifies the (coordinator, precondition) pair; value equality guards the
// same-coordinator-same-context race between two different clients.
func mutationMatches(m paxos.Mutation, version store.VectorClockVersion, wr keyWrite) bool {
	return m.Value == wr.value && m.Deleted == wr.deleted &&
		(store.VectorClockVersion{Clocks: m.Clocks}).Equal(version)
}

// casCommittedHere reports whether the survivors already contain exactly
// this request's mutation.
func casCommittedHere(survivors []SiblingResponse, version store.VectorClockVersion, wr keyWrite) bool {
	for _, s := range survivors {
		if s.Value == wr.value && s.Deleted == wr.deleted &&
			(store.VectorClockVersion{Clocks: s.Clocks}).Equal(version) {
			return true
		}
	}
	return false
}

// casPreconditionHolds reports whether version strictly dominates every
// committed sibling (an empty sibling set — key absent — always passes).
func casPreconditionHolds(survivors []SiblingResponse, version store.VectorClockVersion) bool {
	for _, s := range survivors {
		if !(store.VectorClockVersion{Clocks: s.Clocks}).HappensBefore(version) {
			return false
		}
	}
	return true
}

// serialRead serves GET ?consistency=serial: a linearizable read. It runs
// the prepare phase (so it observes every decided round through majority
// intersection) and finishes any in-flight accepted mutation before
// answering — without that, a round that was accepted by a majority but
// only partially committed could be visible to one read and invisible to
// the next.
func (h *Handler) serialRead(w http.ResponseWriter, key string) {
	replicas, majority, ok := h.casReplicas(key)
	if !ok {
		http.Error(w, errNoNodes, http.StatusServiceUnavailable)
		return
	}
	ballot := h.nextBallot()
	promises, _ := h.preparePhase(key, ballot, replicas, majority)
	if len(promises) < majority {
		http.Error(w, errCASUnavailable, http.StatusServiceUnavailable)
		return
	}

	responses := make([]NodeResponse, 0, len(promises)+1)
	for _, p := range promises {
		responses = append(responses, p.Entry)
	}
	if acc := pendingAccepted(promises); acc != nil {
		if !h.finishRound(*acc, ballot, replicas, majority) {
			http.Error(w, errCASUnavailable, http.StatusServiceUnavailable)
			return
		}
		responses = append(responses, NodeResponse{
			ID:       acc.Ballot.Node,
			Siblings: []SiblingResponse{{Value: acc.Value, Clocks: acc.Clocks, Deleted: acc.Deleted}},
		})
	}
	h.writeKeyResponse(w, replicas[0], mergeResponses(responses))
}
