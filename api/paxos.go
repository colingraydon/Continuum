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
		go h.prepareReplica(key, b, n, results, rejected)
	}
	for range replicas {
		if r := <-results; r != nil {
			if promised = append(promised, *r); len(promised) >= majority {
				break
			}
		}
	}
	return promised, drainHighest(rejected)
}

// prepareReplica runs one replica's prepare and reports the result on the
// channels: the promise on results (nil on error or rejection), and a
// rejecting replica's higher ballot on rejected.
func (h *Handler) prepareReplica(key string, b paxos.Ballot, n *ring.Node, results chan<- *prepareResponse, rejected chan<- paxos.Ballot) {
	resp, err := h.sendPrepare(n, key, b)
	switch {
	case err != nil:
		log.Printf("paxos prepare %s on %s: %v", key, n.ID, err)
		results <- nil
	case !resp.OK:
		rejected <- resp.Promised
		results <- nil
	default:
		results <- resp
	}
}

// sendPrepare dispatches a prepare to n, short-circuiting to a local call for
// self.
func (h *Handler) sendPrepare(n *ring.Node, key string, b paxos.Ballot) (*prepareResponse, error) {
	if n.ID == h.selfID {
		resp, err := h.localPrepare(key, b)
		return &resp, err
	}
	var resp prepareResponse
	err := h.postPaxos(n.Address, pathPaxosPrepare, prepareRequest{Key: key, Ballot: b}, &resp)
	return &resp, err
}

// drainHighest returns the greatest ballot buffered on ch (zero if none),
// draining it without blocking.
func drainHighest(ch <-chan paxos.Ballot) (higher paxos.Ballot) {
	for {
		select {
		case b := <-ch:
			if higher.Less(b) {
				higher = b
			}
		default:
			return higher
		}
	}
}

// proposePhase runs accept on every replica, returning at majority acks (or
// when every replica has answered); returns the ack count and the highest
// rejecting ballot.
func (h *Handler) proposePhase(m paxos.Mutation, replicas []*ring.Node, majority int) (acks int, higher paxos.Ballot) {
	results := make(chan *paxos.Promise, len(replicas))
	for _, n := range replicas {
		go func(n *ring.Node) { results <- h.sendPropose(n, m) }(n)
	}
	for range replicas {
		switch p := <-results; {
		case p == nil:
			// Transport or apply error: not a vote either way.
		case p.OK:
			if acks++; acks >= majority {
				return acks, higher
			}
		case higher.Less(p.Promised):
			higher = p.Promised
		}
	}
	return acks, higher
}

// sendPropose dispatches an accept to n (local call for self) and returns the
// promise, or nil on error.
func (h *Handler) sendPropose(n *ring.Node, m paxos.Mutation) *paxos.Promise {
	var p paxos.Promise
	var err error
	if n.ID == h.selfID {
		p, err = h.acceptor.Accept(m)
	} else {
		err = h.postPaxos(n.Address, pathPaxosPropose, m, &p)
	}
	if err != nil {
		log.Printf("paxos propose %s on %s: %v", m.Key, n.ID, err)
		return nil
	}
	return &p
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

// casReplicas resolves the replica set that may vote in a paxos round for
// key, and the round's majority size. Quorums must come from a stable set,
// so three rules apply. First, the walk ignores health verdicts: the
// healthy walk resizes with suspicion and quorums over a shifting set lose
// the intersection guarantee. Second, the majority denominator is the
// replication factor capped by *total known membership including dead
// members* — never by the locally resolvable replica set. A partitioned
// node declares its unreachable peers dead and drops them from its ring; if
// that shrank the denominator, the node would become a "majority of one"
// and decide rounds disjointly from the real majority on the other side
// (this forked histories in the asymmetric-partition fault scenario).
// A dead replica still counts toward the quorum size; it just cannot vote.
// Third, bootstrapping replicas are excluded from voting for the same
// denominator-unchanged reason: a node that discarded its data through the
// downtime gate rejoins with promises intact but a store that can no longer
// vouch for the keys it replicates, and a prepare majority leaning on its
// absent state merges to stale history (finding #10). It votes again once
// repair clears the flag.
func (h *Handler) casReplicas(key string) ([]*ring.Node, int, bool) {
	all := h.ring.GetReplicationNodes(key, h.replicationFactor)
	replicas := make([]*ring.Node, 0, len(all))
	for _, n := range all {
		if m, ok := h.memberList.Get(n.ID); ok && m.Bootstrapping {
			continue
		}
		replicas = append(replicas, n)
	}
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
		if h.casAttempt(w, key, version, wr, replicas, majority, &proposed) {
			return
		}
	}
	http.Error(w, errCASContended, http.StatusServiceUnavailable)
}

// casAttempt runs one paxos round for a conditional write. It returns true
// once it has written the client response (success or a terminal error), and
// false when the round was outpaced by a higher ballot and should be retried
// with a fresh one. proposed is threaded across attempts so a precondition
// failure after this request has ever proposed degrades to a retryable 503.
func (h *Handler) casAttempt(w http.ResponseWriter, key string, version store.VectorClockVersion, wr keyWrite, replicas []*ring.Node, majority int, proposed *bool) (done bool) {
	ballot := h.nextBallot()
	promises, higher := h.preparePhase(key, ballot, replicas, majority)
	if len(promises) < majority {
		return h.retryOrFail(w, higher)
	}

	// Finish an in-flight round first: its mutation may already hold a
	// majority of accepts, i.e. be decided without having been applied. If
	// it is this request's own mutation from an earlier attempt, finishing
	// it is success; otherwise the client retries against the settled state.
	if acc := pendingAccepted(promises); acc != nil {
		if h.finishRound(*acc, ballot, replicas, majority) && mutationMatches(*acc, version, wr) {
			h.casSucceeded(w, version)
		} else {
			http.Error(w, errCASInFlight, http.StatusServiceUnavailable)
		}
		return true
	}

	survivors := committedSurvivors(promises)

	// An earlier attempt's proposal may have been resurrected and committed
	// by a rival round while this coordinator saw only timeouts. The
	// committed sibling carries this request's exact version and value, so
	// it is provably this write: report success.
	if casCommittedHere(survivors, version, wr) {
		h.casSucceeded(w, version)
		return true
	}

	// Precondition: the write's version must strictly dominate every
	// committed sibling the majority can prove — same client-facing
	// semantics as before, but against quorum-merged state instead of one
	// primary's local view.
	if !casPreconditionHolds(survivors, version) {
		h.casPreconditionFailed(w, *proposed)
		return true
	}

	m := paxos.Mutation{Key: key, Value: wr.value, Deleted: wr.deleted, Clocks: version.Clocks, Ballot: ballot}
	*proposed = true
	acks, higher := h.proposePhase(m, replicas, majority)
	if acks < majority {
		return h.retryOrFail(w, higher)
	}

	if h.commitPhase(m, replicas, majority) < majority {
		// Decided but under-applied: a later round will finish it, but this
		// client cannot be told it committed durably everywhere.
		http.Error(w, errFailedWrite, http.StatusServiceUnavailable)
		return true
	}
	h.casSucceeded(w, version)
	return true
}

// retryOrFail handles a phase that fell short of majority: a higher observed
// ballot means a concurrent round outpaced us, so observe it and signal a
// retry (done=false); otherwise the quorum is genuinely unavailable and we
// write a retryable 503 (done=true).
func (h *Handler) retryOrFail(w http.ResponseWriter, higher paxos.Ballot) (done bool) {
	if !higher.IsZero() {
		h.observeBallot(higher)
		return false
	}
	http.Error(w, errCASUnavailable, http.StatusServiceUnavailable)
	return true
}

// casSucceeded writes the 204 for a committed conditional write, returning
// the write's clock so the client can chain the next CAS or session read.
func (h *Handler) casSucceeded(w http.ResponseWriter, version store.VectorClockVersion) {
	setSessionClockHeader(w, version.Clocks)
	w.WriteHeader(http.StatusNoContent)
}

// casPreconditionFailed reports a failed precondition: 412 when no side
// effect is possible, or a retryable 503 once this request has proposed and
// its mutation may have committed and been superseded since.
func (h *Handler) casPreconditionFailed(w http.ResponseWriter, proposed bool) {
	if proposed {
		http.Error(w, errCASUnavailable, http.StatusServiceUnavailable)
		return
	}
	http.Error(w, errCASConflict, http.StatusPreconditionFailed)
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
