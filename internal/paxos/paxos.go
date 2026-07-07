// Package paxos implements the replica-side acceptor for single-decree
// Paxos per key — the Cassandra lightweight-transaction shape. Each CAS
// mutation runs one consensus round among the key's replica set: a
// coordinator prepares a ballot, gathers promises from a majority (learning
// both the committed state and any accepted-but-uncommitted mutation it must
// finish first), proposes the mutation, and commits once a majority accepts.
// There is no leader and no log: agreement is re-established per round, so
// membership churn never leaves a stale leader serving CAS from state that
// missed a write — the failure mode the previous primary-serialized design
// had (fault harness finding #7).
//
// Safety rests on acceptors remembering their promises across restarts, so
// the acceptor persists every state change to its own append-only log before
// acknowledging (the same pattern as the hint log). A crash-restarted node
// that forgot a promise could otherwise let two disjoint majorities accept
// conflicting values for one ballot.
package paxos

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/colingraydon/continuum/internal/wal"
)

// Ballot orders proposal attempts. Counter is the primary key (coordinators
// derive it from wall time, bumped past any ballot they have seen); Node
// breaks ties so two coordinators can never mint equal ballots.
type Ballot struct {
	Counter uint64 `json:"counter"`
	Node    string `json:"node"`
}

// Less reports whether b orders strictly before o.
func (b Ballot) Less(o Ballot) bool {
	if b.Counter != o.Counter {
		return b.Counter < o.Counter
	}
	return b.Node < o.Node
}

func (b Ballot) IsZero() bool { return b.Counter == 0 && b.Node == "" }

func (b Ballot) String() string { return fmt.Sprintf("%d@%s", b.Counter, b.Node) }

// Mutation is one round's value: the complete intended write, self-contained
// so any coordinator that learns it from a promise can re-propose and commit
// it verbatim (finishing another coordinator's in-flight round).
type Mutation struct {
	Key     string            `json:"key"`
	Value   string            `json:"value,omitempty"`
	Deleted bool              `json:"deleted,omitempty"`
	Clocks  map[string]uint64 `json:"clocks"` // the write's full vector clock version
	Ballot  Ballot            `json:"ballot"`
}

// keyState is one key's acceptor state. Promised gates prepares and accepts;
// Accepted holds the round in flight, cleared by commit.
type keyState struct {
	Promised Ballot    `json:"promised"`
	Accepted *Mutation `json:"accepted,omitempty"`
}

// Promise is a successful Prepare response: the acceptor will reject
// anything below the promised ballot, and reports the accepted-uncommitted
// mutation (if any) that the coordinator must finish before its own round.
type Promise struct {
	OK       bool      `json:"ok"`
	Promised Ballot    `json:"promised"` // on !OK: the higher ballot that won
	Accepted *Mutation `json:"accepted,omitempty"`
}

// Acceptor holds per-key Paxos state, durably when opened with a log
// directory. All methods are safe for concurrent use.
type Acceptor struct {
	mu   sync.Mutex
	keys map[string]*keyState
	log  *wal.Writer // nil = memory-only (no persistence configured)
}

// NewAcceptor returns a memory-only acceptor: promises do not survive a
// restart. Use OpenAcceptor wherever the store itself is persistent.
func NewAcceptor() *Acceptor {
	return &Acceptor{keys: make(map[string]*keyState)}
}

// logRecord is one persisted state change: the full post-change state for a
// key. Replay keeps the last record per key.
type logRecord struct {
	Key   string   `json:"key"`
	State keyState `json:"state"`
}

// OpenAcceptor opens (or creates) a persistent acceptor backed by an
// append-only log in dir. Recovery replays the log, keeping the last record
// per key, then compacts it to one record per live key so the log does not
// grow with CAS traffic across restarts.
func OpenAcceptor(dir string) (*Acceptor, error) {
	a := &Acceptor{keys: make(map[string]*keyState)}

	r, err := wal.NewReader(dir)
	if err != nil {
		return nil, fmt.Errorf("paxos: open log reader: %w", err)
	}
	var lastSeq uint64
	replayed := 0
	for {
		rec, err := r.Next()
		if err != nil {
			break // io.EOF, or a torn tail already truncated by the reader
		}
		var lr logRecord
		if err := json.Unmarshal(rec.Payload, &lr); err != nil {
			continue // skip an undecodable record; the frame CRC already passed
		}
		st := lr.State
		a.keys[lr.Key] = &st
		lastSeq = rec.Seq
		replayed++
	}
	if err := r.Close(); err != nil {
		return nil, fmt.Errorf("paxos: close log reader: %w", err)
	}

	w, err := wal.Open(dir)
	if err != nil {
		return nil, fmt.Errorf("paxos: open log: %w", err)
	}
	a.log = w

	// Compact: re-append one record per live key, then drop the replayed
	// prefix. Skipped when the log is already minimal.
	if replayed > len(a.keys) {
		for key, st := range a.keys {
			if err := a.appendLocked(key, st); err != nil {
				return nil, fmt.Errorf("paxos: compact: %w", err)
			}
		}
		if err := w.Sync(); err != nil {
			return nil, fmt.Errorf("paxos: compact sync: %w", err)
		}
		if err := w.TruncateThrough(lastSeq); err != nil {
			return nil, fmt.Errorf("paxos: compact truncate: %w", err)
		}
	}
	return a, nil
}

// appendLocked writes key's current state to the log. Callers must hold a.mu
// (or be single-threaded, as in recovery).
func (a *Acceptor) appendLocked(key string, st *keyState) error {
	if a.log == nil {
		return nil
	}
	payload, err := json.Marshal(logRecord{Key: key, State: *st})
	if err != nil {
		return err
	}
	_, err = a.log.Append(payload)
	return err
}

// persistLocked appends and fsyncs key's state. The fsync must complete
// before the acceptor's reply leaves the node: a promise that is not durable
// is a promise a crash can revoke, and revoked promises are how two
// majorities accept conflicting values.
func (a *Acceptor) persistLocked(key string, st *keyState) error {
	if err := a.appendLocked(key, st); err != nil {
		return err
	}
	if a.log == nil {
		return nil
	}
	return a.log.Sync()
}

// Prepare handles a coordinator's prepare: promise ballot b if nothing
// higher was already promised, reporting any accepted-uncommitted mutation.
func (a *Acceptor) Prepare(key string, b Ballot) (Promise, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	st, ok := a.keys[key]
	if !ok {
		st = &keyState{}
		a.keys[key] = st
	}
	if st.Promised.IsZero() || st.Promised.Less(b) {
		st.Promised = b
		if err := a.persistLocked(key, st); err != nil {
			return Promise{}, err
		}
	}
	if b.Less(st.Promised) {
		return Promise{OK: false, Promised: st.Promised}, nil
	}
	return Promise{OK: true, Promised: st.Promised, Accepted: st.Accepted}, nil
}

// Accept handles a coordinator's propose: accept the mutation unless a
// higher ballot has been promised since the prepare.
func (a *Acceptor) Accept(m Mutation) (Promise, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	st, ok := a.keys[m.Key]
	if !ok {
		st = &keyState{}
		a.keys[m.Key] = st
	}
	if m.Ballot.Less(st.Promised) {
		return Promise{OK: false, Promised: st.Promised}, nil
	}
	st.Promised = m.Ballot
	cp := m
	st.Accepted = &cp
	if err := a.persistLocked(m.Key, st); err != nil {
		return Promise{}, err
	}
	return Promise{OK: true, Promised: st.Promised}, nil
}

// Commit clears the accepted mutation for key if it belongs to ballot b (a
// later round may already be in flight; its state must survive). The caller
// applies the mutation to the store before calling Commit, so a crash
// between the two only re-commits idempotently on the next round.
func (a *Acceptor) Commit(key string, b Ballot) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	st, ok := a.keys[key]
	if !ok || st.Accepted == nil || st.Accepted.Ballot != b {
		return nil
	}
	st.Accepted = nil
	return a.persistLocked(key, st)
}

// Close flushes and closes the log. Memory-only acceptors are a no-op.
func (a *Acceptor) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.log == nil {
		return nil
	}
	return a.log.Close()
}
