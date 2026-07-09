//go:build sim

package sim

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	"github.com/colingraydon/continuum/internal/gossip"
)

// simNet is the in-memory network every node in a simulated cluster speaks
// through: node-to-node HTTP (replica fan-out, CAS forwarding, hint delivery,
// anti-entropy sync, migration) rides linkFrom round-trippers, and gossip
// datagrams ride simConn. All fault decisions — directed partitions, seeded
// message drops, added latency — are made here, per (from, to) edge, from a
// single seeded RNG, so a run's fault behavior derives from its seed rather
// than from separate proxy processes.
type simNet struct {
	mu    sync.Mutex
	rng   *rand.Rand
	nodes map[string]*simNode // node id -> current incarnation
	hosts map[string]string   // advertised host:port (HTTP and gossip) -> node id

	blocked  map[string]map[string]bool // from id -> to id -> blocked
	dropProb float64                    // per-message drop probability on unblocked edges
	delay    time.Duration              // fixed added latency on every delivered message
}

func newSimNet(seed int64) *simNet {
	return &simNet{
		rng:     rand.New(rand.NewSource(seed)),
		nodes:   make(map[string]*simNode),
		hosts:   make(map[string]string),
		blocked: make(map[string]map[string]bool),
	}
}

// register installs (or replaces, on restart) the node reachable at its
// advertised HTTP and gossip addresses.
func (sn *simNet) register(n *simNode) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	sn.nodes[n.id] = n
	sn.hosts[n.httpAddr] = n.id
	sn.hosts[n.gossipAddr] = n.id
}

// block cuts the directed edge from -> to.
func (sn *simNet) block(from, to string) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	if sn.blocked[from] == nil {
		sn.blocked[from] = make(map[string]bool)
	}
	sn.blocked[from][to] = true
}

// isolate blackholes all traffic into id from every other node, an
// asymmetric partition: id's own outbound traffic still flows.
func (sn *simNet) isolate(id string) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	for other := range sn.nodes {
		if other == id {
			continue
		}
		if sn.blocked[other] == nil {
			sn.blocked[other] = make(map[string]bool)
		}
		sn.blocked[other][id] = true
	}
}

// partition splits the cluster into two sides, blocking every edge that
// crosses the cut in both directions.
func (sn *simNet) partition(sideA, sideB []string) {
	for _, a := range sideA {
		for _, b := range sideB {
			sn.block(a, b)
			sn.block(b, a)
		}
	}
}

// healAll removes every block and burst fault.
func (sn *simNet) healAll() {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	sn.blocked = make(map[string]map[string]bool)
	sn.dropProb = 0
	sn.delay = 0
}

func (sn *simNet) setDropProb(p float64) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	sn.dropProb = p
}

func (sn *simNet) setDelay(d time.Duration) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	sn.delay = d
}

// route resolves a destination host and decides this message's fate. It
// returns the live target node (nil if the target is down or unknown),
// whether the message is lost (blocked edge or seeded drop), and the latency
// to add before delivery. The client edge ("client" as from) is never
// faulted: the harness's own traffic must reflect cluster state, not luck.
func (sn *simNet) route(from, host string) (target *simNode, lost bool, delay time.Duration) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	id, ok := sn.hosts[host]
	if !ok {
		return nil, false, 0
	}
	n := sn.nodes[id]
	if n == nil || !n.running.Load() {
		return nil, false, 0
	}
	if from != clientID {
		if sn.blocked[from][id] {
			return n, true, 0
		}
		if sn.dropProb > 0 && sn.rng.Float64() < sn.dropProb {
			return n, true, 0
		}
	}
	return n, false, sn.delay
}

// clientID is the reserved edge source for harness traffic.
const clientID = "client"

// errUnreachable mimics a refused connection: the target process is gone.
type errUnreachable struct{ host string }

func (e errUnreachable) Error() string {
	return fmt.Sprintf("sim: connect %s: connection refused", e.host)
}

// linkFrom is the http.RoundTripper for all HTTP a given node (or the test
// client) sends: it resolves the target in-process and serves the request
// straight through the target's mux. A blocked or dropped request behaves
// like a real one — the caller waits until its own timeout fires — because
// hint buffering, quorum clamping, and CAS fail-closed paths all key off
// timeouts, not instant errors.
type linkFrom struct {
	net  *simNet
	from string
}

func (l linkFrom) RoundTrip(req *http.Request) (*http.Response, error) {
	target, lost, delay := l.net.route(l.from, req.URL.Host)
	if target == nil {
		return nil, errUnreachable{req.URL.Host}
	}
	if lost {
		<-req.Context().Done()
		return nil, req.Context().Err()
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
	}

	// Serve on a fresh server-form request so the handler goroutine can
	// outlive a caller that times out without racing the caller's teardown.
	inner := httptest.NewRequest(req.Method, req.URL.String(), req.Body)
	inner.Header = req.Header.Clone()

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		target.mux.ServeHTTP(rec, inner)
		done <- rec
	}()
	select {
	case rec := <-done:
		resp := rec.Result()
		resp.Request = req
		return resp, nil
	case <-req.Context().Done():
		return nil, req.Context().Err()
	}
}

// simConn is the gossip.Conn for one node: datagrams are JSON round-tripped
// (like the real UDP codec) and delivered into the target's incoming channel,
// subject to the same edge faults as HTTP. Lost datagrams vanish silently —
// UDP semantics — and a full receive buffer drops, like a real socket.
type simConn struct {
	net  *simNet
	from string
	in   chan *gossip.GossipMessage
}

func newSimConn(net *simNet, from string) *simConn {
	return &simConn{net: net, from: from, in: make(chan *gossip.GossipMessage, 256)}
}

func (c *simConn) Start() {}
func (c *simConn) Stop()  {}

func (c *simConn) Incoming() <-chan *gossip.GossipMessage { return c.in }

func (c *simConn) Send(address string, msg *gossip.GossipMessage) error {
	target, lost, delay := c.net.route(c.from, address)
	if target == nil || lost {
		return nil // UDP: fire and forget
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	deliver := func() {
		var copied gossip.GossipMessage
		if err := json.Unmarshal(payload, &copied); err != nil {
			return
		}
		conn := target.conn
		if conn == nil || !target.running.Load() {
			return
		}
		select {
		case conn.in <- &copied:
		default: // receive buffer full: drop, like a real socket
		}
	}
	if delay > 0 {
		time.AfterFunc(delay, deliver)
		return nil
	}
	deliver()
	return nil
}
