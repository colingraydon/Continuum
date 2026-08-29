//go:build sim

package sim

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colingraydon/continuum/api"
	"github.com/colingraydon/continuum/internal/antientropy"
	"github.com/colingraydon/continuum/internal/gossip"
	"github.com/colingraydon/continuum/internal/hintstore"
	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/store"
)

// Compressed timing: the production second-scale intervals shrink to
// milliseconds so a full fault cycle (fault -> suspect -> dead -> heal ->
// repair) fits in a few hundred milliseconds of wall clock and one `make sim`
// explores many seeds.
const (
	gossipInterval = 25 * time.Millisecond
	staleThreshold = 250 * time.Millisecond
	syncInterval   = 100 * time.Millisecond
	hintInterval   = 100 * time.Millisecond
	replicaTimeout = 100 * time.Millisecond
	clientTimeout  = 500 * time.Millisecond
	vnodesPerNode  = 8 // few vnodes: anti-entropy round-robins one primary vnode per tick
	hintCapPerNode = 10_000
	hintTTL        = time.Hour
)

// simNode is one in-process Continuum node: real store, ring, member list,
// gossiper, anti-entropy manager, hint store, and HTTP handler — the same
// wiring as cmd/continuum, minus sockets (simNet), disks (memory store), and
// signal handling (ctx cancel). A crash is therefore a full state loss for
// the node; the cluster must repair it through replication, hints, and
// anti-entropy, which is exactly what the checks assert.
type simNode struct {
	id         string
	dc         string // failure domain above the zone; "" in single-DC runs
	httpAddr   string
	gossipAddr string

	store    *store.Store
	ring     *ring.Ring
	ml       *gossip.MemberList
	gossiper *gossip.Gossiper
	ae       *antientropy.Manager
	h        *api.Handler
	mux      http.Handler
	conn     *simConn

	cancel  context.CancelFunc
	running atomic.Bool
}

type simConfig struct {
	nodes             int
	replicationFactor int
	writeQuorum       int
	readQuorum        int
	// dcs assigns a data center to each node by index; nil leaves every node
	// unlabeled, which is the single-DC shape the other scenarios run in.
	dcs []string
	// dcReplication installs per-DC replica targets on every node's ring. When
	// set it replaces replicationFactor with its sum, mirroring how
	// REPLICATION_FACTOR_BY_DC overrides REPLICATION_FACTOR in main.go.
	dcReplication map[string]int
}

func (c simConfig) withDefaults() simConfig {
	if c.nodes == 0 {
		c.nodes = 3
	}
	if len(c.dcReplication) > 0 {
		total := 0
		for _, n := range c.dcReplication {
			total += n
		}
		c.replicationFactor = total
	}
	if c.replicationFactor == 0 {
		c.replicationFactor = 3
	}
	// Majority of the effective RF: 2 for the default RF 3, 4 for a 3+3
	// two-DC table. Sizing it here keeps the cluster-wide quorum genuinely
	// cluster-wide, so a cross-DC partition can be seen to break it.
	if c.writeQuorum == 0 {
		c.writeQuorum = c.replicationFactor/2 + 1
	}
	if c.readQuorum == 0 {
		c.readQuorum = c.replicationFactor/2 + 1
	}
	return c
}

// dcOf returns the configured data center for the node at index i.
func (c simConfig) dcOf(i int) string {
	if i < len(c.dcs) {
		return c.dcs[i]
	}
	return ""
}

type simCluster struct {
	t      *testing.T
	cfg    simConfig
	net    *simNet
	client *http.Client // rides the sim net on the never-faulted client edge

	mu    sync.Mutex
	nodes []*simNode // guarded: restart swaps entries while workloads read
}

// node returns the current instance at slot i (restart replaces instances).
func (c *simCluster) node(i int) *simNode {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[i]
}

// snapshot returns a copy of the current node slice.
func (c *simCluster) snapshot() []*simNode {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*simNode, len(c.nodes))
	copy(out, c.nodes)
	return out
}

// startNode builds and starts a node with the given identity, mirroring the
// production wiring in cmd/continuum/main.go.
func (c *simCluster) startNode(id, dc string) *simNode {
	n := &simNode{
		id:         id,
		dc:         dc,
		httpAddr:   id + ":80",
		gossipAddr: id + ":81",
	}
	s := store.New()
	r := ring.NewRing(vnodesPerNode)
	hs := hintstore.New(hintCapPerNode, hintTTL)

	var hptr atomic.Pointer[api.Handler]
	ml := gossip.NewMemberList(id, n.httpAddr, func(m *gossip.Member, status gossip.MemberStatus) {
		switch status {
		case gossip.MemberAlive:
			r.AddZonedNodeDC(m.ID, m.Address, m.DC, m.Zone, m.Weight)
			if h := hptr.Load(); h != nil {
				go h.DeliverHints(m.ID, m.Address)
			}
		case gossip.MemberDead:
			r.RemoveNode(m.ID)
		case gossip.MemberBootstrapped:
			if h := hptr.Load(); h != nil {
				go h.CleanupStaleKeys()
			}
		}
	})
	ml.SetSelfGossipAddr(n.gossipAddr)
	// Stamp the DC before gossip starts so peers learn it on the first
	// exchange, exactly as applySelfMetadata does in main.go.
	if dc != "" {
		ml.SetSelfDC(dc)
	}
	// Per-DC targets must be installed before any node joins the ring, or the
	// first placements would be computed cluster-wide.
	if len(c.cfg.dcReplication) > 0 {
		r.SetDCReplication(c.cfg.dcReplication)
	}
	r.SetHealthFilter(func(nodeID string) bool {
		m, ok := ml.Get(nodeID)
		return ok && m.Status == gossip.MemberAlive
	})

	n.conn = newSimConn(c.net, id)
	g := gossip.NewGossiper(id, "81", ml, n.conn)
	g.SetTiming(gossipInterval, staleThreshold)

	ae := antientropy.New(r, s, id, c.cfg.replicationFactor, replicaTimeout)
	ae.SetSyncInterval(syncInterval)
	ae.SetHTTPTransport(linkFrom{net: c.net, from: id})
	s.SetOnUpdate(ae.Update)
	s.SetOnEvict(ae.RemoveFromTrees)

	h := api.NewHandler(r, ml, s, api.HandlerConfig{
		SelfID:            id,
		SelfDC:            dc,
		ReplicationFactor: c.cfg.replicationFactor,
		WriteQuorum:       c.cfg.writeQuorum,
		ReadQuorum:        c.cfg.readQuorum,
		ReplicaTimeout:    replicaTimeout,
		Transport:         linkFrom{net: c.net, from: id},
	}, hs)
	h.SetSyncTreeProvider(ae)
	hptr.Store(h)

	ctx, cancel := context.WithCancel(context.Background())
	g.Start(ctx)
	ae.Start(ctx)
	go func() { // periodic hint delivery sweep, as in main.go
		ticker := time.NewTicker(hintInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				h.DeliverPendingHints()
			case <-ctx.Done():
				return
			}
		}
	}()

	r.AddZonedNodeDC(id, n.httpAddr, dc, "", 1.0)

	n.store, n.ring, n.ml, n.gossiper, n.ae, n.h = s, r, ml, g, ae, h
	n.mux = api.BuildMux(h)
	n.cancel = cancel
	n.running.Store(true)
	c.net.register(n)
	return n
}

// crash stops the node abruptly: background loops die, its address becomes
// unreachable, and — the store being memory-only — its entire dataset is
// gone. Peers must detect the death by staleness.
func (c *simCluster) crash(n *simNode) {
	n.running.Store(false)
	n.cancel()
}

// restart replaces a crashed node with a fresh, empty instance under the
// same identity, re-meshes membership, and bootstraps gossip from the
// running peers. The bootstrap matters: peers hold the node's pre-crash
// entry (dead, or alive with a high heartbeat) and stop gossiping to dead
// members, so without the WantReply bootstrap the fresh incarnation-zero
// node may never learn the stale claim it must refute — production restarts
// do the same via SEED_NODES (gossip finding #3).
func (c *simCluster) restart(n *simNode) *simNode {
	fresh := c.startNode(n.id, n.dc)
	var peers []string
	c.mu.Lock()
	for i, existing := range c.nodes {
		if existing.id == n.id {
			c.nodes[i] = fresh
		} else if existing.running.Load() {
			peers = append(peers, existing.httpAddr)
		}
	}
	c.mu.Unlock()
	c.mesh()
	fresh.gossiper.Bootstrap(peers)
	return fresh
}

func newSimCluster(t *testing.T, cfg simConfig, seed int64) *simCluster {
	t.Helper()
	cfg = cfg.withDefaults()
	net := newSimNet(seed)
	c := &simCluster{
		t:   t,
		cfg: cfg,
		net: net,
		client: &http.Client{
			Timeout:   clientTimeout,
			Transport: linkFrom{net: net, from: clientID},
		},
	}
	for i := 0; i < cfg.nodes; i++ {
		c.nodes = append(c.nodes, c.startNode(fmt.Sprintf("n%d", i+1), cfg.dcOf(i)))
	}
	t.Cleanup(func() {
		for _, n := range c.snapshot() {
			if n.running.Load() {
				n.cancel()
			}
		}
	})
	c.mesh()
	c.waitFullRing(10 * time.Second)
	return c
}

// mesh registers every running node with every other over the same POST
// /nodes path production uses.
func (c *simCluster) mesh() {
	nodes := c.snapshot()
	for _, a := range nodes {
		for _, b := range nodes {
			if a != b && a.running.Load() && b.running.Load() {
				body := fmt.Sprintf(`{"id":%q,"address":%q,"gossip_address":%q}`, b.id, b.httpAddr, b.gossipAddr)
				resp, err := c.client.Post("http://"+a.httpAddr+"/nodes", "application/json", strings.NewReader(body))
				if err != nil {
					continue
				}
				resp.Body.Close()
			}
		}
	}
}

func (c *simCluster) running() []*simNode {
	var out []*simNode
	for _, n := range c.snapshot() {
		if n.running.Load() {
			out = append(out, n)
		}
	}
	return out
}

// ringIDs returns the node IDs in n's ring, resolved through the HTTP API so
// the harness observes what clients would. A nil error with an empty map means
// the ring really is empty; an error means the node could not be reached.
// Keeping those apart matters when diagnosing a convergence failure, where
// "unreachable" and "converged to nothing" are very different bugs.
func (c *simCluster) ringIDs(n *simNode) (map[string]bool, error) {
	resp, err := c.client.Get("http://" + n.httpAddr + "/nodes")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var nodes []struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		return nil, err
	}
	ids := make(map[string]bool, len(nodes))
	for _, nr := range nodes {
		ids[nr.ID] = true
	}
	return ids, nil
}

// memberStates reports each member's gossip status as n sees it, so a
// convergence dump shows why a peer is missing from the ring (declared dead)
// rather than only that it is missing.
//
// Bounded by a timeout because it runs on the failure path: reading the member
// list takes ml.mu, and the failure being diagnosed may itself be a node
// wedged holding that lock. A diagnostic that blocks turns a legible test
// failure into a bare five-minute timeout panic, which is how the deadlock
// this guards against first presented.
func (c *simCluster) memberStates(n *simNode) map[string]string {
	type result struct{ states map[string]string }
	ch := make(chan result, 1)
	go func() {
		out := make(map[string]string)
		for _, m := range n.ml.GetAll() {
			out[m.ID] = fmt.Sprintf("%s/inc=%d/hb=%d", m.Status, m.Incarnation, m.Heartbeat)
		}
		ch <- result{out}
	}()
	select {
	case r := <-ch:
		return r.states
	case <-time.After(2 * time.Second):
		return map[string]string{"<unreadable>": "member list lock held; node likely wedged"}
	}
}

// waitFullRing waits until every running node's ring holds every running
// node, re-meshing along the way (a restarted node needs re-registration to
// short-circuit convergence, as in the fault harness).
func (c *simCluster) waitFullRing(timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for !c.ringsConverged() {
		if time.Now().After(deadline) {
			c.dumpRings()
			c.t.Fatalf("cluster did not converge to a full ring within %v", timeout)
		}
		c.mesh()
		time.Sleep(20 * time.Millisecond)
	}
}

// ringsConverged reports whether every running node's ring contains every
// running node.
func (c *simCluster) ringsConverged() bool {
	running := c.running()
	for _, a := range running {
		ids, err := c.ringIDs(a)
		if err != nil {
			return false
		}
		for _, b := range running {
			if !ids[b.id] {
				return false
			}
		}
	}
	return true
}

// dumpRings logs each running node's ring view, for diagnosing a convergence
// failure.
func (c *simCluster) dumpRings() {
	for _, n := range c.running() {
		ids, err := c.ringIDs(n)
		if err != nil {
			c.t.Logf("%s ring: UNREACHABLE (%v); members: %v", n.id, err, c.memberStates(n))
			continue
		}
		c.t.Logf("%s ring: %v; members: %v", n.id, ids, c.memberStates(n))
	}
}

// replicaSet resolves which nodes own key, asking any running node's ring.
func (c *simCluster) replicaSet(key string) []*simNode {
	nodes := c.snapshot()
	byID := make(map[string]*simNode, len(nodes))
	for _, n := range nodes {
		byID[n.id] = n
	}
	for _, n := range c.running() {
		nodes := n.ring.GetReplicationNodes(key, c.cfg.replicationFactor)
		if len(nodes) == 0 {
			continue
		}
		out := make([]*simNode, 0, len(nodes))
		for _, rn := range nodes {
			if sn, ok := byID[rn.ID]; ok {
				out = append(out, sn)
			}
		}
		return out
	}
	return nil
}

// nodesInDC returns the running nodes labeled dc.
func (c *simCluster) nodesInDC(dc string) []*simNode {
	var out []*simNode
	for _, n := range c.running() {
		if n.dc == dc {
			out = append(out, n)
		}
	}
	return out
}

// partitionDCs severs every link between the two data centers in both
// directions — a WAN cut. Each side stays fully connected internally, which is
// what separates this from the single-node partitions the seeded schedule
// generates: both sides remain quorate within themselves.
func (c *simCluster) partitionDCs(a, b string) {
	var idsA, idsB []string
	for _, n := range c.nodesInDC(a) {
		idsA = append(idsA, n.id)
	}
	for _, n := range c.nodesInDC(b) {
		idsB = append(idsB, n.id)
	}
	c.net.partition(idsA, idsB)
}

// waitDCsPropagated waits until every running node's ring has learned every
// running node's DC label. Placement only becomes DC-aware once the labels
// arrive through gossip, so a test that cuts the WAN before then would be
// asserting against a half-formed topology.
func (c *simCluster) waitDCsPropagated(timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if c.dcsPropagated() {
			return
		}
		if time.Now().After(deadline) {
			for _, n := range c.running() {
				c.t.Logf("%s ring DCs: %v", n.id, c.ringDCs(n))
			}
			c.t.Fatalf("DC labels did not propagate within %v", timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// ringDCs reports node id -> DC as seen in n's local ring.
func (c *simCluster) ringDCs(n *simNode) map[string]string {
	out := make(map[string]string)
	for _, rn := range n.ring.GetNodes() {
		out[rn.ID] = rn.DC
	}
	return out
}

// dcsPropagated reports whether every running node agrees on every peer's DC.
func (c *simCluster) dcsPropagated() bool {
	running := c.running()
	for _, a := range running {
		seen := c.ringDCs(a)
		for _, b := range running {
			if seen[b.id] != b.dc {
				return false
			}
		}
	}
	return true
}
