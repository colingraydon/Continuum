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
}

func (c simConfig) withDefaults() simConfig {
	if c.nodes == 0 {
		c.nodes = 3
	}
	if c.replicationFactor == 0 {
		c.replicationFactor = 3
	}
	if c.writeQuorum == 0 {
		c.writeQuorum = 2
	}
	if c.readQuorum == 0 {
		c.readQuorum = 2
	}
	return c
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
func (c *simCluster) startNode(id string) *simNode {
	n := &simNode{
		id:         id,
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
			r.AddWeightedNode(m.ID, m.Address, m.Weight)
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

	r.AddWeightedNode(id, n.httpAddr, 1.0)

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
// same identity and re-meshes membership.
func (c *simCluster) restart(n *simNode) *simNode {
	fresh := c.startNode(n.id)
	c.mu.Lock()
	for i, existing := range c.nodes {
		if existing.id == n.id {
			c.nodes[i] = fresh
		}
	}
	c.mu.Unlock()
	c.mesh()
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
		c.nodes = append(c.nodes, c.startNode(fmt.Sprintf("n%d", i+1)))
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
// the harness observes what clients would.
func (c *simCluster) ringIDs(n *simNode) map[string]bool {
	resp, err := c.client.Get("http://" + n.httpAddr + "/nodes")
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	var nodes []struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		return nil
	}
	ids := make(map[string]bool, len(nodes))
	for _, nr := range nodes {
		ids[nr.ID] = true
	}
	return ids
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
		ids := c.ringIDs(a)
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
		c.t.Logf("%s ring: %v", n.id, c.ringIDs(n))
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
