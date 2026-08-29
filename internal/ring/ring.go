package ring

import (
	"math"
	"sync"
	"sync/atomic"
)

type Ring struct {
	mu           sync.RWMutex
	tree         *Tree
	nodes        map[string]*Node
	replicas     int
	vnodeCounts  map[string]int
	keyCounts    map[string]*atomic.Int64
	onUpdate     func(nodeCount, vnodeCount int)
	healthFilter func(nodeID string) bool
}

func NewRing(replicas int) *Ring {
	return &Ring{
		tree:        NewTree(),
		nodes:       make(map[string]*Node),
		replicas:    replicas,
		vnodeCounts: make(map[string]int),
		keyCounts:   make(map[string]*atomic.Int64),
		onUpdate:    func(nodeCount, vnodeCount int) { /* no-op until caller registers a callback */ },
	}
}

func (r *Ring) SetUpdateCallback(fn func(nodeCount, vnodeCount int)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onUpdate = fn
}

func (r *Ring) SetHealthFilter(fn func(nodeID string) bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.healthFilter = fn
}

// AddNode adds a node with the default weight of 1.0, receiving r.replicas vnodes.
func (r *Ring) AddNode(id, address string) {
	r.AddWeightedNode(id, address, 1.0)
}

// AddWeightedNode adds a node with a capacity weight relative to the base replica
// count. A weight of 2.0 gives twice as many vnodes as the default; 0.5 gives half.
// Weights <= 0 are treated as 1.0. The vnode count is always at least 1.
func (r *Ring) AddWeightedNode(id, address string, weight float64) {
	r.AddZonedNode(id, address, "", weight)
}

// AddZonedNode adds a node labeled with the failure-domain zone it lives in
// (rack, availability zone). Replica placement spreads each key's replica set
// across distinct zones when it can; an empty zone leaves the node out of that
// spreading entirely. Weight semantics match AddWeightedNode. The node's DC is
// left empty; use AddZonedNodeDC to label it.
func (r *Ring) AddZonedNode(id, address, zone string, weight float64) {
	r.AddZonedNodeDC(id, address, "", zone, weight)
}

// AddZonedNodeDC adds a node labeled with both its data center and its
// failure-domain zone. Zone semantics match AddZonedNode; DC is recorded and
// surfaced but does not yet affect placement (multi-DC placement lands in a
// later PR). Weight semantics match AddWeightedNode.
func (r *Ring) AddZonedNodeDC(id, address, dc, zone string, weight float64) {
	if weight <= 0 {
		weight = 1.0
	}
	vnodes := int(math.Round(float64(r.replicas) * weight))
	if vnodes < 1 {
		vnodes = 1
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Re-adding an existing node (a metadata refresh from gossip) must clear
	// its old vnodes first: a lowered weight would otherwise leave orphaned
	// vnodes in the tree beyond the new count.
	if old, exists := r.nodes[id]; exists {
		r.tree.Remove(old, r.vnodeCounts[id])
	}

	node := &Node{ID: id, Address: address, DC: dc, Zone: zone}
	r.nodes[id] = node
	r.vnodeCounts[id] = vnodes
	r.keyCounts[id] = &atomic.Int64{}
	r.tree.Insert(node, vnodes)
	r.onUpdate(len(r.nodes), r.tree.Tree.Size())
}

func (r *Ring) RemoveNode(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	node, exists := r.nodes[id]
	if !exists {
		return
	}

	r.tree.Remove(node, r.vnodeCounts[id])
	delete(r.nodes, id)
	delete(r.vnodeCounts, id)
	delete(r.keyCounts, id)
	r.onUpdate(len(r.nodes), r.tree.Tree.Size())
}

// walkDistinct visits each distinct node in clockwise ring order, starting at
// the first vnode with hash >= start and wrapping around, until visit returns
// false or every node has been seen. Must be called with r.mu held.
func (r *Ring) walkDistinct(start uint32, visit func(*Node) bool) {
	seen := make(map[string]bool)
	it := r.tree.Tree.Iterator()

	// Advance to the ceiling vnode (first hash >= target).
	for it.Next() {
		vnode := it.Value().(*VNode)
		if vnode.Hash < start {
			continue
		}
		seen[vnode.Node.ID] = true
		if !visit(vnode.Node) {
			return
		}
		break
	}

	// Continue clockwise, wrapping around, until every node has been visited.
	for len(seen) < len(r.nodes) {
		if !it.Next() {
			// First() positions the iterator ON the lowest-hash vnode.
			it.First()
		}
		vnode := it.Value().(*VNode)
		if seen[vnode.Node.ID] {
			continue
		}
		seen[vnode.Node.ID] = true
		if !visit(vnode.Node) {
			return
		}
	}
}

// walkRing walks the ring clockwise from hash, collecting up to factor unique
// nodes spread across failure-domain zones: a node whose zone is already
// represented in the set is passed over as long as unvisited nodes might still
// bring new zones. If the walk exhausts every node before the set fills, the
// passed-over nodes take the remaining slots in ring order, so the set never
// shrinks just because the cluster has fewer zones than replicas. Unzoned
// nodes (Zone == "") never conflict. Must be called with r.mu held.
func (r *Ring) walkRing(hash uint32, factor int) []*Node {
	// Callers clamp factor, but re-clamp here: factor originates in request
	// bodies (POST /replicate), and this bounds the allocation below even if
	// a future caller forgets.
	if factor <= 0 {
		return nil
	}
	if factor > len(r.nodes) {
		factor = len(r.nodes)
	}
	result := make([]*Node, 0, factor)
	usedZones := make(map[string]bool)
	var zoneSkipped []*Node

	r.walkDistinct(hash, func(n *Node) bool {
		if n.Zone != "" && usedZones[n.Zone] {
			zoneSkipped = append(zoneSkipped, n)
			return true
		}
		usedZones[n.Zone] = true
		result = append(result, n)
		return len(result) < factor
	})

	for _, n := range zoneSkipped {
		if len(result) == factor {
			break
		}
		result = append(result, n)
	}
	return result
}

// walkRingHealthy walks the ring clockwise from hash, returning the first
// node that passes the health filter. Must be called with r.mu held.
func (r *Ring) walkRingHealthy(hash uint32) (*Node, bool) {
	var found *Node
	r.walkDistinct(hash, func(n *Node) bool {
		if r.healthFilter(n.ID) {
			found = n
			return false
		}
		return true
	})
	if found == nil {
		return nil, false
	}
	r.keyCounts[found.ID].Add(1)
	return found, true
}

func (r *Ring) GetNode(key string) (*Node, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.tree.Tree.Size() == 0 {
		return nil, false
	}

	hash := computeHash(key)

	if r.healthFilter == nil {
		vnode, found := r.tree.GetNext(hash)
		if !found {
			return nil, false
		}
		r.keyCounts[vnode.Node.ID].Add(1)
		return vnode.Node, true
	}

	return r.walkRingHealthy(hash)
}

func (r *Ring) GetReplicationNodes(key string, factor int) []*Node {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.tree.Tree.Size() == 0 {
		return nil
	}
	if factor > len(r.nodes) {
		factor = len(r.nodes)
	}
	return r.walkRing(computeHash(key), factor)
}

// GetHealthyReplicationNodes returns the healthy members of key's zone-aware
// replica set, topped up with healthy substitutes for any unhealthy intended
// owner, and separately returns those unhealthy owners - the nodes whose
// slots the substitutes take over (sloppy quorum: the coordinator writes to
// the substitutes and buffers a hint per skipped owner). Substitutes are
// drawn in ring order, preferring zones the healthy set does not already
// cover. With no health filter, or a fully healthy replica set, the result
// is identical to GetReplicationNodes with an empty skipped list.
func (r *Ring) GetHealthyReplicationNodes(key string, factor int) (nodes, skipped []*Node) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.tree.Tree.Size() == 0 {
		return nil, nil
	}
	if factor > len(r.nodes) {
		factor = len(r.nodes)
	}

	hash := computeHash(key)

	// Intended owners come from the zone-aware walk, ignoring health: the
	// owner set must be what every node would compute, so hints buffered for
	// an unhealthy owner land on the node that owns the key once it recovers.
	owners := r.walkRing(hash, factor)
	usedZones := make(map[string]bool)
	taken := make(map[string]bool, len(owners))
	for _, n := range owners {
		taken[n.ID] = true
		if r.isHealthy(n) {
			nodes = append(nodes, n)
			usedZones[n.Zone] = true
		} else {
			skipped = append(skipped, n)
		}
	}
	if len(skipped) == 0 {
		return nodes, nil
	}
	return r.fillSubstitutes(hash, factor, nodes, taken, usedZones), skipped
}

// isHealthy reports whether n passes the health filter; a ring without a
// filter treats every node as healthy. Must be called with r.mu held.
func (r *Ring) isHealthy(n *Node) bool {
	return r.healthFilter == nil || r.healthFilter(n.ID)
}

// fillSubstitutes tops nodes up to factor with healthy non-owners in ring
// order, zones the healthy set does not already cover first, so a rack outage
// does not collapse the write set into the surviving owners' zones. Must be
// called with r.mu held.
func (r *Ring) fillSubstitutes(hash uint32, factor int, nodes []*Node, taken, usedZones map[string]bool) []*Node {
	var candidates []*Node
	r.walkDistinct(hash, func(n *Node) bool {
		if !taken[n.ID] && r.isHealthy(n) {
			candidates = append(candidates, n)
		}
		return true
	})
	for _, n := range candidates {
		if len(nodes) == factor {
			return nodes
		}
		if n.Zone == "" || !usedZones[n.Zone] {
			usedZones[n.Zone] = true
			taken[n.ID] = true
			nodes = append(nodes, n)
		}
	}
	for _, n := range candidates {
		if len(nodes) == factor {
			break
		}
		if !taken[n.ID] {
			taken[n.ID] = true
			nodes = append(nodes, n)
		}
	}
	return nodes
}

func (r *Ring) NodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

func (r *Ring) GetNodes() []*Node {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodes := make([]*Node, 0, len(r.nodes))
	for _, n := range r.nodes {
		nodes = append(nodes, n)
	}

	return nodes
}

// VnodeRange is the half-open hash range (Start, End] owned by a vnode.
// If Start >= End the range wraps around zero (the vnode is the first
// clockwise entry in the ring).
type VnodeRange struct {
	Start uint32 // exclusive lower bound
	End   uint32 // inclusive upper bound (the vnode's own hash)
}

// Contains reports whether hash falls within this vnode's range.
func (vr VnodeRange) Contains(hash uint32) bool {
	if vr.Start < vr.End {
		return hash > vr.Start && hash <= vr.End
	}
	// Wrapping range: covers (Start, MaxUint32] ∪ [0, End]
	return hash > vr.Start || hash <= vr.End
}

// GetReplicationNodesForHash is like GetReplicationNodes but accepts a raw
// hash instead of a key string. Used by the anti-entropy manager to look up
// replicas for a vnode without needing a representative key.
func (r *Ring) GetReplicationNodesForHash(hash uint32, factor int) []*Node {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.tree.Tree.Size() == 0 {
		return nil
	}
	if factor > len(r.nodes) {
		factor = len(r.nodes)
	}
	return r.walkRing(hash, factor)
}

// GetVnodeRange returns the VnodeRange whose End equals endHash.
// Used by the sync endpoint to determine which keys belong to a requested vnode.
func (r *Ring) GetVnodeRange(endHash uint32) (VnodeRange, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.tree.Tree.Size() == 0 {
		return VnodeRange{}, false
	}

	var vnodes []*VNode
	it := r.tree.Tree.Iterator()
	for it.Next() {
		vnodes = append(vnodes, it.Value().(*VNode))
	}

	n := len(vnodes)
	for i, vn := range vnodes {
		if vn.Hash == endHash {
			start := vnodes[(i-1+n)%n].Hash
			return VnodeRange{Start: start, End: endHash}, true
		}
	}
	return VnodeRange{}, false
}

// GetPrimaryVnodeRanges returns the hash ranges for which nodeID is the
// primary replica (i.e. first clockwise owner). The manager uses this to
// know which vnodes to drive anti-entropy for.
func (r *Ring) GetPrimaryVnodeRanges(nodeID string) []VnodeRange {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.tree.Tree.Size() == 0 {
		return nil
	}

	var vnodes []*VNode
	it := r.tree.Tree.Iterator()
	for it.Next() {
		vnodes = append(vnodes, it.Value().(*VNode))
	}

	n := len(vnodes)
	var ranges []VnodeRange
	for i, vn := range vnodes {
		if vn.Node.ID != nodeID {
			continue
		}
		start := vnodes[(i-1+n)%n].Hash
		ranges = append(ranges, VnodeRange{Start: start, End: vn.Hash})
	}
	return ranges
}

// GetReplicaVnodeRanges returns the hash ranges for every vnode that nodeID
// replicates — i.e. nodeID is among the first factor distinct owners walking
// clockwise from the vnode. This is a superset of GetPrimaryVnodeRanges (the
// vnodes where nodeID is the first owner). Anti-entropy uses it so a node can
// maintain a Merkle tree for every range it may be asked to serve sync state
// for, not just the ranges it drives sync for.
func (r *Ring) GetReplicaVnodeRanges(nodeID string, factor int) []VnodeRange {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.tree.Tree.Size() == 0 {
		return nil
	}
	if factor > len(r.nodes) {
		factor = len(r.nodes)
	}

	var vnodes []*VNode
	it := r.tree.Tree.Iterator()
	for it.Next() {
		vnodes = append(vnodes, it.Value().(*VNode))
	}

	n := len(vnodes)
	var ranges []VnodeRange
	for i, vn := range vnodes {
		if !nodeInSet(r.walkRing(vn.Hash, factor), nodeID) {
			continue
		}
		start := vnodes[(i-1+n)%n].Hash
		ranges = append(ranges, VnodeRange{Start: start, End: vn.Hash})
	}
	return ranges
}

// nodeInSet reports whether any node in nodes has the given ID.
func nodeInSet(nodes []*Node, id string) bool {
	for _, n := range nodes {
		if n.ID == id {
			return true
		}
	}
	return false
}
