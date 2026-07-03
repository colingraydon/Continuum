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
	if weight <= 0 {
		weight = 1.0
	}
	vnodes := int(math.Round(float64(r.replicas) * weight))
	if vnodes < 1 {
		vnodes = 1
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	node := NewNode(id, address)
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

// walkRing walks the ring clockwise from hash, collecting up to factor unique
// nodes. Must be called with r.mu held.
func (r *Ring) walkRing(hash uint32, factor int) []*Node {
	seen := make(map[string]bool)
	var result []*Node
	it := r.tree.Tree.Iterator()

	// Advance to the ceiling vnode (first hash >= target).
	for it.Next() {
		vnode := it.Value().(*VNode)
		if vnode.Hash < hash {
			continue
		}
		if !seen[vnode.Node.ID] {
			seen[vnode.Node.ID] = true
			result = append(result, vnode.Node)
			if len(result) == factor {
				return result
			}
		}
		break
	}

	// Continue clockwise, wrapping around, until factor nodes are collected.
	for len(result) < factor {
		if !it.Next() {
			// First() positions the iterator ON the lowest-hash vnode.
			it.First()
		}
		vnode := it.Value().(*VNode)
		if seen[vnode.Node.ID] {
			continue
		}
		seen[vnode.Node.ID] = true
		result = append(result, vnode.Node)
	}
	return result
}

// walkRingHealthy walks the ring clockwise from hash, returning the first
// node that passes the health filter. Must be called with r.mu held.
func (r *Ring) walkRingHealthy(hash uint32) (*Node, bool) {
	seen := make(map[string]bool)
	it := r.tree.Tree.Iterator()

	// Advance to the ceiling vnode (first hash >= target).
	for it.Next() {
		vnode := it.Value().(*VNode)
		if vnode.Hash < hash {
			continue
		}
		if !seen[vnode.Node.ID] {
			seen[vnode.Node.ID] = true
			if r.healthFilter(vnode.Node.ID) {
				r.keyCounts[vnode.Node.ID].Add(1)
				return vnode.Node, true
			}
		}
		break
	}

	// Continue clockwise, wrapping around, until a healthy node is found.
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
		if r.healthFilter(vnode.Node.ID) {
			r.keyCounts[vnode.Node.ID].Add(1)
			return vnode.Node, true
		}
	}
	return nil, false
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
