package ring

import (
	"sync"
	"sync/atomic"
)

type Ring struct {
	mu           sync.RWMutex
	tree         *Tree
	nodes        map[string]*Node
	replicas     int
	keyCounts    map[string]*atomic.Int64
	onUpdate     func(nodeCount, vnodeCount int)
	healthFilter func(nodeID string) bool
}

func NewRing(replicas int) *Ring {
	return &Ring{
		tree:      NewTree(),
		nodes:     make(map[string]*Node),
		replicas:  replicas,
		keyCounts: make(map[string]*atomic.Int64),
		onUpdate:  func(nodeCount, vnodeCount int) {},
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

func (r *Ring) AddNode(id, address string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	node := NewNode(id, address)
	r.nodes[id] = node
	r.keyCounts[id] = &atomic.Int64{}
	r.tree.Insert(node, r.replicas)
	r.onUpdate(len(r.nodes), r.tree.Tree.Size())
}

func (r *Ring) RemoveNode(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	node, exists := r.nodes[id]
	if !exists {
		return
	}

	r.tree.Remove(node, r.replicas)
	delete(r.nodes, id)
	delete(r.keyCounts, id)
	r.onUpdate(len(r.nodes), r.tree.Tree.Size())
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

	// Walk the ring from the key's position, skipping nodes that fail the
	// health filter. Mirrors the GetReplicationNodes iterator pattern.
	seen := make(map[string]bool)
	it := r.tree.Tree.Iterator()

	// Find the ceiling vnode and check it first.
	for it.Next() {
		vnode := it.Value().(*VNode)
		if vnode.Hash >= hash {
			if !seen[vnode.Node.ID] {
				seen[vnode.Node.ID] = true
				if r.healthFilter(vnode.Node.ID) {
					r.keyCounts[vnode.Node.ID].Add(1)
					return vnode.Node, true
				}
			}
			break
		}
	}

	// Continue walking forward, wrapping around, until all distinct nodes
	// have been checked.
	for len(seen) < len(r.nodes) {
		if !it.Next() {
			it.First()
			it.Next()
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

func (r *Ring) GetReplicationNodes(key string, factor int) []*Node {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.tree.Tree.Size() == 0 {
		return nil
	}

	if factor > len(r.nodes) {
	    factor = len(r.nodes)
	}

	seen := make(map[string]bool)
	var result []*Node

	hash := computeHash(key)

	it := r.tree.Tree.Iterator()

	for it.Next() {
		vnode := it.Value().(*VNode)
		if vnode.Hash >= hash {
			if !seen[vnode.Node.ID] {
				seen[vnode.Node.ID] = true
				result = append(result, vnode.Node)
				if len(result) == factor {
					return result
				}
			}
			break
		}
	}

	for len(result) < factor {
		if !it.Next() {
			it.First()
			it.Next()
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