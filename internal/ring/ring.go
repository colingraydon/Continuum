package ring

import "sync"

type Ring struct {
	mu       sync.RWMutex
	tree     *Tree
	nodes    map[string]*Node
	replicas int
}

func NewRing(replicas int) *Ring {
	return &Ring{
		tree:     NewTree(),
		nodes:    make(map[string]*Node),
		replicas: replicas,
	}
}

func (r *Ring) AddNode(id, address string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	node := NewNode(id, address)
	r.nodes[id] = node
	r.tree.Insert(node, r.replicas)
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
}

func (r *Ring) GetNode(key string) (*Node, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.tree.Tree.Size() == 0 {
		return nil, false
	}

	hash := computeHash(key)
	vnode, found := r.tree.GetNext(hash)
	if !found {
		return nil, false
	}

	return vnode.Node, true
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