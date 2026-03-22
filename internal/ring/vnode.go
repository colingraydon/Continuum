package ring

import (
	"fmt"

	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/spaolacci/murmur3"
)

type Tree struct {
	Tree *redblacktree.Tree
}

type VNode struct {
	Node *Node
	Hash uint32
}

func NewTree() *Tree {
	return &Tree {
		Tree: redblacktree.NewWithIntComparator(),
	}
}

func (t *Tree) Insert(node *Node, replicas int) {
	for i := 0; i < replicas; i++ {
		hash := computeHash(generateHashInput(node.ID, i))
		t.Tree.Put(int(hash), &VNode{Hash: hash, Node: node})
	}
}

func (t *Tree) Remove(node *Node, replicas int) {
	for i := 0; i < replicas; i++ {
		hash := computeHash(generateHashInput(node.ID, i))
		t.Tree.Remove(int(hash))
	}
}

func (t *Tree) GetNext(hash uint32) (*VNode, bool) {
	node, found := t.Tree.Ceiling(int(hash))
	if found {
		return node.Value.(*VNode), true
	}

	left := t.Tree.Left()
	if left != nil {
		return left.Value.(*VNode), true
	}

	return nil, false
}

func computeHash(key string) uint32 {
	return murmur3.Sum32([]byte(key))
}

func generateHashInput(nodeID string, replicaNumber int) string {
	return fmt.Sprintf("%s#%d", nodeID, replicaNumber)
}
