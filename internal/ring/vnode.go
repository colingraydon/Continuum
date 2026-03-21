package ring

import "github.com/emirpasic/gods/trees/redblacktree"

type Tree struct {
	Tree *redblacktree.Tree
}
func NewTree() *Tree {
	return &Tree{};
}