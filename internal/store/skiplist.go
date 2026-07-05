package store

import "math/rand"

// skiplist is an ordered string-keyed map backing a memtable. It is not safe
// for concurrent mutation: the owning Store serializes all writes under s.mu,
// and a frozen memtable is immutable, so concurrent readers need no extra
// locking. Iteration walks the level-0 chain in ascending key order.
//
// Levels are chosen with probability slP per additional level up to slMaxLevel,
// giving expected O(log n) search over the memtable's bounded key count.
const (
	slMaxLevel = 20
	slP        = 0.5
)

type slNode struct {
	key  string
	val  memValue
	next []*slNode
}

type skiplist struct {
	head  *slNode // sentinel; its key is never yielded
	level int     // highest level currently in use (1..slMaxLevel)
	n     int
	rng   *rand.Rand
}

func newSkiplist() *skiplist {
	return &skiplist{
		head:  &slNode{next: make([]*slNode, slMaxLevel)},
		level: 1,
		// Fixed seed: the level distribution needs randomness but not
		// unpredictability, and a fixed seed keeps behavior reproducible.
		rng: rand.New(rand.NewSource(1)),
	}
}

func (s *skiplist) randomLevel() int {
	lvl := 1
	for lvl < slMaxLevel && s.rng.Float64() < slP {
		lvl++
	}
	return lvl
}

// get returns the value stored for key, if present.
func (s *skiplist) get(key string) (memValue, bool) {
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < key {
			x = x.next[i]
		}
	}
	x = x.next[0]
	if x != nil && x.key == key {
		return x.val, true
	}
	return memValue{}, false
}

// set inserts key or updates its value in place.
func (s *skiplist) set(key string, val memValue) {
	var update [slMaxLevel]*slNode
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < key {
			x = x.next[i]
		}
		update[i] = x
	}
	if nxt := x.next[0]; nxt != nil && nxt.key == key {
		nxt.val = val
		return
	}
	lvl := s.randomLevel()
	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}
	n := &slNode{key: key, val: val, next: make([]*slNode, lvl)}
	for i := 0; i < lvl; i++ {
		n.next[i] = update[i].next[i]
		update[i].next[i] = n
	}
	s.n++
}

// del removes key, reporting whether it was present.
func (s *skiplist) del(key string) bool {
	var update [slMaxLevel]*slNode
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < key {
			x = x.next[i]
		}
		update[i] = x
	}
	target := x.next[0]
	if target == nil || target.key != key {
		return false
	}
	for i := 0; i < s.level; i++ {
		if update[i].next[i] != target {
			break
		}
		update[i].next[i] = target.next[i]
	}
	for s.level > 1 && s.head.next[s.level-1] == nil {
		s.level--
	}
	s.n--
	return true
}

// seekPrev returns the last node whose key is strictly less than start, or the
// head sentinel if none. Advancing from it yields the first key >= start.
func (s *skiplist) seekPrev(start string) *slNode {
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < start {
			x = x.next[i]
		}
	}
	return x
}
