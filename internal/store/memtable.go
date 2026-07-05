package store

import "time"

// memEntryOverhead is the per-entry constant added to the memtable size
// estimate on top of key and value bytes.
const memEntryOverhead = 64

// memValue is one key's state within a memtable generation. When evicted is
// true it is an evict marker (entry is empty and shadows older generations);
// otherwise entry holds the key's complete sibling set and age is the
// tombstone wall-time, zero unless the entry is a lone tombstone.
type memValue struct {
	entry   Entry
	age     time.Time
	evicted bool
}

// memtable is an ordered in-memory write set backed by a skiplist. The active
// store memtable and each frozen (flushing) generation are memtables. All
// mutation is serialized by the owning Store's mutex; a frozen memtable is
// immutable after the freeze, so concurrent reads need no further locking.
//
// Keeping keys ordered makes flush a straight in-order walk and lets prefix
// scans seek to the range start instead of sorting the whole table per call.
type memtable struct {
	list  *skiplist
	seq   uint64 // highest WAL sequence covered by this memtable
	bytes int64  // running size estimate; additive, reset when a fresh memtable is created
}

func newMemtable() *memtable {
	return &memtable{list: newSkiplist()}
}

// get returns the stored value for key, if present.
func (m *memtable) get(key string) (memValue, bool) { return m.list.get(key) }

// len reports the number of keys (including evict markers) in the memtable.
func (m *memtable) len() int { return m.list.n }

// empty reports whether the memtable holds no keys or evict markers.
func (m *memtable) empty() bool { return m.list.n == 0 }

// putEntry installs a live or tombstone entry, clearing any prior evict marker.
// valueSize is the byte length of the written value (0 for a tombstone) and
// feeds only the size estimate.
func (m *memtable) putEntry(key string, e Entry, age time.Time, valueSize int) {
	m.list.set(key, memValue{entry: e, age: age})
	m.bytes += int64(len(key)+valueSize) + memEntryOverhead
}

// evict installs an evict marker so a copy of key in an older generation stays
// shadowed until compaction removes both.
func (m *memtable) evict(key string) {
	m.list.set(key, memValue{evicted: true})
	m.bytes += int64(len(key)) + memEntryOverhead
}

// remove deletes a key outright. Used to GC an expired tombstone that nothing
// below shadows.
func (m *memtable) remove(key string) { m.list.del(key) }

// setAge overrides the tombstone age of an existing entry in place, leaving
// the size estimate unchanged. No-op when the key is absent.
func (m *memtable) setAge(key string, age time.Time) {
	if v, ok := m.list.get(key); ok {
		v.age = age
		m.list.set(key, v)
	}
}

// memIter walks a memtable's keys in ascending order. cur points at the node
// before the next one to yield; the first next() advances onto it.
type memIter struct{ cur *slNode }

// iter returns an iterator positioned before the first key.
func (m *memtable) iter() *memIter { return &memIter{cur: m.list.head} }

// seek returns an iterator positioned so the first next() yields the first key
// greater than or equal to start.
func (m *memtable) seek(start string) *memIter { return &memIter{cur: m.list.seekPrev(start)} }

func (it *memIter) next() bool {
	if it.cur == nil {
		return false
	}
	it.cur = it.cur.next[0]
	return it.cur != nil
}

func (it *memIter) key() string     { return it.cur.key }
func (it *memIter) value() memValue { return it.cur.val }
