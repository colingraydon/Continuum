package sstable

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// Cache is a byte-capped LRU over decompressed data blocks, shared by any
// number of Readers. A hit serves a block without the disk read, CRC check,
// or decompression a cold read pays. Point lookups populate the cache;
// iterators only consult it, so full-table scans and compactions cannot
// evict the hot set. Safe for concurrent use. A nil *Cache is valid and
// caches nothing.
type Cache struct {
	capacity int64

	mu     sync.Mutex
	ll     *list.List // MRU at front
	items  map[cacheKey]*list.Element
	used   int64
	hits   uint64
	misses uint64
}

// cacheKey identifies one data block: the owning Reader's unique id and the
// block's file offset. Reader ids are never reused, so a closed table's
// stale entries can only age out — they are never wrongly served.
type cacheKey struct {
	table  uint64
	offset uint64
}

type cacheEntry struct {
	key   cacheKey
	block []byte
}

// cacheEntryOverhead approximates per-entry bookkeeping (map slot, list
// element, header) charged against capacity alongside the block bytes.
const cacheEntryOverhead = 64

// nextTableID hands out cache identities to Readers.
var nextTableID atomic.Uint64

// NewCache returns a cache holding up to capacityBytes of decompressed
// blocks. capacityBytes <= 0 returns nil (caching disabled).
func NewCache(capacityBytes int64) *Cache {
	if capacityBytes <= 0 {
		return nil
	}
	return &Cache{
		capacity: capacityBytes,
		ll:       list.New(),
		items:    make(map[cacheKey]*list.Element),
	}
}

func (c *Cache) get(table, offset uint64) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	k := cacheKey{table: table, offset: offset}
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[k]
	if !ok {
		c.misses++
		return nil, false
	}
	c.hits++
	c.ll.MoveToFront(el)
	return el.Value.(*cacheEntry).block, true
}

// put inserts a block, evicting least-recently-used entries to stay within
// capacity. Blocks larger than the whole capacity are not cached at all —
// admitting one would wipe everything else for a single-use entry.
func (c *Cache) put(table, offset uint64, block []byte) {
	if c == nil {
		return
	}
	charge := int64(len(block)) + cacheEntryOverhead
	if charge > c.capacity {
		return
	}
	k := cacheKey{table: table, offset: offset}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[k]; ok {
		// Tables are immutable, so an existing entry holds the same bytes.
		c.ll.MoveToFront(el)
		return
	}
	c.items[k] = c.ll.PushFront(&cacheEntry{key: k, block: block})
	c.used += charge
	for c.used > c.capacity {
		back := c.ll.Back()
		e := back.Value.(*cacheEntry)
		c.ll.Remove(back)
		delete(c.items, e.key)
		c.used -= int64(len(e.block)) + cacheEntryOverhead
	}
}

// CacheStats is a point-in-time snapshot of cache effectiveness and size.
type CacheStats struct {
	Hits    uint64
	Misses  uint64
	Bytes   int64 // charged bytes currently held, including overhead
	Entries int
}

// Stats returns current counters. Safe on a nil cache (all zeros).
func (c *Cache) Stats() CacheStats {
	if c == nil {
		return CacheStats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return CacheStats{
		Hits:    c.hits,
		Misses:  c.misses,
		Bytes:   c.used,
		Entries: len(c.items),
	}
}
