package merkle

import (
	"encoding/binary"
	"sort"
	"sync"

	"github.com/spaolacci/murmur3"
)

// BucketCount is the number of hash-range buckets per tree.
const BucketCount = 16

type bucket struct {
	entries map[string]uint32 // key to value hash
}

func newBucket() bucket {
	return bucket{entries: make(map[string]uint32)}
}

// hash returns a deterministic aggregate hash of all entries in the bucket.
func (b *bucket) hash() uint32 {
	if len(b.entries) == 0 {
		return 0
	}
	keys := make([]string, 0, len(b.entries))
	for k := range b.entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	h := murmur3.New32()
	var buf [4]byte
	for _, k := range keys {
		h.Write([]byte(k))
		binary.BigEndian.PutUint32(buf[:], b.entries[k])
		h.Write(buf[:])
	}
	return h.Sum32()
}

// Tree is a Merkle tree over a set of key/value-hash pairs, partitioned into
// BucketCount buckets. One tree per vnode. Keys are assigned to
// buckets by murmur3(key) % BucketCount.
type Tree struct {
	mu      sync.RWMutex
	buckets [BucketCount]bucket
}

func New() *Tree {
	t := &Tree{}
	for i := range t.buckets {
		t.buckets[i] = newBucket()
	}
	return t
}

func bucketIndex(key string) int {
	return int(murmur3.Sum32([]byte(key))) % BucketCount
}

// Update inserts or replaces the value hash for key. hash should be
// murmur3(value) - the same Hash field already stored on store.Sibling.
func (t *Tree) Update(key string, hash uint32) {
	i := bucketIndex(key)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buckets[i].entries[key] = hash
}

// Remove deletes key from the tree. No-op if key is not present.
func (t *Tree) Remove(key string) {
	i := bucketIndex(key)
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.buckets[i].entries, key)
}

// BucketHash returns the aggregate hash for bucket i.
func (t *Tree) BucketHash(i int) uint32 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.buckets[i].hash()
}

// RootHash returns the hash of all bucket hashes in order. Two trees with
// identical contents will always produce the same root hash.
func (t *Tree) RootHash() uint32 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	h := murmur3.New32()
	var buf [4]byte
	for i := range t.buckets {
		binary.BigEndian.PutUint32(buf[:], t.buckets[i].hash())
		h.Write(buf[:])
	}
	return h.Sum32()
}

// BucketKeys returns the sorted keys in bucket i.
func (t *Tree) BucketKeys(i int) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	keys := make([]string, 0, len(t.buckets[i].entries))
	for k := range t.buckets[i].entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// HashKey returns murmur3(key) — the hash used to place a key on the ring and
// assign it to a vnode range. Exported so callers can check range membership
// without duplicating the hash function.
func HashKey(key string) uint32 {
	return murmur3.Sum32([]byte(key))
}

// BucketIndex returns the bucket index for key. Same assignment as Update.
func BucketIndex(key string) int {
	return bucketIndex(key)
}

// ComputeBucketHash computes the aggregate hash for a set of (key, entryHash)
// pairs using the same algorithm as the tree's internal bucket. Exported so
// the sync endpoint can compute bucket hashes on-the-fly from the store.
func ComputeBucketHash(entries map[string]uint32) uint32 {
	if len(entries) == 0 {
		return 0
	}
	keys := make([]string, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := murmur3.New32()
	var buf [4]byte
	for _, k := range keys {
		h.Write([]byte(k))
		binary.BigEndian.PutUint32(buf[:], entries[k])
		h.Write(buf[:])
	}
	return h.Sum32()
}

// ComputeRootHash computes the root hash from a slice of bucket hashes, using
// the same algorithm as RootHash.
func ComputeRootHash(bucketHashes []uint32) uint32 {
	h := murmur3.New32()
	var buf [4]byte
	for _, bh := range bucketHashes {
		binary.BigEndian.PutUint32(buf[:], bh)
		h.Write(buf[:])
	}
	return h.Sum32()
}
