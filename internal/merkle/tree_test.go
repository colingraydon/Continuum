package merkle

import (
	"fmt"
	"sync"
	"testing"
)

func TestEmptyTreesMatch(t *testing.T) {
	a, b := New(), New()
	if a.RootHash() != b.RootHash() {
		t.Fatal("two empty trees should have the same root hash")
	}
}

func TestRootHashChangesOnUpdate(t *testing.T) {
	tree := New()
	before := tree.RootHash()
	tree.Update("key1", 0xdeadbeef)
	if tree.RootHash() == before {
		t.Fatal("root hash should change after Update")
	}
}

func TestDeterminism(t *testing.T) {
	keys := []struct {
		key  string
		hash uint32
	}{
		{"apple", 0x11111111},
		{"banana", 0x22222222},
		{"cherry", 0x33333333},
	}

	a := New()
	for _, kh := range keys {
		a.Update(kh.key, kh.hash)
	}

	// Insert in reverse order.
	b := New()
	for i := len(keys) - 1; i >= 0; i-- {
		b.Update(keys[i].key, keys[i].hash)
	}

	if a.RootHash() != b.RootHash() {
		t.Fatal("insertion order should not affect root hash")
	}
	for i := 0; i < BucketCount; i++ {
		if a.BucketHash(i) != b.BucketHash(i) {
			t.Fatalf("bucket %d hash differs between trees with same content", i)
		}
	}
}

func TestRemoveRestoresHash(t *testing.T) {
	before := New()
	before.Update("k1", 0xaaa)
	before.Update("k2", 0xbbb)

	after := New()
	after.Update("k1", 0xaaa)
	after.Update("k2", 0xbbb)
	after.Update("k3", 0xccc)
	after.Remove("k3")

	if before.RootHash() != after.RootHash() {
		t.Fatal("tree after remove should match tree that never had the key")
	}
}

func TestRemoveNonexistentKey(t *testing.T) {
	tree := New()
	tree.Update("k1", 0x1)
	before := tree.RootHash()
	tree.Remove("nonexistent")
	if tree.RootHash() != before {
		t.Fatal("removing a nonexistent key should not change root hash")
	}
}

func TestIdempotentUpdate(t *testing.T) {
	a := New()
	a.Update("k1", 0x42)

	b := New()
	b.Update("k1", 0x42)
	b.Update("k1", 0x42) // duplicate

	if a.RootHash() != b.RootHash() {
		t.Fatal("duplicate Update with same hash should be idempotent")
	}
}

func TestUpdateChangesHash(t *testing.T) {
	tree := New()
	tree.Update("k1", 0x1)
	h1 := tree.RootHash()
	tree.Update("k1", 0x2)
	h2 := tree.RootHash()
	if h1 == h2 {
		t.Fatal("updating a key to a new hash should change root hash")
	}
}

func TestBucketIsolation(t *testing.T) {
	tree := New()
	// Populate every bucket with at least one key by brute force.
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("key-%d", i)
		tree.Update(k, uint32(i))
	}

	// Snapshot all bucket hashes.
	before := make([]uint32, BucketCount)
	for i := range before {
		before[i] = tree.BucketHash(i)
	}

	// Find a key and its bucket, then change just that key.
	target := "key-42"
	targetBucket := bucketIndex(target)
	tree.Update(target, 0xffffffff)

	changed := 0
	for i := 0; i < BucketCount; i++ {
		if tree.BucketHash(i) != before[i] {
			changed++
			if i != targetBucket {
				t.Errorf("bucket %d changed but only bucket %d should have", i, targetBucket)
			}
		}
	}
	if changed != 1 {
		t.Errorf("expected exactly 1 bucket to change, got %d", changed)
	}
}

func TestBucketKeys(t *testing.T) {
	tree := New()
	// Find two keys that land in the same bucket.
	var sameKeys []string
	targetBucket := -1
	for i := 0; i < 1000 && len(sameKeys) < 2; i++ {
		k := fmt.Sprintf("probe-%d", i)
		b := bucketIndex(k)
		if targetBucket == -1 {
			targetBucket = b
			sameKeys = append(sameKeys, k)
		} else if b == targetBucket {
			sameKeys = append(sameKeys, k)
		}
	}

	for _, k := range sameKeys {
		tree.Update(k, 0x1)
	}

	got := tree.BucketKeys(targetBucket)
	if len(got) != len(sameKeys) {
		t.Fatalf("expected %d keys in bucket %d, got %d", len(sameKeys), targetBucket, len(got))
	}
	// BucketKeys must be sorted.
	for i := 1; i < len(got); i++ {
		if got[i] < got[i-1] {
			t.Errorf("BucketKeys not sorted: %v", got)
		}
	}
}

func TestConcurrentSafety(t *testing.T) {
	tree := New()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			k := fmt.Sprintf("key-%d", n)
			tree.Update(k, uint32(n))
			tree.RootHash()
			tree.BucketHash(n % BucketCount)
			tree.BucketKeys(n % BucketCount)
			tree.Remove(k)
		}(i)
	}
	wg.Wait()
}

func TestHashKey(t *testing.T) {
	h1, h2 := HashKey("mykey"), HashKey("mykey")
	if h1 != h2 {
		t.Error("HashKey should be deterministic")
	}
	if HashKey("aaa") == HashKey("bbb") {
		t.Error("HashKey should differ for distinct keys")
	}
	// Consistent with bucketIndex: HashKey(k) % BucketCount == BucketIndex(k).
	key := "consistent-test"
	if int(HashKey(key))%BucketCount != BucketIndex(key) {
		t.Error("HashKey % BucketCount should equal BucketIndex")
	}
}

func TestBucketIndex(t *testing.T) {
	key := "testkey"
	idx := BucketIndex(key)
	if idx < 0 || idx >= BucketCount {
		t.Fatalf("BucketIndex out of range: %d", idx)
	}
	// Update the key and verify it lands in the expected bucket.
	tree := New()
	tree.Update(key, 0x1234)
	keys := tree.BucketKeys(idx)
	found := false
	for _, k := range keys {
		if k == key {
			found = true
		}
	}
	if !found {
		t.Errorf("key %q not in expected bucket %d", key, idx)
	}
}

func TestComputeBucketHash(t *testing.T) {
	// Empty map returns zero.
	if h := ComputeBucketHash(map[string]uint32{}); h != 0 {
		t.Errorf("empty map: expected 0, got %d", h)
	}

	// Matches the tree's bucket hash for the same entries.
	key := "probe"
	hash := uint32(0xabcdef01)
	idx := BucketIndex(key)
	tree := New()
	tree.Update(key, hash)

	got := ComputeBucketHash(map[string]uint32{key: hash})
	want := tree.BucketHash(idx)
	if got != want {
		t.Errorf("ComputeBucketHash=%d, tree.BucketHash=%d", got, want)
	}

	// Order-independent (same result regardless of map iteration order).
	a := ComputeBucketHash(map[string]uint32{"z": 1, "a": 2})
	b := ComputeBucketHash(map[string]uint32{"a": 2, "z": 1})
	if a != b {
		t.Error("ComputeBucketHash should be order-independent")
	}
}

func TestComputeRootHash(t *testing.T) {
	// Deterministic for the same input.
	h1 := ComputeRootHash([]uint32{1, 2, 3})
	h2 := ComputeRootHash([]uint32{1, 2, 3})
	if h1 != h2 {
		t.Error("ComputeRootHash should be deterministic")
	}

	// Order-sensitive (unlike bucket hash, root hash is over ordered buckets).
	if ComputeRootHash([]uint32{1, 2, 3}) == ComputeRootHash([]uint32{3, 2, 1}) {
		t.Error("ComputeRootHash should be order-sensitive")
	}

	// Matches tree.RootHash for the same bucket hash sequence.
	tree := New()
	tree.Update("k1", 0x1111)
	tree.Update("k2", 0x2222)

	buckets := make([]uint32, BucketCount)
	for i := range buckets {
		buckets[i] = tree.BucketHash(i)
	}
	if ComputeRootHash(buckets) != tree.RootHash() {
		t.Error("ComputeRootHash(bucket hashes) should equal tree.RootHash()")
	}
}

// TestDumpRoundTrip: a tree rebuilt from Dump's pairs is indistinguishable
// from the original - identical root and per-bucket hashes.
func TestDumpRoundTrip(t *testing.T) {
	orig := New()
	for i := 0; i < 500; i++ {
		orig.Update(fmt.Sprintf("dump-k%03d", i), uint32(i*31+7))
	}

	dump := orig.Dump()
	if len(dump) != 500 {
		t.Fatalf("Dump returned %d entries, want 500", len(dump))
	}
	rebuilt := New()
	for k, h := range dump {
		rebuilt.Update(k, h)
	}

	if rebuilt.RootHash() != orig.RootHash() {
		t.Error("rebuilt root hash differs from original")
	}
	for i := 0; i < BucketCount; i++ {
		if rebuilt.BucketHash(i) != orig.BucketHash(i) {
			t.Errorf("bucket %d hash differs after round trip", i)
		}
	}
}

func TestDumpEmptyTree(t *testing.T) {
	if dump := New().Dump(); len(dump) != 0 {
		t.Errorf("empty tree dumped %d entries", len(dump))
	}
}
