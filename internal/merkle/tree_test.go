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
