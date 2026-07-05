package sstable

import (
	"bytes"
	"fmt"
	"testing"
)

// openCachedTable builds a table and opens it with the given cache attached.
func openCachedTable(t *testing.T, opts Options, entries []kv, c *Cache) *Reader {
	t.Helper()
	r := openTable(t, buildTable(t, opts, entries))
	r.SetCache(c)
	return r
}

func TestCacheHitOnRepeatedGet(t *testing.T) {
	entries := sortedEntries(500)
	c := NewCache(1 << 20)
	r := openCachedTable(t, Options{BlockSize: 256}, entries, c)

	key := entries[42].key
	for i := 0; i < 3; i++ {
		v, ok, err := r.Get(key)
		if err != nil || !ok || !bytes.Equal(v, entries[42].value) {
			t.Fatalf("Get #%d = (%v, %v)", i, ok, err)
		}
	}
	st := c.Stats()
	if st.Hits != 2 || st.Misses != 1 {
		t.Fatalf("Stats = %+v, want 2 hits / 1 miss", st)
	}
	if st.Entries != 1 || st.Bytes <= 0 {
		t.Fatalf("Stats = %+v, want 1 charged entry", st)
	}
}

func TestCachedGetValueIsACopy(t *testing.T) {
	entries := sortedEntries(100)
	c := NewCache(1 << 20)
	r := openCachedTable(t, Options{BlockSize: 256}, entries, c)

	key, want := entries[10].key, entries[10].value
	v1, _, err := r.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	for i := range v1 {
		v1[i] ^= 0xFF // a caller mutating its result must not corrupt the cache
	}
	v2, ok, err := r.Get(key)
	if err != nil || !ok || !bytes.Equal(v2, want) {
		t.Fatalf("Get after caller mutation = %q, want %q (ok=%v err=%v)", v2, want, ok, err)
	}
}

func TestCacheEvictsToCapacity(t *testing.T) {
	entries := sortedEntries(1000)
	// Capacity fits only a few 256-byte blocks.
	c := NewCache(1024)
	r := openCachedTable(t, Options{BlockSize: 256}, entries, c)

	for _, e := range entries {
		if _, ok, err := r.Get(e.key); err != nil || !ok {
			t.Fatalf("Get(%q): ok=%v err=%v", e.key, ok, err)
		}
	}
	st := c.Stats()
	if st.Bytes > 1024 {
		t.Fatalf("cache holds %d bytes, capacity 1024", st.Bytes)
	}
	if st.Entries == 0 {
		t.Fatal("cache is empty; eviction removed everything")
	}
}

func TestCacheSharedAcrossReaders(t *testing.T) {
	c := NewCache(1 << 20)
	mk := func(tag string) []kv {
		out := make([]kv, 100)
		for i := range out {
			out[i] = kv{
				key:   []byte(fmt.Sprintf("key-%05d", i)),
				value: []byte(fmt.Sprintf("%s-value-%05d", tag, i)),
			}
		}
		return out
	}
	aEntries, bEntries := mk("a"), mk("b")
	// Identical layout means identical block offsets in both tables; only the
	// per-reader table id keeps the entries apart.
	a := openCachedTable(t, Options{BlockSize: 256}, aEntries, c)
	b := openCachedTable(t, Options{BlockSize: 256}, bEntries, c)

	for i := range aEntries {
		av, _, err := a.Get(aEntries[i].key)
		if err != nil || !bytes.Equal(av, aEntries[i].value) {
			t.Fatalf("a.Get(%q) = %q, err=%v", aEntries[i].key, av, err)
		}
		bv, _, err := b.Get(bEntries[i].key)
		if err != nil || !bytes.Equal(bv, bEntries[i].value) {
			t.Fatalf("b.Get(%q) = %q, err=%v", bEntries[i].key, bv, err)
		}
	}
}

func TestIteratorConsultsButDoesNotFillCache(t *testing.T) {
	entries := sortedEntries(500)
	c := NewCache(1 << 20)
	r := openCachedTable(t, Options{BlockSize: 256}, entries, c)

	it := r.Iter()
	for it.Next() { //nolint:revive // drain
	}
	if err := it.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	if st := c.Stats(); st.Entries != 0 {
		t.Fatalf("scan populated the cache with %d entries, want 0", st.Entries)
	}

	// A point lookup fills its block; a scan crossing it now gets a hit.
	if _, ok, err := r.Get(entries[0].key); err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	before := c.Stats()
	it = r.Iter()
	for it.Next() { //nolint:revive // drain
	}
	if err := it.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	if after := c.Stats(); after.Hits <= before.Hits {
		t.Fatalf("iterator did not hit the cached block: hits %d -> %d", before.Hits, after.Hits)
	}
}

func TestCacheRejectsOversizedBlock(t *testing.T) {
	big := bytes.Repeat([]byte("v"), 4096)
	entries := []kv{{key: []byte("a"), value: big}}
	c := NewCache(256) // smaller than the only block
	r := openCachedTable(t, Options{}, entries, c)

	if v, ok, err := r.Get([]byte("a")); err != nil || !ok || !bytes.Equal(v, big) {
		t.Fatalf("Get = (%v, %v)", ok, err)
	}
	if st := c.Stats(); st.Entries != 0 || st.Bytes != 0 {
		t.Fatalf("oversized block was cached: %+v", st)
	}
}

func TestNilCacheIsSafe(t *testing.T) {
	var c *Cache // NewCache(0) returns nil too
	if got := NewCache(0); got != nil {
		t.Fatal("NewCache(0) should return nil")
	}
	if _, ok := c.get(1, 0); ok {
		t.Fatal("nil cache reported a hit")
	}
	c.put(1, 0, []byte("x"))
	if st := c.Stats(); st != (CacheStats{}) {
		t.Fatalf("nil cache Stats = %+v, want zeros", st)
	}

	entries := sortedEntries(100)
	r := openCachedTable(t, Options{BlockSize: 256}, entries, nil)
	for _, e := range entries {
		v, ok, err := r.Get(e.key)
		if err != nil || !ok || !bytes.Equal(v, e.value) {
			t.Fatalf("Get(%q) with nil cache = (%v, %v)", e.key, ok, err)
		}
	}
}

func TestConcurrentCachedGets(t *testing.T) {
	entries := sortedEntries(500)
	c := NewCache(4096) // small enough that eviction runs during the test
	r := openCachedTable(t, Options{BlockSize: 256}, entries, c)

	done := make(chan error, 8)
	for g := 0; g < 8; g++ {
		go func() {
			for _, e := range entries {
				v, ok, err := r.Get(e.key)
				if err != nil || !ok || !bytes.Equal(v, e.value) {
					done <- fmt.Errorf("Get(%q) = (%v, %v)", e.key, ok, err)
					return
				}
			}
			done <- nil
		}()
	}
	for g := 0; g < 8; g++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}
