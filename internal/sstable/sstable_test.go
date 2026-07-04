package sstable

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

type kv struct {
	key, value []byte
}

// sortedEntries returns n entries with strictly increasing keys and values
// of varying sizes, including empty values.
func sortedEntries(n int) []kv {
	out := make([]kv, n)
	for i := range out {
		key := fmt.Sprintf("key-%05d", i)
		var value []byte
		if i%7 != 3 { // every 7th-ish entry keeps an empty value
			value = bytes.Repeat([]byte{byte(i)}, i%200)
		}
		out[i] = kv{key: []byte(key), value: value}
	}
	return out
}

func buildTable(t *testing.T, opts Options, entries []kv) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf, opts)
	for _, e := range entries {
		if err := w.Add(e.key, e.value); err != nil {
			t.Fatalf("Add(%q): %v", e.key, err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	return buf.Bytes()
}

func openTable(t *testing.T, data []byte) *Reader {
	t.Helper()
	r, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	return r
}

func TestRoundTrip(t *testing.T) {
	entries := sortedEntries(1000)
	// Small block size forces many blocks so lookups cross block boundaries.
	data := buildTable(t, Options{BlockSize: 256}, entries)
	r := openTable(t, data)

	if r.Count() != 1000 {
		t.Fatalf("Count = %d, want 1000", r.Count())
	}
	for _, e := range entries {
		v, ok, err := r.Get(e.key)
		if err != nil {
			t.Fatalf("Get(%q): %v", e.key, err)
		}
		if !ok {
			t.Fatalf("Get(%q): not found", e.key)
		}
		if !bytes.Equal(v, e.value) {
			t.Fatalf("Get(%q) = %q, want %q", e.key, v, e.value)
		}
	}
}

func TestGetAbsentKeys(t *testing.T) {
	data := buildTable(t, Options{BlockSize: 256}, sortedEntries(1000))
	r := openTable(t, data)

	absent := [][]byte{
		[]byte("aaa"),         // before smallest
		[]byte("key-00500x"),  // between existing keys
		[]byte("key-005005"),  // shares prefix with a real key
		[]byte("zzz"),         // after largest
		[]byte("absent-9999"), // nowhere near
	}
	for _, key := range absent {
		_, ok, err := r.Get(key)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if ok {
			t.Fatalf("Get(%q): found, want absent", key)
		}
	}
}

func TestIteratorOrder(t *testing.T) {
	entries := sortedEntries(500)
	data := buildTable(t, Options{BlockSize: 128}, entries)
	r := openTable(t, data)

	it := r.Iter()
	i := 0
	for it.Next() {
		if i >= len(entries) {
			t.Fatalf("iterator yielded more than %d entries", len(entries))
		}
		if !bytes.Equal(it.Key(), entries[i].key) {
			t.Fatalf("entry %d: key = %q, want %q", i, it.Key(), entries[i].key)
		}
		if !bytes.Equal(it.Value(), entries[i].value) {
			t.Fatalf("entry %d: value = %q, want %q", i, it.Value(), entries[i].value)
		}
		i++
	}
	if err := it.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	if i != len(entries) {
		t.Fatalf("iterated %d entries, want %d", i, len(entries))
	}
}

func TestIterFrom(t *testing.T) {
	entries := sortedEntries(500)
	// Small blocks force the seek to cross the block index, not just scan.
	data := buildTable(t, Options{BlockSize: 128}, entries)
	r := openTable(t, data)

	cases := []struct {
		name    string
		start   []byte
		wantIdx int // index into entries of the first yielded key; -1 = none
	}{
		{"nil start scans all", nil, 0},
		{"before smallest", []byte("aaa"), 0},
		{"exact first key", []byte("key-00000"), 0},
		{"exact mid key", []byte("key-00250"), 250},
		{"between keys", []byte("key-00250a"), 251},
		{"exact last key", []byte("key-00499"), 499},
		{"past largest", []byte("zzz"), -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertIterFrom(t, r, entries, tc.start, tc.wantIdx)
		})
	}
}

// assertIterFrom checks that IterFrom(start) yields entries[wantIdx:] exactly,
// in order; wantIdx == -1 asserts an empty iteration.
func assertIterFrom(t *testing.T, r *Reader, entries []kv, start []byte, wantIdx int) {
	t.Helper()
	it := r.IterFrom(start)
	i := wantIdx
	for it.Next() {
		if i < 0 || i >= len(entries) {
			t.Fatalf("unexpected entry %q", it.Key())
		}
		assertEntryAt(t, it, entries, i)
		i++
	}
	if err := it.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	want := len(entries)
	if wantIdx == -1 {
		want = -1
	}
	if i != want {
		t.Fatalf("stopped at entry index %d, want %d", i, want)
	}
}

// assertEntryAt checks the iterator's current entry against entries[i].
func assertEntryAt(t *testing.T, it *Iterator, entries []kv, i int) {
	t.Helper()
	if !bytes.Equal(it.Key(), entries[i].key) {
		t.Fatalf("key = %q, want %q", it.Key(), entries[i].key)
	}
	if !bytes.Equal(it.Value(), entries[i].value) {
		t.Fatalf("value for %q = %q, want %q", it.Key(), it.Value(), entries[i].value)
	}
}

func TestEmptyTable(t *testing.T) {
	data := buildTable(t, Options{}, nil)
	r := openTable(t, data)

	if r.Count() != 0 {
		t.Fatalf("Count = %d, want 0", r.Count())
	}
	if r.Smallest() != nil || r.Largest() != nil {
		t.Fatalf("Smallest/Largest = %q/%q, want nil/nil", r.Smallest(), r.Largest())
	}
	if _, ok, err := r.Get([]byte("anything")); err != nil || ok {
		t.Fatalf("Get on empty table = (%v, %v), want (false, nil)", ok, err)
	}
	if it := r.Iter(); it.Next() || it.Err() != nil {
		t.Fatalf("empty table iterator yielded an entry or errored: %v", it.Err())
	}
}

func TestSmallestLargest(t *testing.T) {
	entries := sortedEntries(100)
	data := buildTable(t, Options{BlockSize: 128}, entries)
	r := openTable(t, data)

	if !bytes.Equal(r.Smallest(), entries[0].key) {
		t.Fatalf("Smallest = %q, want %q", r.Smallest(), entries[0].key)
	}
	if !bytes.Equal(r.Largest(), entries[len(entries)-1].key) {
		t.Fatalf("Largest = %q, want %q", r.Largest(), entries[len(entries)-1].key)
	}
}

func TestAddRejectsUnsortedKeys(t *testing.T) {
	w := NewWriter(&bytes.Buffer{}, Options{})
	if err := w.Add([]byte("b"), nil); err != nil {
		t.Fatalf("Add(b): %v", err)
	}
	if err := w.Add([]byte("a"), nil); err == nil {
		t.Fatal("Add(a) after Add(b): want error")
	}
	if err := w.Add([]byte("b"), nil); err == nil {
		t.Fatal("duplicate Add(b): want error")
	}
}

func TestAddRejectsInvalidEntries(t *testing.T) {
	w := NewWriter(&bytes.Buffer{}, Options{})
	if err := w.Add(nil, nil); err == nil {
		t.Fatal("Add with empty key: want error")
	}
	if err := w.Add(bytes.Repeat([]byte("k"), maxKeyLen+1), nil); err == nil {
		t.Fatal("Add with oversized key: want error")
	}
}

func TestWriterLifecycle(t *testing.T) {
	w := NewWriter(&bytes.Buffer{}, Options{})
	if err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if err := w.Finish(); err == nil {
		t.Fatal("second Finish: want error")
	}
	if err := w.Add([]byte("a"), nil); err == nil {
		t.Fatal("Add after Finish: want error")
	}
}

func TestEntryLargerThanBlock(t *testing.T) {
	big := bytes.Repeat([]byte("v"), 1024)
	entries := []kv{
		{[]byte("a"), big},
		{[]byte("b"), []byte("small")},
		{[]byte("c"), big},
	}
	data := buildTable(t, Options{BlockSize: 64}, entries)
	r := openTable(t, data)

	for _, e := range entries {
		v, ok, err := r.Get(e.key)
		if err != nil || !ok || !bytes.Equal(v, e.value) {
			t.Fatalf("Get(%q) = (%v, %v, %v)", e.key, ok, err, v != nil)
		}
	}
}

func TestDataBlockCorruption(t *testing.T) {
	entries := sortedEntries(1000)
	data := buildTable(t, Options{BlockSize: 256}, entries)
	// Flip a byte inside the first data block (which starts at offset 0).
	data[20] ^= 0xFF
	r := openTable(t, data)

	if _, _, err := r.Get(entries[0].key); err == nil {
		t.Fatal("Get on corrupted block: want error")
	}
	// A key in the last block is unaffected.
	last := entries[len(entries)-1]
	if v, ok, err := r.Get(last.key); err != nil || !ok || !bytes.Equal(v, last.value) {
		t.Fatalf("Get(%q) on clean block = (%v, %v)", last.key, ok, err)
	}
	// Full iteration trips on the corrupted block.
	it := r.Iter()
	for it.Next() { //nolint:revive // draining the iterator to reach the error
	}
	if it.Err() == nil {
		t.Fatal("iterator over corrupted block: want error")
	}
}

func TestIndexCorruption(t *testing.T) {
	data := buildTable(t, Options{BlockSize: 256}, sortedEntries(100))
	f, err := decodeFooter(data[len(data)-footerSize:])
	if err != nil {
		t.Fatalf("decodeFooter: %v", err)
	}
	data[f.indexOff] ^= 0xFF
	if _, err := NewReader(bytes.NewReader(data), int64(len(data))); err == nil {
		t.Fatal("NewReader with corrupted index: want error")
	}
}

func TestFooterCorruption(t *testing.T) {
	data := buildTable(t, Options{}, sortedEntries(10))

	badMagic := append([]byte(nil), data...)
	badMagic[len(badMagic)-1] ^= 0xFF
	if _, err := NewReader(bytes.NewReader(badMagic), int64(len(badMagic))); err == nil {
		t.Fatal("NewReader with corrupted magic: want error")
	}

	badCRC := append([]byte(nil), data...)
	badCRC[len(badCRC)-footerSize] ^= 0xFF // first footer byte (index_off)
	if _, err := NewReader(bytes.NewReader(badCRC), int64(len(badCRC))); err == nil {
		t.Fatal("NewReader with corrupted footer field: want error")
	}

	truncated := data[:footerSize-1]
	if _, err := NewReader(bytes.NewReader(truncated), int64(len(truncated))); err == nil {
		t.Fatal("NewReader on truncated file: want error")
	}
}

func TestOpenFile(t *testing.T) {
	entries := sortedEntries(100)
	data := buildTable(t, Options{BlockSize: 256}, entries)
	path := filepath.Join(t.TempDir(), "000001.sst")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()
	for _, e := range entries {
		v, ok, err := r.Get(e.key)
		if err != nil || !ok || !bytes.Equal(v, e.value) {
			t.Fatalf("Get(%q) = (%v, %v)", e.key, ok, err)
		}
	}
}

func TestConcurrentGets(t *testing.T) {
	entries := sortedEntries(500)
	data := buildTable(t, Options{BlockSize: 256}, entries)
	r := openTable(t, data)

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
