package store

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"
)

// mv builds a memValue carrying a live entry whose single sibling value marks
// which write produced it, so tests can assert in-place updates.
func mv(tag string) memValue {
	return memValue{entry: Entry{Siblings: []Sibling{{Value: tag}}}}
}

func (s *skiplist) keysInOrder() []string {
	var out []string
	for x := s.head.next[0]; x != nil; x = x.next[0] {
		out = append(out, x.key)
	}
	return out
}

func TestSkiplistGetSetOrder(t *testing.T) {
	sl := newSkiplist()
	// Insert out of order; iteration must come back sorted.
	for _, k := range []string{"m", "a", "z", "c", "a", "b"} {
		sl.set(k, mv(k))
	}
	want := []string{"a", "b", "c", "m", "z"}
	if got := sl.keysInOrder(); !equalStrings(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
	if sl.n != len(want) {
		t.Fatalf("n = %d, want %d (duplicate insert must update, not add)", sl.n, len(want))
	}
	if v, ok := sl.get("c"); !ok || v.entry.Siblings[0].Value != "c" {
		t.Fatalf("get(c) = (%+v, %v)", v, ok)
	}
	if _, ok := sl.get("absent"); ok {
		t.Fatalf("get(absent) reported present")
	}
}

func TestSkiplistSetUpdatesInPlace(t *testing.T) {
	sl := newSkiplist()
	sl.set("k", mv("first"))
	sl.set("k", mv("second"))
	if sl.n != 1 {
		t.Fatalf("n = %d, want 1", sl.n)
	}
	if v, _ := sl.get("k"); v.entry.Siblings[0].Value != "second" {
		t.Fatalf("value = %q, want second", v.entry.Siblings[0].Value)
	}
}

func TestSkiplistDelete(t *testing.T) {
	sl := newSkiplist()
	for _, k := range []string{"a", "b", "c", "d"} {
		sl.set(k, mv(k))
	}
	if !sl.del("b") {
		t.Fatal("del(b) reported absent")
	}
	if sl.del("b") {
		t.Fatal("second del(b) reported present")
	}
	if sl.del("absent") {
		t.Fatal("del(absent) reported present")
	}
	if got, want := sl.keysInOrder(), []string{"a", "c", "d"}; !equalStrings(got, want) {
		t.Fatalf("after delete order = %v, want %v", got, want)
	}
	if sl.n != 3 {
		t.Fatalf("n = %d, want 3", sl.n)
	}
}

func TestSkiplistSeekPrev(t *testing.T) {
	sl := newSkiplist()
	for _, k := range []string{"b", "d", "f"} {
		sl.set(k, mv(k))
	}
	cases := []struct {
		start string
		want  string // first key >= start, "" if none
	}{
		{"a", "b"},
		{"b", "b"},
		{"c", "d"},
		{"f", "f"},
		{"g", ""},
	}
	for _, tc := range cases {
		prev := sl.seekPrev(tc.start)
		var got string
		if prev.next[0] != nil {
			got = prev.next[0].key
		}
		if got != tc.want {
			t.Errorf("seekPrev(%q) → first %q, want %q", tc.start, got, tc.want)
		}
	}
}

// TestSkiplistFuzzAgainstMap exercises interleaved set/del/get against a
// reference map and checks ordered iteration matches a sorted key set.
func TestSkiplistFuzzAgainstMap(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	sl := newSkiplist()
	ref := map[string]string{}
	for i := 0; i < 20000; i++ {
		k := fmt.Sprintf("k%03d", rng.Intn(400))
		switch rng.Intn(3) {
		case 0, 1:
			tag := fmt.Sprintf("v%d", i)
			sl.set(k, mv(tag))
			ref[k] = tag
		default:
			sl.del(k)
			delete(ref, k)
		}
	}
	if sl.n != len(ref) {
		t.Fatalf("n = %d, want %d", sl.n, len(ref))
	}
	wantKeys := make([]string, 0, len(ref))
	for k := range ref {
		wantKeys = append(wantKeys, k)
	}
	sort.Strings(wantKeys)
	if got := sl.keysInOrder(); !equalStrings(got, wantKeys) {
		t.Fatalf("ordered keys diverged from reference (%d vs %d)", len(got), len(wantKeys))
	}
	for k, want := range ref {
		v, ok := sl.get(k)
		if !ok || v.entry.Siblings[0].Value != want {
			t.Fatalf("get(%q) = (%+v, %v), want %q", k, v, ok, want)
		}
	}
}

// TestSkiplistBuildsLevels guards against a degenerate level generator: with
// 1000 keys the skiplist must grow express lanes above level 1, or it has
// silently become a plain linked list (correct but O(n) search).
func TestSkiplistBuildsLevels(t *testing.T) {
	sl := newSkiplist()
	for i := 0; i < 1000; i++ {
		sl.set(fmt.Sprintf("k%04d", i), mv("v"))
	}
	if sl.level <= 1 {
		t.Fatalf("skiplist level = %d after 1000 inserts; randomLevel is degenerate", sl.level)
	}
}

// TestMemIterPastEnd exercises the iterator's exhaustion guard: calling next
// after it has already returned false stays false instead of dereferencing a
// nil node.
func TestMemIterPastEnd(t *testing.T) {
	m := newMemtable()
	m.putEntry("a", Entry{Siblings: []Sibling{{Value: "1"}}}, time.Time{}, 1)
	it := m.iter()
	if !it.next() {
		t.Fatal("want first key")
	}
	if it.next() {
		t.Fatal("want end after one key")
	}
	if it.next() {
		t.Fatal("next past end must stay false")
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
