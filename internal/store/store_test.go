package store

import (
	"testing"
)

func clock(counters map[string]uint64) VectorClockVersion {
	return VectorClockVersion{Clocks: counters}
}

func TestPutAndGet(t *testing.T) {
	s := New()
	v := NewClock().Increment("node1")
	s.Put("k", "v", v)

	e, ok := s.Get("k")
	if !ok {
		t.Fatal("expected entry to exist")
	}
	if e.Value != "v" {
		t.Errorf("expected 'v', got %q", e.Value)
	}
	if e.Hash == 0 {
		t.Error("expected non-zero hash")
	}
}

func TestGetMissing(t *testing.T) {
	s := New()
	_, ok := s.Get("missing")
	if ok {
		t.Error("expected miss for unknown key")
	}
}

func TestNewerClockWins(t *testing.T) {
	s := New()
	s.Put("k", "old", clock(map[string]uint64{"node1": 1}))
	s.Put("k", "new", clock(map[string]uint64{"node1": 2}))

	e, _ := s.Get("k")
	if e.Value != "new" {
		t.Errorf("expected 'new', got %q", e.Value)
	}
}

func TestOlderClockDropped(t *testing.T) {
	s := New()
	s.Put("k", "new", clock(map[string]uint64{"node1": 2}))
	s.Put("k", "old", clock(map[string]uint64{"node1": 1}))

	e, _ := s.Get("k")
	if e.Value != "new" {
		t.Errorf("expected 'new' to win, got %q", e.Value)
	}
}

func TestConcurrentClocksKeepExisting(t *testing.T) {
	s := New()
	// node1 and node2 each wrote independently — neither happens-before the other
	s.Put("k", "first", clock(map[string]uint64{"node1": 1}))
	s.Put("k", "second", clock(map[string]uint64{"node2": 1}))

	e, _ := s.Get("k")
	if e.Value != "first" {
		t.Errorf("expected 'first' to hold on concurrent writes, got %q", e.Value)
	}
}

func TestEqualClocksKeepExisting(t *testing.T) {
	s := New()
	s.Put("k", "first", clock(map[string]uint64{"node1": 1}))
	s.Put("k", "second", clock(map[string]uint64{"node1": 1}))

	e, _ := s.Get("k")
	if e.Value != "first" {
		t.Errorf("expected 'first' to hold on equal clocks, got %q", e.Value)
	}
}

func TestHashDeterministic(t *testing.T) {
	s := New()
	s.Put("k", "v", NewClock().Increment("node1"))
	e1, _ := s.Get("k")

	s2 := New()
	s2.Put("k", "v", NewClock().Increment("node1").Increment("node1"))
	e2, _ := s2.Get("k")

	if e1.Hash != e2.Hash {
		t.Errorf("hash should depend only on value, got %d vs %d", e1.Hash, e2.Hash)
	}
}

func TestHashDiffersForDifferentValues(t *testing.T) {
	s := New()
	v := NewClock().Increment("node1")
	s.Put("a", "foo", v)
	s.Put("b", "bar", v)

	ea, _ := s.Get("a")
	eb, _ := s.Get("b")
	if ea.Hash == eb.Hash {
		t.Error("expected different hashes for different values")
	}
}

func TestHappensBefore(t *testing.T) {
	cases := []struct {
		name   string
		a, b   VectorClockVersion
		aBeforeB bool
		bBeforeA bool
	}{
		{
			name:     "strictly before",
			a:        clock(map[string]uint64{"n1": 1}),
			b:        clock(map[string]uint64{"n1": 2}),
			aBeforeB: true,
			bBeforeA: false,
		},
		{
			name:     "equal",
			a:        clock(map[string]uint64{"n1": 1}),
			b:        clock(map[string]uint64{"n1": 1}),
			aBeforeB: false,
			bBeforeA: false,
		},
		{
			name:     "concurrent",
			a:        clock(map[string]uint64{"n1": 1}),
			b:        clock(map[string]uint64{"n2": 1}),
			aBeforeB: false,
			bBeforeA: false,
		},
		{
			name:     "empty happens-before non-empty",
			a:        NewClock(),
			b:        clock(map[string]uint64{"n1": 1}),
			aBeforeB: true,
			bBeforeA: false,
		},
		{
			name:     "multi-node before",
			a:        clock(map[string]uint64{"n1": 1, "n2": 1}),
			b:        clock(map[string]uint64{"n1": 2, "n2": 2}),
			aBeforeB: true,
			bBeforeA: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.HappensBefore(tc.b); got != tc.aBeforeB {
				t.Errorf("a.HappensBefore(b): expected %v, got %v", tc.aBeforeB, got)
			}
			if got := tc.b.HappensBefore(tc.a); got != tc.bBeforeA {
				t.Errorf("b.HappensBefore(a): expected %v, got %v", tc.bBeforeA, got)
			}
		})
	}
}

func TestIncrement(t *testing.T) {
	v := NewClock().Increment("node1").Increment("node1").Increment("node2")
	if v.Clocks["node1"] != 2 {
		t.Errorf("expected node1=2, got %d", v.Clocks["node1"])
	}
	if v.Clocks["node2"] != 1 {
		t.Errorf("expected node2=1, got %d", v.Clocks["node2"])
	}
}

func TestIncrementDoesNotMutateReceiver(t *testing.T) {
	original := NewClock().Increment("node1")
	_ = original.Increment("node1")
	if original.Clocks["node1"] != 1 {
		t.Errorf("Increment mutated receiver: node1=%d", original.Clocks["node1"])
	}
}
