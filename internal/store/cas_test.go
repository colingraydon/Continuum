package store

import (
	"errors"
	"testing"
)

func TestPutCASInsertsMissingKey(t *testing.T) {
	s := New()
	if err := s.PutCAS("k", "v", clock(map[string]uint64{"node1": 1})); err != nil {
		t.Fatalf("PutCAS on missing key: %v", err)
	}
	e, ok, _ := s.Get("k")
	if !ok {
		t.Fatal("expected entry to exist")
	}
	if len(e.Siblings) != 1 || e.Siblings[0].Value != "v" {
		t.Errorf("unexpected entry: %+v", e)
	}
}

func TestPutCASDominatingClockReplaces(t *testing.T) {
	s := New()
	s.Put("k", "old", clock(map[string]uint64{"node1": 1}))
	if err := s.PutCAS("k", "new", clock(map[string]uint64{"node1": 2})); err != nil {
		t.Fatalf("PutCAS with dominating clock: %v", err)
	}
	e, _, _ := s.Get("k")
	if len(e.Siblings) != 1 || e.Siblings[0].Value != "new" {
		t.Errorf("expected single sibling 'new', got %+v", e)
	}
}

func TestPutCASConcurrentClockRejected(t *testing.T) {
	s := New()
	s.Put("k", "existing", clock(map[string]uint64{"node1": 1}))
	err := s.PutCAS("k", "racer", clock(map[string]uint64{"node2": 1}))
	if !errors.Is(err, ErrCASConflict) {
		t.Fatalf("expected ErrCASConflict, got %v", err)
	}
	// The conflicting write must not have created a sibling.
	e, _, _ := s.Get("k")
	if len(e.Siblings) != 1 || e.Siblings[0].Value != "existing" {
		t.Errorf("expected store unchanged, got %+v", e)
	}
}

func TestPutCASStaleClockRejected(t *testing.T) {
	s := New()
	s.Put("k", "current", clock(map[string]uint64{"node1": 2}))
	// A plain Put would silently drop this dominated write; CAS surfaces it.
	err := s.PutCAS("k", "stale", clock(map[string]uint64{"node1": 1}))
	if !errors.Is(err, ErrCASConflict) {
		t.Fatalf("expected ErrCASConflict, got %v", err)
	}
}

func TestPutCASEqualClockRejected(t *testing.T) {
	s := New()
	s.Put("k", "current", clock(map[string]uint64{"node1": 1}))
	err := s.PutCAS("k", "again", clock(map[string]uint64{"node1": 1}))
	if !errors.Is(err, ErrCASConflict) {
		t.Fatalf("expected ErrCASConflict, got %v", err)
	}
}

func TestPutCASMustDominateAllSiblings(t *testing.T) {
	s := New()
	s.Put("k", "first", clock(map[string]uint64{"node1": 1}))
	s.Put("k", "second", clock(map[string]uint64{"node2": 1}))

	// Dominates only one of the two siblings: rejected.
	err := s.PutCAS("k", "partial", clock(map[string]uint64{"node1": 2}))
	if !errors.Is(err, ErrCASConflict) {
		t.Fatalf("expected ErrCASConflict, got %v", err)
	}

	// Dominates both: accepted and resolves the conflict.
	if err := s.PutCAS("k", "resolved", clock(map[string]uint64{"node1": 1, "node2": 1})); err != nil {
		t.Fatalf("PutCAS dominating all siblings: %v", err)
	}
	e, _, _ := s.Get("k")
	if len(e.Siblings) != 1 || e.Siblings[0].Value != "resolved" {
		t.Errorf("expected single sibling 'resolved', got %+v", e)
	}
}

func TestPutCASResurrectsTombstone(t *testing.T) {
	s := New()
	s.Delete("k", clock(map[string]uint64{"node1": 1}))
	if err := s.PutCAS("k", "back", clock(map[string]uint64{"node1": 2})); err != nil {
		t.Fatalf("PutCAS over tombstone: %v", err)
	}
	e, _, _ := s.Get("k")
	if len(e.Siblings) != 1 || e.Siblings[0].Deleted || e.Siblings[0].Value != "back" {
		t.Errorf("expected live sibling 'back', got %+v", e)
	}
}

func TestDeleteCASDominatingClockWritesTombstone(t *testing.T) {
	s := New()
	s.Put("k", "v", clock(map[string]uint64{"node1": 1}))
	if err := s.DeleteCAS("k", clock(map[string]uint64{"node1": 2})); err != nil {
		t.Fatalf("DeleteCAS with dominating clock: %v", err)
	}
	e, ok, _ := s.Get("k")
	if !ok || len(e.Siblings) != 1 || !e.Siblings[0].Deleted {
		t.Errorf("expected single tombstone sibling, got ok=%v %+v", ok, e)
	}
}

func TestDeleteCASConcurrentClockRejected(t *testing.T) {
	s := New()
	s.Put("k", "v", clock(map[string]uint64{"node1": 1}))
	err := s.DeleteCAS("k", clock(map[string]uint64{"node2": 1}))
	if !errors.Is(err, ErrCASConflict) {
		t.Fatalf("expected ErrCASConflict, got %v", err)
	}
	// The value must survive: no tombstone sibling was added.
	e, _, _ := s.Get("k")
	if len(e.Siblings) != 1 || e.Siblings[0].Deleted {
		t.Errorf("expected live entry unchanged, got %+v", e)
	}
}

func TestDeleteCASMissingKeyWritesTombstone(t *testing.T) {
	s := New()
	if err := s.DeleteCAS("k", clock(map[string]uint64{"node1": 1})); err != nil {
		t.Fatalf("DeleteCAS on missing key: %v", err)
	}
	e, ok, _ := s.Get("k")
	if !ok || len(e.Siblings) != 1 || !e.Siblings[0].Deleted {
		t.Errorf("expected tombstone, got ok=%v %+v", ok, e)
	}
}
