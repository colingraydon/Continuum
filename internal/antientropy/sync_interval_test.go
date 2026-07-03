package antientropy

import (
	"testing"
	"time"

	"github.com/colingraydon/continuum/internal/ring"
	"github.com/colingraydon/continuum/internal/store"
)

func TestSetSyncInterval(t *testing.T) {
	m := New(ring.NewRing(4), store.New(), "self", 2, time.Second)
	if m.syncEvery != syncInterval {
		t.Fatalf("expected default sync interval %v, got %v", syncInterval, m.syncEvery)
	}

	m.SetSyncInterval(2 * time.Second)
	if m.syncEvery != 2*time.Second {
		t.Errorf("expected override to 2s, got %v", m.syncEvery)
	}

	// Non-positive values are ignored, keeping the current interval.
	m.SetSyncInterval(0)
	if m.syncEvery != 2*time.Second {
		t.Errorf("zero interval should be ignored, got %v", m.syncEvery)
	}
	m.SetSyncInterval(-time.Second)
	if m.syncEvery != 2*time.Second {
		t.Errorf("negative interval should be ignored, got %v", m.syncEvery)
	}
}
