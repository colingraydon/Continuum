package stats

import (
	"testing"

	"github.com/colingraydon/continuum/internal/health"
	"github.com/colingraydon/continuum/internal/ring"
)

func newTestAggregator() *Aggregator {
	r := ring.NewRing(10)
	c := health.NewChecker(health.DefaultConfig(), nil)
	return NewAggregator(r, c)
}

func TestNewAggregator(t *testing.T) {
	// Arrange + Act
	a := newTestAggregator()

	// Assert
	if a == nil {
		t.Fatal("expected aggregator to not be nil")
	}
}

func TestGetStatsEmptyRing(t *testing.T) {
	// Arrange
	a := newTestAggregator()

	// Act
	stats := a.GetStats()

	// Assert
	if stats.TotalNodes != 0 {
		t.Errorf("expected 0 nodes, got %d", stats.TotalNodes)
	}
	if stats.HealthyNodes != 0 {
		t.Errorf("expected 0 healthy nodes, got %d", stats.HealthyNodes)
	}
	if stats.SuspectNodes != 0 {
		t.Errorf("expected 0 suspect nodes, got %d", stats.SuspectNodes)
	}
	if stats.DeadNodes != 0 {
		t.Errorf("expected 0 dead nodes, got %d", stats.DeadNodes)
	}
}

func TestGetStatsHealthyNodes(t *testing.T) {
	// Arrange
	r := ring.NewRing(10)
	c := health.NewChecker(health.DefaultConfig(), nil)
	a := NewAggregator(r, c)
	r.AddNode("node1", "10.0.0.1")
	r.AddNode("node2", "10.0.0.2")
	c.AddNode("node1", "10.0.0.1")
	c.AddNode("node2", "10.0.0.2")

	// Act
	stats := a.GetStats()

	// Assert
	if stats.HealthyNodes != 2 {
		t.Errorf("expected 2 healthy nodes, got %d", stats.HealthyNodes)
	}
	if stats.SuspectNodes != 0 {
		t.Errorf("expected 0 suspect nodes, got %d", stats.SuspectNodes)
	}
	if stats.DeadNodes != 0 {
		t.Errorf("expected 0 dead nodes, got %d", stats.DeadNodes)
	}
}

func TestGetStatsSuspectNodes(t *testing.T) {
	// Arrange
	r := ring.NewRing(10)
	c := health.NewChecker(health.DefaultConfig(), nil)
	a := NewAggregator(r, c)
	r.AddNode("node1", "10.0.0.1")
	c.AddNode("node1", "localhost:19999")
	c.CheckNode("node1")

	// Act
	stats := a.GetStats()

	// Assert
	if stats.SuspectNodes != 1 {
		t.Errorf("expected 1 suspect node, got %d", stats.SuspectNodes)
	}
}

func TestGetStatsDeadNodes(t *testing.T) {
	// Arrange
	r := ring.NewRing(10)
	c := health.NewChecker(health.DefaultConfig(), nil)
	a := NewAggregator(r, c)
	r.AddNode("node1", "10.0.0.1")
	c.AddNode("node1", "localhost:19999")
	c.CheckNode("node1")
	c.CheckNode("node1")
	c.CheckNode("node1")

	// Act
	stats := a.GetStats()

	// Assert
	if stats.DeadNodes != 1 {
		t.Errorf("expected 1 dead node, got %d", stats.DeadNodes)
	}
}

func TestGetStatsTotalNodes(t *testing.T) {
	// Arrange
	r := ring.NewRing(10)
	c := health.NewChecker(health.DefaultConfig(), nil)
	a := NewAggregator(r, c)
	r.AddNode("node1", "10.0.0.1")
	r.AddNode("node2", "10.0.0.2")
	r.AddNode("node3", "10.0.0.3")
	c.AddNode("node1", "10.0.0.1")
	c.AddNode("node2", "10.0.0.2")
	c.AddNode("node3", "10.0.0.3")

	// Act
	stats := a.GetStats()

	// Assert
	if stats.TotalNodes != 3 {
		t.Errorf("expected 3 total nodes, got %d", stats.TotalNodes)
	}
}