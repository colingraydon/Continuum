// internal/ring/stats_test.go
package ring

import (
	"fmt"
	"testing"
)

func TestGetStatsEmptyRing(t *testing.T) {
	// Arrange
	r := NewRing(150)

	// Act
	stats := r.GetStats()

	// Assert
	if stats.TotalNodes != 0 {
		t.Errorf("expected 0 nodes, got %d", stats.TotalNodes)
	}
	if stats.TotalVNodes != 0 {
		t.Errorf("expected 0 vnodes, got %d", stats.TotalVNodes)
	}
	if stats.MostLoaded != "" {
		t.Errorf("expected empty most loaded, got %s", stats.MostLoaded)
	}
	if stats.LeastLoaded != "" {
		t.Errorf("expected empty least loaded, got %s", stats.LeastLoaded)
	}
	if stats.Variance != 0 {
		t.Errorf("expected 0 variance, got %f", stats.Variance)
	}
	if len(stats.Distribution) != 0 {
		t.Errorf("expected empty distribution, got %d", len(stats.Distribution))
	}
}

func TestGetStatsTotalCounts(t *testing.T) {
	// Arrange
	r := NewRing(150)
	r.AddNode("node1", "10.0.0.1")
	r.AddNode("node2", "10.0.0.2")
	r.AddNode("node3", "10.0.0.3")

	// Act
	stats := r.GetStats()

	// Assert
	if stats.TotalNodes != 3 {
		t.Errorf("expected 3 nodes, got %d", stats.TotalNodes)
	}
	if stats.TotalVNodes != 450 {
		t.Errorf("expected 450 vnodes, got %d", stats.TotalVNodes)
	}
}

func TestGetStatsDistributionLength(t *testing.T) {
	// Arrange
	r := NewRing(150)
	r.AddNode("node1", "10.0.0.1")
	r.AddNode("node2", "10.0.0.2")

	// Act
	stats := r.GetStats()

	// Assert
	if len(stats.Distribution) != 2 {
		t.Errorf("expected 2 distribution entries, got %d", len(stats.Distribution))
	}
}

func TestGetStatsPercentagesSumTo100(t *testing.T) {
	// Arrange
	r := NewRing(150)
	r.AddNode("node1", "10.0.0.1")
	r.AddNode("node2", "10.0.0.2")
	r.AddNode("node3", "10.0.0.3")

	for i := 0; i < 1000; i++ {
		r.GetNode(fmt.Sprintf("key-%d", i))
	}

	// Act
	stats := r.GetStats()

	// Assert
	total := 0.0
	for _, n := range stats.Distribution {
		total += n.Percentage
	}
	if total < 99.9 || total > 100.1 {
		t.Errorf("expected percentages to sum to ~100, got %f", total)
	}
}
func TestGetStatsVNodeCountsMatchReplicas(t *testing.T) {
	// Arrange
	r := NewRing(150)
	r.AddNode("node1", "10.0.0.1")

	// Act
	stats := r.GetStats()

	// Assert
	if len(stats.Distribution) != 1 {
		t.Fatalf("expected 1 distribution entry, got %d", len(stats.Distribution))
	}
	if stats.Distribution[0].VNodeCount != 150 {
		t.Errorf("expected 150 vnodes, got %d", stats.Distribution[0].VNodeCount)
	}
}

func TestGetStatsMostAndLeastLoaded(t *testing.T) {
	// Arrange
	r := NewRing(150)
	r.AddNode("node1", "10.0.0.1")
	r.AddNode("node2", "10.0.0.2")

	// Act
	stats := r.GetStats()

	// Assert
	if stats.MostLoaded == "" {
		t.Error("expected most loaded to be set")
	}
	if stats.LeastLoaded == "" {
		t.Error("expected least loaded to be set")
	}
}

func TestGetStatsSingleNodeIsLeastAndMostLoaded(t *testing.T) {
	// Arrange
	r := NewRing(150)
	r.AddNode("node1", "10.0.0.1")

	// Act
	stats := r.GetStats()

	// Assert
	if stats.MostLoaded != "node1" {
		t.Errorf("expected node1 as most loaded, got %s", stats.MostLoaded)
	}
	if stats.LeastLoaded != "node1" {
		t.Errorf("expected node1 as least loaded, got %s", stats.LeastLoaded)
	}
}

func TestGetStatsVarianceIsZeroForSingleNode(t *testing.T) {
	// Arrange
	r := NewRing(150)
	r.AddNode("node1", "10.0.0.1")

	for i := 0; i < 1000; i++ {
		r.GetNode(fmt.Sprintf("key-%d", i))
	}

	// Act
	stats := r.GetStats()

	// Assert
	if stats.Variance != 0 {
		t.Errorf("expected 0 variance for single node, got %f", stats.Variance)
	}
}


func TestGetStatsVarianceIsLowWithManyReplicas(t *testing.T) {
	// Arrange
	r := NewRing(150)
	r.AddNode("node1", "10.0.0.1")
	r.AddNode("node2", "10.0.0.2")
	r.AddNode("node3", "10.0.0.3")

	for i := 0; i < 1000; i++ {
		r.GetNode(fmt.Sprintf("key-%d", i))
	}

	// Act
	stats := r.GetStats()

	// Assert
	if stats.Variance > 10.0 {
		t.Errorf("expected low variance with 150 replicas, got %f", stats.Variance)
	}
}

func TestGetStatsRemovedNodeNotInDistribution(t *testing.T) {
	// Arrange
	r := NewRing(150)
	r.AddNode("node1", "10.0.0.1")
	r.AddNode("node2", "10.0.0.2")
	r.RemoveNode("node1")

	// Act
	stats := r.GetStats()

	// Assert
	if stats.TotalNodes != 1 {
		t.Errorf("expected 1 node after removal, got %d", stats.TotalNodes)
	}
	for _, n := range stats.Distribution {
		if n.NodeID == "node1" {
			t.Error("expected node1 to be removed from distribution")
		}
	}
}