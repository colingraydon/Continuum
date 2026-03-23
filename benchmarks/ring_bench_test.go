package benchmarks

import (
	"fmt"
	"testing"

	"github.com/colingraydon/continuum/internal/ring"
)

func BenchmarkGetNodeSmallRing(b *testing.B) {
	// Arrange
	r := ring.NewRing(150)
	r.AddNode("node1", "10.0.0.1")
	r.AddNode("node2", "10.0.0.2")
	r.AddNode("node3", "10.0.0.3")
	b.ResetTimer()

	// Act
	for i := 0; i < b.N; i++ {
		r.GetNode(fmt.Sprintf("key-%d", i))
	}
}

func BenchmarkGetNodeLargeRing(b *testing.B) {
	// Arrange
	r := ring.NewRing(150)
	for i := 0; i < 100; i++ {
		r.AddNode(fmt.Sprintf("node%d", i), fmt.Sprintf("10.0.0.%d", i))
	}
	b.ResetTimer()

	// Act
	for i := 0; i < b.N; i++ {
		r.GetNode(fmt.Sprintf("key-%d", i))
	}
}

func BenchmarkGetNodeParallel(b *testing.B) {
	// Arrange
	r := ring.NewRing(150)
	r.AddNode("node1", "10.0.0.1")
	r.AddNode("node2", "10.0.0.2")
	r.AddNode("node3", "10.0.0.3")
	b.ResetTimer()

	// Act
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			r.GetNode(fmt.Sprintf("key-%d", i))
			i++
		}
	})
}

func BenchmarkAddNode(b *testing.B) {
	// Arrange
	r := ring.NewRing(150)
	b.ResetTimer()

	// Act
	for i := 0; i < b.N; i++ {
		r.AddNode(fmt.Sprintf("node%d", i), fmt.Sprintf("10.0.0.%d", i))
	}
}

func BenchmarkRemoveNode(b *testing.B) {
	// Arrange
	r := ring.NewRing(150)
	for i := 0; i < b.N; i++ {
		r.AddNode(fmt.Sprintf("node%d", i), fmt.Sprintf("10.0.0.%d", i))
	}
	b.ResetTimer()

	// Act
	for i := 0; i < b.N; i++ {
		r.RemoveNode(fmt.Sprintf("node%d", i))
	}
}

func benchmarkDistribution(b *testing.B, replicas int) {
	b.Helper()
	r := ring.NewRing(replicas)
	r.AddNode("node1", "10.0.0.1")
	r.AddNode("node2", "10.0.0.2")
	r.AddNode("node3", "10.0.0.3")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r.GetNode(fmt.Sprintf("key-%d", i))
	}
}

func BenchmarkDistributionReplicas10(b *testing.B)  { benchmarkDistribution(b, 10) }
func BenchmarkDistributionReplicas50(b *testing.B)  { benchmarkDistribution(b, 50) }
func BenchmarkDistributionReplicas150(b *testing.B) { benchmarkDistribution(b, 150) }
func BenchmarkDistributionReplicas500(b *testing.B) { benchmarkDistribution(b, 500) }

func BenchmarkDistributionUniformity(b *testing.B) {
	// Arrange
	r := ring.NewRing(150)
	r.AddNode("node1", "10.0.0.1")
	r.AddNode("node2", "10.0.0.2")
	r.AddNode("node3", "10.0.0.3")
	b.ResetTimer()

	// Act
	for i := 0; i < b.N; i++ {
		r.GetNode(fmt.Sprintf("key-%d", i))
	}

	stats := r.GetStats()
	b.ReportMetric(stats.Variance, "variance")
	b.ReportMetric(float64(stats.TotalVNodes), "vnodes")
}

func BenchmarkConcurrentReads(b *testing.B) {
	// Arrange
	r := ring.NewRing(150)
	r.AddNode("node1", "10.0.0.1")
	r.AddNode("node2", "10.0.0.2")
	r.AddNode("node3", "10.0.0.3")
	b.ResetTimer()

	// Act
	b.SetParallelism(32)
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			r.GetNode(fmt.Sprintf("key-%d", i))
			i++
		}
	})
}

func BenchmarkConcurrentReadsAndWrites(b *testing.B) {
	// Arrange
	r := ring.NewRing(150)
	r.AddNode("node1", "10.0.0.1")
	r.AddNode("node2", "10.0.0.2")
	r.AddNode("node3", "10.0.0.3")
	b.ResetTimer()

	// Act
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%100 == 0 {
				r.AddNode(fmt.Sprintf("node-tmp-%d", i), "10.0.0.255")
				r.RemoveNode(fmt.Sprintf("node-tmp-%d", i))
			} else {
				r.GetNode(fmt.Sprintf("key-%d", i))
			}
			i++
		}
	})
}