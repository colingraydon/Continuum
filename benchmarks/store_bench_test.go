package benchmarks

import (
	"fmt"
	"testing"

	"github.com/colingraydon/continuum/internal/store"
)

func BenchmarkStorePut(b *testing.B) {
	s := store.New()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		v := store.NewClock().Increment("node1")
		s.Put(fmt.Sprintf("key-%d", i), "value", v)
	}
}

func BenchmarkStoreGet(b *testing.B) {
	s := store.New()
	for i := 0; i < 1000; i++ {
		v := store.NewClock().Increment("node1")
		s.Put(fmt.Sprintf("key-%d", i), "value", v)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Get(fmt.Sprintf("key-%d", i%1000))
	}
}

func BenchmarkStorePutConflict(b *testing.B) {
	// Simulates a replica receiving a write it already has (older clock dropped).
	s := store.New()
	newer := store.NewClock().Increment("node1").Increment("node1")
	s.Put("key", "new", newer)
	older := store.NewClock().Increment("node1")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Put("key", "old", older)
	}
}

func BenchmarkStorePutParallel(b *testing.B) {
	s := store.New()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			v := store.NewClock().Increment("node1")
			s.Put(fmt.Sprintf("key-%d", i), "value", v)
			i++
		}
	})
}

func BenchmarkStoreConcurrentReadsAndWrites(b *testing.B) {
	s := store.New()
	for i := 0; i < 1000; i++ {
		v := store.NewClock().Increment("node1")
		s.Put(fmt.Sprintf("key-%d", i), "value", v)
	}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10 == 0 {
				v := store.NewClock().Increment("node1")
				s.Put(fmt.Sprintf("key-%d", i), "value", v)
			} else {
				s.Get(fmt.Sprintf("key-%d", i%1000))
			}
			i++
		}
	})
}

func BenchmarkVectorClockIncrement(b *testing.B) {
	v := store.NewClock()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		v = v.Increment("node1")
	}
}

func BenchmarkVectorClockHappensBefore(b *testing.B) {
	a := store.NewClock().Increment("node1")
	c := store.NewClock().Increment("node1").Increment("node1")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		a.HappensBefore(c)
	}
}
