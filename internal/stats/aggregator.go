package stats

import (
	"github.com/colingraydon/continuum/internal/health"
	"github.com/colingraydon/continuum/internal/ring"
)

type Aggregator struct {
	ring    *ring.Ring
	checker *health.Checker
}

func NewAggregator(r *ring.Ring, c *health.Checker) *Aggregator {
	return &Aggregator{ring: r, checker: c}
}

func (a *Aggregator) GetStats() ring.RingStats {
	stats := a.ring.GetStats()
	for _, n := range a.ring.GetNodes() {
		status, _ := a.checker.GetStatus(n.ID)
		switch status {
		case health.StatusHealthy:
			stats.HealthyNodes++
		case health.StatusSuspect:
			stats.SuspectNodes++
		case health.StatusDead:
			stats.DeadNodes++
		}
	}
	return stats
}