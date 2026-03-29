package stats

import (
	"github.com/colingraydon/continuum/internal/gossip"
	"github.com/colingraydon/continuum/internal/ring"
)

type Aggregator struct {
	ring       *ring.Ring
	memberList *gossip.MemberList
}

func NewAggregator(r *ring.Ring, ml *gossip.MemberList) *Aggregator {
	return &Aggregator{ring: r, memberList: ml}
}

func (a *Aggregator) GetStats() ring.RingStats {
	stats := a.ring.GetStats()
	for _, n := range a.ring.GetNodes() {
		m, ok := a.memberList.Get(n.ID)
		if !ok {
			stats.DeadNodes++
			continue
		}
		switch m.Status {
		case gossip.MemberAlive:
			stats.HealthyNodes++
		case gossip.MemberSuspect:
			stats.SuspectNodes++
		case gossip.MemberDead:
			stats.DeadNodes++
		}
	}
	return stats
}
