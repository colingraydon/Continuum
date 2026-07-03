package gossip

import (
	"sync"
	"time"
)

type MemberStatus int

const (
	MemberAlive MemberStatus = iota
	MemberSuspect
	MemberDead
	// MemberBootstrapped is a callback-only signal fired when a member
	// transitions from Bootstrapping=true to Bootstrapping=false. It is never
	// stored in Member.Status; callers use it to trigger local cleanup.
	MemberBootstrapped
)

func (s MemberStatus) String() string {
	switch s {
	case MemberAlive:
		return "alive"
	case MemberSuspect:
		return "suspect"
	case MemberDead:
		return "dead"
	case MemberBootstrapped:
		return "bootstrapped"
	default:
		return "unknown"
	}
}

type Member struct {
	ID      string
	Address string
	// GossipAddr is the UDP address this member receives gossip on. Empty for
	// members that predate the field or were registered without one; senders
	// fall back to the member's host on their own gossip port (the legacy
	// same-port-everywhere assumption, which holds in the Docker setup).
	GossipAddr string
	// Incarnation is the node's epoch, advanced only by the node itself — on a
	// restart via refutation, or to refute a suspect/dead claim (see
	// refuteSelf). It is the primary precedence key in Merge; Heartbeat only
	// breaks ties within the same incarnation. This is what lets a
	// crash-restarted node, whose heartbeat resets to zero, reclaim its
	// identity without waiting to out-count its pre-crash heartbeat.
	Incarnation   uint64
	Heartbeat     uint64
	UpdatedAt     time.Time
	Status        MemberStatus
	Bootstrapping bool
	Weight        float64 // relative capacity; 0 is treated as 1.0 by the ring
}

type MemberList struct {
	mu       sync.RWMutex
	members  map[string]*Member
	self     *Member
	onChange func(member *Member, status MemberStatus)
	// persistIncarnation, if set, is called (outside mu) whenever this node
	// advances its own incarnation, so the new epoch can be durably recorded
	// before it is gossiped onward. See SetIncarnationSink.
	persistIncarnation func(uint64)
}

func NewMemberList(selfID, selfAddress string, onChange func(member *Member, status MemberStatus)) *MemberList {
	self := &Member{
		ID:        selfID,
		Address:   selfAddress,
		Heartbeat: 0,
		UpdatedAt: time.Now(),
		Status:    MemberAlive,
		Weight:    1.0,
	}
	ml := &MemberList{
		members:  make(map[string]*Member),
		self:     self,
		onChange: onChange,
	}
	ml.members[selfID] = self
	return ml
}

// SetSelfGossipAddr sets the UDP address this node advertises for gossip and
// increments its heartbeat so the change propagates to peers. Without it,
// peers assume this node listens on the same gossip port they do.
func (ml *MemberList) SetSelfGossipAddr(addr string) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.self.GossipAddr = addr
	ml.self.Heartbeat++
	ml.self.UpdatedAt = time.Now()
}

// SetSelfWeight sets this node's capacity weight and increments its heartbeat so
// the change propagates to peers on the next gossip round. A weight of 2.0 causes
// other nodes to assign this node twice as many vnodes on their local rings.
func (ml *MemberList) SetSelfWeight(weight float64) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.self.Weight = weight
	ml.self.Heartbeat++
	ml.self.UpdatedAt = time.Now()
}

func (ml *MemberList) IncrementHeartbeat() {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.self.Heartbeat++
	ml.self.UpdatedAt = time.Now()
}

// SetSelfIncarnation sets this node's incarnation (epoch). Used at startup to
// restore a persisted incarnation so a crash-restarted node's gossip
// immediately supersedes the stale entry peers remember, rather than depending
// on receiving an inbound refutation trigger. Advancing the incarnation is by
// itself enough for peers to accept the update; no heartbeat bump is needed.
func (ml *MemberList) SetSelfIncarnation(v uint64) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.self.Incarnation = v
	ml.self.UpdatedAt = time.Now()
}

// SetIncarnationSink registers a callback invoked, outside the member lock,
// whenever this node advances its own incarnation via refutation — so the new
// epoch can be persisted before it is gossiped onward. Persisting outside the
// lock keeps a slow fsync off the gossip path; the small window where the
// advance is in memory but not yet on disk falls back to refutation, since a
// value that was never gossiped cannot have been remembered by any peer.
func (ml *MemberList) SetIncarnationSink(fn func(uint64)) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.persistIncarnation = fn
}

func (ml *MemberList) notifyMemberChange(m *Member, ok bool, prevStatus MemberStatus, wasBootstrapping bool) {
	if ml.onChange == nil {
		return
	}
	if !ok || prevStatus != m.Status {
		ml.onChange(m, m.Status)
	} else if wasBootstrapping && !m.Bootstrapping {
		ml.onChange(m, MemberBootstrapped)
	}
}

// supersedes reports whether the incoming view of a member should replace the
// one we currently hold. Incarnation dominates; heartbeat only breaks ties
// within the same incarnation. Heartbeat advances once per gossip round and
// signals liveness, but it resets to zero on restart, so it cannot be the
// primary key — otherwise a rejoined node's fresh state would lose to the stale
// entry peers remember until it counted all the way back up.
func supersedes(incoming, existing *Member) bool {
	if incoming.Incarnation != existing.Incarnation {
		return incoming.Incarnation > existing.Incarnation
	}
	return incoming.Heartbeat > existing.Heartbeat
}

// refuteSelf reacts to gossip that carries this node's own state. After a
// crash-restart the node's incarnation is back at zero while peers still hold
// its pre-crash (higher) incarnation, and peers may be spreading a suspect or
// dead claim about it. In either case the node advances its incarnation just
// past the stale value and keeps asserting Alive, so its next gossip round
// supersedes the stale entry within a round or two instead of waiting to
// out-count a pre-crash heartbeat. Must be called with ml.mu held.
func (ml *MemberList) refuteSelf(incoming *Member) {
	switch {
	case incoming.Incarnation > ml.self.Incarnation:
		ml.self.Incarnation = incoming.Incarnation + 1
	case incoming.Incarnation == ml.self.Incarnation && incoming.Status != MemberAlive:
		ml.self.Incarnation++
	default:
		return
	}
	ml.self.UpdatedAt = time.Now()
}

func (ml *MemberList) Merge(incoming []*Member) {
	ml.mu.Lock()

	beforeInc := ml.self.Incarnation
	for _, m := range incoming {
		if m.ID == ml.self.ID {
			ml.refuteSelf(m)
			continue
		}
		existing, ok := ml.members[m.ID]
		if !ok || supersedes(m, existing) {
			wasBootstrapping := ok && existing.Bootstrapping
			prevStatus := MemberAlive
			if ok {
				prevStatus = existing.Status
			}
			// Stamp with the local clock: the wire value is the sender's wall
			// time, and the stale checker compares against our own clock.
			m.UpdatedAt = time.Now()
			ml.members[m.ID] = m
			ml.notifyMemberChange(m, ok, prevStatus, wasBootstrapping)
		}
	}
	afterInc := ml.self.Incarnation
	sink := ml.persistIncarnation
	ml.mu.Unlock()

	// Persist a refutation-driven advance before it propagates. Done outside
	// the lock so the fsync does not stall concurrent membership reads.
	if sink != nil && afterInc != beforeInc {
		sink(afterInc)
	}
}

// SetBootstrapping updates the Bootstrapping flag for id and increments its
// heartbeat so the change propagates via gossip. When transitioning to false,
// the onChange callback is fired with MemberBootstrapped so callers can trigger
// cleanup (e.g. evicting keys that migrated to the newly-ready node).
func (ml *MemberList) SetBootstrapping(id string, v bool) {
	ml.mu.Lock()
	m, ok := ml.members[id]
	if !ok || m.Bootstrapping == v {
		ml.mu.Unlock()
		return
	}
	wasBootstrapping := m.Bootstrapping
	m.Bootstrapping = v
	m.Heartbeat++
	m.UpdatedAt = time.Now()
	onChange := ml.onChange
	ml.mu.Unlock()
	if !v && wasBootstrapping && onChange != nil {
		onChange(m, MemberBootstrapped)
	}
}

func (ml *MemberList) GetAll() []*Member {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	members := make([]*Member, 0, len(ml.members))
	for _, m := range ml.members {
		members = append(members, m)
	}
	return members
}

func (ml *MemberList) GetAlive() []*Member {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	members := make([]*Member, 0)
	for _, m := range ml.members {
		if m.Status == MemberAlive {
			members = append(members, m)
		}
	}
	return members
}

func (ml *MemberList) MarkSuspect(id string) {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	m, ok := ml.members[id]
	if !ok || m.Status != MemberAlive {
		return
	}
	m.Status = MemberSuspect
	if ml.onChange != nil {
		ml.onChange(m, MemberSuspect)
	}
}

func (ml *MemberList) MarkDead(id string) {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	m, ok := ml.members[id]
	if !ok || m.Status == MemberDead {
		return
	}
	m.Status = MemberDead
	if ml.onChange != nil {
		ml.onChange(m, MemberDead)
	}
}

func (ml *MemberList) Add(id, address string) {
	ml.AddWithGossip(id, address, "")
}

// AddWithGossip registers a member along with the UDP address it receives
// gossip on. An empty gossipAddr leaves senders on the legacy assumption that
// the member shares their gossip port.
func (ml *MemberList) AddWithGossip(id, address, gossipAddr string) {
	ml.mu.Lock()
	m := &Member{
		ID:         id,
		Address:    address,
		GossipAddr: gossipAddr,
		Heartbeat:  0,
		UpdatedAt:  time.Now(),
		Status:     MemberAlive,
	}
	ml.members[id] = m
	onChange := ml.onChange
	ml.mu.Unlock()

	if onChange != nil {
		onChange(m, MemberAlive)
	}
}

func (ml *MemberList) Get(id string) (*Member, bool) {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	m, ok := ml.members[id]
	return m, ok
}

func (ml *MemberList) Size() int {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	return len(ml.members)
}
