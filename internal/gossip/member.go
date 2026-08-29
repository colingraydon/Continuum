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
	// DC is the data center this member lives in — the failure domain enclosing
	// Zone. It is propagated and surfaced but carries no placement meaning yet
	// (multi-DC placement lands in a later PR). Older nodes drop the field from
	// gossip, leaving it empty; the field rides the wire automatically because
	// Member is JSON-encoded by field name with no tags.
	DC string
	// Zone is the failure domain (rack, availability zone) this member lives
	// in, nested within DC; replica placement spreads each key's replicas
	// across zones. Empty means unzoned. Older nodes drop the field from
	// gossip, so a mixed cluster degrades to per-node placement rather than
	// failing.
	Zone string
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

// SetSelfZone sets this node's failure-domain zone and increments its
// heartbeat so the change propagates to peers on the next gossip round. Other
// nodes use the zone to spread replica sets across failure domains on their
// local rings.
func (ml *MemberList) SetSelfZone(zone string) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.self.Zone = zone
	ml.self.Heartbeat++
	ml.self.UpdatedAt = time.Now()
}

// SetSelfDC sets this node's data center and increments its heartbeat so the
// change propagates to peers on the next gossip round. The DC is surfaced to
// peers now; it gains placement and quorum meaning in a later PR.
func (ml *MemberList) SetSelfDC(dc string) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.self.DC = dc
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

// memberEvent is one onChange call deferred until ml.mu is released.
type memberEvent struct {
	m      *Member
	status MemberStatus
}

// memberEvents returns the callbacks that replacing prev with m implies,
// without invoking any of them. prev is a snapshot of the entry being
// replaced, nil for a first sighting. Beyond status transitions, a metadata
// change (dc, zone, weight, address) on a member that stays alive re-fires
// Alive: the ring only learns about members through this callback, so a member
// first registered without metadata (a mesh stub via POST /nodes, or an entry
// from a peer that has not yet heard the member's own gossip) would otherwise
// keep its empty dc/zone and default weight on the ring forever.
//
// Callers must fire these *after* releasing ml.mu. The ring's change handler
// takes the ring lock, and the ring's health filter takes ml.mu from inside
// ring methods that already hold the ring lock — so calling out while holding
// ml.mu closes an ml.mu -> ring.mu -> ml.mu cycle and deadlocks the node.
//
// The event carries a copy: mutators write member structs in place under
// ml.mu, so handing a live pointer to a callback running outside the lock
// would race with every later status change.
func memberEvents(m, prev *Member) []memberEvent {
	cp := *m
	if prev == nil || prev.Status != m.Status {
		return []memberEvent{{&cp, cp.Status}}
	}
	var out []memberEvent
	if m.Status == MemberAlive &&
		(prev.DC != m.DC || prev.Zone != m.Zone || prev.Weight != m.Weight || prev.Address != m.Address) {
		out = append(out, memberEvent{&cp, MemberAlive})
	}
	if prev.Bootstrapping && !m.Bootstrapping {
		out = append(out, memberEvent{&cp, MemberBootstrapped})
	}
	return out
}

// fire invokes each deferred callback. Must be called with ml.mu released.
func fire(onChange func(*Member, MemberStatus), events []memberEvent) {
	if onChange == nil {
		return
	}
	for _, e := range events {
		onChange(e.m, e.status)
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

	var events []memberEvent
	beforeInc := ml.self.Incarnation
	for _, m := range incoming {
		if m.ID == ml.self.ID {
			ml.refuteSelf(m)
			continue
		}
		existing, ok := ml.members[m.ID]
		if !ok || supersedes(m, existing) {
			var prev *Member
			if ok {
				cp := *existing
				prev = &cp
			}
			// Stamp with the local clock: the wire value is the sender's wall
			// time, and the stale checker compares against our own clock.
			m.UpdatedAt = time.Now()
			ml.members[m.ID] = m
			events = append(events, memberEvents(m, prev)...)
		}
	}
	afterInc := ml.self.Incarnation
	sink := ml.persistIncarnation
	onChange := ml.onChange
	ml.mu.Unlock()

	fire(onChange, events)

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

// GetAll returns a snapshot copy of every member. Copies, not the internal
// structs: mutators write members in place under ml.mu, so a caller reading a
// shared pointer outside the lock (the gossip round marshaling the list, the
// stale checker, the ring health filter) would race with every status change.
func (ml *MemberList) GetAll() []*Member {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	members := make([]*Member, 0, len(ml.members))
	for _, m := range ml.members {
		cp := *m
		members = append(members, &cp)
	}
	return members
}

// GetAlive returns a snapshot copy of every alive member. See GetAll for why
// copies.
func (ml *MemberList) GetAlive() []*Member {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	members := make([]*Member, 0)
	for _, m := range ml.members {
		if m.Status == MemberAlive {
			cp := *m
			members = append(members, &cp)
		}
	}
	return members
}

func (ml *MemberList) MarkSuspect(id string) {
	ml.mu.Lock()
	m, ok := ml.members[id]
	if !ok || m.Status != MemberAlive {
		ml.mu.Unlock()
		return
	}
	m.Status = MemberSuspect
	cp := *m
	onChange := ml.onChange
	ml.mu.Unlock()

	fire(onChange, []memberEvent{{&cp, MemberSuspect}})
}

func (ml *MemberList) MarkDead(id string) {
	ml.mu.Lock()
	m, ok := ml.members[id]
	if !ok || m.Status == MemberDead {
		ml.mu.Unlock()
		return
	}
	m.Status = MemberDead
	cp := *m
	onChange := ml.onChange
	ml.mu.Unlock()

	fire(onChange, []memberEvent{{&cp, MemberDead}})
}

func (ml *MemberList) Add(id, address string) {
	ml.AddWithGossip(id, address, "")
}

// AddWithGossip registers a member along with the UDP address it receives
// gossip on. An empty gossipAddr leaves senders on the legacy assumption that
// the member shares their gossip port.
//
// Re-registering a member we already hold advances past its current
// incarnation rather than restarting at zero. Incarnation is the primary
// precedence key in Merge, so a zero-incarnation entry loses to every copy of
// the state it was meant to replace: registering a node that peers still
// believe dead at incarnation N would be reverted by the next gossip round
// carrying that entry, evicting the node from the ring again. Since only the
// node itself may advance its own incarnation, this cannot be an arbitrary
// jump — one past what we hold is exactly enough to supersede the stale claim
// without racing the node's own refutation.
func (ml *MemberList) AddWithGossip(id, address, gossipAddr string) {
	ml.mu.Lock()
	incarnation := uint64(0)
	if existing, ok := ml.members[id]; ok {
		incarnation = existing.Incarnation
		// Only outrank the entry when it currently contradicts "alive";
		// re-registering an already-alive member must stay a no-op as far as
		// precedence goes, or repeated calls would inflate the epoch and
		// outrank the node's own future state.
		if existing.Status != MemberAlive {
			incarnation++
		}
	}
	m := &Member{
		ID:          id,
		Address:     address,
		GossipAddr:  gossipAddr,
		Incarnation: incarnation,
		Heartbeat:   0,
		UpdatedAt:   time.Now(),
		Status:      MemberAlive,
	}
	ml.members[id] = m
	onChange := ml.onChange
	ml.mu.Unlock()

	if onChange != nil {
		onChange(m, MemberAlive)
	}
}

// Get returns a snapshot copy of the member with the given id. See GetAll
// for why a copy.
func (ml *MemberList) Get(id string) (*Member, bool) {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	m, ok := ml.members[id]
	if !ok {
		return nil, false
	}
	cp := *m
	return &cp, true
}

func (ml *MemberList) Size() int {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	return len(ml.members)
}
