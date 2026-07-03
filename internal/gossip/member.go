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
	ID            string
	Address       string
	// GossipAddr is the UDP address this member receives gossip on. Empty for
	// members that predate the field or were registered without one; senders
	// fall back to the member's host on their own gossip port (the legacy
	// same-port-everywhere assumption, which holds in the Docker setup).
	GossipAddr    string
	Heartbeat     uint64
	UpdatedAt     time.Time
	Status        MemberStatus
	Bootstrapping bool
	Weight        float64 // relative capacity; 0 is treated as 1.0 by the ring
}

type MemberList struct {
	mu      sync.RWMutex
	members map[string]*Member
	self    *Member
	onChange func(member *Member, status MemberStatus)
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

func (ml *MemberList) Merge(incoming []*Member) {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	for _, m := range incoming {
		if m.ID == ml.self.ID {
			continue
		}
		existing, ok := ml.members[m.ID]
		if !ok || m.Heartbeat > existing.Heartbeat {
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