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
)

func (s MemberStatus) String() string {
	switch s {
	case MemberAlive:
		return "alive"
	case MemberSuspect:
		return "suspect"
	case MemberDead:
		return "dead"
	default:
		return "unknown"
	}
}

type Member struct {
	ID        string
	Address   string
	Heartbeat uint64
	UpdatedAt time.Time
	Status    MemberStatus
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
	}
	ml := &MemberList{
		members:  make(map[string]*Member),
		self:     self,
		onChange: onChange,
	}
	ml.members[selfID] = self
	return ml
}

func (ml *MemberList) IncrementHeartbeat() {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.self.Heartbeat++
	ml.self.UpdatedAt = time.Now()
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
			previous := MemberAlive
			if ok {
				previous = existing.Status
			}
			ml.members[m.ID] = m
			if !ok || previous != m.Status {
				if ml.onChange != nil {
					ml.onChange(m, m.Status)
				}
			}
		}
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
	ml.mu.Lock()
	m := &Member{
		ID:        id,
		Address:   address,
		Heartbeat: 0,
		UpdatedAt: time.Now(),
		Status:    MemberAlive,
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