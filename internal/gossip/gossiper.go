package gossip

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"
)

const (
	fanout         = 3
	gossipInterval = 1 * time.Second
	staleThreshold = 5 * time.Second
)

type Gossiper struct {
	memberList *MemberList
	transport  *Transport
	selfID     string
	gossipPort string
}

func NewGossiper(selfID string, gossipPort string, ml *MemberList, t *Transport) *Gossiper {
	return &Gossiper{
		memberList: ml,
		transport:  t,
		selfID:     selfID,
		gossipPort: gossipPort,
	}
}

func (g *Gossiper) Start(ctx context.Context) {
	g.transport.Start()
	go g.gossipLoop(ctx)
	go g.receiveLoop(ctx)
	go g.staleLoop(ctx)
}

func (g *Gossiper) Stop() {
	g.transport.Stop()
}

func (g *Gossiper) gossipLoop(ctx context.Context) {
	ticker := time.NewTicker(gossipInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			g.gossipRound()
		case <-ctx.Done():
			return
		}
	}
}

func (g *Gossiper) gossipRound() {
	g.memberList.IncrementHeartbeat()

	peers := g.selectPeers()
	if len(peers) == 0 {
		return
	}

	members := g.memberList.GetAll()
	msg := &GossipMessage{
		Type:    MessagePushPull,
		From:    g.selfID,
		Members: members,
	}

	for _, peer := range peers {
		if err := g.transport.Send(g.peerGossipAddr(peer), msg); err != nil {
			log.Printf("failed to gossip to peer %s: %v", peer.ID, err)
		}
	}
}

// peerGossipAddr returns the UDP address a gossip datagram for m should be
// sent to: the member's advertised gossip address when known, otherwise the
// member's host on this node's own gossip port (the legacy assumption that
// every node shares one gossip port).
func (g *Gossiper) peerGossipAddr(m *Member) string {
	if m.GossipAddr != "" {
		return m.GossipAddr
	}
	return fmt.Sprintf("%s:%s", gossipHost(m.Address), g.gossipPort)
}

func (g *Gossiper) receiveLoop(ctx context.Context) {
	for {
		select {
		case msg := <-g.transport.Incoming():
			g.handleMessage(msg)
		case <-ctx.Done():
			return
		}
	}
}

func (g *Gossiper) handleMessage(msg *GossipMessage) {
	switch msg.Type {
	case MessagePushPull:
		g.memberList.Merge(msg.Members)
		if msg.WantReply {
			g.replyTo(msg)
		}
	}
}

// replyTo answers a WantReply message (a bootstrap) by pushing our full member
// list back to the sender. The reply itself carries WantReply=false, so it
// never triggers a reply of its own — a bootstrap is at most two datagrams per
// seed. The sender needs this because peers that marked it dead have stopped
// gossiping to it; the reply is how it relearns the cluster and sees the stale
// entry it must refute.
func (g *Gossiper) replyTo(msg *GossipMessage) {
	addr := g.senderGossipAddr(msg)
	if addr == "" {
		return
	}
	reply := &GossipMessage{
		Type:    MessagePushPull,
		From:    g.selfID,
		Members: g.memberList.GetAll(),
	}
	if err := g.transport.Send(addr, reply); err != nil {
		log.Printf("failed to reply to bootstrap from %s: %v", msg.From, err)
	}
}

// senderGossipAddr resolves the UDP address to reply to. The sender knows its
// own gossip address best, so prefer its self-entry in the message; fall back
// to whatever we have recorded for it, and give up if we know neither.
func (g *Gossiper) senderGossipAddr(msg *GossipMessage) string {
	for _, m := range msg.Members {
		if m.ID == msg.From {
			return g.peerGossipAddr(m)
		}
	}
	if m, ok := g.memberList.Get(msg.From); ok {
		return g.peerGossipAddr(m)
	}
	return ""
}

func (g *Gossiper) staleLoop(ctx context.Context) {
	ticker := time.NewTicker(gossipInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			g.checkStale()
		case <-ctx.Done():
			return
		}
	}
}

func (g *Gossiper) checkStale() {
	members := g.memberList.GetAll()
	for _, m := range members {
		if m.ID == g.selfID {
			continue
		}
		if m.Status == MemberDead {
			continue
		}
		if time.Since(m.UpdatedAt) > staleThreshold {
			switch m.Status {
			case MemberAlive:
				g.memberList.MarkSuspect(m.ID)
			case MemberSuspect:
				g.memberList.MarkDead(m.ID)
			}
		}
	}
}

func (g *Gossiper) selectPeers() []*Member {
	alive := g.memberList.GetAlive()
	peers := make([]*Member, 0)
	for _, m := range alive {
		if m.ID != g.selfID {
			peers = append(peers, m)
		}
	}

	// shuffle and take up to fanout
	rand.Shuffle(len(peers), func(i, j int) {
		peers[i], peers[j] = peers[j], peers[i]
	})

	if len(peers) > fanout {
		return peers[:fanout]
	}
	return peers
}

// NotifyDead marks self as dead in the member list and broadcasts the updated
// state to all currently alive peers. Called during graceful shutdown so peers
// stop routing traffic to this node immediately rather than waiting for the
// stale threshold.
func (g *Gossiper) NotifyDead() {
	g.memberList.MarkDead(g.selfID)

	peers := g.memberList.GetAlive()
	members := g.memberList.GetAll()
	msg := &GossipMessage{
		Type:    MessagePushPull,
		From:    g.selfID,
		Members: members,
	}

	for _, peer := range peers {
		if err := g.transport.Send(g.peerGossipAddr(peer), msg); err != nil {
			log.Printf("shutdown: failed to notify peer %s: %v", peer.ID, err)
		}
	}
}

func (g *Gossiper) Bootstrap(seedNodes []string) {
	members := g.memberList.GetAll()
	msg := &GossipMessage{
		Type:      MessagePushPull,
		From:      g.selfID,
		WantReply: true,
		Members:   members,
	}

	for _, addr := range seedNodes {
		gossipAddr := fmt.Sprintf("%s:%s", gossipHost(addr), g.gossipPort)
		if err := g.transport.Send(gossipAddr, msg); err != nil {
			log.Printf("failed to bootstrap from seed %s: %v", addr, err)
		}
	}
}

// gossipHost extracts the hostname from an address that may or may not include a port.
func gossipHost(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return address
	}
	return host
}
