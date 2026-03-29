package gossip

import (
	"context"
	"fmt"
	"log"
	"math/rand"
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
		addr := fmt.Sprintf("%s:%s", peer.Address, g.gossipPort)
		if err := g.transport.Send(addr, msg); err != nil {
			log.Printf("failed to gossip to peer %s: %v", peer.ID, err)
		}
	}
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
	case MessagePush, MessagePushPull:
		g.memberList.Merge(msg.Members)
	}
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
			if m.Status == MemberAlive {
				g.memberList.MarkSuspect(m.ID)
			} else if m.Status == MemberSuspect {
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

func (g *Gossiper) Bootstrap(seedNodes []string) {
	members := g.memberList.GetAll()
	msg := &GossipMessage{
		Type:    MessagePushPull,
		From:    g.selfID,
		Members: members,
	}

	for _, addr := range seedNodes {
		gossipAddr := fmt.Sprintf("%s:%s", addr, g.gossipPort)
		if err := g.transport.Send(gossipAddr, msg); err != nil {
			log.Printf("failed to bootstrap from seed %s: %v", addr, err)
		}
	}
}