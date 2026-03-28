package gossip

import (
	"encoding/json"
	"fmt"
	"net"
	"time"
)

const maxUDPPacketSize = 65536

type MessageType int

const (
	MessagePush MessageType = iota
	MessagePull
	MessagePushPull
)

type GossipMessage struct {
	Type    MessageType `json:"type"`
	From    string      `json:"from"`
	Members []*Member   `json:"members"`
}

type Transport struct {
	conn     *net.UDPConn
	port     string
	incoming chan *GossipMessage
}

func NewTransport(port string) (*Transport, error) {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%s", port))
	if err != nil {
		return nil, fmt.Errorf("failed to resolve UDP address: %w", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on UDP port %s: %w", port, err)
	}

	return &Transport{
		conn:     conn,
		port:     port,
		incoming: make(chan *GossipMessage, 256),
	}, nil
}

func (t *Transport) Start() {
	go t.listen()
}

func (t *Transport) Stop() {
	t.conn.Close()
}

func (t *Transport) listen() {
	buf := make([]byte, maxUDPPacketSize)
	for {
		n, _, err := t.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}

		var msg GossipMessage
		if err := json.Unmarshal(buf[:n], &msg); err != nil {
			continue
		}

		select {
		case t.incoming <- &msg:
		default:
			// drop message if channel is full
		}
	}
}

func (t *Transport) Send(address string, msg *GossipMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal gossip message: %w", err)
	}

	if len(data) > maxUDPPacketSize {
		return fmt.Errorf("message too large for UDP packet: %d bytes", len(data))
	}

	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return fmt.Errorf("failed to resolve peer address: %w", err)
	}

	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return fmt.Errorf("failed to dial peer: %w", err)
	}
	defer conn.Close()

	if err := conn.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return fmt.Errorf("failed to set write deadline: %w", err)
	}

	_, err = conn.Write(data)
	return err
}

func (t *Transport) Incoming() <-chan *GossipMessage {
	return t.incoming
}