// internal/gossip/transport_test.go
package gossip

import (
	"fmt"
	"net"
	"testing"
	"time"
)

func newTestTransport(t *testing.T) *Transport {
	t.Helper()
	transport, err := NewTransport("0")
	if err != nil {
		t.Fatalf("failed to create transport: %v", err)
	}
	t.Cleanup(func() { transport.Stop() })
	return transport
}

func TestNewTransport(t *testing.T) {
	// Arrange + Act
	transport := newTestTransport(t)

	// Assert
	if transport == nil {
		t.Fatal("expected transport to not be nil")
	}
}

func TestNewTransportInvalidPort(t *testing.T) {
	// Arrange + Act
	_, err := NewTransport("invalid")

	// Assert
	if err == nil {
		t.Fatal("expected error for invalid port")
	}
}

func TestTransportStartStop(t *testing.T) {
	// Arrange
	transport := newTestTransport(t)

	// Act + Assert — should not panic
	transport.Start()
	transport.Stop()
}

func TestTransportIncomingChannel(t *testing.T) {
	// Arrange
	transport := newTestTransport(t)

	// Act
	ch := transport.Incoming()

	// Assert
	if ch == nil {
		t.Fatal("expected incoming channel to not be nil")
	}
}

func TestTransportSendAndReceive(t *testing.T) {
	// Arrange
	receiver := newTestTransport(t)
	receiver.Start()

	port := receiver.conn.LocalAddr().(*net.UDPAddr).Port
	sender, err := NewTransport("0")
	if err != nil {
		t.Fatalf("failed to create sender: %v", err)
	}
	defer sender.Stop()

	msg := &GossipMessage{
		Type: MessagePushPull,
		From: "node1",
		Members: []*Member{
			{ID: "node1", Address: "10.0.0.1", Heartbeat: 1, UpdatedAt: time.Now(), Status: MemberAlive},
		},
	}

	// Act
	if err := sender.Send(fmt.Sprintf("127.0.0.1:%d", port), msg); err != nil {
		t.Fatalf("failed to send message: %v", err)
	}

	// Assert
	select {
	case received := <-receiver.Incoming():
		if received.From != "node1" {
			t.Errorf("expected from node1, got %s", received.From)
		}
		if received.Type != MessagePushPull {
			t.Errorf("expected PushPull, got %d", received.Type)
		}
		if len(received.Members) != 1 {
			t.Errorf("expected 1 member, got %d", len(received.Members))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestTransportSendInvalidAddress(t *testing.T) {
	// Arrange
	transport := newTestTransport(t)

	msg := &GossipMessage{
		Type:    MessagePush,
		From:    "node1",
		Members: []*Member{},
	}

	// Act
	err := transport.Send("invalid-address", msg)

	// Assert
	if err == nil {
		t.Fatal("expected error for invalid address")
	}
}

func TestTransportMessageTypes(t *testing.T) {
	// Arrange
	receiver := newTestTransport(t)
	receiver.Start()

	port := receiver.conn.LocalAddr().(*net.UDPAddr).Port
	sender, err := NewTransport("0")
	if err != nil {
		t.Fatalf("failed to create sender: %v", err)
	}
	defer sender.Stop()

	tests := []struct {
		msgType MessageType
	}{
		{MessagePush},
		{MessagePull},
		{MessagePushPull},
	}

	for _, tt := range tests {
		// Act
		msg := &GossipMessage{Type: tt.msgType, From: "node1", Members: []*Member{}}
		if err := sender.Send(fmt.Sprintf("127.0.0.1:%d", port), msg); err != nil {
			t.Fatalf("failed to send message: %v", err)
		}

		// Assert
		select {
		case received := <-receiver.Incoming():
			if received.Type != tt.msgType {
				t.Errorf("expected type %d, got %d", tt.msgType, received.Type)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for message type %d", tt.msgType)
		}
	}
}

func TestTransportDropsMessagesWhenFull(t *testing.T) {
	// Arrange
	transport := newTestTransport(t)
	transport.Start()

	port := transport.conn.LocalAddr().(*net.UDPAddr).Port
	sender, err := NewTransport("0")
	if err != nil {
		t.Fatalf("failed to create sender: %v", err)
	}
	defer sender.Stop()

	// Act — flood with 300 messages, channel only holds 256
	for i := 0; i < 300; i++ {
		msg := &GossipMessage{Type: MessagePush, From: "node1", Members: []*Member{}}
		if err := sender.Send(fmt.Sprintf("127.0.0.1:%d", port), msg); err != nil {
			t.Fatalf("error sending messages: %v", err)
		}
	}

	// Assert — should not panic or deadlock
	time.Sleep(100 * time.Millisecond)
}