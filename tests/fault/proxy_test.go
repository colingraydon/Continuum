//go:build fault

package fault

import (
	"io"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"
)

// tcpProxy sits in front of a node's HTTP listener. The node advertises the
// proxy address (SELF_ADDRESS) while binding its real port (HTTP_BIND_PORT),
// so every inter-node request flows through the proxy and can be faulted. The
// harness itself talks to the node's bind port directly and is never affected.
//
// Faults: Blackhole drops new connections on the floor (immediate close, which
// peers see as a reset) and severs established ones; SetLatency delays every
// forwarded chunk, simulating a slow link.
type tcpProxy struct {
	ln     net.Listener
	target string

	mu        sync.Mutex
	blackhole bool
	latency   time.Duration
	conns     map[net.Conn]struct{}
	closed    bool
}

func newTCPProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("tcp proxy listen: %v", err)
	}
	p := &tcpProxy{ln: ln, target: target, conns: make(map[net.Conn]struct{})}
	go p.acceptLoop()
	t.Cleanup(p.Close)
	return p
}

func (p *tcpProxy) Addr() string { return p.ln.Addr().String() }

func (p *tcpProxy) acceptLoop() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		if p.blackhole || p.closed {
			p.mu.Unlock()
			_ = c.Close()
			continue
		}
		p.conns[c] = struct{}{}
		p.mu.Unlock()
		go p.forward(c)
	}
}

func (p *tcpProxy) forward(client net.Conn) {
	defer p.dropConn(client)
	upstream, err := net.Dial("tcp", p.target)
	if err != nil {
		return // node down: closing the client conn signals the failure
	}
	p.mu.Lock()
	if p.blackhole || p.closed {
		p.mu.Unlock()
		_ = upstream.Close()
		return
	}
	p.conns[upstream] = struct{}{}
	p.mu.Unlock()
	defer p.dropConn(upstream)

	done := make(chan struct{}, 2)
	go func() { p.copyChunks(upstream, client); done <- struct{}{} }()
	go func() { p.copyChunks(client, upstream); done <- struct{}{} }()
	<-done // either direction closing tears down both via the deferred closes
}

// copyChunks is io.Copy with an optional per-chunk delay so latency can be
// injected mid-connection.
func (p *tcpProxy) copyChunks(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			p.mu.Lock()
			delay := p.latency
			p.mu.Unlock()
			if delay > 0 {
				time.Sleep(delay)
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *tcpProxy) dropConn(c net.Conn) {
	p.mu.Lock()
	delete(p.conns, c)
	p.mu.Unlock()
	_ = c.Close()
}

// Blackhole starts refusing new connections and severs established ones.
func (p *tcpProxy) Blackhole() {
	p.mu.Lock()
	p.blackhole = true
	conns := make([]net.Conn, 0, len(p.conns))
	for c := range p.conns {
		conns = append(conns, c)
	}
	p.conns = make(map[net.Conn]struct{})
	p.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

// Heal resumes normal forwarding.
func (p *tcpProxy) Heal() {
	p.mu.Lock()
	p.blackhole = false
	p.mu.Unlock()
}

// SetLatency delays every forwarded chunk by d. Zero disables the delay.
func (p *tcpProxy) SetLatency(d time.Duration) {
	p.mu.Lock()
	p.latency = d
	p.mu.Unlock()
}

func (p *tcpProxy) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	conns := make([]net.Conn, 0, len(p.conns))
	for c := range p.conns {
		conns = append(conns, c)
	}
	p.conns = make(map[net.Conn]struct{})
	p.mu.Unlock()
	_ = p.ln.Close()
	for _, c := range conns {
		_ = c.Close()
	}
}

// udpProxy sits in front of a node's gossip listener. The node advertises the
// proxy address (GOSSIP_ADVERTISE_ADDR) while binding its real port
// (GOSSIP_PORT), so every inbound gossip datagram flows through the proxy.
// Gossip datagrams are one-way fire-and-forget, so forwarding is one-way too.
//
// Faults: Blackhole drops everything; SetDropPermille drops a random fraction
// of datagrams, simulating a lossy network.
type udpProxy struct {
	conn   *net.UDPConn
	target *net.UDPAddr

	mu          sync.Mutex
	blackhole   bool
	dropPermill int
	rng         *rand.Rand
	closed      bool
}

func newUDPProxy(t *testing.T, target string) *udpProxy {
	t.Helper()
	laddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("udp proxy resolve: %v", err)
	}
	conn, err := net.ListenUDP("udp", laddr)
	if err != nil {
		t.Fatalf("udp proxy listen: %v", err)
	}
	taddr, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		t.Fatalf("udp proxy target: %v", err)
	}
	p := &udpProxy{conn: conn, target: taddr, rng: rand.New(rand.NewSource(time.Now().UnixNano()))}
	go p.forwardLoop()
	t.Cleanup(p.Close)
	return p
}

func (p *udpProxy) Addr() string { return p.conn.LocalAddr().String() }

func (p *udpProxy) forwardLoop() {
	buf := make([]byte, 64<<10)
	for {
		n, _, err := p.conn.ReadFromUDP(buf)
		if err != nil {
			if err == io.EOF {
				return
			}
			p.mu.Lock()
			closed := p.closed
			p.mu.Unlock()
			if closed {
				return
			}
			continue
		}
		p.mu.Lock()
		drop := p.blackhole || (p.dropPermill > 0 && p.rng.Intn(1000) < p.dropPermill)
		p.mu.Unlock()
		if drop {
			continue
		}
		_, _ = p.conn.WriteToUDP(buf[:n], p.target)
	}
}

func (p *udpProxy) Blackhole() {
	p.mu.Lock()
	p.blackhole = true
	p.mu.Unlock()
}

func (p *udpProxy) Heal() {
	p.mu.Lock()
	p.blackhole = false
	p.dropPermill = 0
	p.mu.Unlock()
}

// SetDropPermille drops n out of every 1000 datagrams at random.
func (p *udpProxy) SetDropPermille(n int) {
	p.mu.Lock()
	p.dropPermill = n
	p.mu.Unlock()
}

func (p *udpProxy) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.mu.Unlock()
	_ = p.conn.Close()
}
