package health

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

type NodeStatus int

const (
	StatusHealthy  NodeStatus = iota
	StatusSuspect
	StatusDead
)

func (s NodeStatus) String() string {
	switch s {
	case StatusHealthy:
		return "healthy"
	case StatusSuspect:
		return "suspect"
	case StatusDead:
		return "dead"
	default:
		return "unknown"
	}
}

type NodeState struct {
	ID               string
	Address          string
	Status           NodeStatus
	ConsecutiveFails int
}

type Config struct {
	Interval         time.Duration
	FailureThreshold int
	HTTPTimeout      time.Duration
}

func DefaultConfig() Config {
	return Config{
		Interval:         5 * time.Second,
		FailureThreshold: 3,
		HTTPTimeout:      2 * time.Second,
	}
}

type OnStatusChange func(nodeID string, status NodeStatus)

type Checker struct {
	mu       sync.RWMutex
	nodes    map[string]*NodeState
	config   Config
	client   *http.Client
	onChange OnStatusChange
	cancel   context.CancelFunc
}

func NewChecker(config Config, onChange OnStatusChange) *Checker {
	return &Checker{
		nodes:    make(map[string]*NodeState),
		config:   config,
		client:   &http.Client{Timeout: config.HTTPTimeout},
		onChange: onChange,
	}
}

func (c *Checker) AddNode(id, address string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodes[id] = &NodeState{
		ID:      id,
		Address: address,
		Status:  StatusHealthy,
	}
}

func (c *Checker) RemoveNode(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.nodes, id)
}

func (c *Checker) GetStatus(id string) (NodeStatus, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	node, exists := c.nodes[id]
	if !exists {
		return StatusDead, false
	}
	return node.Status, true
}

func (c *Checker) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	go func() {
		ticker := time.NewTicker(c.config.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c.checkAll()
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (c *Checker) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
}

func (c *Checker) checkAll() {
	c.mu.RLock()
	ids := make([]string, 0, len(c.nodes))
	for id := range c.nodes {
		ids = append(ids, id)
	}
	c.mu.RUnlock()

	for _, id := range ids {
		c.checkNode(id)
	}
}

func (c *Checker) checkNode(id string) {
	c.mu.RLock()
	node, exists := c.nodes[id]
	if !exists {
		c.mu.RUnlock()
		return
	}
	address := node.Address
	c.mu.RUnlock()

	url := fmt.Sprintf("http://%s/health", address)
	resp, err := c.client.Get(url)
	if err != nil {
		c.recordFailure(id)
		return
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("failed to close response body: %v", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		c.recordFailure(id)
		return
	}

	c.recordSuccess(id)
}

func (c *Checker) recordSuccess(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, exists := c.nodes[id]
	if !exists {
		return
	}

	previous := node.Status
	node.ConsecutiveFails = 0
	node.Status = StatusHealthy

	if previous != StatusHealthy && c.onChange != nil {
		c.onChange(id, StatusHealthy)
	}
}

func (c *Checker) recordFailure(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, exists := c.nodes[id]
	if !exists {
		return
	}

	node.ConsecutiveFails++
	previous := node.Status

	if node.ConsecutiveFails >= c.config.FailureThreshold {
		node.Status = StatusDead
	} else {
		node.Status = StatusSuspect
	}

	if previous != node.Status && c.onChange != nil {
		c.onChange(id, node.Status)
	}
}