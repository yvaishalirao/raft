package rpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"raft-engine/raft"
)

type rpcEnvelope struct {
	From    string
	RPCType string
	Args    any
	Reply   chan rpcResult
}

type rpcResult struct {
	Reply any
	Err   error
}

type routeKey struct {
	from, to string
}

type InMemoryRouter struct {
	mu       sync.Mutex
	channels map[string]chan rpcEnvelope
	drops    map[routeKey]bool
	delays   map[routeKey]time.Duration
}

func NewInMemoryRouter(nodeIDs []string) *InMemoryRouter {
	r := &InMemoryRouter{
		channels: make(map[string]chan rpcEnvelope),
		drops:    make(map[routeKey]bool),
		delays:   make(map[routeKey]time.Duration),
	}
	for _, id := range nodeIDs {
		r.channels[id] = make(chan rpcEnvelope)
	}
	return r
}

func (r *InMemoryRouter) SetDrop(from, to string, drop bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.drops[routeKey{from, to}] = drop
}

func (r *InMemoryRouter) SetDelay(from, to string, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.delays[routeKey{from, to}] = d
}

func (r *InMemoryRouter) ShouldDrop(from, to string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.drops[routeKey{from, to}]
}

func (r *InMemoryRouter) DelayFor(from, to string) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.delays[routeKey{from, to}]
}

// Inbox returns the channel a test can read from to observe messages
// addressed to nodeID, standing in for that node's not-yet-implemented handler.
func (r *InMemoryRouter) Inbox(nodeID string) chan rpcEnvelope {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.channels[nodeID]
}

func (r *InMemoryRouter) Transport(nodeID string) raft.Transport {
	return &InMemoryTransport{router: r, nodeID: nodeID}
}

type InMemoryTransport struct {
	router *InMemoryRouter
	nodeID string
}

func (t *InMemoryTransport) LocalID() string { return t.nodeID }

func (t *InMemoryTransport) Send(ctx context.Context, target string, rpcType string, args any) (any, error) {
	if t.router.ShouldDrop(t.nodeID, target) {
		return nil, fmt.Errorf("message from %s to %s dropped", t.nodeID, target)
	}

	if delay := t.router.DelayFor(t.nodeID, target); delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	t.router.mu.Lock()
	ch, ok := t.router.channels[target]
	t.router.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("unknown target %q", target)
	}

	replyCh := make(chan rpcResult, 1)
	env := rpcEnvelope{From: t.nodeID, RPCType: rpcType, Args: args, Reply: replyCh}

	select {
	case ch <- env:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case res := <-replyCh:
		return res.Reply, res.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
