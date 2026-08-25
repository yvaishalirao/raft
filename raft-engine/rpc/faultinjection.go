package rpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"raft-engine/raft"
)

type faultKey struct {
	from, to string
}

// FaultState holds fault-injection configuration per (from,to) node pair —
// dropped, delay, and partitioned — all behind a mutex, mutated only
// through its exported setters. It is the single source of truth
// FaultInjectingTransport consults before ever calling the real transport.
type FaultState struct {
	mu          sync.Mutex
	dropped     map[faultKey]bool
	delay       map[faultKey]time.Duration
	partitioned map[faultKey]bool
}

func NewFaultState() *FaultState {
	return &FaultState{
		dropped:     make(map[faultKey]bool),
		delay:       make(map[faultKey]time.Duration),
		partitioned: make(map[faultKey]bool),
	}
}

func (s *FaultState) SetDrop(from, to string, dropped bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropped[faultKey{from, to}] = dropped
}

func (s *FaultState) SetDelay(from, to string, d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delay[faultKey{from, to}] = d
}

func (s *FaultState) SetPartition(from, to string, partitioned bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.partitioned[faultKey{from, to}] = partitioned
}

func (s *FaultState) isDropped(from, to string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dropped[faultKey{from, to}]
}

func (s *FaultState) delayFor(from, to string) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.delay[faultKey{from, to}]
}

func (s *FaultState) isPartitioned(from, to string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.partitioned[faultKey{from, to}]
}

// FaultInjectingTransport wraps any raft.Transport generically — it works
// unchanged over InMemoryTransport or GRPCTransport — consulting shared
// FaultState before delegating Send to the wrapped transport. A dropped or
// partitioned pair never reaches the inner transport at all; a delayed
// pair sleeps first. raft/ never sees this type or FaultState at all: from
// the core's point of view, a fault looks exactly like ordinary network
// unreliability.
type FaultInjectingTransport struct {
	inner  raft.Transport
	state  *FaultState
	selfID string
}

func NewFaultInjectingTransport(inner raft.Transport, state *FaultState) *FaultInjectingTransport {
	return &FaultInjectingTransport{inner: inner, state: state, selfID: inner.LocalID()}
}

func (t *FaultInjectingTransport) LocalID() string { return t.selfID }

func (t *FaultInjectingTransport) Send(ctx context.Context, target string, rpcType string, args any) (any, error) {
	if t.state.isDropped(t.selfID, target) || t.state.isPartitioned(t.selfID, target) {
		return nil, fmt.Errorf("faultinjection: message from %s to %s blocked", t.selfID, target)
	}

	if d := t.state.delayFor(t.selfID, target); d > 0 {
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return t.inner.Send(ctx, target, rpcType, args)
}

func (t *FaultInjectingTransport) Recv(ctx context.Context) (raft.RPC, bool) {
	return t.inner.Recv(ctx)
}
