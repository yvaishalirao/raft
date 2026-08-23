package raft

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

const (
	DefaultElectionTimeoutMin = 150 * time.Millisecond
	DefaultElectionTimeoutMax = 300 * time.Millisecond
	DefaultHeartbeatInterval  = 50 * time.Millisecond
)

type Node struct {
	mu sync.Mutex

	id          string
	currentTerm int64
	votedFor    string
	role        Role
	log         []LogEntry
	commitIndex int64
	lastApplied int64
	termHistory []int64

	peers     []string
	transport Transport

	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
	electionTimer      *time.Timer
	heartbeatInterval  time.Duration
	rng                *rand.Rand

	onRoleChange func(Role, int64)
}

// NodeOption configures optional Node behavior at construction time.
type NodeOption func(*Node)

// WithElectionTimeout overrides the default randomized election timeout range.
func WithElectionTimeout(min, max time.Duration) NodeOption {
	return func(n *Node) {
		n.electionTimeoutMin = min
		n.electionTimeoutMax = max
	}
}

// WithHeartbeatInterval overrides the default leader heartbeat interval.
func WithHeartbeatInterval(d time.Duration) NodeOption {
	return func(n *Node) {
		n.heartbeatInterval = d
	}
}

// WithRandSource overrides the source of randomness backing election-timeout
// jitter, enabling deterministic tests.
func WithRandSource(src rand.Source) NodeOption {
	return func(n *Node) {
		n.rng = rand.New(src)
	}
}

// WithOnRoleChange registers a callback invoked whenever the node's role
// changes, carrying the new role and the term at which it changed.
func WithOnRoleChange(fn func(Role, int64)) NodeOption {
	return func(n *Node) {
		n.onRoleChange = fn
	}
}

func NewNode(id string, peers []string, transport Transport, opts ...NodeOption) *Node {
	n := &Node{
		id:                 id,
		peers:              peers,
		transport:          transport,
		role:               Follower,
		electionTimeoutMin: DefaultElectionTimeoutMin,
		electionTimeoutMax: DefaultElectionTimeoutMax,
		heartbeatInterval:  DefaultHeartbeatInterval,
		rng:                rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	for _, opt := range opts {
		opt(n)
	}
	return n
}

// Run is a stub background loop; it becomes the election/heartbeat/dispatch
// loop once the in-memory harness wires real peer communication (Session 2
// task 3.5).
func (n *Node) Run(ctx context.Context) error {
	return nil
}

// Role returns the node's current role. Safe for concurrent use.
func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// mutateLog is the only sanctioned path for mutating n.log. It panics if a
// leader attempts any operation other than "append" — leaders must never
// rewrite or truncate their own log (Invariant I-02).
func (n *Node) mutateLog(op string, fn func()) {
	if n.role == Leader && op != "append" {
		panic("illegal leader log mutation: " + op)
	}
	fn()
}

// appendAsLeader is the only exposed leader-log-mutation path.
func (n *Node) appendAsLeader(e LogEntry) {
	n.mutateLog("append", func() {
		n.log = append(n.log, e)
	})
}

// truncateFrom is used only during follower conflict resolution; the guard
// ensures it can never run while n.role==Leader.
func (n *Node) truncateFrom(index int64) {
	n.mutateLog("truncate", func() {
		for i, e := range n.log {
			if e.Index == index {
				n.log = n.log[:i]
				return
			}
		}
	})
}
