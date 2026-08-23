package raft

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"
)

const (
	DefaultElectionTimeoutMin = 150 * time.Millisecond
	DefaultElectionTimeoutMax = 300 * time.Millisecond
	DefaultHeartbeatInterval  = 50 * time.Millisecond
)

var (
	errUnexpectedRPCArgs = errors.New("raft: unexpected RPC argument type")
	errUnknownRPCType    = errors.New("raft: unknown RPC type")
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
	heartbeatInterval  time.Duration
	rng                *rand.Rand

	// resetElectionSignal lets other goroutines (RPC handlers) ask the
	// runElectionTimer goroutine to restart its timer, without ever touching
	// the *time.Timer itself from outside its owning goroutine — sharing a
	// single Timer's channel across goroutines is a well-known Go race.
	resetElectionSignal chan struct{}

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
		id:                  id,
		peers:               peers,
		transport:           transport,
		role:                Follower,
		electionTimeoutMin:  DefaultElectionTimeoutMin,
		electionTimeoutMax:  DefaultElectionTimeoutMax,
		heartbeatInterval:   DefaultHeartbeatInterval,
		rng:                 rand.New(rand.NewSource(time.Now().UnixNano())),
		resetElectionSignal: make(chan struct{}, 1),
	}
	for _, opt := range opts {
		opt(n)
	}
	return n
}

// Run starts the election timer and then dispatches every inbound RPC to
// its handler until ctx is done.
func (n *Node) Run(ctx context.Context) error {
	go n.runElectionTimer(ctx)

	for {
		rpc, ok := n.transport.Recv(ctx)
		if !ok {
			return nil
		}

		switch rpc.Type {
		case "RequestVote":
			args, ok := rpc.Args.(RequestVoteArgs)
			if !ok {
				rpc.Reply(nil, errUnexpectedRPCArgs)
				continue
			}
			rpc.Reply(n.HandleRequestVote(args), nil)

		case "AppendEntries":
			args, ok := rpc.Args.(AppendEntriesArgs)
			if !ok {
				rpc.Reply(nil, errUnexpectedRPCArgs)
				continue
			}
			rpc.Reply(n.HandleAppendEntries(args), nil)

		default:
			rpc.Reply(nil, errUnknownRPCType)
		}
	}
}

// Role returns the node's current role. Safe for concurrent use.
func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// Term returns the node's current term. Safe for concurrent use.
func (n *Node) Term() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

// ID returns the node's own ID.
func (n *Node) ID() string {
	return n.id
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
