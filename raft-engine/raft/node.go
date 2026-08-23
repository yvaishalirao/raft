package raft

import (
	"context"
	"sync"
	"time"
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

	electionTimeout time.Duration
	onRoleChange    func(Role, int64)
}

func NewNode(id string, peers []string, transport Transport) *Node {
	return &Node{
		id:        id,
		peers:     peers,
		transport: transport,
		role:      Follower,
	}
}

// Run is a stub background loop; it becomes the election/heartbeat loop in Session 3.
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
