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
