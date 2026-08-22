package raft

import "testing"

func TestNewNode_StartsFollower(t *testing.T) {
	n := NewNode("node-0", []string{"node-1", "node-2"}, nil)
	if n.role != Follower {
		t.Fatalf("expected role Follower, got %v", n.role)
	}
}

func TestNewNode_TermZero(t *testing.T) {
	n := NewNode("node-0", []string{"node-1", "node-2"}, nil)
	if n.currentTerm != 0 {
		t.Fatalf("expected currentTerm 0, got %d", n.currentTerm)
	}
}
