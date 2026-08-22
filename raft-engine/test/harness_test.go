package test

import (
	"testing"

	"raft-engine/raft"
)

func TestHarnessBoot(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.Shutdown()

	if len(c.Nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(c.Nodes))
	}
}

func TestHarnessBoot_AllFollower(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.Shutdown()

	for _, n := range c.Nodes {
		if n.Role() != raft.Follower {
			t.Fatalf("expected node to start as Follower, got %v", n.Role())
		}
	}
}

func TestHarnessShutdown(t *testing.T) {
	c := NewCluster(t, 3)
	c.Shutdown()
	c.Shutdown()
}
