package test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"raft-engine/raft"
)

// TestStateMachineSafety_ExtendedRandomizedRun drives a 5-node cluster
// through a randomized mix of writes, crashes (of the leader or a random
// non-leader), and elections. Each node's own apply loop independently
// records what it applied into a shared, mutex-protected map. Afterward,
// for every index more than one node applied, the command must be
// identical across all of them — the actual customer-visible failure every
// lower invariant in this project exists to prevent.
func TestStateMachineSafety_ExtendedRandomizedRun(t *testing.T) {
	var mu sync.Mutex
	applied := make(map[string]map[int64][]byte) // nodeID -> index -> command

	c := NewCluster(t, 5,
		WithApplyObserver(func(nodeID string, e raft.LogEntry) {
			mu.Lock()
			defer mu.Unlock()
			if applied[nodeID] == nil {
				applied[nodeID] = make(map[int64][]byte)
			}
			applied[nodeID][e.Index] = append([]byte(nil), e.Command...)
		}),
	)
	defer c.Shutdown()

	rng := rand.New(rand.NewSource(1))

	for i := 0; i < 50; i++ {
		switch rng.Intn(3) {
		case 0: // write
			if leader := anyLeader(c); leader != nil {
				wctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				_, _ = leader.Propose(wctx, []byte(fmt.Sprintf("cmd-%d", i)))
				cancel()
			}
		case 1: // crash a random non-leader, then bring it back
			leader := anyLeader(c)
			if target := randomNodeExcluding(c, rng, leaderIDOrEmpty(leader)); target != "" {
				c.KillNode(target)
				time.Sleep(30 * time.Millisecond)
				c.Revive(target)
			}
		case 2: // crash the leader itself, then bring it back
			if leader := anyLeader(c); leader != nil {
				id := leader.ID()
				c.KillNode(id)
				time.Sleep(30 * time.Millisecond)
				c.Revive(id)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Let the cluster settle and finish replicating/applying before checking.
	time.Sleep(1 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	byIndex := make(map[int64]map[string][]byte)
	for nodeID, entries := range applied {
		for idx, cmd := range entries {
			if byIndex[idx] == nil {
				byIndex[idx] = make(map[string][]byte)
			}
			byIndex[idx][nodeID] = cmd
		}
	}

	for idx, byNode := range byIndex {
		if len(byNode) < 2 {
			continue
		}
		var firstNode string
		var first []byte
		for nodeID, cmd := range byNode {
			if first == nil {
				firstNode, first = nodeID, cmd
				continue
			}
			if string(cmd) != string(first) {
				t.Fatalf("state machine safety violated at index %d: node %s applied %q, node %s applied %q",
					idx, firstNode, first, nodeID, cmd)
			}
		}
	}
}

func anyLeader(c *Cluster) *raft.Node {
	for _, n := range c.Nodes {
		if n.Role() == raft.Leader {
			return n
		}
	}
	return nil
}

func leaderIDOrEmpty(leader *raft.Node) string {
	if leader == nil {
		return ""
	}
	return leader.ID()
}

func randomNodeExcluding(c *Cluster, rng *rand.Rand, excludeID string) string {
	candidates := make([]string, 0, len(c.Nodes))
	for _, n := range c.Nodes {
		if n.ID() != excludeID {
			candidates = append(candidates, n.ID())
		}
	}
	if len(candidates) == 0 {
		return ""
	}
	return candidates[rng.Intn(len(candidates))]
}
