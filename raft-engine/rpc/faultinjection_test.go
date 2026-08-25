package rpc

import (
	"context"
	"testing"
	"time"

	"raft-engine/raft"
)

func idsExcept(ids []string, self string) []string {
	peers := make([]string, 0, len(ids)-1)
	for _, id := range ids {
		if id != self {
			peers = append(peers, id)
		}
	}
	return peers
}

func containsID(ids []string, target string) bool {
	for _, id := range ids {
		if id == target {
			return true
		}
	}
	return false
}

// buildFaultInjectedCluster wires an N-node in-memory cluster where every
// node's transport is wrapped in FaultInjectingTransport over the shared
// state, and starts each node running. Callers must call the returned
// cancel func when done.
func buildFaultInjectedCluster(ids []string, state *FaultState) (nodes []*raft.Node, cancel func()) {
	router := NewInMemoryRouter(ids)
	nodes = make([]*raft.Node, len(ids))
	var cancels []context.CancelFunc

	for i, id := range ids {
		wrapped := NewFaultInjectingTransport(router.Transport(id), state)
		nodes[i] = raft.NewNode(id, idsExcept(ids, id), wrapped,
			raft.WithElectionTimeout(50*time.Millisecond, 100*time.Millisecond),
		)
		ctx, c := context.WithCancel(context.Background())
		cancels = append(cancels, c)
		go nodes[i].Run(ctx)
	}

	return nodes, func() {
		for _, c := range cancels {
			c()
		}
	}
}

// TestFaultInjection_NoFaultsEquivalent confirms that with no faults
// active, a cluster wrapped in FaultInjectingTransport behaves exactly like
// one using the unwrapped transport directly: it elects a leader and
// commits, same as any InMemoryTransport-only cluster.
func TestFaultInjection_NoFaultsEquivalent(t *testing.T) {
	ids := []string{"node-0", "node-1", "node-2"}
	state := NewFaultState() // no faults set

	nodes, cancel := buildFaultInjectedCluster(ids, state)
	defer cancel()

	deadline := time.Now().Add(2 * time.Second)
	var leader *raft.Node
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.Role() == raft.Leader {
				leader = n
				break
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if leader == nil {
		t.Fatal("no leader elected through an unfaulted FaultInjectingTransport — should be identical to the unwrapped transport")
	}

	wctx, wcancel := context.WithTimeout(context.Background(), time.Second)
	defer wcancel()
	if _, err := leader.Propose(wctx, []byte("x")); err != nil {
		t.Fatalf("commit failed through an unfaulted FaultInjectingTransport: %v", err)
	}
}

// TestFaultInjection_PartitionMatchesDropRate confirms raft/'s behavior is
// indistinguishable whether a minority is cut off via SetPartition or via
// an equivalent SetDrop on every pair — from the core's point of view a
// partition is just another shape of message loss. This is a supplementary
// behavioral check; TestIsolation_NoFaultInjectionReferences (the static
// grep) is the primary guarantee per INVARIANTS.md.
func TestFaultInjection_PartitionMatchesDropRate(t *testing.T) {
	scenarios := []struct {
		name  string
		apply func(state *FaultState, a, b string)
	}{
		{"partition", func(state *FaultState, a, b string) {
			state.SetPartition(a, b, true)
		}},
		{"equivalent_drop_rate", func(state *FaultState, a, b string) {
			state.SetDrop(a, b, true)
		}},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			ids := []string{"node-0", "node-1", "node-2", "node-3", "node-4"}
			minority := ids[:2]
			majority := ids[2:]

			state := NewFaultState()
			for _, a := range minority {
				for _, b := range majority {
					scenario.apply(state, a, b)
					scenario.apply(state, b, a)
				}
			}

			nodes, cancel := buildFaultInjectedCluster(ids, state)
			defer cancel()

			deadline := time.Now().Add(1 * time.Second)
			for time.Now().Before(deadline) {
				for _, n := range nodes {
					if containsID(minority, n.ID()) && n.Role() == raft.Leader {
						t.Fatalf("%s: minority node %s became Leader — partition and equivalent drop rate must behave the same", scenario.name, n.ID())
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
		})
	}
}
