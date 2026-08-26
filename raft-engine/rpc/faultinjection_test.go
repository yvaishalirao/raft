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
		// Election timeout must stay comfortably larger than the (default
		// 50ms) heartbeat interval — NFR3 — or a follower's timer can fire
		// in the gap between heartbeats even under healthy conditions.
		wrapped := NewFaultInjectingTransport(router.Transport(id), state)
		nodes[i] = raft.NewNode(id, idsExcept(ids, id), wrapped,
			raft.WithElectionTimeout(150*time.Millisecond, 300*time.Millisecond),
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

type nodeSnapshot struct {
	term   int64
	role   raft.Role
	logLen int
}

func snapshotNode(n *raft.Node) nodeSnapshot {
	return nodeSnapshot{term: n.Term(), role: n.Role(), logLen: len(n.LogEntries())}
}

// newKillTestPair builds a caller and a real, running target node ("node-1")
// whose election timeout is set far beyond any of these tests' windows, so
// its own timer never fires and its state can only change via a successful
// external RPC — exactly what SetKilled is supposed to prevent.
func newKillTestPair(t *testing.T) (state *FaultState, caller *FaultInjectingTransport, target *raft.Node) {
	t.Helper()

	ids := []string{"node-0", "node-1"}
	router := NewInMemoryRouter(ids)
	state = NewFaultState()

	target = raft.NewNode("node-1", []string{"node-0"},
		NewFaultInjectingTransport(router.Transport("node-1"), state),
		raft.WithElectionTimeout(10*time.Second, 20*time.Second),
	)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go target.Run(ctx)

	caller = NewFaultInjectingTransport(router.Transport("node-0"), state)
	return state, caller, target
}

// TestFaultInjection_KilledNodeStaysDead sends 20 RPCs to a killed node
// over roughly 2 seconds (simulating retries) and asserts every single one
// fails.
func TestFaultInjection_KilledNodeStaysDead(t *testing.T) {
	state, caller, _ := newKillTestPair(t)
	state.SetKilled("node-1", true)

	failures := 0
	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		_, err := caller.Send(ctx, "node-1", "AppendEntries", raft.AppendEntriesArgs{Term: 5, LeaderID: "node-0"})
		cancel()
		if err != nil {
			failures++
		}
		time.Sleep(100 * time.Millisecond)
	}

	if failures != 20 {
		t.Fatalf("expected all 20 attempts against a killed node to fail, got %d/20 failures", failures)
	}
}

// TestFaultInjection_KilledNodeStateFrozen confirms a killed node's
// internal Raft state (term, role, log) never changes while repeatedly
// probed — the RPCs use a higher term than the target's own, so if the
// kill somehow failed to block delivery, the term would visibly change.
func TestFaultInjection_KilledNodeStateFrozen(t *testing.T) {
	state, caller, target := newKillTestPair(t)

	before := snapshotNode(target)
	state.SetKilled("node-1", true)

	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		_, _ = caller.Send(ctx, "node-1", "AppendEntries", raft.AppendEntriesArgs{Term: 5, LeaderID: "node-0"})
		cancel()
		time.Sleep(100 * time.Millisecond)
	}

	after := snapshotNode(target)
	if before != after {
		t.Fatalf("killed node's state changed during the kill window: before=%+v after=%+v", before, after)
	}
}

// TestFaultInjection_RestartRestoresResponse confirms an explicit
// SetKilled(id, false) — and only that — restores responsiveness.
func TestFaultInjection_RestartRestoresResponse(t *testing.T) {
	state, caller, _ := newKillTestPair(t)
	state.SetKilled("node-1", true)

	kctx, kcancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	_, err := caller.Send(kctx, "node-1", "AppendEntries", raft.AppendEntriesArgs{Term: 1, LeaderID: "node-0"})
	kcancel()
	if err == nil {
		t.Fatal("expected Send to a killed node to fail")
	}

	state.SetKilled("node-1", false) // explicit restart

	rctx, rcancel := context.WithTimeout(context.Background(), time.Second)
	defer rcancel()
	reply, err := caller.Send(rctx, "node-1", "AppendEntries", raft.AppendEntriesArgs{Term: 1, LeaderID: "node-0"})
	if err != nil {
		t.Fatalf("expected Send to succeed immediately after an explicit restart, got: %v", err)
	}
	if _, ok := reply.(raft.AppendEntriesReply); !ok {
		t.Fatalf("unexpected reply type: %T", reply)
	}
}
