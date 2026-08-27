package viz

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"raft-engine/raft"
	"raft-engine/rpc"
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

// buildVizCluster wires sink to every node before starting it, so an
// election that completes right after Run begins can never be missed.
func buildVizCluster(t *testing.T, n int, sink func(raft.Event)) (nodes []*raft.Node, cancels []context.CancelFunc) {
	t.Helper()

	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("node-%d", i)
	}
	router := rpc.NewInMemoryRouter(ids)

	for _, id := range ids {
		node := raft.NewNode(id, idsExcept(ids, id), router.Transport(id),
			raft.WithElectionTimeout(150*time.Millisecond, 300*time.Millisecond),
		)
		if sink != nil {
			node.SetEventSink(sink)
		}
		nodes = append(nodes, node)

		ctx, c := context.WithCancel(context.Background())
		cancels = append(cancels, c)
		go node.Run(ctx)
	}

	return nodes, cancels
}

func cancelAll(cancels []context.CancelFunc) {
	for _, c := range cancels {
		c()
	}
}

func waitForLeader(t *testing.T, nodes []*raft.Node, timeout time.Duration) *raft.Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.Role() == raft.Leader {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no leader elected within %v", timeout)
	return nil
}

func TestEvents_EmittedOnRoleChange(t *testing.T) {
	var mu sync.Mutex
	var events []raft.Event
	sink := func(ev raft.Event) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	nodes, cancels := buildVizCluster(t, 3, sink)
	defer cancelAll(cancels)

	waitForLeader(t, nodes, 5*time.Second)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		for _, ev := range events {
			if ev.Type == raft.RoleChange && ev.Role == "Leader" {
				mu.Unlock()
				return
			}
		}
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no RoleChange event with Role==Leader received")
}

func TestEvents_EmittedOnLogAppend(t *testing.T) {
	var mu sync.Mutex
	appendCount := 0
	sink := func(ev raft.Event) {
		if ev.Type == raft.LogAppend {
			mu.Lock()
			appendCount++
			mu.Unlock()
		}
	}

	nodes, cancels := buildVizCluster(t, 3, sink)
	defer cancelAll(cancels)

	leader := waitForLeader(t, nodes, 5*time.Second)

	const proposals = 3
	for i := 0; i < proposals; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := leader.Propose(ctx, []byte(fmt.Sprintf("cmd-%d", i)))
		cancel()
		if err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		count := appendCount
		mu.Unlock()
		if count >= proposals {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected at least %d LogAppend events, got %d", proposals, appendCount)
}

func TestEvents_EmittedOnCommit(t *testing.T) {
	var mu sync.Mutex
	var commitEvents []raft.Event
	sink := func(ev raft.Event) {
		if ev.Type == raft.Commit {
			mu.Lock()
			commitEvents = append(commitEvents, ev)
			mu.Unlock()
		}
	}

	nodes, cancels := buildVizCluster(t, 3, sink)
	defer cancelAll(cancels)

	leader := waitForLeader(t, nodes, 5*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_, err := leader.Propose(ctx, []byte("cmd"))
	cancel()
	if err != nil {
		t.Fatalf("propose: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		var last raft.Event
		n := len(commitEvents)
		if n > 0 {
			last = commitEvents[n-1]
		}
		mu.Unlock()
		if n > 0 && last.CommitIndex == leader.CommitIndex() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("commit event CommitIndex never matched node's actual commitIndex (%d)", leader.CommitIndex())
}
