package kv

import (
	"context"
	"encoding/json"
	"fmt"
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

// buildKVCluster builds an n-node in-memory cluster with a kv.Store wired
// to each node, and starts every node running. Per-node cancels are
// returned (not just one aggregate) so a test can kill a single node
// while the rest of the cluster keeps running.
func buildKVCluster(t *testing.T, n int) (stores []*Store, nodes []*raft.Node, cancels []context.CancelFunc) {
	t.Helper()

	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("node-%d", i)
	}
	router := rpc.NewInMemoryRouter(ids)

	for _, id := range ids {
		// Election timeout must stay comfortably larger than the (default
		// 50ms) heartbeat interval, or a follower's timer can fire in the
		// gap between heartbeats even under healthy conditions, causing
		// spurious re-elections that make "the leader" a moving target
		// mid-test.
		node := raft.NewNode(id, idsExcept(ids, id), router.Transport(id),
			raft.WithElectionTimeout(150*time.Millisecond, 300*time.Millisecond),
		)
		store := NewStore(node)
		nodes = append(nodes, node)
		stores = append(stores, store)

		ctx, c := context.WithCancel(context.Background())
		cancels = append(cancels, c)
		go node.Run(ctx)
	}

	return stores, nodes, cancels
}

func cancelAll(cancels []context.CancelFunc) {
	for _, c := range cancels {
		c()
	}
}

func waitForLeader(t *testing.T, nodes []*raft.Node, timeout time.Duration) *raft.Node {
	t.Helper()
	return waitForLeaderExcluding(t, nodes, "", timeout)
}

func waitForLeaderExcluding(t *testing.T, nodes []*raft.Node, excludeID string, timeout time.Duration) *raft.Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.ID() != excludeID && n.Role() == raft.Leader {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no leader elected within %v (excluding %q)", timeout, excludeID)
	return nil
}

func storeFor(stores []*Store, nodes []*raft.Node, target *raft.Node) *Store {
	for i, n := range nodes {
		if n == target {
			return stores[i]
		}
	}
	return nil
}

func indexOf(nodes []*raft.Node, target *raft.Node) int {
	for i, n := range nodes {
		if n == target {
			return i
		}
	}
	return -1
}

// waitForValue polls Get until it returns want, or fails the test after
// timeout. A commit (which HandleSet blocks on) and its later application
// via the apply loop's own separate ticker are two distinct steps — a
// caller must never assume synchronous visibility right after HandleSet
// returns.
func waitForValue(t *testing.T, s *Store, key, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if v, ok := s.Get(key); ok && v == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, ok := s.Get(key)
	t.Fatalf("timed out waiting for Get(%q)==%q, got value=%q ok=%v", key, want, got, ok)
}

func TestStore_SetThenGet(t *testing.T) {
	stores, nodes, cancels := buildKVCluster(t, 1)
	defer cancelAll(cancels)

	leader := waitForLeader(t, nodes, 2*time.Second)
	leaderStore := storeFor(stores, nodes, leader)

	if err := leaderStore.HandleSet("x", "1"); err != nil {
		t.Fatalf("HandleSet failed: %v", err)
	}
	waitForValue(t, leaderStore, "x", "1", time.Second)
}

func TestStore_GetMissingKey(t *testing.T) {
	node := raft.NewNode("node-0", nil, nil)
	store := NewStore(node)

	if _, ok := store.Get("nope"); ok {
		t.Fatal("expected ok=false for a never-set key")
	}
}

// TestStore_ApplyAfterCommitOnly confirms a write is never visible via GET
// on any node until it is genuinely committed. An entry appended by a
// leader that then immediately crashes before replicating it is lost (the
// correct, expected Raft behavior), not silently resurrected — only a
// later, actually-committed write from the successor leader may ever make
// the key visible.
func TestStore_ApplyAfterCommitOnly(t *testing.T) {
	stores, nodes, cancels := buildKVCluster(t, 5)
	defer cancelAll(cancels)

	leader := waitForLeader(t, nodes, 2*time.Second)
	leaderIdx := indexOf(nodes, leader)

	payload, err := json.Marshal(command{Op: "SET", Key: "x", Value: "1"})
	if err != nil {
		t.Fatalf("encode command: %v", err)
	}
	if _, err := leader.ProposeAsync(payload); err != nil {
		t.Fatalf("ProposeAsync failed: %v", err)
	}

	// Crash only the leader, before this entry can reach a majority — it
	// is now permanently unreplicated and unrecoverable.
	cancels[leaderIdx]()

	deadline500 := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline500) {
		for i, s := range stores {
			if i == leaderIdx {
				continue
			}
			if _, ok := s.Get("x"); ok {
				t.Fatalf("key became visible on node-%d within 500ms of an uncommitted crash", i)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	// A new leader emerges among the survivors and legitimately commits
	// the same key — only this write may make it visible.
	newLeader := waitForLeaderExcluding(t, nodes, leader.ID(), 3*time.Second)
	newLeaderStore := storeFor(stores, nodes, newLeader)

	if err := newLeaderStore.HandleSet("x", "1"); err != nil {
		t.Fatalf("new leader failed to SET x: %v", err)
	}

	deadline3s := time.Now().Add(3 * time.Second)
	visible := false
	for time.Now().Before(deadline3s) {
		if v, ok := newLeaderStore.Get("x"); ok && v == "1" {
			visible = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !visible {
		t.Fatal("expected x to become visible once the new leader actually committed it")
	}
}
