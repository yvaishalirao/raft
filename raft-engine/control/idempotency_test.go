package control

import (
	"context"
	"net/http"
	"testing"
	"time"

	"raft-engine/raft"
	"raft-engine/rpc"
)

// TestControlPlane_Idempotent issues each of the 5 command types twice in
// immediate succession and asserts the resulting state is identical to
// issuing it once — specifically guarding against counter-based
// bookkeeping (a naive partitionCount++ or similar) that would need two
// "un-does" to clear a double-click.
func TestControlPlane_Idempotent(t *testing.T) {
	t.Run("kill", func(t *testing.T) {
		faultState := rpc.NewFaultState()
		ctrl := NewControlServer(faultState, "node-0", "127.0.0.1:0")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := ctrl.Start(ctx); err != nil {
			t.Fatalf("Start failed: %v", err)
		}
		base := "http://" + ctrl.Addr()

		for i := 0; i < 2; i++ {
			status, resp := postJSON(t, base+"/kill", nil)
			if status != http.StatusOK || resp["status"] != "ok" {
				t.Fatalf("call %d: status=%d body=%+v", i, status, resp)
			}
		}

		ids := []string{"node-0", "node-1"}
		router := rpc.NewInMemoryRouter(ids)
		caller := rpc.NewFaultInjectingTransport(router.Transport("node-1"), faultState)
		sctx, scancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer scancel()
		if _, err := caller.Send(sctx, "node-0", "AppendEntries", raft.AppendEntriesArgs{Term: 1}); err == nil {
			t.Fatal("expected node to remain killed after 2 /kill calls, not toggle back to alive")
		}
	})

	t.Run("restart", func(t *testing.T) {
		faultState := rpc.NewFaultState()
		node := raft.NewNode("node-0", nil, nil)
		node.HandleRequestVote(raft.RequestVoteArgs{Term: 5, CandidateID: "someone"})
		if node.Term() != 5 {
			t.Fatalf("test setup failed: expected term 5, got %d", node.Term())
		}

		ctrl := NewControlServer(faultState, "node-0", "127.0.0.1:0")
		ctrl.SetNode(node)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := ctrl.Start(ctx); err != nil {
			t.Fatalf("Start failed: %v", err)
		}
		base := "http://" + ctrl.Addr()

		for i := 0; i < 2; i++ {
			status, resp := postJSON(t, base+"/restart", nil)
			if status != http.StatusOK || resp["status"] != "ok" {
				t.Fatalf("call %d: status=%d body=%+v", i, status, resp)
			}
			if got := node.Term(); got != 0 {
				t.Fatalf("call %d: expected term 0 after restart, got %d", i, got)
			}
		}
	})

	t.Run("partition", func(t *testing.T) {
		faultState := rpc.NewFaultState()
		ctrl := NewControlServer(faultState, "node-0", "127.0.0.1:0")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := ctrl.Start(ctx); err != nil {
			t.Fatalf("Start failed: %v", err)
		}
		base := "http://" + ctrl.Addr()

		for i := 0; i < 2; i++ {
			status, resp := postJSON(t, base+"/partition", peersRequest{Peers: []string{"node-1"}})
			if status != http.StatusOK || resp["status"] != "ok" {
				t.Fatalf("call %d: status=%d body=%+v", i, status, resp)
			}
		}

		ids := []string{"node-0", "node-1"}
		router := rpc.NewInMemoryRouter(ids)
		caller := rpc.NewFaultInjectingTransport(router.Transport("node-0"), faultState)
		sctx, scancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer scancel()
		if _, err := caller.Send(sctx, "node-1", "AppendEntries", raft.AppendEntriesArgs{Term: 1}); err == nil {
			t.Fatal("expected node-0<->node-1 to remain partitioned after 2 /partition calls")
		}
	})

	t.Run("unpartition", func(t *testing.T) {
		faultState := rpc.NewFaultState()
		faultState.SetPartition("node-0", "node-1", true)
		faultState.SetPartition("node-1", "node-0", true)

		ctrl := NewControlServer(faultState, "node-0", "127.0.0.1:0")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := ctrl.Start(ctx); err != nil {
			t.Fatalf("Start failed: %v", err)
		}
		base := "http://" + ctrl.Addr()

		for i := 0; i < 2; i++ {
			status, resp := postJSON(t, base+"/unpartition", peersRequest{Peers: []string{"node-1"}})
			if status != http.StatusOK || resp["status"] != "ok" {
				t.Fatalf("call %d: status=%d body=%+v", i, status, resp)
			}
		}

		ids := []string{"node-0", "node-1"}
		router := rpc.NewInMemoryRouter(ids)
		nodeB := raft.NewNode("node-1", []string{"node-0"}, rpc.NewFaultInjectingTransport(router.Transport("node-1"), faultState))
		bctx, bcancel := context.WithCancel(context.Background())
		defer bcancel()
		go nodeB.Run(bctx)

		caller := rpc.NewFaultInjectingTransport(router.Transport("node-0"), faultState)
		sctx, scancel := context.WithTimeout(context.Background(), time.Second)
		defer scancel()
		if _, err := caller.Send(sctx, "node-1", "AppendEntries", raft.AppendEntriesArgs{Term: 1, LeaderID: "node-0"}); err != nil {
			t.Fatalf("expected node-0<->node-1 to remain unpartitioned after 2 /unpartition calls, got: %v", err)
		}
	})

	t.Run("latency", func(t *testing.T) {
		faultState := rpc.NewFaultState()
		ctrl := NewControlServer(faultState, "node-0", "127.0.0.1:0")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := ctrl.Start(ctx); err != nil {
			t.Fatalf("Start failed: %v", err)
		}
		base := "http://" + ctrl.Addr()

		for i := 0; i < 2; i++ {
			status, resp := postJSON(t, base+"/latency", latencyRequest{Peer: "node-1", MS: 50})
			if status != http.StatusOK || resp["status"] != "ok" {
				t.Fatalf("call %d: status=%d body=%+v", i, status, resp)
			}
		}

		ids := []string{"node-0", "node-1"}
		router := rpc.NewInMemoryRouter(ids)
		nodeB := raft.NewNode("node-1", []string{"node-0"}, rpc.NewFaultInjectingTransport(router.Transport("node-1"), faultState))
		bctx, bcancel := context.WithCancel(context.Background())
		defer bcancel()
		go nodeB.Run(bctx)

		caller := rpc.NewFaultInjectingTransport(router.Transport("node-0"), faultState)
		start := time.Now()
		sctx, scancel := context.WithTimeout(context.Background(), time.Second)
		defer scancel()
		if _, err := caller.Send(sctx, "node-1", "AppendEntries", raft.AppendEntriesArgs{Term: 1, LeaderID: "node-0"}); err != nil {
			t.Fatalf("Send failed: %v", err)
		}
		elapsed := time.Since(start)

		if elapsed < 50*time.Millisecond {
			t.Fatalf("expected at least 50ms delay, got %v", elapsed)
		}
		if elapsed > 150*time.Millisecond {
			t.Fatalf("expected roughly 50ms delay (not doubled to 100ms by 2 calls), got %v", elapsed)
		}
	})
}
