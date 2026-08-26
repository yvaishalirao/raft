package control

import (
	"context"
	"net/http"
	"testing"
	"time"

	"raft-engine/raft"
	"raft-engine/rpc"
)

// TestCommands_Restart confirms /restart both resets the node's term/
// votedFor AND un-kills the transport — a restart is not just un-killing.
func TestCommands_Restart(t *testing.T) {
	ids := []string{"node-0", "node-1"}
	router := rpc.NewInMemoryRouter(ids)
	faultState := rpc.NewFaultState()

	node := raft.NewNode("node-0", []string{"node-1"},
		rpc.NewFaultInjectingTransport(router.Transport("node-0"), faultState),
		raft.WithElectionTimeout(10*time.Second, 20*time.Second),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go node.Run(ctx)

	node.HandleRequestVote(raft.RequestVoteArgs{Term: 7, CandidateID: "someone"})
	if node.Term() != 7 {
		t.Fatalf("test setup failed: expected term 7, got %d", node.Term())
	}
	faultState.SetKilled("node-0", true)

	ctrl := NewControlServer(faultState, "node-0", "127.0.0.1:0")
	ctrl.SetNode(node)
	cctx, ccancel := context.WithCancel(context.Background())
	defer ccancel()
	if err := ctrl.Start(cctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	status, resp := postJSON(t, "http://"+ctrl.Addr()+"/restart", nil)
	if status != http.StatusOK || resp["status"] != "ok" {
		t.Fatalf("/restart: status=%d body=%+v", status, resp)
	}

	if got := node.Term(); got != 0 {
		t.Fatalf("expected currentTerm 0 after restart, got %d", got)
	}

	caller := rpc.NewFaultInjectingTransport(router.Transport("node-1"), faultState)
	sctx, scancel := context.WithTimeout(context.Background(), time.Second)
	defer scancel()
	if _, err := caller.Send(sctx, "node-0", "AppendEntries", raft.AppendEntriesArgs{Term: 1, LeaderID: "node-1"}); err != nil {
		t.Fatalf("expected node-0 to respond after /restart un-killed it, got: %v", err)
	}
}

// TestCommands_Partition confirms /partition and /unpartition toggle
// partitioned state symmetrically for every listed peer, and leave
// unlisted peers unaffected.
func TestCommands_Partition(t *testing.T) {
	ids := []string{"node-0", "node-1", "node-2", "node-3"}
	router := rpc.NewInMemoryRouter(ids)
	faultState := rpc.NewFaultState()

	ctrl := NewControlServer(faultState, "node-0", "127.0.0.1:0")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := ctrl.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	base := "http://" + ctrl.Addr()

	status, resp := postJSON(t, base+"/partition", peersRequest{Peers: []string{"node-1", "node-2"}})
	if status != http.StatusOK || resp["status"] != "ok" {
		t.Fatalf("/partition: status=%d body=%+v", status, resp)
	}

	caller := rpc.NewFaultInjectingTransport(router.Transport("node-0"), faultState)

	for _, peer := range []string{"node-1", "node-2"} {
		sctx, scancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		_, err := caller.Send(sctx, peer, "AppendEntries", raft.AppendEntriesArgs{Term: 1})
		scancel()
		if err == nil {
			t.Fatalf("expected node-0<->%s to be partitioned", peer)
		}
	}

	nodeD := raft.NewNode("node-3", []string{"node-0"}, rpc.NewFaultInjectingTransport(router.Transport("node-3"), faultState))
	dctx, dcancel := context.WithCancel(context.Background())
	defer dcancel()
	go nodeD.Run(dctx)

	sctx, scancel := context.WithTimeout(context.Background(), time.Second)
	if _, err := caller.Send(sctx, "node-3", "AppendEntries", raft.AppendEntriesArgs{Term: 1, LeaderID: "node-0"}); err != nil {
		t.Fatalf("expected node-0<->node-3 unaffected by partitioning node-1/node-2, got: %v", err)
	}
	scancel()

	status, resp = postJSON(t, base+"/unpartition", peersRequest{Peers: []string{"node-1", "node-2"}})
	if status != http.StatusOK || resp["status"] != "ok" {
		t.Fatalf("/unpartition: status=%d body=%+v", status, resp)
	}

	nodeB := raft.NewNode("node-1", []string{"node-0"}, rpc.NewFaultInjectingTransport(router.Transport("node-1"), faultState))
	bctx, bcancel := context.WithCancel(context.Background())
	defer bcancel()
	go nodeB.Run(bctx)

	sctx2, scancel2 := context.WithTimeout(context.Background(), time.Second)
	defer scancel2()
	if _, err := caller.Send(sctx2, "node-1", "AppendEntries", raft.AppendEntriesArgs{Term: 1, LeaderID: "node-0"}); err != nil {
		t.Fatalf("expected node-0<->node-1 reachable after /unpartition, got: %v", err)
	}
}

// TestCommands_Latency confirms /latency delays only the specified
// (nodeID, peer) pair, leaving other pairs unaffected.
func TestCommands_Latency(t *testing.T) {
	ids := []string{"node-0", "node-1", "node-2"}
	router := rpc.NewInMemoryRouter(ids)
	faultState := rpc.NewFaultState()

	ctrl := NewControlServer(faultState, "node-0", "127.0.0.1:0")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := ctrl.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	status, resp := postJSON(t, "http://"+ctrl.Addr()+"/latency", latencyRequest{Peer: "node-1", MS: 100})
	if status != http.StatusOK || resp["status"] != "ok" {
		t.Fatalf("/latency: status=%d body=%+v", status, resp)
	}

	caller := rpc.NewFaultInjectingTransport(router.Transport("node-0"), faultState)

	nodeB := raft.NewNode("node-1", []string{"node-0"}, rpc.NewFaultInjectingTransport(router.Transport("node-1"), faultState))
	bctx, bcancel := context.WithCancel(context.Background())
	defer bcancel()
	go nodeB.Run(bctx)

	nodeC := raft.NewNode("node-2", []string{"node-0"}, rpc.NewFaultInjectingTransport(router.Transport("node-2"), faultState))
	cctx, ccancel := context.WithCancel(context.Background())
	defer ccancel()
	go nodeC.Run(cctx)

	start := time.Now()
	sctx, scancel := context.WithTimeout(context.Background(), time.Second)
	_, err := caller.Send(sctx, "node-1", "AppendEntries", raft.AppendEntriesArgs{Term: 1, LeaderID: "node-0"})
	scancel()
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Send to node-1 failed: %v", err)
	}
	if elapsed < 100*time.Millisecond {
		t.Fatalf("expected at least 100ms delay to node-1, got %v", elapsed)
	}

	start2 := time.Now()
	sctx2, scancel2 := context.WithTimeout(context.Background(), time.Second)
	_, err = caller.Send(sctx2, "node-2", "AppendEntries", raft.AppendEntriesArgs{Term: 1, LeaderID: "node-0"})
	scancel2()
	elapsed2 := time.Since(start2)
	if err != nil {
		t.Fatalf("Send to node-2 failed: %v", err)
	}
	if elapsed2 > 50*time.Millisecond {
		t.Fatalf("expected node-0<->node-2 unaffected by node-1's latency, took %v", elapsed2)
	}
}

// TestCommands_InvalidPeer confirms malformed input returns a structured
// JSON error with a 4xx status, never a panic.
func TestCommands_InvalidPeer(t *testing.T) {
	faultState := rpc.NewFaultState()
	ctrl := NewControlServer(faultState, "node-0", "127.0.0.1:0")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := ctrl.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	base := "http://" + ctrl.Addr()

	cases := []struct {
		name string
		path string
		body any
	}{
		{"empty peers list", "/partition", peersRequest{Peers: nil}},
		{"empty peer id", "/partition", peersRequest{Peers: []string{""}}},
		{"empty latency peer", "/latency", latencyRequest{Peer: "", MS: 10}},
		{"negative latency", "/latency", latencyRequest{Peer: "node-1", MS: -5}},
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("handler panicked on invalid input: %v", r)
		}
	}()

	for _, tc := range cases {
		status, resp := postJSON(t, base+tc.path, tc.body)
		if status < 400 || status >= 500 {
			t.Fatalf("%s: expected a 4xx status, got %d", tc.name, status)
		}
		if resp["status"] != "error" {
			t.Fatalf("%s: expected status=error in body, got %+v", tc.name, resp)
		}
	}
}
