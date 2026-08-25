package rpc

import (
	"context"
	"net"
	"testing"
	"time"

	"raft-engine/raft"
)

func TestGRPC_ServerStarts(t *testing.T) {
	node := raft.NewNode("node-0", nil, nil)
	srv, lis, err := NewGRPCServer(node, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewGRPCServer failed: %v", err)
	}
	defer srv.Stop()
	go srv.Serve(lis)

	conn, err := net.DialTimeout("tcp", lis.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect to the running server: %v", err)
	}
	conn.Close()
}

func TestGRPC_RequestVoteRoundTrip(t *testing.T) {
	nodeB := raft.NewNode("node-b", []string{"node-a"}, nil)

	srvB, lisB, err := NewGRPCServer(nodeB, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewGRPCServer failed: %v", err)
	}
	defer srvB.Stop()
	go srvB.Serve(lisB)

	transportA := NewGRPCTransport("node-a", map[string]string{"node-b": lisB.Addr().String()})
	defer transportA.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	reply, err := transportA.Send(ctx, "node-b", "RequestVote", raft.RequestVoteArgs{
		Term:        1,
		CandidateID: "node-a",
	})
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	rv, ok := reply.(raft.RequestVoteReply)
	if !ok {
		t.Fatalf("unexpected reply type: %T", reply)
	}
	if !rv.VoteGranted {
		t.Fatalf("expected the vote to be granted, got %+v", rv)
	}
}

func TestGRPC_AppendEntriesRoundTrip(t *testing.T) {
	nodeB := raft.NewNode("node-b", []string{"node-a"}, nil)

	srvB, lisB, err := NewGRPCServer(nodeB, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewGRPCServer failed: %v", err)
	}
	defer srvB.Stop()
	go srvB.Serve(lisB)

	transportA := NewGRPCTransport("node-a", map[string]string{"node-b": lisB.Addr().String()})
	defer transportA.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	entries := []raft.LogEntry{
		{Index: 1, Term: 1, Command: []byte("SET x 1")},
		{Index: 2, Term: 1, Command: []byte("SET y 2")},
	}
	reply, err := transportA.Send(ctx, "node-b", "AppendEntries", raft.AppendEntriesArgs{
		Term:     1,
		LeaderID: "node-a",
		Entries:  entries,
	})
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	ar, ok := reply.(raft.AppendEntriesReply)
	if !ok || !ar.Success {
		t.Fatalf("expected a successful AppendEntriesReply, got %+v (type ok=%v)", reply, ok)
	}

	got := nodeB.LogEntries()
	if len(got) != len(entries) {
		t.Fatalf("expected %d entries replicated, got %d", len(entries), len(got))
	}
	for i, want := range entries {
		if got[i].Index != want.Index || got[i].Term != want.Term || string(got[i].Command) != string(want.Command) {
			t.Fatalf("entry %d mismatch: got %+v, want %+v (entries must survive byte-for-byte)", i, got[i], want)
		}
	}
}

func TestGRPC_TimeoutReturnsError(t *testing.T) {
	// Reserve a port, then immediately close it so nothing is listening —
	// a guaranteed-unreachable but syntactically valid address.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to reserve a port: %v", err)
	}
	deadAddr := lis.Addr().String()
	lis.Close()

	transport := NewGRPCTransport("node-a", map[string]string{"node-b": deadAddr})
	transport.CallTimeout = 500 * time.Millisecond
	defer transport.Close()

	start := time.Now()
	_, err = transport.Send(context.Background(), "node-b", "RequestVote", raft.RequestVoteArgs{Term: 1})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error calling an unreachable peer, got nil")
	}
	if elapsed > transport.CallTimeout+500*time.Millisecond {
		t.Fatalf("expected Send to return within CallTimeout+buffer (%v), took %v", transport.CallTimeout+500*time.Millisecond, elapsed)
	}
}

func TestGRPC_ConnectionReuse(t *testing.T) {
	nodeB := raft.NewNode("node-b", []string{"node-a"}, nil)
	srvB, lisB, err := NewGRPCServer(nodeB, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewGRPCServer failed: %v", err)
	}
	defer srvB.Stop()
	go srvB.Serve(lisB)

	transportA := NewGRPCTransport("node-a", map[string]string{"node-b": lisB.Addr().String()})
	defer transportA.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for i := 0; i < 2; i++ {
		if _, err := transportA.Send(ctx, "node-b", "RequestVote", raft.RequestVoteArgs{Term: 1}); err != nil {
			t.Fatalf("call %d failed: %v", i, err)
		}
	}

	transportA.mu.Lock()
	connCount := len(transportA.conns)
	transportA.mu.Unlock()

	if connCount != 1 {
		t.Fatalf("expected exactly 1 cached connection after 2 calls to the same peer, got %d", connCount)
	}
}

func TestGRPC_NeverPanicsOnDeadConn(t *testing.T) {
	nodeB := raft.NewNode("node-b", []string{"node-a"}, nil)
	srvB, lisB, err := NewGRPCServer(nodeB, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewGRPCServer failed: %v", err)
	}
	addr := lisB.Addr().String()
	go srvB.Serve(lisB)
	srvB.Stop() // kill it immediately, before any call is made

	transportA := NewGRPCTransport("node-a", map[string]string{"node-b": addr})
	transportA.CallTimeout = 300 * time.Millisecond
	defer transportA.Close()

	for i := 0; i < 5; i++ {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("call %d panicked: %v", i, r)
				}
			}()
			_, _ = transportA.Send(context.Background(), "node-b", "RequestVote", raft.RequestVoteArgs{Term: 1})
		}()
	}
}
