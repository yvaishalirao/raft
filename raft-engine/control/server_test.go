package control

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
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

// postJSON POSTs body (or no body, if nil) as JSON to url and returns the
// status code and decoded JSON response body.
func postJSON(t *testing.T, url string, body any) (statusCode int, parsed map[string]any) {
	t.Helper()

	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("failed to encode request body: %v", err)
		}
	}

	resp, err := http.Post(url, "application/json", &buf)
	if err != nil {
		t.Fatalf("POST %s failed: %v", url, err)
	}
	defer resp.Body.Close()

	parsed = map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&parsed)
	return resp.StatusCode, parsed
}

// TestControlPlane_SeparateListener confirms the control HTTP server and
// the Raft gRPC server for the same node are bound to distinct
// listeners/sockets, and that concurrent traffic against both is race-free
// (run under go test -race) — neither shares a lock with the other.
func TestControlPlane_SeparateListener(t *testing.T) {
	node := raft.NewNode("node-0", nil, nil)
	grpcSrv, grpcLis, err := rpc.NewGRPCServer(node, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewGRPCServer failed: %v", err)
	}
	defer grpcSrv.Stop()
	go grpcSrv.Serve(grpcLis)

	faultState := rpc.NewFaultState()
	ctrl := NewControlServer(faultState, "node-0", "127.0.0.1:0")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := ctrl.Start(ctx); err != nil {
		t.Fatalf("ControlServer.Start failed: %v", err)
	}

	if grpcLis.Addr().String() == ctrl.Addr() {
		t.Fatalf("expected distinct listeners, both bound to %s", ctrl.Addr())
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			_ = node.Role()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			postJSON(t, "http://"+ctrl.Addr()+"/kill", nil)
			postJSON(t, "http://"+ctrl.Addr()+"/restart", nil)
		}
	}()
	wg.Wait()
}

// TestControlPlane_KillEndpoint confirms POST /kill actually applies
// rpc.FaultState.SetKilled — verified behaviorally, since FaultState
// exposes no getter.
func TestControlPlane_KillEndpoint(t *testing.T) {
	ids := []string{"node-0", "node-1"}
	router := rpc.NewInMemoryRouter(ids)
	faultState := rpc.NewFaultState()

	caller := rpc.NewFaultInjectingTransport(router.Transport("node-0"), faultState)

	ctrl := NewControlServer(faultState, "node-1", "127.0.0.1:0")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := ctrl.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	status, resp := postJSON(t, "http://"+ctrl.Addr()+"/kill", nil)
	if status != http.StatusOK || resp["status"] != "ok" {
		t.Fatalf("unexpected /kill response: status=%d body=%+v", status, resp)
	}

	sctx, scancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer scancel()
	if _, err := caller.Send(sctx, "node-1", "AppendEntries", raft.AppendEntriesArgs{Term: 1}); err == nil {
		t.Fatal("expected Send to a killed node to fail after POST /kill")
	}
}

// TestControlPlane_ReachableDuringPartition is the literal Risk-4 test:
// with the target fully cut off from its Raft peers on the data plane, all
// 5 control commands must still succeed against it in a single run.
func TestControlPlane_ReachableDuringPartition(t *testing.T) {
	ids := []string{"node-0", "node-1", "node-2", "node-3", "node-4"}
	router := rpc.NewInMemoryRouter(ids)
	faultState := rpc.NewFaultState()
	target := "node-0"

	var cancels []context.CancelFunc
	ctrls := make(map[string]*ControlServer, len(ids))
	for _, id := range ids {
		wrapped := rpc.NewFaultInjectingTransport(router.Transport(id), faultState)
		n := raft.NewNode(id, idsExcept(ids, id), wrapped,
			raft.WithElectionTimeout(10*time.Second, 20*time.Second),
		)

		ctrl := NewControlServer(faultState, id, "127.0.0.1:0")
		ctrl.SetNode(n)
		ctx, cancel := context.WithCancel(context.Background())
		cancels = append(cancels, cancel)
		if err := ctrl.Start(ctx); err != nil {
			t.Fatalf("Start failed for %s: %v", id, err)
		}
		ctrls[id] = ctrl

		go n.Run(ctx)
	}
	defer func() {
		for _, c := range cancels {
			c()
		}
	}()

	for _, id := range ids {
		if id != target {
			faultState.SetPartition(target, id, true)
			faultState.SetPartition(id, target, true)
		}
	}

	base := "http://" + ctrls[target].Addr()
	calls := []struct {
		name string
		path string
		body any
	}{
		{"kill", "/kill", nil},
		{"restart", "/restart", nil},
		{"partition", "/partition", peersRequest{Peers: []string{"node-1"}}},
		{"unpartition", "/unpartition", peersRequest{Peers: idsExcept(ids, target)}},
		{"latency", "/latency", latencyRequest{Peer: "node-1", MS: 5}},
	}

	for _, call := range calls {
		status, resp := postJSON(t, base+call.path, call.body)
		if status != http.StatusOK || resp["status"] != "ok" {
			t.Fatalf("%s while target was data-plane-partitioned: status=%d body=%+v", call.name, status, resp)
		}
	}
}

// TestControlPlane_UnpartitionWhilePartitioned confirms /unpartition,
// issued against a currently-partitioned node, actually restores Raft
// connectivity — not just returns status:'ok'.
func TestControlPlane_UnpartitionWhilePartitioned(t *testing.T) {
	ids := []string{"node-0", "node-1"}
	router := rpc.NewInMemoryRouter(ids)
	faultState := rpc.NewFaultState()

	var cancels []context.CancelFunc
	for _, id := range ids {
		wrapped := rpc.NewFaultInjectingTransport(router.Transport(id), faultState)
		n := raft.NewNode(id, idsExcept(ids, id), wrapped,
			raft.WithElectionTimeout(10*time.Second, 20*time.Second),
		)
		ctx, cancel := context.WithCancel(context.Background())
		cancels = append(cancels, cancel)
		go n.Run(ctx)
	}
	defer func() {
		for _, c := range cancels {
			c()
		}
	}()

	ctrl := NewControlServer(faultState, "node-1", "127.0.0.1:0")
	cctx, ccancel := context.WithCancel(context.Background())
	defer ccancel()
	if err := ctrl.Start(cctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	faultState.SetPartition("node-0", "node-1", true)
	faultState.SetPartition("node-1", "node-0", true)

	caller := rpc.NewFaultInjectingTransport(router.Transport("node-0"), faultState)

	sctx1, scancel1 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	_, err := caller.Send(sctx1, "node-1", "AppendEntries", raft.AppendEntriesArgs{Term: 1, LeaderID: "node-0"})
	scancel1()
	if err == nil {
		t.Fatal("test setup sanity check failed: expected Send to fail while partitioned")
	}

	status, resp := postJSON(t, "http://"+ctrl.Addr()+"/unpartition", peersRequest{Peers: []string{"node-0"}})
	if status != http.StatusOK || resp["status"] != "ok" {
		t.Fatalf("/unpartition while partitioned: status=%d body=%+v", status, resp)
	}

	sctx2, scancel2 := context.WithTimeout(context.Background(), time.Second)
	defer scancel2()
	reply, err := caller.Send(sctx2, "node-1", "AppendEntries", raft.AppendEntriesArgs{Term: 1, LeaderID: "node-0"})
	if err != nil {
		t.Fatalf("expected Send to succeed after /unpartition, got: %v", err)
	}
	if _, ok := reply.(raft.AppendEntriesReply); !ok {
		t.Fatalf("unexpected reply type: %T", reply)
	}
}
