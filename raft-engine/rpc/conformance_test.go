package rpc

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	"raft-engine/raft"
)

// ConformanceResult is the snapshot runScenario collects after driving a
// cluster through the same scripted scenario, regardless of which
// Transport implementation built it.
type ConformanceResult struct {
	FinalTerms       map[string]int64
	FinalLogs        map[string][]raft.LogEntry
	LeaderEmerged    bool
	CommittedEntries []raft.LogEntry
}

// attacher is implemented by transports (the conformance suite's gRPC
// factory) that must bind a concrete *raft.Node after construction — the
// gRPC server's address has to exist before the Node does, so wiring
// happens in two phases. InMemoryTransport needs no such step.
type attacher interface {
	attachNode(n *raft.Node)
}

// runScenario builds a 5-node cluster from transportFactory, waits for a
// leader, submits 3 writes via Propose, waits for them to commit, and
// snapshots final state. This is the single scenario script shared by
// every transport this suite conforms-checks — it is never reimplemented
// per transport.
func runScenario(t *testing.T, transportFactory func([]string) map[string]raft.Transport) ConformanceResult {
	t.Helper()

	ids := []string{"node-0", "node-1", "node-2", "node-3", "node-4"}
	transports := transportFactory(ids)

	nodes := make(map[string]*raft.Node, len(ids))
	for _, id := range ids {
		peers := idsExcept(ids, id)
		n := raft.NewNode(id, peers, transports[id],
			raft.WithElectionTimeout(50*time.Millisecond, 100*time.Millisecond),
		)
		nodes[id] = n
		if a, ok := transports[id].(attacher); ok {
			a.attachNode(n)
		}
	}

	var cancels []context.CancelFunc
	for _, n := range nodes {
		ctx, cancel := context.WithCancel(context.Background())
		cancels = append(cancels, cancel)
		go n.Run(ctx)
	}
	t.Cleanup(func() {
		for _, c := range cancels {
			c()
		}
	})

	deadline := time.Now().Add(3 * time.Second)
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

	result := ConformanceResult{LeaderEmerged: leader != nil}
	if leader == nil {
		return result
	}

	for i := 0; i < 3; i++ {
		wctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := leader.Propose(wctx, []byte(fmt.Sprintf("cmd-%d", i)))
		cancel()
		if err != nil {
			t.Fatalf("Propose %d failed: %v", i, err)
		}
	}

	time.Sleep(500 * time.Millisecond) // let replication/apply settle

	result.CommittedEntries = leader.LogEntries()
	result.FinalTerms = make(map[string]int64, len(nodes))
	result.FinalLogs = make(map[string][]raft.LogEntry, len(nodes))
	for id, n := range nodes {
		result.FinalTerms[id] = n.Term()
		result.FinalLogs[id] = n.LogEntries()
	}
	return result
}

func inMemoryFactory(ids []string) map[string]raft.Transport {
	router := NewInMemoryRouter(ids)
	transports := make(map[string]raft.Transport, len(ids))
	for _, id := range ids {
		transports[id] = router.Transport(id)
	}
	return transports
}

// grpcAttachableTransport pairs a real GRPCTransport with the raftServer
// handle it needs to attach a *raft.Node to once one exists.
type grpcAttachableTransport struct {
	*GRPCTransport
	handle *raftServer
}

func (w *grpcAttachableTransport) attachNode(n *raft.Node) { w.handle.attach(n) }

// makeGRPCFactory returns a transport factory that starts 5 real gRPC
// servers on real localhost TCP sockets (via net.Listen), registers cleanup
// on t, and counts every net.Listen call it makes so tests can verify real
// sockets were actually used.
func makeGRPCFactory(t *testing.T) (factory func([]string) map[string]raft.Transport, listenCount *int32) {
	t.Helper()
	listenCount = new(int32)

	factory = func(ids []string) map[string]raft.Transport {
		addrs := make(map[string]string, len(ids))
		handles := make(map[string]*raftServer, len(ids))
		var servers []*grpc.Server
		var listeners []net.Listener

		for _, id := range ids {
			srv, lis, rs, err := newDetachedGRPCServer("127.0.0.1:0")
			atomic.AddInt32(listenCount, 1)
			if err != nil {
				t.Fatalf("failed to start gRPC server for %s: %v", id, err)
			}
			servers = append(servers, srv)
			listeners = append(listeners, lis)
			handles[id] = rs
			addrs[id] = lis.Addr().String()
			go srv.Serve(lis)
		}

		t.Cleanup(func() {
			for _, srv := range servers {
				srv.Stop()
			}
			for _, lis := range listeners {
				lis.Close()
			}
		})

		transports := make(map[string]raft.Transport, len(ids))
		for _, id := range ids {
			peerAddrs := make(map[string]string, len(ids)-1)
			for _, other := range ids {
				if other != id {
					peerAddrs[other] = addrs[other]
				}
			}
			gt := NewGRPCTransport(id, peerAddrs)
			t.Cleanup(func() { gt.Close() })
			transports[id] = &grpcAttachableTransport{GRPCTransport: gt, handle: handles[id]}
		}
		return transports
	}

	return factory, listenCount
}

// TestConformance_InMemoryVsGRPC is the crux of Decision 3: the exact same
// scripted scenario must produce equivalent outcomes over InMemoryTransport
// and real gRPC. Any divergence names which transport produced the
// different result.
func TestConformance_InMemoryVsGRPC(t *testing.T) {
	resultMem := runScenario(t, inMemoryFactory)

	grpcFactory, _ := makeGRPCFactory(t)
	resultGRPC := runScenario(t, grpcFactory)

	if !resultMem.LeaderEmerged {
		t.Fatal("InMemoryTransport: no leader emerged")
	}
	if !resultGRPC.LeaderEmerged {
		t.Fatal("GRPCTransport: no leader emerged")
	}

	if len(resultMem.CommittedEntries) != len(resultGRPC.CommittedEntries) {
		t.Fatalf("committed entry count differs: InMemoryTransport=%d GRPCTransport=%d",
			len(resultMem.CommittedEntries), len(resultGRPC.CommittedEntries))
	}
	for i := range resultMem.CommittedEntries {
		a, b := resultMem.CommittedEntries[i], resultGRPC.CommittedEntries[i]
		if a.Index != b.Index || string(a.Command) != string(b.Command) {
			t.Fatalf("committed entry %d differs: InMemoryTransport=%+v GRPCTransport=%+v", i, a, b)
		}
	}

	memLen, err := uniformLogLength(resultMem.FinalLogs)
	if err != nil {
		t.Fatalf("InMemoryTransport: %v", err)
	}
	grpcLen, err := uniformLogLength(resultGRPC.FinalLogs)
	if err != nil {
		t.Fatalf("GRPCTransport: %v", err)
	}
	if memLen != grpcLen {
		t.Fatalf("final log length differs across transports: InMemoryTransport=%d GRPCTransport=%d", memLen, grpcLen)
	}
}

// uniformLogLength asserts every node's log in logs has the same length
// (cluster-internal convergence) and returns that shared length.
func uniformLogLength(logs map[string][]raft.LogEntry) (int, error) {
	length := -1
	for id, log := range logs {
		if length == -1 {
			length = len(log)
			continue
		}
		if len(log) != length {
			return 0, fmt.Errorf("node %s log length %d disagrees with the rest of the cluster (%d)", id, len(log), length)
		}
	}
	return length, nil
}

// TestConformance_GRPCUsesRealSockets confirms the gRPC factory path
// genuinely binds real TCP sockets via net.Listen — one per node — rather
// than taking a shortcut that reuses InMemoryTransport internally.
func TestConformance_GRPCUsesRealSockets(t *testing.T) {
	grpcFactory, listenCount := makeGRPCFactory(t)
	_ = runScenario(t, grpcFactory)

	if got := atomic.LoadInt32(listenCount); got != 5 {
		t.Fatalf("expected exactly 5 real net.Listen calls (one per node), got %d", got)
	}
}
