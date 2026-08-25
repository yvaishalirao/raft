package rpc

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"raft-engine/raft"
	raftpb "raft-engine/rpc/proto"
)

// raftServer adapts a *raft.Node to the generated gRPC service interface,
// translating protobuf types to/from raft's own plain Go argument/reply
// types at this boundary — the Raft core itself never sees protobuf. node
// is an atomic pointer rather than a plain field so a server can be
// constructed (and its listener address known) before the Node it will
// dispatch to exists — the conformance suite's gRPC factory needs exactly
// that ordering, since peer addresses must be known before nodes are built.
type raftServer struct {
	raftpb.UnimplementedRaftRPCServer
	node atomic.Pointer[raft.Node]
}

func (s *raftServer) attach(n *raft.Node) { s.node.Store(n) }

func (s *raftServer) RequestVote(ctx context.Context, in *raftpb.RequestVoteArgs) (*raftpb.RequestVoteReply, error) {
	n := s.node.Load()
	if n == nil {
		return nil, fmt.Errorf("grpc: server not yet attached to a node")
	}
	reply := n.HandleRequestVote(raft.RequestVoteArgs{
		Term:         in.GetTerm(),
		CandidateID:  in.GetCandidateId(),
		LastLogIndex: in.GetLastLogIndex(),
		LastLogTerm:  in.GetLastLogTerm(),
	})
	return &raftpb.RequestVoteReply{Term: reply.Term, VoteGranted: reply.VoteGranted}, nil
}

func (s *raftServer) AppendEntries(ctx context.Context, in *raftpb.AppendEntriesArgs) (*raftpb.AppendEntriesReply, error) {
	n := s.node.Load()
	if n == nil {
		return nil, fmt.Errorf("grpc: server not yet attached to a node")
	}

	pbEntries := in.GetEntries()
	entries := make([]raft.LogEntry, len(pbEntries))
	for i, e := range pbEntries {
		entries[i] = raft.LogEntry{Index: e.GetIndex(), Term: e.GetTerm(), Command: e.GetCommand()}
	}

	reply := n.HandleAppendEntries(raft.AppendEntriesArgs{
		Term:         in.GetTerm(),
		LeaderID:     in.GetLeaderId(),
		PrevLogIndex: in.GetPrevLogIndex(),
		PrevLogTerm:  in.GetPrevLogTerm(),
		Entries:      entries,
		LeaderCommit: in.GetLeaderCommit(),
	})
	return &raftpb.AppendEntriesReply{
		Term:          reply.Term,
		Success:       reply.Success,
		ConflictIndex: reply.ConflictIndex,
	}, nil
}

// NewGRPCServer starts a gRPC server dispatching RequestVote and
// AppendEntries to node. It returns the server and its listener unstarted
// (Serve not yet called) so the caller controls the server's lifecycle.
func NewGRPCServer(node *raft.Node, addr string) (*grpc.Server, net.Listener, error) {
	srv, lis, rs, err := newDetachedGRPCServer(addr)
	if err != nil {
		return nil, nil, err
	}
	rs.attach(node)
	return srv, lis, nil
}

// newDetachedGRPCServer starts listening and registers the RaftRPC service
// without yet knowing which Node it will dispatch to — used where the
// listener's address must be known before the Node exists (see raftServer).
// Call attach on the returned *raftServer once the Node is built, before
// any real traffic is expected.
func newDetachedGRPCServer(addr string) (*grpc.Server, net.Listener, *raftServer, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("grpc: listen on %s: %w", addr, err)
	}

	rs := &raftServer{}
	srv := grpc.NewServer()
	raftpb.RegisterRaftRPCServer(srv, rs)

	return srv, lis, rs, nil
}

// GRPCTransport implements raft.Transport over real gRPC: it dials peers
// lazily on first use and reuses the connection thereafter.
type GRPCTransport struct {
	selfID    string
	peerAddrs map[string]string

	// DialTimeout bounds how long a single underlying connection attempt
	// may take before grpc-go's internal retry/backoff gives up and tries
	// again. CallTimeout bounds every individual RPC — Send wraps its call
	// in context.WithTimeout(ctx, CallTimeout) so a slow or dead peer can
	// never block the caller indefinitely.
	DialTimeout time.Duration
	CallTimeout time.Duration

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn

	// inbox exists only to satisfy raft.Transport's Recv method. Real
	// inbound RPCs never flow through it — raftServer calls the Node's
	// handlers directly from the gRPC server goroutine, since gRPC's own
	// request/response model already is the "receive" point. A
	// GRPCTransport-backed Node's Run() dispatch loop simply idles on
	// Recv until ctx is done.
	inbox chan raft.RPC
}

const (
	DefaultDialTimeout = 2 * time.Second
	DefaultCallTimeout = 2 * time.Second
)

func NewGRPCTransport(selfID string, peerAddrs map[string]string) *GRPCTransport {
	return &GRPCTransport{
		selfID:      selfID,
		peerAddrs:   peerAddrs,
		DialTimeout: DefaultDialTimeout,
		CallTimeout: DefaultCallTimeout,
		conns:       make(map[string]*grpc.ClientConn),
		inbox:       make(chan raft.RPC),
	}
}

func (t *GRPCTransport) LocalID() string { return t.selfID }

// clientFor returns a client for target, reusing a single cached
// *grpc.ClientConn per peer (lazy-dial on first use) rather than dialing
// per call.
func (t *GRPCTransport) clientFor(target string) (raftpb.RaftRPCClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if conn, ok := t.conns[target]; ok {
		return raftpb.NewRaftRPCClient(conn), nil
	}

	addr, ok := t.peerAddrs[target]
	if !ok {
		return nil, fmt.Errorf("grpc: no address known for peer %q", target)
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{MinConnectTimeout: t.DialTimeout}),
	)
	if err != nil {
		return nil, fmt.Errorf("grpc: dial %s: %w", addr, err)
	}
	t.conns[target] = conn
	return raftpb.NewRaftRPCClient(conn), nil
}

// Send never panics and never blocks past CallTimeout: a dead peer, a
// connection that can't be established, or a call that times out all
// surface as a plain wrapped error.
func (t *GRPCTransport) Send(ctx context.Context, target string, rpcType string, args any) (any, error) {
	client, err := t.clientFor(target)
	if err != nil {
		return nil, fmt.Errorf("grpc: %w", err)
	}

	cctx, cancel := context.WithTimeout(ctx, t.CallTimeout)
	defer cancel()

	switch rpcType {
	case "RequestVote":
		a, ok := args.(raft.RequestVoteArgs)
		if !ok {
			return nil, fmt.Errorf("grpc: unexpected args type for RequestVote: %T", args)
		}
		reply, err := client.RequestVote(cctx, &raftpb.RequestVoteArgs{
			Term:         a.Term,
			CandidateId:  a.CandidateID,
			LastLogIndex: a.LastLogIndex,
			LastLogTerm:  a.LastLogTerm,
		})
		if err != nil {
			return nil, fmt.Errorf("grpc: RequestVote to %s: %w", target, err)
		}
		return raft.RequestVoteReply{Term: reply.GetTerm(), VoteGranted: reply.GetVoteGranted()}, nil

	case "AppendEntries":
		a, ok := args.(raft.AppendEntriesArgs)
		if !ok {
			return nil, fmt.Errorf("grpc: unexpected args type for AppendEntries: %T", args)
		}
		pbEntries := make([]*raftpb.LogEntry, len(a.Entries))
		for i, e := range a.Entries {
			pbEntries[i] = &raftpb.LogEntry{Index: e.Index, Term: e.Term, Command: e.Command}
		}
		reply, err := client.AppendEntries(cctx, &raftpb.AppendEntriesArgs{
			Term:         a.Term,
			LeaderId:     a.LeaderID,
			PrevLogIndex: a.PrevLogIndex,
			PrevLogTerm:  a.PrevLogTerm,
			Entries:      pbEntries,
			LeaderCommit: a.LeaderCommit,
		})
		if err != nil {
			return nil, fmt.Errorf("grpc: AppendEntries to %s: %w", target, err)
		}
		return raft.AppendEntriesReply{
			Term:          reply.GetTerm(),
			Success:       reply.GetSuccess(),
			ConflictIndex: reply.GetConflictIndex(),
		}, nil

	default:
		return nil, fmt.Errorf("grpc: unknown RPC type %q", rpcType)
	}
}

func (t *GRPCTransport) Recv(ctx context.Context) (raft.RPC, bool) {
	select {
	case rpc := <-t.inbox:
		return rpc, true
	case <-ctx.Done():
		return raft.RPC{}, false
	}
}

// Close closes every cached connection.
func (t *GRPCTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	var firstErr error
	for _, conn := range t.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
