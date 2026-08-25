package rpc

import (
	"context"
	"fmt"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"raft-engine/raft"
	raftpb "raft-engine/rpc/proto"
)

// raftServer adapts a *raft.Node to the generated gRPC service interface,
// translating protobuf types to/from raft's own plain Go argument/reply
// types at this boundary — the Raft core itself never sees protobuf.
type raftServer struct {
	raftpb.UnimplementedRaftRPCServer
	node *raft.Node
}

func (s *raftServer) RequestVote(ctx context.Context, in *raftpb.RequestVoteArgs) (*raftpb.RequestVoteReply, error) {
	reply := s.node.HandleRequestVote(raft.RequestVoteArgs{
		Term:         in.GetTerm(),
		CandidateID:  in.GetCandidateId(),
		LastLogIndex: in.GetLastLogIndex(),
		LastLogTerm:  in.GetLastLogTerm(),
	})
	return &raftpb.RequestVoteReply{Term: reply.Term, VoteGranted: reply.VoteGranted}, nil
}

func (s *raftServer) AppendEntries(ctx context.Context, in *raftpb.AppendEntriesArgs) (*raftpb.AppendEntriesReply, error) {
	pbEntries := in.GetEntries()
	entries := make([]raft.LogEntry, len(pbEntries))
	for i, e := range pbEntries {
		entries[i] = raft.LogEntry{Index: e.GetIndex(), Term: e.GetTerm(), Command: e.GetCommand()}
	}

	reply := s.node.HandleAppendEntries(raft.AppendEntriesArgs{
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
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("grpc: listen on %s: %w", addr, err)
	}

	srv := grpc.NewServer()
	raftpb.RegisterRaftRPCServer(srv, &raftServer{node: node})

	return srv, lis, nil
}

// GRPCTransport implements raft.Transport over real gRPC: it dials peers
// lazily on first use and reuses the connection thereafter.
type GRPCTransport struct {
	selfID    string
	peerAddrs map[string]string

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

func NewGRPCTransport(selfID string, peerAddrs map[string]string) *GRPCTransport {
	return &GRPCTransport{
		selfID:    selfID,
		peerAddrs: peerAddrs,
		conns:     make(map[string]*grpc.ClientConn),
		inbox:     make(chan raft.RPC),
	}
}

func (t *GRPCTransport) LocalID() string { return t.selfID }

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

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("grpc: dial %s: %w", addr, err)
	}
	t.conns[target] = conn
	return raftpb.NewRaftRPCClient(conn), nil
}

func (t *GRPCTransport) Send(ctx context.Context, target string, rpcType string, args any) (any, error) {
	client, err := t.clientFor(target)
	if err != nil {
		return nil, err
	}

	switch rpcType {
	case "RequestVote":
		a, ok := args.(raft.RequestVoteArgs)
		if !ok {
			return nil, fmt.Errorf("grpc: unexpected args type for RequestVote: %T", args)
		}
		reply, err := client.RequestVote(ctx, &raftpb.RequestVoteArgs{
			Term:         a.Term,
			CandidateId:  a.CandidateID,
			LastLogIndex: a.LastLogIndex,
			LastLogTerm:  a.LastLogTerm,
		})
		if err != nil {
			return nil, err
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
		reply, err := client.AppendEntries(ctx, &raftpb.AppendEntriesArgs{
			Term:         a.Term,
			LeaderId:     a.LeaderID,
			PrevLogIndex: a.PrevLogIndex,
			PrevLogTerm:  a.PrevLogTerm,
			Entries:      pbEntries,
			LeaderCommit: a.LeaderCommit,
		})
		if err != nil {
			return nil, err
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
