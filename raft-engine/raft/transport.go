package raft

import "context"

type Transport interface {
	Send(ctx context.Context, target string, rpcType string, args any) (reply any, err error)
	// Recv blocks until an inbound RPC arrives for this node or ctx is
	// done. ok is false only on ctx cancellation. The handler must call
	// RPC.Reply exactly once.
	Recv(ctx context.Context) (RPC, bool)
	LocalID() string
}

// RPC is one inbound request delivered by a Transport, along with the Reply
// closure a handler calls exactly once to send the response back.
type RPC struct {
	Type  string
	Args  any
	Reply func(reply any, err error)
}
