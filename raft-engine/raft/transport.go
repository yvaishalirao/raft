package raft

import "context"

type Transport interface {
	Send(ctx context.Context, target string, rpcType string, args any) (reply any, err error)
	LocalID() string
}
