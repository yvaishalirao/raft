package raft

import (
	"errors"
	"testing"
)

func TestMajority_UsesConfiguredSize(t *testing.T) {
	// Only node-1 would ever actually respond; node-2/3/4 are unreachable.
	// majority() must still reflect the fixed, configured cluster size (5
	// nodes total), never a live count of reachable peers.
	ft := &fakeTransport{
		id: "node-0",
		sendFunc: func(target, rpcType string, args any) (any, error) {
			if target == "node-1" {
				return RequestVoteReply{VoteGranted: true}, nil
			}
			return nil, errors.New("unreachable")
		},
	}
	n := NewNode("node-0", []string{"node-1", "node-2", "node-3", "node-4"}, ft)

	if got := n.majority(); got != 3 {
		t.Fatalf("expected majority()==3 for a 5-node cluster regardless of reachability, got %d", got)
	}
}
