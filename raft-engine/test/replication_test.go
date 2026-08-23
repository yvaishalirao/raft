package test

import (
	"testing"

	"raft-engine/raft"
)

// TestLogMatching_PairwiseConsistency drives a 3-node in-memory cluster
// through interleaved AppendEntries exchanges — including entries a node
// straggles on and a forced leader change that overwrites an uncommitted
// entry — then asserts Log Matching pairwise: for every index where two
// nodes both have an entry and the terms match, the commands must match too.
func TestLogMatching_PairwiseConsistency(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.Shutdown()

	n0, n1, n2 := c.Nodes[0], c.Nodes[1], c.Nodes[2]

	// Term 1: entry 1 replicated to all three nodes.
	for _, n := range []*raft.Node{n0, n1, n2} {
		reply := n.HandleAppendEntries(raft.AppendEntriesArgs{
			Term:         1,
			LeaderID:     "leader-1",
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      []raft.LogEntry{{Index: 1, Term: 1, Command: []byte("a")}},
		})
		if !reply.Success {
			t.Fatalf("term-1 entry 1 rejected: %+v", reply)
		}
	}

	// Term 1: entry 2 replicated only to n0 and n1 — n2 straggles and
	// misses it entirely.
	for _, n := range []*raft.Node{n0, n1} {
		reply := n.HandleAppendEntries(raft.AppendEntriesArgs{
			Term:         1,
			LeaderID:     "leader-1",
			PrevLogIndex: 1,
			PrevLogTerm:  1,
			Entries:      []raft.LogEntry{{Index: 2, Term: 1, Command: []byte("b")}},
		})
		if !reply.Success {
			t.Fatalf("term-1 entry 2 rejected: %+v", reply)
		}
	}

	// Forced leader change to term 2: the new leader overwrites index 2
	// with a different command. n0/n1 must truncate their stale term-1
	// entry before appending; n2 gets it as a fresh append.
	for _, n := range []*raft.Node{n0, n1, n2} {
		reply := n.HandleAppendEntries(raft.AppendEntriesArgs{
			Term:         2,
			LeaderID:     "leader-2",
			PrevLogIndex: 1,
			PrevLogTerm:  1,
			Entries:      []raft.LogEntry{{Index: 2, Term: 2, Command: []byte("c")}},
		})
		if !reply.Success {
			t.Fatalf("term-2 entry 2 rejected: %+v", reply)
		}
	}

	assertLogMatchingPairwise(t, c.Nodes)
}

// assertLogMatchingPairwise walks every pair of nodes' logs and asserts:
// for every index where both have an entry and the terms match, the
// commands also match.
func assertLogMatchingPairwise(t *testing.T, nodes []*raft.Node) {
	t.Helper()

	logs := make([][]raft.LogEntry, len(nodes))
	for i, n := range nodes {
		logs[i] = n.LogEntries()
	}

	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			for _, ea := range logs[i] {
				for _, eb := range logs[j] {
					if ea.Index != eb.Index || ea.Term != eb.Term {
						continue
					}
					if string(ea.Command) != string(eb.Command) {
						t.Fatalf("log matching violated between %s and %s at index %d term %d: %q vs %q",
							nodes[i].ID(), nodes[j].ID(), ea.Index, ea.Term, ea.Command, eb.Command)
					}
				}
			}
		}
	}
}
