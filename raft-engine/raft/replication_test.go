package raft

import "testing"

func TestAppendEntries_RejectsMismatch(t *testing.T) {
	n := NewNode("node-0", nil, nil)
	n.log = []LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}

	reply := n.HandleAppendEntries(AppendEntriesArgs{
		Term:         1,
		LeaderID:     "node-1",
		PrevLogIndex: 2,
		PrevLogTerm:  2, // we have term 1 at index 2; leader expects term 2
	})

	if reply.Success {
		t.Fatal("expected AppendEntries to be rejected on prev-log term mismatch")
	}
	if reply.ConflictIndex == 0 {
		t.Fatal("expected ConflictIndex to be set on rejection")
	}
}

func TestAppendEntries_TruncatesConflict(t *testing.T) {
	n := NewNode("node-0", nil, nil)
	n.log = []LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}}

	reply := n.HandleAppendEntries(AppendEntriesArgs{
		Term:         2,
		LeaderID:     "node-1",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []LogEntry{{Index: 2, Term: 2, Command: []byte("new")}},
	})

	if !reply.Success {
		t.Fatalf("expected success, got failure with conflict index %d", reply.ConflictIndex)
	}
	if len(n.log) != 2 {
		t.Fatalf("expected old index-3 entry truncated away, log length 2, got %d", len(n.log))
	}
	if n.log[1].Term != 2 || string(n.log[1].Command) != "new" {
		t.Fatalf("expected index 2 to be the new term-2 entry, got %+v", n.log[1])
	}
}

func TestAppendEntries_CommitAdvance(t *testing.T) {
	n := NewNode("node-0", nil, nil)

	reply := n.HandleAppendEntries(AppendEntriesArgs{
		Term:         1,
		LeaderID:     "node-1",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{Index: 1, Term: 1, Command: []byte("a")},
			{Index: 2, Term: 1, Command: []byte("b")},
			{Index: 3, Term: 1, Command: []byte("c")},
		},
		LeaderCommit: 2,
	})

	if !reply.Success {
		t.Fatal("expected success")
	}
	if n.commitIndex != 2 {
		t.Fatalf("expected commitIndex=min(2,3)=2, got %d", n.commitIndex)
	}
}
