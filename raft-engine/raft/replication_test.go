package raft

import (
	"context"
	"testing"
)

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

func TestCommitIndex_RequiresCurrentTermEntry(t *testing.T) {
	n := NewNode("node-0", []string{"node-1", "node-2", "node-3", "node-4"}, &fakeTransport{id: "node-0"})

	n.mu.Lock()
	n.currentTerm = 2
	n.role = Leader
	n.log = []LogEntry{
		{Index: 1, Term: 1, Command: []byte("old")},
		{Index: 2, Term: 2, Command: []byte("new")},
	}
	n.nextIndex = map[string]int64{"node-1": 3, "node-2": 3, "node-3": 3, "node-4": 3}
	// A majority (3/4 peers, plus self) has replicated the prior-term entry
	// at index 1, but only a minority has reached the current-term entry
	// at index 2.
	n.matchIndex = map[string]int64{"node-1": 1, "node-2": 1, "node-3": 1, "node-4": 0}
	n.advanceCommitIndexLocked()
	got := n.commitIndex
	n.mu.Unlock()

	if got != 0 {
		t.Fatalf("expected commitIndex to stay 0 — a prior-term entry must never be committed by replication count alone, got %d", got)
	}

	// Now a majority also reaches index 2, a current-term entry.
	n.mu.Lock()
	n.matchIndex["node-3"] = 2
	n.matchIndex["node-4"] = 2
	n.advanceCommitIndexLocked()
	got = n.commitIndex
	n.mu.Unlock()

	if got != 2 {
		t.Fatalf("expected commitIndex to advance to 2 once a current-term entry is majority-replicated, got %d", got)
	}
}

func TestCommitIndex_NeverRegresses(t *testing.T) {
	n := NewNode("node-0", []string{"node-1", "node-2"}, &fakeTransport{id: "node-0"})

	var history []int64
	record := func() {
		n.mu.Lock()
		history = append(history, n.commitIndex)
		n.mu.Unlock()
	}

	// Term 1: leader commits index 1.
	n.mu.Lock()
	n.currentTerm = 1
	n.role = Leader
	n.log = []LogEntry{{Index: 1, Term: 1, Command: []byte("a")}}
	n.nextIndex = map[string]int64{"node-1": 2, "node-2": 2}
	n.matchIndex = map[string]int64{"node-1": 1, "node-2": 1}
	n.advanceCommitIndexLocked()
	n.mu.Unlock()
	record()

	// Term 2: a new leadership term begins with an extended log and fresh
	// (unreplicated) match state — commitIndex must not regress even though
	// the new term's own entry isn't majority-replicated yet.
	n.mu.Lock()
	n.currentTerm = 2
	n.role = Leader
	n.log = append(n.log, LogEntry{Index: 2, Term: 2, Command: []byte("b")})
	n.nextIndex = map[string]int64{"node-1": 3, "node-2": 3}
	n.matchIndex = map[string]int64{"node-1": 0, "node-2": 0}
	n.advanceCommitIndexLocked()
	n.mu.Unlock()
	record()

	// The term-2 entry now reaches a majority too.
	n.mu.Lock()
	n.matchIndex = map[string]int64{"node-1": 2, "node-2": 2}
	n.advanceCommitIndexLocked()
	n.mu.Unlock()
	record()

	for i := 1; i < len(history); i++ {
		if history[i] < history[i-1] {
			t.Fatalf("commitIndex regressed across the run: %v", history)
		}
	}
	if got := history[len(history)-1]; got != 2 {
		t.Fatalf("expected final commitIndex 2, got %d (history: %v)", got, history)
	}
}

func TestReplicate_BacktracksOnConflict(t *testing.T) {
	calls := 0
	ft := &fakeTransport{id: "node-0"}
	ft.sendFunc = func(target, rpcType string, args any) (any, error) {
		calls++
		if calls == 1 {
			return AppendEntriesReply{Success: false, ConflictIndex: 2}, nil
		}
		return AppendEntriesReply{Success: true}, nil
	}

	n := NewNode("node-0", []string{"node-1"}, ft)
	n.mu.Lock()
	n.currentTerm = 1
	n.role = Leader
	n.log = []LogEntry{
		{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1},
		{Index: 4, Term: 1}, {Index: 5, Term: 1},
	}
	n.nextIndex = map[string]int64{"node-1": 5}
	n.matchIndex = map[string]int64{"node-1": 0}
	n.mu.Unlock()

	n.replicateTo(context.Background(), "node-1")

	if calls != 2 {
		t.Fatalf("expected exactly 2 Send calls (one conflict, one success after backtracking), got %d", calls)
	}

	n.mu.Lock()
	next := n.nextIndex["node-1"]
	match := n.matchIndex["node-1"]
	n.mu.Unlock()

	if match != 5 {
		t.Fatalf("expected matchIndex to reach 5 after the successful retry, got %d", match)
	}
	if next != 6 {
		t.Fatalf("expected nextIndex to become 6 after the successful retry, got %d", next)
	}
}
