package raft

import "testing"

func TestTerm_Monotonic(t *testing.T) {
	n := NewNode("node-0", nil, nil)

	terms := []int64{1, 3, 3, 7, 7, 7, 10}
	prev := n.currentTerm

	for _, term := range terms {
		n.updateTerm(term)
		if n.currentTerm < prev {
			t.Fatalf("term decreased: %d -> %d", prev, n.currentTerm)
		}
		prev = n.currentTerm
	}

	if n.currentTerm != 10 {
		t.Fatalf("expected final term 10, got %d", n.currentTerm)
	}
}

func TestTerm_RejectsLower(t *testing.T) {
	n := NewNode("node-0", nil, nil)
	n.updateTerm(5)

	if ok := n.updateTerm(5); ok {
		t.Fatal("expected updateTerm to reject an equal term")
	}
	if n.currentTerm != 5 {
		t.Fatalf("term should be unchanged at 5, got %d", n.currentTerm)
	}

	if ok := n.updateTerm(3); ok {
		t.Fatal("expected updateTerm to reject a lower term")
	}
	if n.currentTerm != 5 {
		t.Fatalf("term should be unchanged at 5, got %d", n.currentTerm)
	}
}

func TestVote_FirstGranted(t *testing.T) {
	n := NewNode("node-0", nil, nil)

	reply := n.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node-1"})

	if !reply.VoteGranted {
		t.Fatal("expected first vote in a fresh term to be granted")
	}
	if n.votedFor != "node-1" {
		t.Fatalf("expected votedFor=node-1, got %q", n.votedFor)
	}
}

func TestVote_GrantedOnce(t *testing.T) {
	n := NewNode("node-0", nil, nil)

	first := n.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node-1"})
	if !first.VoteGranted {
		t.Fatal("expected first vote to be granted")
	}

	second := n.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node-2"})
	if second.VoteGranted {
		t.Fatal("expected second vote request in the same term, different candidate, to be rejected")
	}

	// Re-voting for the SAME candidate within the same term stays idempotent.
	repeat := n.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node-1"})
	if !repeat.VoteGranted {
		t.Fatal("expected a retried request from the already-voted-for candidate to remain granted")
	}
}

func TestVote_RejectsStaleTerm(t *testing.T) {
	n := NewNode("node-0", nil, nil)
	n.updateTerm(5)

	reply := n.HandleRequestVote(RequestVoteArgs{Term: 3, CandidateID: "node-1"})

	if reply.VoteGranted {
		t.Fatal("expected a stale-term request to be rejected")
	}
	if reply.Term != 5 {
		t.Fatalf("expected reply term 5, got %d", reply.Term)
	}
}

func TestVote_RejectsStaleLog(t *testing.T) {
	n := NewNode("node-0", nil, nil)
	n.log = []LogEntry{{Index: 1, Term: 5}, {Index: 2, Term: 5}}

	reply := n.HandleRequestVote(RequestVoteArgs{
		Term:         6,
		CandidateID:  "node-1",
		LastLogIndex: 1,
		LastLogTerm:  3,
	})

	if reply.VoteGranted {
		t.Fatal("expected a candidate with a stale log to be rejected even in a fresh term")
	}
}

func TestTerm_RestartResets(t *testing.T) {
	n := NewNode("node-0", nil, nil)
	n.updateTerm(9)
	n.votedFor = "node-1"

	n.restart()

	if n.currentTerm != 0 {
		t.Fatalf("expected currentTerm 0 after restart, got %d", n.currentTerm)
	}
	if n.votedFor != "" {
		t.Fatalf("expected votedFor cleared after restart, got %q", n.votedFor)
	}
	if n.termHistory != nil {
		t.Fatalf("expected termHistory cleared after restart, got %v", n.termHistory)
	}
}
