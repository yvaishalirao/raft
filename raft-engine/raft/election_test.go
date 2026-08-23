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
