package raft

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestApplyLoop_InOrder(t *testing.T) {
	n := NewNode("node-0", nil, nil)
	n.mu.Lock()
	n.log = []LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}}
	n.commitIndex = 3
	n.mu.Unlock()

	var mu sync.Mutex
	var appliedIndexes []int64
	apply := func(e LogEntry) {
		mu.Lock()
		appliedIndexes = append(appliedIndexes, e.Index)
		mu.Unlock()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.runApplyLoop(ctx, apply)

	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		done := len(appliedIndexes) >= 3
		mu.Unlock()
		if done {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for all committed entries to be applied")
		case <-time.After(10 * time.Millisecond):
		}
	}

	mu.Lock()
	defer mu.Unlock()
	for i, idx := range appliedIndexes {
		if idx != int64(i+1) {
			t.Fatalf("expected strictly increasing indexes starting at 1 with no skips, got %v", appliedIndexes)
		}
	}
}

func TestApplyLoop_NeverAppliesUncommitted(t *testing.T) {
	n := NewNode("node-0", nil, nil)
	n.mu.Lock()
	n.log = []LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}}
	n.commitIndex = 1 // only index 1 is committed
	n.mu.Unlock()

	var mu sync.Mutex
	var appliedIndexes []int64
	violation := false
	apply := func(e LogEntry) {
		n.mu.Lock()
		commitAtCallTime := n.commitIndex
		n.mu.Unlock()

		mu.Lock()
		if e.Index > commitAtCallTime {
			violation = true
		}
		appliedIndexes = append(appliedIndexes, e.Index)
		mu.Unlock()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.runApplyLoop(ctx, apply)

	// Give the loop time to (wrongly) race ahead if it had a bug.
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if violation {
		t.Fatal("apply() was called for an index beyond commitIndex at the time of the call")
	}
	if len(appliedIndexes) != 1 || appliedIndexes[0] != 1 {
		t.Fatalf("expected exactly index 1 applied (commitIndex never advanced past it), got %v", appliedIndexes)
	}
}

// TestApplyLoop_PanicsOnPrematureApply exercises the defense-in-depth
// guard directly: tryApplyNext's own loop condition makes it mathematically
// unreachable via normal operation (lastApplied<commitIndex always implies
// lastApplied+1<=commitIndex for integers), so this test invokes the guard
// function itself with a hand-crafted index beyond commitIndex — the
// "throwaway test double" scenario 8.3 calls for.
func TestApplyLoop_PanicsOnPrematureApply(t *testing.T) {
	n := NewNode("node-0", nil, nil)
	n.mu.Lock()
	n.commitIndex = 2
	n.mu.Unlock()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected a panic when asserting an index beyond commitIndex")
		}
	}()

	n.mu.Lock()
	defer n.mu.Unlock()
	n.assertNotBeyondCommitLocked(5)
}
