package test

import (
	"context"
	"testing"
	"time"

	"raft-engine/raft"
)

// TestLeaderCompleteness_KillLeaderMidWrite is the literal live-demo
// scenario: commit a write, kill the leader, force an election among the
// survivors, and confirm the committed entry survived into the new
// leader's log. This must exist and stay green before any live rehearsal.
func TestLeaderCompleteness_KillLeaderMidWrite(t *testing.T) {
	c := NewCluster(t, 5)
	defer c.Shutdown()

	leader := waitForLeader(t, c, 5*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	index, err := leader.Propose(ctx, []byte("SET x 1"))
	if err != nil {
		t.Fatalf("Propose failed: %v", err)
	}

	killedID := leader.ID()
	c.KillNode(killedID)

	newLeader := waitForLeaderExcluding(t, c, killedID, 5*time.Second)

	entry, ok := entryAt(newLeader, index)
	if !ok {
		t.Fatalf("new leader %s log is missing the committed index %d", newLeader.ID(), index)
	}
	if string(entry.Command) != "SET x 1" {
		t.Fatalf("new leader has wrong command at index %d: %q", index, entry.Command)
	}
}

// TestLeaderCompleteness_ReelectsPromptly confirms a new leader emerges
// within a bounded time after the leader is killed.
func TestLeaderCompleteness_ReelectsPromptly(t *testing.T) {
	const deadline = 5 * time.Second

	c := NewCluster(t, 5)
	defer c.Shutdown()

	leader := waitForLeader(t, c, deadline)
	killedID := leader.ID()
	c.KillNode(killedID)

	start := time.Now()
	waitForLeaderExcluding(t, c, killedID, deadline)
	if elapsed := time.Since(start); elapsed > deadline {
		t.Fatalf("re-election took %v, longer than the %v test deadline", elapsed, deadline)
	}
}

// TestLeaderCompleteness_RevivedNodeConverges confirms a killed-then-revived
// node discards any stale entries and converges to the new leader's log
// rather than retaining what it had before it died.
func TestLeaderCompleteness_RevivedNodeConverges(t *testing.T) {
	c := NewCluster(t, 5)
	defer c.Shutdown()

	leader := waitForLeader(t, c, 5*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := leader.Propose(ctx, []byte("SET x 1")); err != nil {
		t.Fatalf("first Propose failed: %v", err)
	}

	killedID := leader.ID()
	c.KillNode(killedID)

	newLeader := waitForLeaderExcluding(t, c, killedID, 5*time.Second)

	// A second write under the new leader gives the revived node something
	// to catch up on beyond what it already had before it died.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	if _, err := newLeader.Propose(ctx2, []byte("SET y 2")); err != nil {
		t.Fatalf("second Propose failed: %v", err)
	}

	c.Revive(killedID)

	deadline := time.After(5 * time.Second)
	for {
		revived := c.NodeByID(killedID)
		if logsMatch(revived.LogEntries(), newLeader.LogEntries()) {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("revived node %s never converged to new leader %s's log:\n  revived: %+v\n  leader:  %+v",
				killedID, newLeader.ID(), revived.LogEntries(), newLeader.LogEntries())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func waitForLeader(t *testing.T, c *Cluster, timeout time.Duration) *raft.Node {
	t.Helper()
	return waitForLeaderExcluding(t, c, "", timeout)
}

func waitForLeaderExcluding(t *testing.T, c *Cluster, excludeID string, timeout time.Duration) *raft.Node {
	t.Helper()

	deadline := time.After(timeout)
	for {
		for _, n := range c.Nodes {
			if n.ID() != excludeID && n.Role() == raft.Leader {
				return n
			}
		}
		select {
		case <-deadline:
			t.Fatalf("timed out after %v waiting for a leader (excluding %q)", timeout, excludeID)
			return nil
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func entryAt(n *raft.Node, index int64) (raft.LogEntry, bool) {
	for _, e := range n.LogEntries() {
		if e.Index == index {
			return e, true
		}
	}
	return raft.LogEntry{}, false
}

func logsMatch(a, b []raft.LogEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Index != b[i].Index || a[i].Term != b[i].Term || string(a[i].Command) != string(b[i].Command) {
			return false
		}
	}
	return true
}
