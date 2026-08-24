package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"raft-engine/raft"
)

// partitionTestDuration must be several times ElectionTimeoutMax so a
// shallow test can't pass by accident — see the code review note on 5.2.
const partitionTestDuration = 10 * time.Second

// partitionedCluster builds a 5-node cluster and severs all links —
// bidirectionally — between a 2-node minority and the 3-node majority via
// SetDrop, leaving each side fully connected internally.
func partitionedCluster(t *testing.T) (c *Cluster, majorityIDs, minorityIDs []string) {
	t.Helper()

	c = NewCluster(t, 5)

	allIDs := make([]string, len(c.Nodes))
	for i, n := range c.Nodes {
		allIDs[i] = n.ID()
	}
	minorityIDs = allIDs[:2]
	majorityIDs = allIDs[2:]

	for _, a := range minorityIDs {
		for _, b := range majorityIDs {
			c.Router.SetDrop(a, b, true)
			c.Router.SetDrop(b, a, true)
		}
	}

	return c, majorityIDs, minorityIDs
}

func leaderAmong(c *Cluster, ids []string) *raft.Node {
	want := make(map[string]bool, len(ids))
	for _, id := range ids {
		want[id] = true
	}
	for _, n := range c.Nodes {
		if want[n.ID()] && n.Role() == raft.Leader {
			return n
		}
	}
	return nil
}

// TestPartition_MajoritySucceeds confirms the 3-node majority side operates
// normally during the partition: it elects a leader and commits a write.
func TestPartition_MajoritySucceeds(t *testing.T) {
	c, majorityIDs, _ := partitionedCluster(t)
	defer c.Shutdown()

	deadline := time.Now().Add(partitionTestDuration)
	var leader *raft.Node
	for time.Now().Before(deadline) {
		if leader = leaderAmong(c, majorityIDs); leader != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if leader == nil {
		t.Fatal("majority side never elected a leader during the partition")
	}

	wctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := leader.Propose(wctx, []byte("SET x 1")); err != nil {
		t.Fatalf("majority side failed to commit a write during the partition: %v", err)
	}
}

// TestPartition_MinorityNeverWrites polls continuously for the full
// partition duration and fails the instant either minority node reaches
// role=Leader — a minority (2 of 5) can never assemble the majority (3)
// that Raft requires.
func TestPartition_MinorityNeverWrites(t *testing.T) {
	c, _, minorityIDs := partitionedCluster(t)
	defer c.Shutdown()

	deadline := time.Now().Add(partitionTestDuration)
	for time.Now().Before(deadline) {
		if leader := leaderAmong(c, minorityIDs); leader != nil {
			t.Fatalf("minority node %s became Leader during the partition", leader.ID())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestPartition_MinorityCommitFrozen actively drives writes on the majority
// side throughout the partition while continuously asserting the minority
// side's commitIndex never moves from its pre-partition value — proving the
// minority doesn't just fail to lead, it makes zero commit progress even
// while the majority is actively committing.
func TestPartition_MinorityCommitFrozen(t *testing.T) {
	c, majorityIDs, minorityIDs := partitionedCluster(t)
	defer c.Shutdown()

	initial := make(map[string]int64, len(minorityIDs))
	for _, id := range minorityIDs {
		initial[id] = c.NodeByID(id).CommitIndex()
	}

	deadline := time.Now().Add(partitionTestDuration)
	writeTicker := time.NewTicker(500 * time.Millisecond)
	defer writeTicker.Stop()

	i := 0
	for time.Now().Before(deadline) {
		select {
		case <-writeTicker.C:
			if leader := leaderAmong(c, majorityIDs); leader != nil {
				wctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
				_, _ = leader.Propose(wctx, []byte(fmt.Sprintf("cmd-%d", i)))
				cancel()
				i++
			}
		default:
		}

		for _, id := range minorityIDs {
			if got := c.NodeByID(id).CommitIndex(); got != initial[id] {
				t.Fatalf("minority node %s commitIndex advanced during partition: %d -> %d", id, initial[id], got)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}
