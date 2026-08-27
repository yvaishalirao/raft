package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"raft-engine/raft"
)

// partitionTestDuration must be several times ElectionTimeoutMax so a
// shallow test can't pass by accident.
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

// runHealScenario builds a partitioned cluster, gives one minority node a
// stale, locally-logged-but-uncommitted entry (simulating a write it
// received from an earlier leader before the partition, which could never
// reach majority replication), has the majority commit a genuinely
// different entry, heals the partition, and waits for all 5 logs to
// stabilize. Returns the cluster, the majority leader, the stale node's ID,
// and every node's final stable log.
func runHealScenario(t *testing.T) (c *Cluster, leader *raft.Node, staleNodeID string, stableLogs map[string][]raft.LogEntry) {
	t.Helper()

	var majorityIDs, minorityIDs []string
	c, majorityIDs, minorityIDs = partitionedCluster(t)
	staleNodeID = minorityIDs[0]

	// Inject immediately, before this node's own election timer has any
	// chance to fire, so it's still at term 0 — a value no real election
	// ever produces, guaranteeing the "real" commit below lands at a
	// strictly higher term. That's what makes this a genuine conflict
	// (different term, same index) rather than an identical duplicate,
	// which Log Matching would correctly treat as a no-op, not a conflict.
	reply := c.NodeByID(staleNodeID).HandleAppendEntries(raft.AppendEntriesArgs{
		Term:         0,
		LeaderID:     "phantom-leader",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []raft.LogEntry{{Index: 1, Term: 0, Command: []byte("STALE")}},
	})
	if !reply.Success {
		t.Fatalf("test setup failed: stale entry injection was rejected: %+v", reply)
	}

	deadline := time.Now().Add(partitionTestDuration)
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
	_, err := leader.Propose(wctx, []byte("REAL"))
	cancel()
	if err != nil {
		t.Fatalf("majority failed to commit during the partition: %v", err)
	}

	for _, a := range minorityIDs {
		for _, b := range majorityIDs {
			c.Router.SetDrop(a, b, false)
			c.Router.SetDrop(b, a, false)
		}
	}

	stableLogs = waitForLogsToStabilize(t, c, 5*time.Second, 50*time.Millisecond)
	return c, leader, staleNodeID, stableLogs
}

// requiredStableStreak is how many consecutive unchanged polls are needed
// before logs are declared stable. Reconciliation after a heal can involve
// more than one election round (e.g. a stale node's inflated term — from
// its own futile candidacies while partitioned — can make the healed
// leader step down before a valid new leader re-emerges), and the log
// genuinely doesn't change during the pause between rounds. Requiring only
// a couple of quiet polls mistakes that pause for convergence; the streak
// must span comfortably more than one full election-timeout cycle.
const requiredStableStreak = 20

// waitForLogsToStabilize polls every interval for up to bound and returns
// once every node's log is unchanged across requiredStableStreak
// consecutive polls. Fails the test if that never happens within bound — a
// fixed sleep-then-assert would let this pass on a partially-converged
// state by luck.
func waitForLogsToStabilize(t *testing.T, c *Cluster, bound, interval time.Duration) map[string][]raft.LogEntry {
	t.Helper()

	streak := 0
	prev := snapshotLogs(c)
	deadline := time.Now().Add(bound)

	for time.Now().Before(deadline) {
		time.Sleep(interval)
		curr := snapshotLogs(c)
		if logsSnapshotEqual(prev, curr) {
			streak++
			if streak >= requiredStableStreak {
				return curr
			}
			continue
		}
		streak = 0
		prev = curr
	}

	t.Fatalf("logs never stabilized within %v", bound)
	return nil
}

func snapshotLogs(c *Cluster) map[string][]raft.LogEntry {
	snap := make(map[string][]raft.LogEntry, len(c.Nodes))
	for _, n := range c.Nodes {
		snap[n.ID()] = n.LogEntries()
	}
	return snap
}

func logsSnapshotEqual(a, b map[string][]raft.LogEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for id, logA := range a {
		logB, ok := b[id]
		if !ok || !logsMatch(logA, logB) {
			return false
		}
	}
	return true
}

// TestPartition_HealConverges confirms that once the partition heals, all 5
// nodes converge to the majority leader's log.
func TestPartition_HealConverges(t *testing.T) {
	c, leader, _, stableLogs := runHealScenario(t)
	defer c.Shutdown()

	assertLogMatchingPairwise(t, c.Nodes)

	leaderLog := leader.LogEntries()
	for _, n := range c.Nodes {
		if !logsMatch(stableLogs[n.ID()], leaderLog) {
			t.Fatalf("node %s did not converge to the majority leader's log:\n  got:  %+v\n  want: %+v",
				n.ID(), stableLogs[n.ID()], leaderLog)
		}
	}
}

// TestPartition_HealDiscardsStaleEntries confirms a minority node's stale,
// conflicting, uncommitted entry is discarded during reconciliation, not
// silently merged in as an extra entry alongside the real one.
func TestPartition_HealDiscardsStaleEntries(t *testing.T) {
	c, leader, staleNodeID, stableLogs := runHealScenario(t)
	defer c.Shutdown()

	leaderLog := leader.LogEntries()
	staleLog := stableLogs[staleNodeID]

	if len(staleLog) != len(leaderLog) {
		t.Fatalf("expected the stale node's converged log length (%d) to exactly match the majority leader's (%d) — extra entries were merged in instead of discarded",
			len(staleLog), len(leaderLog))
	}
	for i := range leaderLog {
		if staleLog[i].Term != leaderLog[i].Term || string(staleLog[i].Command) != string(leaderLog[i].Command) {
			t.Fatalf("stale node's post-heal entry at index %d (%+v) doesn't match the majority leader's (%+v)",
				i, staleLog[i], leaderLog[i])
		}
	}
}
