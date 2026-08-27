package test

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"raft-engine/raft"
)

const regressionPartitionDuration = 2 * time.Second

// TestPartition_ExtendedRandomizedRun is a property-style hardening pass:
// run the partition scenario many times (via -count=N) with a randomized
// minority group and randomized election-timing seed each run, asserting
// the partition-safety and heal-convergence properties every time —
// catching a majority-calculation bug subtle enough that a single fixed
// scenario could pass by luck.
func TestPartition_ExtendedRandomizedRun(t *testing.T) {
	seed := time.Now().UnixNano()
	rng := rand.New(rand.NewSource(seed))

	c := NewCluster(t, 5, WithSeed(seed))
	defer c.Shutdown()

	allIDs := make([]string, len(c.Nodes))
	for i, n := range c.Nodes {
		allIDs[i] = n.ID()
	}
	rng.Shuffle(len(allIDs), func(i, j int) { allIDs[i], allIDs[j] = allIDs[j], allIDs[i] })
	minorityIDs := append([]string(nil), allIDs[:2]...)
	majorityIDs := append([]string(nil), allIDs[2:]...)

	t.Logf("seed %d: minority group: %v", seed, minorityIDs)

	for _, a := range minorityIDs {
		for _, b := range majorityIDs {
			c.Router.SetDrop(a, b, true)
			c.Router.SetDrop(b, a, true)
		}
	}

	// Property (a): the majority elects a leader and commits a write.
	deadline := time.Now().Add(regressionPartitionDuration)
	var leader *raft.Node
	for time.Now().Before(deadline) {
		if leader = leaderAmong(c, majorityIDs); leader != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if leader == nil {
		t.Fatalf("seed %d: majority side never elected a leader", seed)
	}

	wctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_, err := leader.Propose(wctx, []byte("REAL"))
	cancel()
	if err != nil {
		t.Fatalf("seed %d: majority failed to commit during the partition: %v", seed, err)
	}

	// Properties (b) and (c): watched continuously for the rest of the
	// partition window.
	initialCommit := make(map[string]int64, len(minorityIDs))
	for _, id := range minorityIDs {
		initialCommit[id] = c.NodeByID(id).CommitIndex()
	}

	remaining := time.Until(deadline)
	if remaining < 500*time.Millisecond {
		remaining = 500 * time.Millisecond
	}
	watchDeadline := time.Now().Add(remaining)
	for time.Now().Before(watchDeadline) {
		if l := leaderAmong(c, minorityIDs); l != nil {
			t.Fatalf("seed %d: minority node %s became Leader during the partition", seed, l.ID())
		}
		for _, id := range minorityIDs {
			if got := c.NodeByID(id).CommitIndex(); got != initialCommit[id] {
				t.Fatalf("seed %d: minority node %s commitIndex advanced during the partition: %d -> %d",
					seed, id, initialCommit[id], got)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Heal and confirm convergence.
	for _, a := range minorityIDs {
		for _, b := range majorityIDs {
			c.Router.SetDrop(a, b, false)
			c.Router.SetDrop(b, a, false)
		}
	}

	stableLogs := waitForLogsToStabilize(t, c, 5*time.Second, 50*time.Millisecond)
	assertLogMatchingPairwise(t, c.Nodes)

	leaderLog := leader.LogEntries()
	for _, n := range c.Nodes {
		if !logsMatch(stableLogs[n.ID()], leaderLog) {
			t.Fatalf("seed %d: node %s did not converge to the majority leader's log", seed, n.ID())
		}
	}
}
