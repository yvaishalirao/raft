package test

import (
	"sync"
	"testing"
	"time"

	"raft-engine/raft"
)

// TestReplication_ExtendedFaultMix is a hardening test, not a new
// invariant: it reuses the fault-mix scenario from
// TestStateMachineSafety_ExtendedRandomizedRun on a fresh NewCluster, then
// additionally runs the Log Matching pairwise check from 4.1 and confirms
// commitIndex non-regression from 4.2 — catching bugs that only manifest
// when multiple fault types interact within one run, rather than in
// isolated per-invariant tests. Run with -count=20 as a small
// property-style regression.
func TestReplication_ExtendedFaultMix(t *testing.T) {
	var mu sync.Mutex
	applied := make(map[string]map[int64][]byte)  // nodeID -> index -> command
	commitHistory := make(map[*raft.Node][]int64) // node object -> observed commitIndex sequence

	c := NewCluster(t, 5,
		WithApplyObserver(func(nodeID string, e raft.LogEntry) {
			mu.Lock()
			defer mu.Unlock()
			if applied[nodeID] == nil {
				applied[nodeID] = make(map[int64][]byte)
			}
			applied[nodeID][e.Index] = append([]byte(nil), e.Command...)
		}),
	)
	defer c.Shutdown()

	// A fresh seed each repetition (this function may run many times under
	// -count=N) so each pass explores a different fault interleaving,
	// rather than replaying the exact same scenario 20 times.
	seed := time.Now().UnixNano()

	runFaultMixScenario(c, seed, 50, func() {
		// Called from the scenario's own goroutine, same one that calls
		// KillNode/Revive — safe to read c.Nodes without a data race. A
		// revived node is a brand-new *raft.Node, so keying by pointer
		// naturally starts a fresh history rather than treating its reset
		// commitIndex as a regression of the node it replaced.
		for _, n := range c.Nodes {
			commitHistory[n] = append(commitHistory[n], n.CommitIndex())
		}
	})

	// Check 1: Log Matching pairwise (task 4.1).
	assertLogMatchingPairwise(t, c.Nodes)

	// Check 2: State machine safety (task 4.4).
	mu.Lock()
	assertStateMachineSafety(t, applied)
	mu.Unlock()

	// Check 3: commitIndex non-regression (task 4.2), within each node
	// object's own observed lifetime.
	for n, history := range commitHistory {
		for i := 1; i < len(history); i++ {
			if history[i] < history[i-1] {
				t.Fatalf("commitIndex regressed on node %s: %v", n.ID(), history)
			}
		}
	}
}
