package test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"raft-engine/raft"
)

const electionSafetyTrialDuration = 2 * time.Second

// TestElectionSafety_PropertyRandomized is the single most-probed guarantee
// in this project: across many randomized timing seeds, no term ever has
// more than one leader. It fails immediately, naming the offending seed and
// term, on the first violation — it does not just count failures.
func TestElectionSafety_PropertyRandomized(t *testing.T) {
	for i := 0; i < 200; i++ {
		seed := int64(i)

		leadersByTerm := runElectionSafetyTrial(t, seed)

		for term, nodeIDs := range leadersByTerm {
			if len(nodeIDs) > 1 {
				t.Fatalf("seed %d: term %d had %d leaders (%v) — election safety violated",
					seed, term, len(nodeIDs), nodeIDs)
			}
		}
	}
}

// TestElectionSafety_Deterministic confirms the property trial itself is
// deterministic given a fixed seed, which is what makes seed numbers in a
// TestElectionSafety_PropertyRandomized failure message reproducible.
func TestElectionSafety_Deterministic(t *testing.T) {
	const seed = int64(12345)

	first := runElectionSafetyTrial(t, seed)
	second := runElectionSafetyTrial(t, seed)

	firstSummary := summarizeLeadersByTerm(first)
	secondSummary := summarizeLeadersByTerm(second)

	if firstSummary != secondSummary {
		t.Fatalf("same seed produced different outcomes:\n  run 1: %s\n  run 2: %s", firstSummary, secondSummary)
	}
}

// runElectionSafetyTrial builds a fresh 5-node cluster seeded from seed,
// lets it run for electionSafetyTrialDuration, and returns every observed
// role=Leader event grouped by term.
func runElectionSafetyTrial(t *testing.T, seed int64) map[int64][]string {
	t.Helper()

	var mu sync.Mutex
	leadersByTerm := make(map[int64][]string)

	c := NewCluster(t, 5,
		WithSeed(seed),
		WithRoleChangeObserver(func(nodeID string, role raft.Role, term int64) {
			if role != raft.Leader {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			leadersByTerm[term] = append(leadersByTerm[term], nodeID)
		}),
	)
	defer c.Shutdown()

	time.Sleep(electionSafetyTrialDuration)

	mu.Lock()
	defer mu.Unlock()
	result := make(map[int64][]string, len(leadersByTerm))
	for term, ids := range leadersByTerm {
		result[term] = append([]string(nil), ids...)
	}
	return result
}

func summarizeLeadersByTerm(leadersByTerm map[int64][]string) string {
	return fmt.Sprintf("%d terms observed leaders: %v", len(leadersByTerm), leadersByTerm)
}
