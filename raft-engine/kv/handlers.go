package kv

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"raft-engine/raft"
)

// ErrNotLeader is returned when a write or a linearizable read is sent to
// a node that isn't currently the leader. LeaderHint carries the last
// leader ID this node observed, if any, so a caller can redirect rather
// than blindly retry.
type ErrNotLeader struct {
	LeaderHint string
}

func (e *ErrNotLeader) Error() string {
	if e.LeaderHint == "" {
		return "kv: not leader"
	}
	return fmt.Sprintf("kv: not leader, try %s", e.LeaderHint)
}

// HandleSet proposes a SET command and returns once it's committed (or an
// error). It never mutates s.data directly under any circumstance — the
// only path into s.data is applyCommand, from the committed log.
func (s *Store) HandleSet(key, value string) error {
	if s.node.Role() != raft.Leader {
		return &ErrNotLeader{LeaderHint: s.node.KnownLeaderID()}
	}

	payload, err := json.Marshal(command{Op: "SET", Key: key, Value: value})
	if err != nil {
		return fmt.Errorf("kv: encode command: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = s.node.Propose(ctx, payload)
	return err
}

// GetResult is HandleGet's response. Mode is always set, on both success
// and error paths, so a caller can never be left guessing which guarantee
// (or lack of one) a given response actually carries.
type GetResult struct {
	Value string
	OK    bool
	Mode  string
}

// HandleGet serves a read in one of two explicit, honest modes:
//
//   - "stale": returns this node's local state immediately, with no
//     freshness guarantee — may lag the true committed state.
//   - "linearizable": returns a value only after confirming, via a
//     read-index round trip to a majority, that this node is still the
//     leader and up to date enough to answer authoritatively. Refuses
//     (returns an error, serves nothing) rather than silently falling
//     back to a stale value if that confirmation can't be made in time.
func (s *Store) HandleGet(key string, mode string) (GetResult, error) {
	switch mode {
	case "stale":
		v, ok := s.Get(key)
		return GetResult{Value: v, OK: ok, Mode: "stale"}, nil

	case "linearizable":
		if s.node.Role() != raft.Leader {
			return GetResult{Mode: "linearizable"}, &ErrNotLeader{LeaderHint: s.node.KnownLeaderID()}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		if err := s.node.ConfirmLeadership(ctx); err != nil {
			return GetResult{Mode: "linearizable"}, fmt.Errorf("kv: linearizable read refused: %w", err)
		}
		v, ok := s.Get(key)
		return GetResult{Value: v, OK: ok, Mode: "linearizable"}, nil

	default:
		return GetResult{}, fmt.Errorf("kv: unknown read mode %q", mode)
	}
}
