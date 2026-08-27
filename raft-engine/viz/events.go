package viz

import (
	"time"

	"raft-engine/raft"
)

type EventType string

const (
	RoleChange EventType = "role_change"
	TermBump   EventType = "term_bump"
	LogAppend  EventType = "log_append"
	Commit     EventType = "commit"
)

type Event struct {
	NodeID      string
	Type        EventType
	Term        int64
	Role        string
	LogLength   int
	CommitIndex int64
	Timestamp   time.Time
}

// FromRaftEvent converts a raft.Event into the visualizer's own Event type.
// The two are declared independently on purpose: raft/ must never import
// viz/, so it defines its own equivalent type instead of this one.
func FromRaftEvent(ev raft.Event) Event {
	return Event{
		NodeID:      ev.NodeID,
		Type:        EventType(ev.Type),
		Term:        ev.Term,
		Role:        ev.Role,
		LogLength:   ev.LogLength,
		CommitIndex: ev.CommitIndex,
		Timestamp:   ev.Timestamp,
	}
}
