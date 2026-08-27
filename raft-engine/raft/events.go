package raft

import "time"

type EventType string

const (
	RoleChange EventType = "role_change"
	TermBump   EventType = "term_bump"
	LogAppend  EventType = "log_append"
	Commit     EventType = "commit"
)

// Event describes a single observable state transition, for consumers like
// a visualizer. raft/ declares this type itself rather than importing one
// from elsewhere, so nothing outside raft/ can create a dependency back
// into it through the event channel.
type Event struct {
	NodeID      string
	Type        EventType
	Term        int64
	Role        string
	LogLength   int
	CommitIndex int64
	Timestamp   time.Time
}

// SetEventSink registers a callback invoked (from its own goroutine) on
// every role change, term bump, leader log append, and commit-index
// advance. Safe to call at any time; nil disables emission.
func (n *Node) SetEventSink(sink func(Event)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.eventSink = sink
}

// emitEventLocked fires ev on the current sink, if any. Callers must
// already hold n.mu. The sink itself runs on a fresh goroutine so a slow
// or blocking consumer can never stall the caller's critical section.
func (n *Node) emitEventLocked(ev Event) {
	sink := n.eventSink
	if sink == nil {
		return
	}
	ev.NodeID = n.id
	ev.Timestamp = time.Now()
	go sink(ev)
}
