package raft

type AppendEntriesArgs struct {
	Term         int64
	LeaderID     string
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []LogEntry
	LeaderCommit int64
}

type AppendEntriesReply struct {
	Term          int64
	Success       bool
	ConflictIndex int64
}

// HandleAppendEntries implements the AppendEntries RPC: term handling and
// election suppression (as heartbeats need), the prev-log consistency
// check, conflict truncation before appending new entries, and commit index
// advancement.
func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		return AppendEntriesReply{Term: n.currentTerm, Success: false}
	}
	if args.Term > n.currentTerm {
		n.updateTerm(args.Term)
	}

	n.role = Follower
	n.resetElectionTimerLocked()

	if args.PrevLogIndex > 0 {
		entry, ok := n.entryAtLocked(args.PrevLogIndex)
		if !ok {
			return AppendEntriesReply{
				Term:          n.currentTerm,
				Success:       false,
				ConflictIndex: n.nextIndexAfterLastLocked(),
			}
		}
		if entry.Term != args.PrevLogTerm {
			return AppendEntriesReply{
				Term:          n.currentTerm,
				Success:       false,
				ConflictIndex: n.firstIndexOfTermLocked(entry.Term),
			}
		}
	}

	// Truncate any conflicting entry before appending, in the same critical
	// section, so a concurrent reader never observes an inconsistent log.
	for _, newEntry := range args.Entries {
		newEntry := newEntry
		existing, ok := n.entryAtLocked(newEntry.Index)
		switch {
		case ok && existing.Term != newEntry.Term:
			n.truncateFrom(newEntry.Index)
			n.mutateLog("append", func() { n.log = append(n.log, newEntry) })
		case !ok:
			n.mutateLog("append", func() { n.log = append(n.log, newEntry) })
		}
		// else: we already have this exact (index, term) entry — a retried
		// or duplicate RPC — nothing to do.
	}

	lastNewIndex := args.PrevLogIndex
	if len(args.Entries) > 0 {
		lastNewIndex = args.Entries[len(args.Entries)-1].Index
	}
	if args.LeaderCommit > n.commitIndex {
		newCommit := args.LeaderCommit
		if lastNewIndex < newCommit {
			newCommit = lastNewIndex
		}
		if newCommit > n.commitIndex {
			n.commitIndex = newCommit
		}
	}

	return AppendEntriesReply{Term: n.currentTerm, Success: true}
}

// entryAtLocked returns the log entry with the given Raft index (1-based),
// or false if it doesn't exist. Index 0 is the implicit "before the log"
// sentinel and never matches. Callers must already hold n.mu.
func (n *Node) entryAtLocked(index int64) (LogEntry, bool) {
	if index <= 0 {
		return LogEntry{}, false
	}
	for _, e := range n.log {
		if e.Index == index {
			return e, true
		}
	}
	return LogEntry{}, false
}

// firstIndexOfTermLocked returns the earliest log index carrying term, used
// for the AppendEntries conflict-index backtracking optimization. Callers
// must already hold n.mu.
func (n *Node) firstIndexOfTermLocked(term int64) int64 {
	for _, e := range n.log {
		if e.Term == term {
			return e.Index
		}
	}
	return 0
}

// nextIndexAfterLastLocked returns one past this node's last log entry —
// used as ConflictIndex when the leader's PrevLogIndex is beyond our log.
// Callers must already hold n.mu.
func (n *Node) nextIndexAfterLastLocked() int64 {
	if len(n.log) == 0 {
		return 1
	}
	return n.log[len(n.log)-1].Index + 1
}

// LogEntries returns a snapshot copy of this node's log. Safe for
// concurrent use.
func (n *Node) LogEntries() []LogEntry {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]LogEntry(nil), n.log...)
}
