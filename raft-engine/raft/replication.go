package raft

import "context"

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
	n.knownLeaderID = args.LeaderID
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

// termAtIndexLocked returns the term of the entry at index, or 0 for
// index<=0 (the implicit "before the log" sentinel) or a missing index.
// Callers must already hold n.mu.
func (n *Node) termAtIndexLocked(index int64) int64 {
	if index <= 0 {
		return 0
	}
	if e, ok := n.entryAtLocked(index); ok {
		return e.Term
	}
	return 0
}

// replicateTo sends AppendEntries to peer carrying whatever entries
// nextIndex[peer] says it's missing (which is empty — a plain heartbeat —
// once the peer is caught up). On success it advances nextIndex/matchIndex
// and tries to advance commitIndex; on a log conflict it backs nextIndex up
// to the follower-reported ConflictIndex and retries, since a single
// RPC's timeout or one node's temporary unavailability shouldn't make a
// caller wait indefinitely, it gives up (returns) on any Send error and
// relies on the next periodic heartbeat tick or Propose call to retry.
func (n *Node) replicateTo(ctx context.Context, peer string) {
	for {
		n.mu.Lock()
		if n.role != Leader {
			n.mu.Unlock()
			return
		}
		term := n.currentTerm
		leaderID := n.id
		leaderCommit := n.commitIndex

		next := n.nextIndex[peer]
		if next < 1 {
			next = 1
		}
		prevIndex := next - 1
		prevTerm := n.termAtIndexLocked(prevIndex)

		var entries []LogEntry
		for _, e := range n.log {
			if e.Index >= next {
				entries = append(entries, e)
			}
		}
		n.mu.Unlock()

		rctx, cancel := context.WithTimeout(ctx, n.heartbeatInterval)
		reply, err := n.transport.Send(rctx, peer, "AppendEntries", AppendEntriesArgs{
			Term:         term,
			LeaderID:     leaderID,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		})
		cancel()
		if err != nil {
			return
		}
		ar, ok := reply.(AppendEntriesReply)
		if !ok {
			return
		}

		n.mu.Lock()
		if ar.Term > n.currentTerm {
			n.updateTerm(ar.Term)
			n.mu.Unlock()
			return
		}
		if n.role != Leader || n.currentTerm != term {
			n.mu.Unlock()
			return
		}

		if ar.Success {
			if len(entries) > 0 {
				n.matchIndex[peer] = entries[len(entries)-1].Index
				n.nextIndex[peer] = n.matchIndex[peer] + 1
			}
			n.advanceCommitIndexLocked()
			n.mu.Unlock()
			return
		}

		// Conflict: back nextIndex up to what the follower reported and
		// retry immediately, rather than decrementing one entry at a time.
		if ar.ConflictIndex > 0 {
			n.nextIndex[peer] = ar.ConflictIndex
		} else if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
		n.mu.Unlock()
	}
}

// advanceCommitIndexLocked implements the Raft leader commit rule: find the
// highest index N such that a majority of matchIndex[peer] >= N (the leader
// itself always counts as caught up on its own log) AND log[N].Term ==
// currentTerm. That last check is the crucial safety rule — a leader may
// never commit an entry from a prior term by replication count alone; it
// commits such entries only indirectly, as a side effect of a later
// current-term entry committing. Callers must already hold n.mu.
func (n *Node) advanceCommitIndexLocked() {
	majority := n.majority()

	for N := n.lastLogIndexLocked(); N > n.commitIndex; N-- {
		entry, ok := n.entryAtLocked(N)
		if !ok || entry.Term != n.currentTerm {
			continue
		}

		count := 1 // the leader's own log
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= N {
				count++
			}
		}
		if count >= majority {
			n.commitIndex = N
			return
		}
	}
}
