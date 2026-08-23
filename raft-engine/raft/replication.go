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

// HandleAppendEntries implements enough of the AppendEntries RPC to support
// leader heartbeats and election suppression: term handling, stepping down
// to Follower, and resetting the election timer. Log matching and commit
// advancement are added in Session 4 (task 4.1).
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

	return AppendEntriesReply{Term: n.currentTerm, Success: true}
}
