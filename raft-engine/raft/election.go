package raft

// updateTerm advances currentTerm if newTerm is strictly greater, resetting
// vote and role as Raft requires whenever a higher term is observed. Callers
// must already hold n.mu — this is an internal step of a larger locked
// operation (HandleRequestVote, HandleAppendEntries, election bookkeeping),
// never a standalone entry point.
func (n *Node) updateTerm(newTerm int64) bool {
	if newTerm <= n.currentTerm {
		return false
	}
	n.currentTerm = newTerm
	n.votedFor = ""
	n.role = Follower
	n.termHistory = append(n.termHistory, newTerm)
	return true
}

type RequestVoteArgs struct {
	Term         int64
	CandidateID  string
	LastLogIndex int64
	LastLogTerm  int64
}

type RequestVoteReply struct {
	Term        int64
	VoteGranted bool
}

// HandleRequestVote implements the RequestVote RPC: reject stale terms,
// adopt newer ones, and grant a vote only once per term to a candidate whose
// log is at least as up-to-date as this node's own.
func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
	}
	if args.Term > n.currentTerm {
		n.updateTerm(args.Term)
	}

	grant := (n.votedFor == "" || n.votedFor == args.CandidateID) &&
		n.candidateLogUpToDate(args.LastLogTerm, args.LastLogIndex)

	if grant {
		n.votedFor = args.CandidateID
	}

	return RequestVoteReply{Term: n.currentTerm, VoteGranted: grant}
}

// candidateLogUpToDate compares (term, index) per the Raft up-to-date rule:
// term is compared first, index only breaks ties within the same term.
// Callers must already hold n.mu.
func (n *Node) candidateLogUpToDate(candLastTerm, candLastIndex int64) bool {
	var myLastIndex, myLastTerm int64
	if len(n.log) > 0 {
		last := n.log[len(n.log)-1]
		myLastIndex, myLastTerm = last.Index, last.Term
	}

	if candLastTerm != myLastTerm {
		return candLastTerm > myLastTerm
	}
	return candLastIndex >= myLastIndex
}

// restart simulates a fresh process restart: the one legitimate, explicitly
// named case where currentTerm decreases, since v1 keeps no persistence
// across restarts. Never called from RPC-handling or election logic — only
// from test code or the process supervisor.
func (n *Node) restart() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.currentTerm = 0
	n.votedFor = ""
	n.termHistory = nil
	n.role = Follower
}
