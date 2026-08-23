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
