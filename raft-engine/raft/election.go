package raft

import (
	"context"
	"sync"
	"time"
)

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
		n.resetElectionTimerLocked()
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

// resetElectionTimer locks n.mu and (re)arms the election timer with a fresh
// random duration.
func (n *Node) resetElectionTimer() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.resetElectionTimerLocked()
}

// resetElectionTimerLocked (re)arms n.electionTimer with a new randomized
// duration in [electionTimeoutMin, electionTimeoutMax). Callers must already
// hold n.mu.
func (n *Node) resetElectionTimerLocked() {
	d := n.randomElectionTimeoutLocked()

	if n.electionTimer == nil {
		n.electionTimer = time.NewTimer(d)
		return
	}
	if !n.electionTimer.Stop() {
		select {
		case <-n.electionTimer.C:
		default:
		}
	}
	n.electionTimer.Reset(d)
}

// randomElectionTimeoutLocked samples a random duration in
// [electionTimeoutMin, electionTimeoutMax). Callers must already hold n.mu.
func (n *Node) randomElectionTimeoutLocked() time.Duration {
	span := int64(n.electionTimeoutMax - n.electionTimeoutMin)
	if span <= 0 {
		return n.electionTimeoutMin
	}
	return n.electionTimeoutMin + time.Duration(n.rng.Int63n(span))
}

// runElectionTimer is the background goroutine that turns election-timeout
// expiry into a Follower→Candidate transition. It must not hold n.mu while
// calling startElection, since that performs blocking network I/O.
func (n *Node) runElectionTimer(ctx context.Context) {
	n.mu.Lock()
	if n.electionTimer == nil {
		n.resetElectionTimerLocked()
	}
	n.mu.Unlock()

	for {
		n.mu.Lock()
		timer := n.electionTimer
		n.mu.Unlock()

		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			n.mu.Lock()
			shouldElect := n.role != Leader
			if shouldElect {
				n.becomeCandidateLocked()
			}
			n.resetElectionTimerLocked()
			n.mu.Unlock()

			if shouldElect {
				go n.startElection(ctx)
			}
		}
	}
}

// becomeCandidateLocked transitions Follower/Candidate → Candidate: bumps
// the term, votes for self. Callers must already hold n.mu.
func (n *Node) becomeCandidateLocked() {
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.termHistory = append(n.termHistory, n.currentTerm)
}

// voteRequestTimeout bounds how long startElection waits for a single peer's
// RequestVote reply before giving up on it.
func (n *Node) voteRequestTimeout() time.Duration {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.electionTimeoutMin < 200*time.Millisecond {
		return n.electionTimeoutMin
	}
	return 200 * time.Millisecond
}

// startElection sends RequestVote to every peer concurrently and waits for
// all replies (or their timeouts) before returning. Vote counting and the
// Candidate→Leader transition are added in task 3.4.
func (n *Node) startElection(ctx context.Context) {
	n.mu.Lock()
	term := n.currentTerm
	candidateID := n.id
	var lastIndex, lastTerm int64
	if len(n.log) > 0 {
		last := n.log[len(n.log)-1]
		lastIndex, lastTerm = last.Index, last.Term
	}
	peers := append([]string(nil), n.peers...)
	n.mu.Unlock()

	var wg sync.WaitGroup
	for _, peer := range peers {
		peer := peer
		wg.Add(1)
		go func() {
			defer wg.Done()

			rctx, cancel := context.WithTimeout(ctx, n.voteRequestTimeout())
			defer cancel()

			_, _ = n.transport.Send(rctx, peer, "RequestVote", RequestVoteArgs{
				Term:         term,
				CandidateID:  candidateID,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			})
		}()
	}
	wg.Wait()
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
