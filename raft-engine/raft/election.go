package raft

import (
	"context"
	"fmt"
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
	prevRole := n.role
	n.currentTerm = newTerm
	n.votedFor = ""
	n.role = Follower
	n.termHistory = append(n.termHistory, newTerm)
	n.emitEventLocked(Event{Type: TermBump, Term: newTerm, Role: n.role.String()})
	if prevRole != Follower {
		n.emitEventLocked(Event{Type: RoleChange, Term: newTerm, Role: n.role.String()})
	}
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

// resetElectionTimer asks the runElectionTimer goroutine to restart its
// timer with a fresh random duration. Safe to call from any goroutine and
// with or without n.mu held: it never touches a *time.Timer directly, only
// sends a coalescing, non-blocking signal.
func (n *Node) resetElectionTimer() {
	select {
	case n.resetElectionSignal <- struct{}{}:
	default:
	}
}

// resetElectionTimerLocked is an alias of resetElectionTimer kept for
// call sites inside functions that already hold n.mu — the signal send
// itself needs no locking, but naming it this way documents the calling
// convention at each site.
func (n *Node) resetElectionTimerLocked() {
	n.resetElectionTimer()
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
// expiry into a Follower→Candidate transition. It is the sole owner of its
// *time.Timer — no other goroutine ever calls Stop/Reset on it or receives
// from its channel, which is what makes concurrent resets from RPC handlers
// safe. It must not hold n.mu while calling startElection, since that
// performs blocking network I/O.
func (n *Node) runElectionTimer(ctx context.Context) {
	n.mu.Lock()
	d := n.randomElectionTimeoutLocked()
	n.mu.Unlock()

	timer := time.NewTimer(d)
	defer timer.Stop()

	drainAndReset := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		n.mu.Lock()
		d := n.randomElectionTimeoutLocked()
		n.mu.Unlock()
		timer.Reset(d)
	}

	for {
		select {
		case <-ctx.Done():
			return

		case <-n.resetElectionSignal:
			drainAndReset()

		case <-timer.C:
			n.mu.Lock()
			shouldElect := n.role != Leader
			if shouldElect {
				n.becomeCandidateLocked()
			}
			d := n.randomElectionTimeoutLocked()
			n.mu.Unlock()

			timer.Reset(d)

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
	n.emitEventLocked(Event{Type: TermBump, Term: n.currentTerm, Role: n.role.String()})
	n.emitEventLocked(Event{Type: RoleChange, Term: n.currentTerm, Role: n.role.String()})
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

// startElection sends RequestVote to every peer concurrently, counts
// vote_granted replies as they arrive, and transitions Candidate→Leader on
// the first majority (self-vote counts). A reply carrying a higher term
// steps this node down to Follower via updateTerm. Waits for all replies (or
// their timeouts) before returning.
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
	majority := n.majority()

	votes := 1 // self-vote
	wonImmediately := votes >= majority && n.becomeLeaderLocked(term)
	n.mu.Unlock()

	if wonImmediately {
		n.fireRoleChange(Leader, term)
		go n.startHeartbeats(ctx)
		return
	}
	if len(peers) == 0 {
		return
	}

	votesMu := &sync.Mutex{}
	decided := false

	var wg sync.WaitGroup
	for _, peer := range peers {
		peer := peer
		wg.Add(1)
		go func() {
			defer wg.Done()

			rctx, cancel := context.WithTimeout(ctx, n.voteRequestTimeout())
			reply, err := n.transport.Send(rctx, peer, "RequestVote", RequestVoteArgs{
				Term:         term,
				CandidateID:  candidateID,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			})
			cancel()
			if err != nil {
				return
			}
			rv, ok := reply.(RequestVoteReply)
			if !ok {
				return
			}

			n.mu.Lock()
			if rv.Term > n.currentTerm {
				n.updateTerm(rv.Term)
				n.mu.Unlock()
				return
			}
			if n.role != Candidate || n.currentTerm != term || !rv.VoteGranted {
				n.mu.Unlock()
				return
			}

			votesMu.Lock()
			votes++
			win := !decided && votes >= majority
			if win {
				decided = true
			}
			votesMu.Unlock()

			becameLeader := false
			if win {
				becameLeader = n.becomeLeaderLocked(term)
			}
			n.mu.Unlock()

			if becameLeader {
				n.fireRoleChange(Leader, term)
				go n.startHeartbeats(ctx)
			}
		}()
	}
	wg.Wait()
}

// becomeLeaderLocked transitions Candidate → Leader if this node is still a
// valid candidate for term (not superseded by a stepdown or a newer
// election). Callers must already hold n.mu, and must not perform
// side-effecting work (callbacks, network I/O) while still holding it.
func (n *Node) becomeLeaderLocked(term int64) bool {
	if n.role != Candidate || n.currentTerm != term {
		return false
	}
	n.role = Leader

	lastIndex := n.lastLogIndexLocked()
	n.nextIndex = make(map[string]int64, len(n.peers))
	n.matchIndex = make(map[string]int64, len(n.peers))
	n.replicatingTo = make(map[string]bool, len(n.peers))
	for _, p := range n.peers {
		n.nextIndex[p] = lastIndex + 1
		n.matchIndex[p] = 0
	}

	n.emitEventLocked(Event{Type: RoleChange, Term: term, Role: n.role.String()})
	return true
}

// fireRoleChange invokes the onRoleChange callback, if any. Must be called
// without holding n.mu.
func (n *Node) fireRoleChange(role Role, term int64) {
	if n.onRoleChange != nil {
		n.onRoleChange(role, term)
	}
}

// startHeartbeats runs as a goroutine for as long as this node remains
// leader of term, calling replicateTo for every peer on a fixed interval.
// This both suppresses followers' elections and, since replicateTo sends
// whatever backlog a peer's nextIndex says it's missing, is how a
// straggling follower catches up outside of an explicit Propose call.
func (n *Node) startHeartbeats(ctx context.Context) {
	n.mu.Lock()
	term := n.currentTerm
	interval := n.heartbeatInterval
	n.mu.Unlock()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.mu.Lock()
			stillLeader := n.role == Leader && n.currentTerm == term
			peers := append([]string(nil), n.peers...)
			n.mu.Unlock()

			if !stillLeader {
				return
			}

			for _, peer := range peers {
				peer := peer
				n.mu.Lock()
				if n.replicatingTo[peer] {
					// A previous tick's replicateTo call to this peer
					// hasn't finished yet — skip rather than pile up
					// another goroutine on top of it.
					n.mu.Unlock()
					continue
				}
				n.replicatingTo[peer] = true
				n.mu.Unlock()

				go func() {
					defer func() {
						n.mu.Lock()
						delete(n.replicatingTo, peer)
						n.mu.Unlock()
					}()
					n.replicateTo(ctx, peer)
				}()
			}
		}
	}
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

// Restart is the exported entry point for restart, reachable by
// legitimate process-supervisor code (the control plane's /restart
// endpoint) — restart itself stays unexported so nothing inside raft/ or
// any RPC-handling path can call it by accident.
func (n *Node) Restart() {
	n.restart()
}

// ConfirmLeadership performs a lightweight read-index check: one round of
// empty AppendEntries (heartbeats, no log entries) sent to every peer
// concurrently, returning nil once a majority (including this node) has
// acknowledged within ctx's deadline while still recognizing this node as
// leader of its current term. This is the basis for serving a
// linearizable read without appending a new log entry — callers must
// serve the read only after this returns nil, never speculatively.
func (n *Node) ConfirmLeadership(ctx context.Context) error {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return errNotLeader
	}
	term := n.currentTerm
	leaderID := n.id
	commit := n.commitIndex
	peers := append([]string(nil), n.peers...)
	majority := n.majority()
	n.mu.Unlock()

	if majority <= 1 {
		return nil // single-node cluster: this node alone is already a majority
	}

	var countMu sync.Mutex
	acks := 1 // self
	confirmed := make(chan struct{})
	var once sync.Once

	var wg sync.WaitGroup
	for _, peer := range peers {
		peer := peer
		wg.Add(1)
		go func() {
			defer wg.Done()
			reply, err := n.transport.Send(ctx, peer, "AppendEntries", AppendEntriesArgs{
				Term:         term,
				LeaderID:     leaderID,
				LeaderCommit: commit,
			})
			if err != nil {
				return
			}
			ar, ok := reply.(AppendEntriesReply)
			if !ok || ar.Term > term || !ar.Success {
				return
			}

			countMu.Lock()
			acks++
			gotMajority := acks >= majority
			countMu.Unlock()
			if gotMajority {
				once.Do(func() { close(confirmed) })
			}
		}()
	}
	go wg.Wait()

	select {
	case <-confirmed:
		n.mu.Lock()
		stillLeader := n.role == Leader && n.currentTerm == term
		n.mu.Unlock()
		if !stillLeader {
			return errNotLeader
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("raft: could not confirm majority leadership within timeout: %w", ctx.Err())
	}
}
