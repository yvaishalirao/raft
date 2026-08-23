package raft

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// fakeTransport is a minimal raft.Transport test double: it records every
// Send call and returns a fixed reply/error, with no real delivery.
type fakeTransport struct {
	mu   sync.Mutex
	id   string
	sent []fakeSentCall

	reply any
	err   error

	// sendFunc, if set, overrides reply/err and lets a test vary the
	// response per target (e.g. some peers grant, some don't).
	sendFunc func(target, rpcType string, args any) (any, error)
}

type fakeSentCall struct {
	target  string
	rpcType string
	args    any
}

func (f *fakeTransport) LocalID() string { return f.id }

func (f *fakeTransport) Send(ctx context.Context, target, rpcType string, args any) (any, error) {
	f.mu.Lock()
	f.sent = append(f.sent, fakeSentCall{target: target, rpcType: rpcType, args: args})
	f.mu.Unlock()

	if f.sendFunc != nil {
		return f.sendFunc(target, rpcType, args)
	}
	return f.reply, f.err
}

func (f *fakeTransport) sentCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sent)
}

func TestTerm_Monotonic(t *testing.T) {
	n := NewNode("node-0", nil, nil)

	terms := []int64{1, 3, 3, 7, 7, 7, 10}
	prev := n.currentTerm

	for _, term := range terms {
		n.updateTerm(term)
		if n.currentTerm < prev {
			t.Fatalf("term decreased: %d -> %d", prev, n.currentTerm)
		}
		prev = n.currentTerm
	}

	if n.currentTerm != 10 {
		t.Fatalf("expected final term 10, got %d", n.currentTerm)
	}
}

func TestTerm_RejectsLower(t *testing.T) {
	n := NewNode("node-0", nil, nil)
	n.updateTerm(5)

	if ok := n.updateTerm(5); ok {
		t.Fatal("expected updateTerm to reject an equal term")
	}
	if n.currentTerm != 5 {
		t.Fatalf("term should be unchanged at 5, got %d", n.currentTerm)
	}

	if ok := n.updateTerm(3); ok {
		t.Fatal("expected updateTerm to reject a lower term")
	}
	if n.currentTerm != 5 {
		t.Fatalf("term should be unchanged at 5, got %d", n.currentTerm)
	}
}

func TestVote_FirstGranted(t *testing.T) {
	n := NewNode("node-0", nil, nil)

	reply := n.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node-1"})

	if !reply.VoteGranted {
		t.Fatal("expected first vote in a fresh term to be granted")
	}
	if n.votedFor != "node-1" {
		t.Fatalf("expected votedFor=node-1, got %q", n.votedFor)
	}
}

func TestVote_GrantedOnce(t *testing.T) {
	n := NewNode("node-0", nil, nil)

	first := n.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node-1"})
	if !first.VoteGranted {
		t.Fatal("expected first vote to be granted")
	}

	second := n.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node-2"})
	if second.VoteGranted {
		t.Fatal("expected second vote request in the same term, different candidate, to be rejected")
	}

	// Re-voting for the SAME candidate within the same term stays idempotent.
	repeat := n.HandleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "node-1"})
	if !repeat.VoteGranted {
		t.Fatal("expected a retried request from the already-voted-for candidate to remain granted")
	}
}

func TestVote_RejectsStaleTerm(t *testing.T) {
	n := NewNode("node-0", nil, nil)
	n.updateTerm(5)

	reply := n.HandleRequestVote(RequestVoteArgs{Term: 3, CandidateID: "node-1"})

	if reply.VoteGranted {
		t.Fatal("expected a stale-term request to be rejected")
	}
	if reply.Term != 5 {
		t.Fatalf("expected reply term 5, got %d", reply.Term)
	}
}

func TestVote_RejectsStaleLog(t *testing.T) {
	n := NewNode("node-0", nil, nil)
	n.log = []LogEntry{{Index: 1, Term: 5}, {Index: 2, Term: 5}}

	reply := n.HandleRequestVote(RequestVoteArgs{
		Term:         6,
		CandidateID:  "node-1",
		LastLogIndex: 1,
		LastLogTerm:  3,
	})

	if reply.VoteGranted {
		t.Fatal("expected a candidate with a stale log to be rejected even in a fresh term")
	}
}

func TestTerm_RestartResets(t *testing.T) {
	n := NewNode("node-0", nil, nil)
	n.updateTerm(9)
	n.votedFor = "node-1"

	n.restart()

	if n.currentTerm != 0 {
		t.Fatalf("expected currentTerm 0 after restart, got %d", n.currentTerm)
	}
	if n.votedFor != "" {
		t.Fatalf("expected votedFor cleared after restart, got %q", n.votedFor)
	}
	if n.termHistory != nil {
		t.Fatalf("expected termHistory cleared after restart, got %v", n.termHistory)
	}
}

func TestTimer_RandomizedRange(t *testing.T) {
	n := NewNode("node-0", []string{"node-1"}, &fakeTransport{id: "node-0"},
		WithElectionTimeout(150*time.Millisecond, 300*time.Millisecond),
		WithRandSource(rand.NewSource(42)),
	)

	seen := map[time.Duration]bool{}
	for i := 0; i < 100; i++ {
		n.mu.Lock()
		d := n.randomElectionTimeoutLocked()
		n.mu.Unlock()

		if d < 150*time.Millisecond || d >= 300*time.Millisecond {
			t.Fatalf("sample %d: timeout %v out of range [150ms,300ms)", i, d)
		}
		seen[d] = true
	}

	if len(seen) < 2 {
		t.Fatal("expected variance across 100 samples, got a constant value")
	}
}

func TestCandidate_IncrementsTermAndVotesSelf(t *testing.T) {
	// A peer with no reply wired up (fakeTransport.reply stays nil) means
	// the type assertion in startElection fails and no vote is ever
	// counted, so this node stays observably in Candidate rather than
	// racing straight through to Leader with zero peers.
	n := NewNode("node-0", []string{"node-1"}, &fakeTransport{id: "node-0"},
		WithElectionTimeout(10*time.Millisecond, 20*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.runElectionTimer(ctx)

	deadline := time.After(2 * time.Second)
	for {
		if n.Role() == Candidate {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for Follower to become Candidate")
		case <-time.After(5 * time.Millisecond):
		}
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.currentTerm != 1 {
		t.Fatalf("expected currentTerm 1 after first election timeout, got %d", n.currentTerm)
	}
	if n.votedFor != "node-0" {
		t.Fatalf("expected votedFor=node-0 (self), got %q", n.votedFor)
	}
}

func TestElection_SendsToAllPeers(t *testing.T) {
	ft := &fakeTransport{id: "node-0", reply: RequestVoteReply{VoteGranted: false}}
	n := NewNode("node-0", []string{"node-1", "node-2", "node-3", "node-4"}, ft)

	n.startElection(context.Background())

	if got := ft.sentCount(); got != 4 {
		t.Fatalf("expected RequestVote sent to all 4 peers, got %d calls", got)
	}
}

func TestElection_MajorityWins(t *testing.T) {
	// 5-node cluster: self + 2 external grants = 3/5, the majority.
	peers := []string{"node-1", "node-2", "node-3", "node-4"}
	grant := map[string]bool{"node-1": true, "node-2": true}

	ft := &fakeTransport{
		id: "node-0",
		sendFunc: func(target, rpcType string, args any) (any, error) {
			return RequestVoteReply{Term: 1, VoteGranted: grant[target]}, nil
		},
	}
	n := NewNode("node-0", peers, ft)

	n.mu.Lock()
	n.becomeCandidateLocked()
	n.mu.Unlock()

	n.startElection(context.Background())

	if got := n.Role(); got != Leader {
		t.Fatalf("expected role Leader with 3/5 grants, got %v", got)
	}
}

func TestElection_MinorityLoses(t *testing.T) {
	// 5-node cluster: self + 1 external grant = 2/5, short of the majority (3).
	peers := []string{"node-1", "node-2", "node-3", "node-4"}
	grant := map[string]bool{"node-1": true}

	ft := &fakeTransport{
		id: "node-0",
		sendFunc: func(target, rpcType string, args any) (any, error) {
			return RequestVoteReply{Term: 1, VoteGranted: grant[target]}, nil
		},
	}
	n := NewNode("node-0", peers, ft)

	n.mu.Lock()
	n.becomeCandidateLocked()
	n.mu.Unlock()

	n.startElection(context.Background())

	if got := n.Role(); got != Candidate {
		t.Fatalf("expected role to remain Candidate with only 2/5 grants, got %v", got)
	}
}

func TestElection_StepsDownOnHigherTerm(t *testing.T) {
	ft := &fakeTransport{
		id: "node-0",
		sendFunc: func(target, rpcType string, args any) (any, error) {
			return RequestVoteReply{Term: 99, VoteGranted: false}, nil
		},
	}
	n := NewNode("node-0", []string{"node-1"}, ft)

	n.mu.Lock()
	n.becomeCandidateLocked()
	n.mu.Unlock()

	n.startElection(context.Background())

	if got := n.Role(); got != Follower {
		t.Fatalf("expected role Follower after a higher-term reply, got %v", got)
	}
	n.mu.Lock()
	term := n.currentTerm
	n.mu.Unlock()
	if term != 99 {
		t.Fatalf("expected currentTerm updated to 99, got %d", term)
	}
}

func TestHeartbeat_SuppressesElection(t *testing.T) {
	n := NewNode("node-0", nil, &fakeTransport{id: "node-0"},
		WithElectionTimeout(50*time.Millisecond, 80*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.runElectionTimer(ctx)

	stop := time.After(1 * time.Second)
	heartbeat := time.NewTicker(20 * time.Millisecond)
	defer heartbeat.Stop()

	for {
		select {
		case <-stop:
			if got := n.Role(); got != Follower {
				t.Fatalf("expected role to remain Follower under steady heartbeats, got %v", got)
			}
			return
		case <-heartbeat.C:
			n.HandleAppendEntries(AppendEntriesArgs{Term: 1, LeaderID: "node-1"})
		}
	}
}
