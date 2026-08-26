package raft

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	DefaultElectionTimeoutMin = 150 * time.Millisecond
	DefaultElectionTimeoutMax = 300 * time.Millisecond
	DefaultHeartbeatInterval  = 50 * time.Millisecond
)

var (
	errUnexpectedRPCArgs   = errors.New("raft: unexpected RPC argument type")
	errUnknownRPCType      = errors.New("raft: unknown RPC type")
	errNotLeader           = errors.New("raft: not leader")
	errProposeNotCommitted = errors.New("raft: entry not committed before leadership ended or context was done")
)

type Node struct {
	mu sync.Mutex

	id          string
	currentTerm int64
	votedFor    string
	role        Role
	log         []LogEntry
	commitIndex int64
	lastApplied int64
	termHistory []int64

	// knownLeaderID is updated whenever this node observes a valid
	// AppendEntries carrying a current-or-newer term, so a follower can
	// point a misdirected client at the leader it currently knows about.
	knownLeaderID string

	peers     []string
	transport Transport

	// nextIndex and matchIndex are leader-only replication state, reset
	// every time this node wins an election (see becomeLeaderLocked).
	// replicatingTo tracks which peers currently have an in-flight
	// replicateTo call, bounding startHeartbeats to at most one concurrent
	// goroutine per peer rather than piling more up on every tick.
	nextIndex     map[string]int64
	matchIndex    map[string]int64
	replicatingTo map[string]bool

	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
	heartbeatInterval  time.Duration
	rng                *rand.Rand

	// resetElectionSignal lets other goroutines (RPC handlers) ask the
	// runElectionTimer goroutine to restart its timer, without ever touching
	// the *time.Timer itself from outside its owning goroutine — sharing a
	// single Timer's channel across goroutines is a well-known Go race.
	resetElectionSignal chan struct{}

	onRoleChange func(Role, int64)
	applyFunc    func(LogEntry)
}

// NodeOption configures optional Node behavior at construction time.
type NodeOption func(*Node)

// WithElectionTimeout overrides the default randomized election timeout range.
func WithElectionTimeout(min, max time.Duration) NodeOption {
	return func(n *Node) {
		n.electionTimeoutMin = min
		n.electionTimeoutMax = max
	}
}

// WithHeartbeatInterval overrides the default leader heartbeat interval.
func WithHeartbeatInterval(d time.Duration) NodeOption {
	return func(n *Node) {
		n.heartbeatInterval = d
	}
}

// WithRandSource overrides the source of randomness backing election-timeout
// jitter, enabling deterministic tests.
func WithRandSource(src rand.Source) NodeOption {
	return func(n *Node) {
		n.rng = rand.New(src)
	}
}

// WithOnRoleChange registers a callback invoked whenever the node's role
// changes, carrying the new role and the term at which it changed.
func WithOnRoleChange(fn func(Role, int64)) NodeOption {
	return func(n *Node) {
		n.onRoleChange = fn
	}
}

// WithApplyFunc registers the state-machine apply callback. When set, Run
// starts a background loop that calls fn, in order, for every newly
// committed log entry. When unset, no apply loop runs.
func WithApplyFunc(fn func(LogEntry)) NodeOption {
	return func(n *Node) {
		n.applyFunc = fn
	}
}

func NewNode(id string, peers []string, transport Transport, opts ...NodeOption) *Node {
	n := &Node{
		id:                  id,
		peers:               peers,
		transport:           transport,
		role:                Follower,
		electionTimeoutMin:  DefaultElectionTimeoutMin,
		electionTimeoutMax:  DefaultElectionTimeoutMax,
		heartbeatInterval:   DefaultHeartbeatInterval,
		rng:                 rand.New(rand.NewSource(time.Now().UnixNano())),
		resetElectionSignal: make(chan struct{}, 1),
	}
	for _, opt := range opts {
		opt(n)
	}
	return n
}

// SetApplyFunc registers the apply-loop callback after construction — for
// callers (like kv.NewStore) that need an existing *Node to build their
// callback from. Must be called before Run(ctx) starts.
func (n *Node) SetApplyFunc(fn func(LogEntry)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.applyFunc = fn
}

// Run starts the election timer, the apply loop (if an apply function was
// configured), and then dispatches every inbound RPC to its handler until
// ctx is done.
func (n *Node) Run(ctx context.Context) error {
	go n.runElectionTimer(ctx)

	n.mu.Lock()
	applyFn := n.applyFunc
	n.mu.Unlock()
	if applyFn != nil {
		go n.runApplyLoop(ctx, applyFn)
	}

	for {
		rpc, ok := n.transport.Recv(ctx)
		if !ok {
			return nil
		}

		switch rpc.Type {
		case "RequestVote":
			args, ok := rpc.Args.(RequestVoteArgs)
			if !ok {
				rpc.Reply(nil, errUnexpectedRPCArgs)
				continue
			}
			rpc.Reply(n.HandleRequestVote(args), nil)

		case "AppendEntries":
			args, ok := rpc.Args.(AppendEntriesArgs)
			if !ok {
				rpc.Reply(nil, errUnexpectedRPCArgs)
				continue
			}
			rpc.Reply(n.HandleAppendEntries(args), nil)

		default:
			rpc.Reply(nil, errUnknownRPCType)
		}
	}
}

// Role returns the node's current role. Safe for concurrent use.
func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// CommitIndex returns the node's current commit index. Safe for concurrent use.
func (n *Node) CommitIndex() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// majority returns the number of votes/replicas needed for a majority of
// the full, fixed, configured cluster size (len(n.peers)+1, including this
// node) — never a count of peers that happen to be reachable or have
// responded right now. n.peers is set once at construction and never
// shrinks, so this is always the same value regardless of live reachability
// (Invariant I-08: computing majority from reachable-peer count is exactly
// the bug that lets a minority partition believe it has quorum).
func (n *Node) majority() int {
	return (len(n.peers)+1)/2 + 1
}

// Term returns the node's current term. Safe for concurrent use.
func (n *Node) Term() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

// ID returns the node's own ID.
func (n *Node) ID() string {
	return n.id
}

// KnownLeaderID returns the ID of the leader this node most recently
// observed a valid AppendEntries from, or "" if none yet. Lets a follower
// redirect a misdirected client rather than just rejecting it blind.
func (n *Node) KnownLeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.knownLeaderID
}

// ProposeAsync appends command to the leader's log and returns immediately
// with the assigned index, without waiting for replication or commit.
// This exists for tests exercising apply-after-commit-only behavior —
// production write paths must use the commit-blocking Propose, or a
// client could observe success before durability is guaranteed.
func (n *Node) ProposeAsync(command []byte) (int64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Leader {
		return 0, errNotLeader
	}
	newIndex := n.lastLogIndexLocked() + 1
	n.appendAsLeader(LogEntry{Index: newIndex, Term: n.currentTerm, Command: command})
	return newIndex, nil
}

// Propose appends command to the leader's log and blocks until it has been
// replicated to a majority (i.e. committed), ctx is done, or this node
// stops being the leader of the term it proposed under — whichever comes
// first. Returns the log index the entry was assigned.
func (n *Node) Propose(ctx context.Context, command []byte) (int64, error) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return 0, errNotLeader
	}
	term := n.currentTerm
	newIndex := n.lastLogIndexLocked() + 1
	n.appendAsLeader(LogEntry{Index: newIndex, Term: term, Command: command})
	// Handle the case where this node alone is already a majority (e.g. a
	// single-node cluster) — advanceCommitIndexLocked is otherwise only
	// triggered from a peer's reply in replicateTo, which never runs if
	// there are no peers to replicate to.
	n.advanceCommitIndexLocked()
	peers := append([]string(nil), n.peers...)
	n.mu.Unlock()

	for _, peer := range peers {
		peer := peer
		go n.replicateTo(ctx, peer)
	}

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		n.mu.Lock()
		committed := n.commitIndex >= newIndex
		stillCurrentLeader := n.role == Leader && n.currentTerm == term
		n.mu.Unlock()

		if committed {
			return newIndex, nil
		}
		if !stillCurrentLeader {
			return newIndex, errProposeNotCommitted
		}

		select {
		case <-ctx.Done():
			return newIndex, ctx.Err()
		case <-ticker.C:
		}
	}
}

// runApplyLoop watches commitIndex and calls apply, in strict index order,
// for each newly committed entry, updating lastApplied as it goes. apply is
// never called for an index beyond commitIndex at the time it's read. Runs
// until ctx is done.
func (n *Node) runApplyLoop(ctx context.Context, apply func(LogEntry)) {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for n.tryApplyNext(apply) {
			}
		}
	}
}

// tryApplyNext applies the next committed-but-unapplied entry, if any.
// Returns true if it applied one (the caller should call again to drain
// any further backlog), false if there's nothing left to apply right now.
func (n *Node) tryApplyNext(apply func(LogEntry)) bool {
	n.mu.Lock()
	if n.lastApplied >= n.commitIndex {
		n.mu.Unlock()
		return false
	}
	nextIndex := n.lastApplied + 1
	n.assertNotBeyondCommitLocked(nextIndex)
	entry, ok := n.entryAtLocked(nextIndex)
	n.mu.Unlock()

	if !ok {
		// Shouldn't happen in v1 (no log compaction), but don't spin
		// forever if it somehow does.
		return false
	}

	apply(entry)

	n.mu.Lock()
	if nextIndex > n.lastApplied {
		n.lastApplied = nextIndex
	}
	n.mu.Unlock()
	return true
}

// assertNotBeyondCommitLocked panics if index is beyond commitIndex —
// defense-in-depth on top of the invariant tryApplyNext's own loop guard
// already maintains by construction (Invariant I-18: apply-after-commit
// only). Callers must already hold n.mu.
func (n *Node) assertNotBeyondCommitLocked(index int64) {
	if index > n.commitIndex {
		panic(fmt.Sprintf("raft: attempted to apply index %d beyond commitIndex %d", index, n.commitIndex))
	}
}

// lastLogIndexLocked returns this node's last log index, or 0 if its log is
// empty. Callers must already hold n.mu.
func (n *Node) lastLogIndexLocked() int64 {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Index
}

// mutateLog is the only sanctioned path for mutating n.log. It panics if a
// leader attempts any operation other than "append" — leaders must never
// rewrite or truncate their own log (Invariant I-02).
func (n *Node) mutateLog(op string, fn func()) {
	if n.role == Leader && op != "append" {
		panic("illegal leader log mutation: " + op)
	}
	fn()
}

// appendAsLeader is the only exposed leader-log-mutation path.
func (n *Node) appendAsLeader(e LogEntry) {
	n.mutateLog("append", func() {
		n.log = append(n.log, e)
	})
}

// truncateFrom is used only during follower conflict resolution; the guard
// ensures it can never run while n.role==Leader.
func (n *Node) truncateFrom(index int64) {
	n.mutateLog("truncate", func() {
		for i, e := range n.log {
			if e.Index == index {
				n.log = n.log[:i]
				return
			}
		}
	})
}
