package raft

import "testing"

func TestLog_Append(t *testing.T) {
	l := &RaftLog{}
	l.Append(LogEntry{Index: 1, Term: 1, Command: []byte("SET x 1")})

	if len(l.entries) != 1 {
		t.Fatalf("expected log length 1, got %d", len(l.entries))
	}
}

func TestLog_Truncate(t *testing.T) {
	l := &RaftLog{}
	l.Append(LogEntry{Index: 1, Term: 1})
	l.Append(LogEntry{Index: 2, Term: 1})
	l.Append(LogEntry{Index: 3, Term: 1})

	l.TruncateFrom(2)

	if len(l.entries) != 1 {
		t.Fatalf("expected 1 entry remaining, got %d", len(l.entries))
	}
	if l.entries[0].Index != 1 {
		t.Fatalf("expected remaining entry to be index 1, got %d", l.entries[0].Index)
	}
}

func TestLog_LeaderGuardPanics(t *testing.T) {
	n := NewNode("node-0", nil, nil)
	n.role = Leader
	n.log = []LogEntry{{Index: 1, Term: 1}}

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when truncating as leader, got none")
		}
	}()

	n.truncateFrom(1)
}

func TestLog_AppendAsLeaderSucceeds(t *testing.T) {
	n := NewNode("node-0", nil, nil)
	n.role = Leader

	n.appendAsLeader(LogEntry{Index: 1, Term: 1})

	if len(n.log) != 1 {
		t.Fatalf("expected node log length 1, got %d", len(n.log))
	}
}

func TestLog_EntryAtOOB(t *testing.T) {
	l := &RaftLog{}
	l.Append(LogEntry{Index: 1, Term: 1})

	_, ok := l.EntryAt(99)
	if ok {
		t.Fatal("expected ok=false for out-of-range index")
	}
}
