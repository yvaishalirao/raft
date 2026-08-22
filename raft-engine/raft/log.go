package raft

type LogEntry struct {
	Index   int64
	Term    int64
	Command []byte
}

type RaftLog struct {
	entries []LogEntry
}

// Append adds an entry to the end of the log. The only method a leader may
// legally use to mutate its own log — see Node.mutateLog.
func (l *RaftLog) Append(e LogEntry) {
	l.entries = append(l.entries, e)
}

// TruncateFrom removes the entry at index and everything after it. Used only
// during follower conflict resolution — never called while role==Leader.
func (l *RaftLog) TruncateFrom(index int64) {
	for i, e := range l.entries {
		if e.Index == index {
			l.entries = l.entries[:i]
			return
		}
	}
}

func (l *RaftLog) EntryAt(index int64) (LogEntry, bool) {
	for _, e := range l.entries {
		if e.Index == index {
			return e, true
		}
	}
	return LogEntry{}, false
}

func (l *RaftLog) LastIndexTerm() (int64, int64) {
	if len(l.entries) == 0 {
		return 0, 0
	}
	last := l.entries[len(l.entries)-1]
	return last.Index, last.Term
}
