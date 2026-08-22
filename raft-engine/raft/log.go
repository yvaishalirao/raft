package raft

type LogEntry struct {
	Index   int64
	Term    int64
	Command []byte
}
