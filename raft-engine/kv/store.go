package kv

import (
	"encoding/json"
	"sync"

	"raft-engine/raft"
)

// command is the JSON encoding of every write this store ever applies.
type command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Store is the minimal in-memory key-value state machine applied from the
// committed Raft log. data is only ever mutated via applyCommand, called
// from node's own apply loop — never directly from a request handler.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
	node *raft.Node
}

func NewStore(node *raft.Node) *Store {
	s := &Store{data: make(map[string]string), node: node}
	node.SetApplyFunc(s.applyCommand)
	return s
}

func (s *Store) applyCommand(entry raft.LogEntry) {
	var cmd command
	if err := json.Unmarshal(entry.Command, &cmd); err != nil {
		return // malformed command; nothing sensible to apply
	}
	if cmd.Op != "SET" {
		return
	}

	s.mu.Lock()
	s.data[cmd.Key] = cmd.Value
	s.mu.Unlock()
}

// Get returns the current locally-applied value for key. This is what
// "stale" reads serve — see HandleGet.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}
