package test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"raft-engine/raft"
	"raft-engine/rpc"
)

type Cluster struct {
	Nodes  []*raft.Node
	Router *rpc.InMemoryRouter

	ids     []string
	cancels []context.CancelFunc
}

// ClusterOption configures optional NewCluster behavior.
type ClusterOption func(*clusterConfig)

type clusterConfig struct {
	seed         int64
	onRoleChange func(nodeID string, role raft.Role, term int64)
}

// WithSeed makes every node's election-timeout jitter deterministic,
// derived from seed. Two clusters built with the same seed and the same
// node count behave identically.
func WithSeed(seed int64) ClusterOption {
	return func(c *clusterConfig) { c.seed = seed }
}

// WithRoleChangeObserver registers a callback invoked whenever any node in
// the cluster changes role, identifying which node it was.
func WithRoleChangeObserver(fn func(nodeID string, role raft.Role, term int64)) ClusterOption {
	return func(c *clusterConfig) { c.onRoleChange = fn }
}

func NewCluster(t *testing.T, n int, opts ...ClusterOption) *Cluster {
	t.Helper()

	cfg := &clusterConfig{seed: time.Now().UnixNano()}
	for _, opt := range opts {
		opt(cfg)
	}

	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("node-%d", i)
	}

	router := rpc.NewInMemoryRouter(ids)

	c := &Cluster{
		Router: router,
		ids:    ids,
	}

	for i, id := range ids {
		id := id
		transport := router.Transport(id)

		nodeOpts := []raft.NodeOption{
			raft.WithRandSource(rand.NewSource(cfg.seed + int64(i))),
		}
		if cfg.onRoleChange != nil {
			nodeOpts = append(nodeOpts, raft.WithOnRoleChange(func(role raft.Role, term int64) {
				cfg.onRoleChange(id, role, term)
			}))
		}

		node := raft.NewNode(id, peersExcept(ids, id), transport, nodeOpts...)
		c.Nodes = append(c.Nodes, node)

		ctx, cancel := context.WithCancel(context.Background())
		c.cancels = append(c.cancels, cancel)
		go node.Run(ctx)
	}

	return c
}

func peersExcept(ids []string, self string) []string {
	peers := make([]string, 0, len(ids)-1)
	for _, id := range ids {
		if id != self {
			peers = append(peers, id)
		}
	}
	return peers
}

// LeaderCount scans nodes for role==Leader at the given term.
func (c *Cluster) LeaderCount(term int64) int {
	count := 0
	for _, n := range c.Nodes {
		if n.Role() == raft.Leader && n.Term() == term {
			count++
		}
	}
	return count
}

func (c *Cluster) Shutdown() {
	for _, cancel := range c.cancels {
		cancel()
	}
}
