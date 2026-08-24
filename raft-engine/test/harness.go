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

// NodeByID returns the cluster's current node for id, or nil if unknown.
// After Revive(id), this returns the new node instance.
func (c *Cluster) NodeByID(id string) *raft.Node {
	for i, nid := range c.ids {
		if nid == id {
			return c.Nodes[i]
		}
	}
	return nil
}

// KillNode simulates a crash: it cancels node id's context, which stops its
// election timer, heartbeats, and RPC dispatch loop, so it stops responding
// to any RPC. The node stays registered with the router (nothing physically
// removes it), it simply never answers again until Revive'd.
func (c *Cluster) KillNode(id string) {
	for i, nid := range c.ids {
		if nid == id {
			c.cancels[i]()
			return
		}
	}
}

// Revive replaces node id with a fresh, empty raft.Node and starts it
// running again. Per the v1 no-persistence design, a restart legitimately
// comes back with no memory of prior state — it must reconcile via
// AppendEntries like any other straggling follower.
func (c *Cluster) Revive(id string, opts ...raft.NodeOption) *raft.Node {
	for i, nid := range c.ids {
		if nid != id {
			continue
		}

		transport := c.Router.Transport(id)
		node := raft.NewNode(id, peersExcept(c.ids, id), transport, opts...)
		c.Nodes[i] = node

		ctx, cancel := context.WithCancel(context.Background())
		c.cancels[i] = cancel
		go node.Run(ctx)

		return node
	}
	return nil
}

func (c *Cluster) Shutdown() {
	for _, cancel := range c.cancels {
		cancel()
	}
}
