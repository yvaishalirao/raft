package test

import (
	"context"
	"fmt"
	"testing"

	"raft-engine/raft"
	"raft-engine/rpc"
)

type Cluster struct {
	Nodes  []*raft.Node
	Router *rpc.InMemoryRouter

	ids     []string
	cancels []context.CancelFunc
}

func NewCluster(t *testing.T, n int) *Cluster {
	t.Helper()

	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("node-%d", i)
	}

	router := rpc.NewInMemoryRouter(ids)

	c := &Cluster{
		Router: router,
		ids:    ids,
	}

	for _, id := range ids {
		transport := router.Transport(id)
		node := raft.NewNode(id, peersExcept(ids, id), transport)
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
// TODO: always returns 0 until Session 3 implements real leader election.
func (c *Cluster) LeaderCount(term int64) int {
	return 0
}

func (c *Cluster) Shutdown() {
	for _, cancel := range c.cancels {
		cancel()
	}
}
