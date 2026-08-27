package control

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"

	"raft-engine/raft"
	"raft-engine/rpc"
)

// ControlServer exposes a small HTTP JSON API for fault-injection control
// — kill, restart, partition, unpartition, latency — on its own listener,
// architecturally separate from the Raft data-path gRPC listener. It
// shares no lock with the data plane: every mutation goes
// through rpc.FaultState's own exported setters (and Node.Restart for
// /restart), each independently synchronized, so the control channel can
// never be blocked by data-plane contention.
type ControlServer struct {
	faultState *rpc.FaultState
	node       *raft.Node
	nodeID     string
	addr       string

	mu         sync.Mutex
	listener   net.Listener
	httpServer *http.Server
}

func NewControlServer(faultState *rpc.FaultState, nodeID string, addr string) *ControlServer {
	return &ControlServer{faultState: faultState, nodeID: nodeID, addr: addr}
}

// SetNode attaches the raft.Node this control server administers, needed
// by /restart to actually reset term/votedFor, not just un-kill the
// transport.
func (c *ControlServer) SetNode(n *raft.Node) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.node = n
}

// Start binds the listener synchronously — so Addr() is valid as soon as
// Start returns — and serves in a background goroutine that stops when ctx
// is done.
func (c *ControlServer) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", c.addr)
	if err != nil {
		return fmt.Errorf("control: listen on %s: %w", c.addr, err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/kill", c.handleKill)
	mux.HandleFunc("/restart", c.handleRestart)
	mux.HandleFunc("/partition", c.handlePartition)
	mux.HandleFunc("/unpartition", c.handleUnpartition)
	mux.HandleFunc("/latency", c.handleLatency)

	srv := &http.Server{Handler: mux}

	c.mu.Lock()
	c.listener = lis
	c.httpServer = srv
	c.mu.Unlock()

	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	go func() {
		_ = srv.Serve(lis)
	}()

	return nil
}

// Addr returns the control server's bound address (valid after Start
// returns), or "" if not yet started.
func (c *ControlServer) Addr() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.listener == nil {
		return ""
	}
	return c.listener.Addr().String()
}
