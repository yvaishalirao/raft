package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"raft-engine/control"
	"raft-engine/kv"
	"raft-engine/raft"
	"raft-engine/rpc"
)

func main() {
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		log.Fatal("NODE_ID is required")
	}

	raftPort := envOr("RAFT_PORT", "7000")
	controlPort := envOr("CONTROL_PORT", "8000")

	peerAddrs, peerIDs, err := parsePeers(os.Getenv("PEERS"))
	if err != nil {
		log.Fatalf("invalid PEERS: %v", err)
	}

	faultState := rpc.NewFaultState()
	grpcTransport := rpc.NewGRPCTransport(nodeID, peerAddrs)
	transport := rpc.NewFaultInjectingTransport(grpcTransport, faultState)

	node := raft.NewNode(nodeID, peerIDs, transport)
	kv.NewStore(node)

	grpcServer, listener, err := rpc.NewGRPCServer(node, ":"+raftPort)
	if err != nil {
		log.Fatalf("failed to start raft grpc server: %v", err)
	}

	controlServer := control.NewControlServer(faultState, nodeID, ":"+controlPort)
	controlServer.SetNode(node)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	go grpcServer.Serve(listener)
	if err := controlServer.Start(ctx); err != nil {
		log.Fatalf("failed to start control server: %v", err)
	}
	go node.Run(ctx)

	log.Printf("node %s up: raft=:%s control=:%s peers=%v", nodeID, raftPort, controlPort, peerIDs)

	<-ctx.Done()
	// A live demo kill uses SIGKILL via the supervisor and never reaches
	// this point — this graceful shutdown only ever runs locally.
	grpcServer.GracefulStop()
	log.Printf("node %s shut down", nodeID)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// parsePeers turns "id=host:port,id=host:port" into an address map and an
// ordered list of peer IDs.
func parsePeers(raw string) (map[string]string, []string, error) {
	addrs := make(map[string]string)
	var ids []string
	if raw == "" {
		return addrs, ids, nil
	}
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			return nil, nil, fmt.Errorf("malformed peer entry %q", entry)
		}
		addrs[parts[0]] = parts[1]
		ids = append(ids, parts[0])
	}
	return addrs, ids, nil
}
