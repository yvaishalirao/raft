package kv

import (
	"errors"
	"testing"
	"time"

	"raft-engine/raft"
)

func TestHandleSet_RejectsOnFollower(t *testing.T) {
	stores, nodes, cancels := buildKVCluster(t, 3)
	defer cancelAll(cancels)

	leader := waitForLeader(t, nodes, 2*time.Second)

	var followerStore *Store
	for i, n := range nodes {
		if n != leader {
			followerStore = stores[i]
			break
		}
	}

	err := followerStore.HandleSet("x", "1")
	var notLeaderErr *ErrNotLeader
	if !errors.As(err, &notLeaderErr) {
		t.Fatalf("expected ErrNotLeader, got %v (%T)", err, err)
	}

	if _, ok := followerStore.Get("x"); ok {
		t.Fatal("expected follower's data map to remain unchanged after a rejected SET")
	}
}

func TestHandleSet_ErrorIncludesLeaderHint(t *testing.T) {
	stores, nodes, cancels := buildKVCluster(t, 3)
	defer cancelAll(cancels)

	leader := waitForLeader(t, nodes, 2*time.Second)

	var followerNode *raft.Node
	var followerStore *Store
	for i, n := range nodes {
		if n != leader {
			followerNode = n
			followerStore = stores[i]
			break
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && followerNode.KnownLeaderID() == "" {
		time.Sleep(10 * time.Millisecond)
	}
	if followerNode.KnownLeaderID() == "" {
		t.Fatal("follower never observed a heartbeat from the leader within 2s")
	}

	err := followerStore.HandleSet("x", "1")
	var notLeaderErr *ErrNotLeader
	if !errors.As(err, &notLeaderErr) {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
	if notLeaderErr.LeaderHint != leader.ID() {
		t.Fatalf("expected leader hint %q, got %q", leader.ID(), notLeaderErr.LeaderHint)
	}
}

func TestHandleSet_SucceedsOnLeader(t *testing.T) {
	stores, nodes, cancels := buildKVCluster(t, 3)
	defer cancelAll(cancels)

	leader := waitForLeader(t, nodes, 2*time.Second)
	leaderStore := storeFor(stores, nodes, leader)

	if err := leaderStore.HandleSet("x", "1"); err != nil {
		t.Fatalf("expected SET on leader to succeed, got: %v", err)
	}
	waitForValue(t, leaderStore, "x", "1", time.Second)
}

func TestHandleGet_ModeTagPresent(t *testing.T) {
	stores, nodes, cancels := buildKVCluster(t, 1)
	defer cancelAll(cancels)

	leader := waitForLeader(t, nodes, 2*time.Second)
	leaderStore := storeFor(stores, nodes, leader)

	res, err := leaderStore.HandleGet("x", "stale")
	if err != nil {
		t.Fatalf("stale GET failed: %v", err)
	}
	if res.Mode != "stale" {
		t.Fatalf("expected Mode=stale, got %q", res.Mode)
	}

	res2, err := leaderStore.HandleGet("x", "linearizable")
	if err != nil {
		t.Fatalf("linearizable GET failed: %v", err)
	}
	if res2.Mode != "linearizable" {
		t.Fatalf("expected Mode=linearizable, got %q", res2.Mode)
	}
}

func TestHandleGet_LinearizableSucceedsOnLeader(t *testing.T) {
	stores, nodes, cancels := buildKVCluster(t, 3)
	defer cancelAll(cancels)

	leader := waitForLeader(t, nodes, 2*time.Second)
	leaderStore := storeFor(stores, nodes, leader)

	if err := leaderStore.HandleSet("x", "42"); err != nil {
		t.Fatalf("HandleSet failed: %v", err)
	}
	waitForValue(t, leaderStore, "x", "42", time.Second)

	res, err := leaderStore.HandleGet("x", "linearizable")
	if err != nil {
		t.Fatalf("linearizable GET failed: %v", err)
	}
	if !res.OK || res.Value != "42" {
		t.Fatalf("expected value=42, got value=%q ok=%v", res.Value, res.OK)
	}
}

func TestHandleGet_LinearizableRejectedOnStaleNode(t *testing.T) {
	stores, nodes, cancels := buildKVCluster(t, 3)
	defer cancelAll(cancels)

	leader := waitForLeader(t, nodes, 2*time.Second)

	var followerStore *Store
	for i, n := range nodes {
		if n != leader {
			followerStore = stores[i]
			break
		}
	}

	_, err := followerStore.HandleGet("x", "linearizable")
	if err == nil {
		t.Fatal("expected linearizable GET on a non-leader to be refused")
	}
}

func TestHandleGet_StaleAlwaysSucceeds(t *testing.T) {
	stores, nodes, cancels := buildKVCluster(t, 3)
	defer cancelAll(cancels)

	leader := waitForLeader(t, nodes, 2*time.Second)
	leaderStore := storeFor(stores, nodes, leader)

	if err := leaderStore.HandleSet("x", "1"); err != nil {
		t.Fatalf("HandleSet failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond) // allow propagation

	for i, n := range nodes {
		if n == leader {
			continue
		}
		if _, err := stores[i].HandleGet("x", "stale"); err != nil {
			t.Fatalf("stale GET on follower %d failed: %v", i, err)
		}
	}
}
