package rpc

import (
	"context"
	"testing"
	"time"
)

func TestInMemory_Delivers(t *testing.T) {
	router := NewInMemoryRouter([]string{"A", "B"})
	transportA := router.Transport("A")

	received := make(chan any, 1)
	go func() {
		env := <-router.Inbox("B")
		received <- env.Args
		env.Reply <- rpcResult{Reply: "ack"}
	}()

	reply, err := transportA.Send(context.Background(), "B", "Ping", "hello")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reply != "ack" {
		t.Fatalf("expected reply %q, got %v", "ack", reply)
	}

	select {
	case args := <-received:
		if args != "hello" {
			t.Fatalf("B received %v, expected %q", args, "hello")
		}
	case <-time.After(time.Second):
		t.Fatal("B never received the message")
	}
}

func TestInMemory_Drop(t *testing.T) {
	router := NewInMemoryRouter([]string{"A", "B"})
	router.SetDrop("A", "B", true)
	transportA := router.Transport("A")

	errCh := make(chan error, 1)
	go func() {
		_, err := transportA.Send(context.Background(), "B", "Ping", "hello")
		errCh <- err
	}()

	select {
	case env := <-router.Inbox("B"):
		t.Fatalf("B should never receive a dropped message, got %v", env.Args)
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected an error for a dropped message, got nil")
		}
	case <-time.After(time.Second):
		t.Fatal("Send did not return after the message was dropped")
	}
}

func TestInMemory_Delay(t *testing.T) {
	router := NewInMemoryRouter([]string{"A", "B"})
	d := 100 * time.Millisecond
	router.SetDelay("A", "B", d)
	transportA := router.Transport("A")

	go func() {
		env := <-router.Inbox("B")
		env.Reply <- rpcResult{Reply: "ack"}
	}()

	start := time.Now()
	_, err := transportA.Send(context.Background(), "B", "Ping", "hello")
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if elapsed < d {
		t.Fatalf("expected delivery to take at least %v, took %v", d, elapsed)
	}
}
