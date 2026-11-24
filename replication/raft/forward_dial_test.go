package raft

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

// stalledListener accepts TCP connections and then says nothing: no TLS
// handshake ever completes against it. It returns its address.
func stalledListener(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var held []net.Conn
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, conn)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range held {
			_ = conn.Close()
		}
	})
	return ln.Addr().String()
}

// TestForwardDialStopsWithItsContext forwards to a leader that accepts the
// connection and then stalls the TLS handshake. The forward ends when its
// context does, by deadline or by cancellation, with the context's error,
// not after the two seconds a dial is otherwise allowed. With a context that
// cannot end, the dial is still held to the caller's timeout.
func TestForwardDialStopsWithItsContext(t *testing.T) {
	// Well under dialTimeout, and far over the 50 ms the contexts last.
	const prompt = dialTimeout / 2

	t.Run("deadline", func(t *testing.T) {
		pool := NewConnPool(stalledListener(t), &tls.Config{InsecureSkipVerify: true}, 1)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err := sendForward(ctx, pool, ForwardRequest{SQL: "INSERT INTO t VALUES (1)"}, 10*time.Second)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("%v; want the context's deadline error", err)
		}
		if took := time.Since(start); took > prompt {
			t.Errorf("took %v against a stalled handshake, not ended by a 50 ms deadline", took)
		}
	})

	t.Run("cancelled", func(t *testing.T) {
		pool := NewConnPool(stalledListener(t), &tls.Config{InsecureSkipVerify: true}, 1)
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(50*time.Millisecond, cancel)
		start := time.Now()
		_, err := pool.GetContext(ctx, 10*time.Second)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("%v; want the context's cancellation error", err)
		}
		if took := time.Since(start); took > prompt {
			t.Errorf("took %v against a stalled handshake, not ended by cancellation", took)
		}
	})

	t.Run("no context, the caller's timeout", func(t *testing.T) {
		pool := NewConnPool(stalledListener(t), &tls.Config{InsecureSkipVerify: true}, 1)
		start := time.Now()
		if _, err := pool.Get(100 * time.Millisecond); err == nil {
			t.Error("a handshake that never completes gave a connection")
		}
		if took := time.Since(start); took > prompt {
			t.Errorf("took %v, not held to the caller's 100 ms", took)
		}
	})
}
