package raft

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"testing"
	"time"
)

// slowForwarder starts a forwarder whose "slow" writes signal applying and then
// wait until the test ends, and a pool to it holding one warm connection, so a
// forward reaches the apply without a dial or a handshake first.
func slowForwarder(t *testing.T) (pool *ConnPool, applying chan struct{}) {
	t.Helper()
	release := make(chan struct{})
	applying = make(chan struct{}, 1)
	f, err := newForwarderWith("127.0.0.1:0", forwarderTLS(t), nil, func(sql string, _ ...any) (int64, error) {
		if sql == "slow" {
			applying <- struct{}{}
			<-release
		}
		return 1, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		close(release)
		_ = f.close()
	})
	pool = NewConnPool(f.ln.Addr().String(), &tls.Config{InsecureSkipVerify: true}, 4)
	t.Cleanup(pool.Close)
	if n, err := sendForward(context.Background(), pool, ForwardRequest{SQL: "fast"}, 10*time.Second); err != nil || n != 1 {
		t.Fatalf("a forward with no end: %d, %v", n, err)
	}
	if len(pool.idle) != 1 {
		t.Fatalf("%d connections kept after an answered forward, want 1", len(pool.idle))
	}
	return pool, applying
}

// TestSendForwardStopsWithItsContext ends a forward the leader is still applying,
// by cancellation and by the context's deadline. Each returns at once with the
// context's error, says the write may still commit, and does not put the
// connection back. Each case has a forwarder, a pool and a context of its own,
// and each checks the leader was applying the write when its context ended, so
// it is the exchange in flight that is stopped and not a forward that never
// left.
func TestSendForwardStopsWithItsContext(t *testing.T) {
	stopped := func(t *testing.T, ctx context.Context, pool *ConnPool, applying chan struct{}, want error) {
		t.Helper()
		start := time.Now()
		_, err := sendForward(ctx, pool, ForwardRequest{SQL: "slow"}, 10*time.Second)
		if !errors.Is(err, want) || !strings.Contains(err.Error(), "may still commit") {
			t.Errorf("%v; want the context's error, %v", err, want)
		}
		if took := time.Since(start); took > 3*time.Second {
			t.Errorf("took %v, not ended by its context", took)
		}
		select {
		case <-applying:
		default:
			t.Error("the context ended before the leader had the write: nothing in flight was stopped")
		}
		if len(pool.idle) != 0 {
			t.Error("a connection in an unknown state went back to the pool")
		}
	}

	t.Run("cancelled in flight", func(t *testing.T) {
		pool, applying := slowForwarder(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Cancel only once the leader is applying the write, and hand the
		// signal on for the check that it was.
		go func() {
			<-applying
			applying <- struct{}{}
			cancel()
		}()
		stopped(t, ctx, pool, applying, context.Canceled)
	})

	t.Run("deadline in flight", func(t *testing.T) {
		pool, applying := slowForwarder(t)
		// The deadline is a real one, so it is set far enough out for a write
		// on a warm connection to be with the leader long before it: the
		// check that it was is what keeps this from passing on a forward
		// whose context had already ended.
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		if ctx.Err() != nil {
			t.Fatal("the context ended before the forward began")
		}
		stopped(t, ctx, pool, applying, context.DeadlineExceeded)
	})

	t.Run("ended before it began", func(t *testing.T) {
		pool, applying := slowForwarder(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := sendForward(ctx, pool, ForwardRequest{SQL: "slow"}, 10*time.Second)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("%v; want the context's error", err)
		}
		select {
		case <-applying:
			t.Error("a forward whose context had ended reached the leader")
		case <-time.After(50 * time.Millisecond):
		}
	})

	t.Run("not ended", func(t *testing.T) {
		pool, _ := slowForwarder(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if n, err := sendForward(ctx, pool, ForwardRequest{SQL: "fast"}, 10*time.Second); err != nil || n != 1 {
			t.Fatalf("a forward whose context did not end: %d, %v", n, err)
		}
		if len(pool.idle) != 1 {
			t.Errorf("%d connections kept after answered forwards, want 1", len(pool.idle))
		}
	})
}
