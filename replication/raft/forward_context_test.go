package raft

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestSendForwardStopsWithItsContext ends a forward the leader is still applying,
// by cancellation and by the context's deadline. Each returns at once with the
// context's error, says the write may still commit, and does not put the
// connection back. A forward with a context that never ends is answered and
// reuses its connection.
func TestSendForwardStopsWithItsContext(t *testing.T) {
	release := make(chan struct{})
	applying := make(chan struct{}, 4)
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
	pool := NewConnPool(f.ln.Addr().String(), &tls.Config{InsecureSkipVerify: true}, 4)
	t.Cleanup(pool.Close)

	cancelled, cancel := context.WithCancel(context.Background())
	go func() {
		<-applying
		cancel()
	}()
	timedOut, stop := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer stop()
	for name, tc := range map[string]struct {
		ctx  context.Context
		want error
	}{
		"cancelled": {cancelled, context.Canceled},
		"timed out": {timedOut, context.DeadlineExceeded},
	} {
		start := time.Now()
		_, err := sendForward(tc.ctx, pool, ForwardRequest{SQL: "slow"}, 10*time.Second)
		if !errors.Is(err, tc.want) || !strings.Contains(err.Error(), "may still commit") {
			t.Errorf("%s: %v; want the context's error", name, err)
		}
		if took := time.Since(start); took > 3*time.Second {
			t.Errorf("%s: took %v, not ended by its context", name, took)
		}
		if len(pool.idle) != 0 {
			t.Errorf("%s: a connection in an unknown state went back to the pool", name)
		}
	}

	if n, err := sendForward(context.Background(), pool, ForwardRequest{SQL: "fast"}, 10*time.Second); err != nil || n != 1 {
		t.Fatalf("a forward with no end: %d, %v", n, err)
	}
	live, end := context.WithCancel(context.Background())
	defer end()
	if n, err := sendForward(live, pool, ForwardRequest{SQL: "fast"}, 10*time.Second); err != nil || n != 1 {
		t.Fatalf("a forward whose context did not end: %d, %v", n, err)
	}
	if len(pool.idle) != 1 {
		t.Errorf("%d connections kept after answered forwards, want 1", len(pool.idle))
	}
}
