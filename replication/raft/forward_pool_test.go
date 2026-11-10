package raft

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestForwardPoolAboveTheDefault runs more concurrent forwards than the old fixed
// pool kept, through a pool sized for them. Each gets its own answer, a failing
// one fails alone, the pool keeps as many warm connections as it was sized
// for, and closing it closes them.
func TestForwardPoolAboveTheDefault(t *testing.T) {
	const size, writes = 2 * defaultPoolSize, 3 * defaultPoolSize
	var accepted, arrived atomic.Int32
	all := make(chan struct{})
	f, err := newForwarderWith("127.0.0.1:0", forwarderTLS(t), nil, func(sql string, _ ...any) (int64, error) {
		// Every write waits here until all have arrived, so each holds a
		// connection of its own at once.
		if arrived.Add(1) == writes {
			close(all)
		}
		select {
		case <-all:
		case <-time.After(5 * time.Second):
			return 0, errors.New("the writes never overlapped")
		}
		n, _ := strconv.Atoi(strings.TrimPrefix(sql, "write "))
		if n%5 == 0 {
			return 0, fmt.Errorf("write %d failed", n)
		}
		return int64(n), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.close() })
	pool := NewConnPool(f.ln.Addr().String(), &tls.Config{InsecureSkipVerify: true}, size)
	var wg sync.WaitGroup
	for i := 1; i <= writes; i++ {
		wg.Go(func() {
			n, err := sendForward(context.Background(), pool, ForwardRequest{SQL: fmt.Sprintf("write %d", i)}, 5*time.Second)
			accepted.Add(1)
			switch {
			case i%5 == 0:
				if err == nil || err.Error() != fmt.Sprintf("write %d failed", i) {
					t.Errorf("write %d: %d, %v; want its own failure", i, n, err)
				}
			case err != nil || n != int64(i):
				t.Errorf("write %d: %d, %v; want its own answer", i, n, err)
			}
		})
	}
	wg.Wait()
	if accepted.Load() != writes {
		t.Fatalf("%d of %d writes answered", accepted.Load(), writes)
	}
	if got := len(pool.idle); got != size {
		t.Errorf("%d connections kept warm, want %d", got, size)
	}
	kept := make([]*RPCConn, 0, size)
	for range size {
		pc := <-pool.idle
		kept = append(kept, pc.conn)
		pool.Put(pc.conn)
	}
	pool.Close()
	for _, c := range kept {
		if _, err := c.Write([]byte{0}); !errors.Is(err, net.ErrClosed) {
			t.Errorf("a kept connection is still open after Close: %v", err)
		}
	}
}

// TestForwardPoolSizeFromConfig sizes a follower's pool from NodeConfig, and
// keeps the old size by default.
func TestForwardPoolSizeFromConfig(t *testing.T) {
	for _, tc := range []struct{ configured, want int }{{0, defaultPoolSize}, {-1, defaultPoolSize}, {3, 3}, {64, 64}} {
		cfg := NodeConfig{ForwardPoolSize: tc.configured}
		cfg.applyDefaults()
		n := &Node{cfg: cfg, forwardPeers: map[string]string{"leader": "127.0.0.1:1"}}
		if pool := n.poolForLeader("leader"); cap(pool.idle) != tc.want {
			t.Errorf("ForwardPoolSize %d: a pool of %d, want %d", tc.configured, cap(pool.idle), tc.want)
		}
	}
}
