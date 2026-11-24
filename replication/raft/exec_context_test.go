package raft_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	memraft "github.com/voicetel/memdb/replication/raft"
)

// blockingStore holds each write until released, and counts what it applied.
type blockingStore struct {
	mockDB
	entered chan struct{}
	release chan struct{}
	applied atomic.Int32
}

func (b *blockingStore) ExecLocalResult(string, ...any) (int64, error) {
	b.entered <- struct{}{}
	<-b.release
	b.applied.Add(1)
	return 1, nil
}

// TestExecResultContextOnTheLeader stops waiting for a commit once the caller's
// context ends, with its error, and the write still commits, as documented.
// An ended context applies nothing, and a context that never ends waits.
func TestExecResultContextOnTheLeader(t *testing.T) {
	t.Parallel()
	store := &blockingStore{entered: make(chan struct{}, 4), release: make(chan struct{})}
	addr := pickFreeAddr(t)
	node, err := memraft.NewNode(store, memraft.NodeConfig{
		NodeID: "node-1", BindAddr: addr, Peers: []string{"node-1=" + addr}, DataDir: t.TempDir(),
		TLSConfig: generateTLSConfig(t), HeartbeatTimeout: 1500 * time.Millisecond, ElectionTimeout: 1500 * time.Millisecond,
		CommitTimeout: 10 * time.Millisecond, ApplyTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Shutdown() })
	waitForLeader(t, node, 10*time.Second)

	ended, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	if _, err := node.ExecResultContext(ended, "INSERT INTO t VALUES (1)"); !errors.Is(err, context.Canceled) {
		t.Errorf("an ended context: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-store.entered
		cancel()
	}()
	if _, err := node.ExecResultContext(ctx, "INSERT INTO t VALUES (2)"); !errors.Is(err, context.Canceled) ||
		!strings.Contains(err.Error(), "may still commit") {
		t.Errorf("a context cancelled during the commit: %v", err)
	}
	store.release <- struct{}{}
	for deadline := time.Now().Add(5 * time.Second); store.applied.Load() != 1; {
		if time.Now().After(deadline) {
			t.Fatal("the write whose caller stopped waiting never committed")
		}
		time.Sleep(10 * time.Millisecond)
	}

	done := make(chan error, 1)
	go func() {
		_, err := node.ExecResult("INSERT INTO t VALUES (3)")
		done <- err
	}()
	<-store.entered
	store.release <- struct{}{}
	if err := <-done; err != nil || store.applied.Load() != 2 {
		t.Errorf("ExecResult: %v, %d applied", err, store.applied.Load())
	}
}
