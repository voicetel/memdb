package raft_test

import (
	"errors"
	"testing"
	"time"

	"github.com/voicetel/memdb/replication"
)

// TestClusterArgsBindAlikeOnLeaderAndFollower writes a time.Time argument on the
// leader and through a follower, and every node stores the same value for both;
// and an argument the log cannot carry is refused with the same error either
// way.
func TestClusterArgsBindAlikeOnLeaderAndFollower(t *testing.T) {
	t.Parallel()
	c := startSQLiteCluster(t)
	at := time.Date(2026, 11, 10, 3, 4, 5, 123456789, time.FixedZone("EST", -5*3600))
	if _, err := c.leader.ExecResult(`INSERT INTO kv VALUES ('leader', ?)`, at); err != nil {
		t.Fatalf("on the leader: %v", err)
	}
	if _, err := c.follower.ExecResult(`INSERT INTO kv VALUES ('follower', ?)`, at); err != nil {
		t.Fatalf("through a follower: %v", err)
	}
	var want string
	for node, store := range c.stores {
		var leader, follower string
		for deadline := time.Now().Add(10 * time.Second); ; {
			err := store.db.QueryRow(`SELECT (SELECT v FROM kv WHERE k = 'leader'), (SELECT v FROM kv WHERE k = 'follower')`).Scan(&leader, &follower)
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s never applied both writes: %v", node.LeaderAddr(), err)
			}
			time.Sleep(10 * time.Millisecond)
		}
		if leader != follower {
			t.Errorf("stored %q from the leader and %q through a follower", leader, follower)
		}
		if want == "" {
			want = leader
		}
		if leader != want {
			t.Errorf("one node stored %q, another %q", leader, want)
		}
	}

	type unsupported struct{ X int }
	_, onLeader := c.leader.ExecResult(`INSERT INTO kv VALUES ('x', ?)`, unsupported{1})
	_, forwarded := c.follower.ExecResult(`INSERT INTO kv VALUES ('x', ?)`, unsupported{1})
	if !errors.Is(onLeader, replication.ErrUnsupportedArgType) || !errors.Is(forwarded, replication.ErrUnsupportedArgType) ||
		onLeader.Error() != forwarded.Error() {
		t.Errorf("an unsupported argument: leader %v, follower %v", onLeader, forwarded)
	}
}
