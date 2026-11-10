package raft_test

import (
	"testing"
	"time"

	memraft "github.com/voicetel/memdb/replication/raft"
)

// TestNode_ElectsLeaderWhenNodeBehindHasNeverVoted is the regression test for a
// majority that could not elect a leader. A node that has only ever heard from a
// leader has never voted or campaigned, so it has no vote record. If the leader is
// lost while that node is behind, it cannot win, and the other node needs its vote.
// A node with no vote record must therefore be able to grant one.
//
// The leader must be lost, not stopped in good order: Shutdown hands leadership
// to the node that is ahead before it stops, and then the node behind is never
// asked for the deciding vote and the test proves nothing. So every node here
// runs with DisableShutdownHandover, and stopping the leader is a crash as far
// as the others can tell.
func TestNode_ElectsLeaderWhenNodeBehindHasNeverVoted(t *testing.T) {
	t.Parallel()
	tlsCfg := generateTLSConfig(t)
	addrs := map[string]string{}
	dirs := map[string]string{}
	var peers []string
	for _, id := range []string{"node-1", "node-2", "node-3"} {
		addrs[id] = pickFreeAddr(t)
		dirs[id] = t.TempDir()
		peers = append(peers, id+"="+addrs[id])
	}
	start := func(id string) (*memraft.Node, *mockDB) {
		t.Helper()
		db := &mockDB{}
		node, err := memraft.NewNode(db, memraft.NodeConfig{
			NodeID:           id,
			BindAddr:         addrs[id],
			Peers:            peers,
			DataDir:          dirs[id],
			TLSConfig:        tlsCfg,
			HeartbeatTimeout: 1500 * time.Millisecond,
			ElectionTimeout:  1500 * time.Millisecond,
			CommitTimeout:    10 * time.Millisecond,
			ApplyTimeout:     10 * time.Second,

			DisableShutdownHandover: true,
		})
		if err != nil {
			t.Fatalf("NewNode %s: %v", id, err)
		}
		t.Cleanup(func() { _ = node.Shutdown() })
		return node, db
	}
	leaderOf := func(within time.Duration, nodes ...*memraft.Node) *memraft.Node {
		t.Helper()
		deadline := time.Now().Add(within)
		for time.Now().Before(deadline) {
			for _, n := range nodes {
				if n.IsLeader() {
					return n
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
		return nil
	}

	// Two nodes elect a leader. The third joins once there is one to hear from, so
	// it has no reason to campaign.
	first, _ := start("node-1")
	second, _ := start("node-2")
	leader := leaderOf(30*time.Second, first, second)
	if leader == nil {
		t.Fatal("two of three nodes elected no leader at the start")
	}
	third, thirdDB := start("node-3")
	if err := leader.Exec(`INSERT INTO t VALUES ('reaches all three')`); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for thirdDB.execCount() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the third node never caught up")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The third falls behind, and then the leader is lost.
	if err := third.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if err := leader.Exec(`INSERT INTO t VALUES ('reaches two')`); err != nil {
		t.Fatal(err)
	}
	survivor := first
	if leader == first {
		survivor = second
	}
	if err := leader.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if survivor.IsLeader() {
		t.Fatal("the leader handed leadership over as it stopped, so the node behind will not be asked to vote")
	}
	third, _ = start("node-3")

	// Two of three is a majority, and only the node ahead can win.
	elected := leaderOf(30*time.Second, survivor, third)
	if elected == nil {
		t.Fatalf("two of three nodes elected no leader: term %s, the node that never voted refused",
			survivor.Stats()["term"])
	}
	if elected != survivor {
		t.Errorf("the node behind won the election")
	}
	if err := elected.Exec(`INSERT INTO t VALUES ('after the election')`); err != nil {
		t.Errorf("write after the election: %v", err)
	}
}
