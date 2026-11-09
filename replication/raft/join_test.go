package raft_test

import (
	"crypto/tls"
	"strings"
	"testing"
	"time"

	memraft "github.com/voicetel/memdb/replication/raft"
)

// joinTestNode starts a node on dataDir, in join mode when join is set.
func joinTestNode(t *testing.T, nodeID string, tlsCfg *tls.Config, raftAddr, fwdAddr, dataDir string,
	peers, fwdPeers []string, join bool, db *mockDB,
) *memraft.Node {
	t.Helper()
	node, err := memraft.NewNode(db, memraft.NodeConfig{
		NodeID:       nodeID,
		BindAddr:     raftAddr,
		ForwardAddr:  fwdAddr,
		Peers:        peers,
		ForwardPeers: fwdPeers,
		DataDir:      dataDir,
		TLSConfig:    tlsCfg,
		Join:         join,
		// See newTestNodeWithForward for why these are generous.
		HeartbeatTimeout: 1500 * time.Millisecond,
		ElectionTimeout:  1500 * time.Millisecond,
		CommitTimeout:    10 * time.Millisecond,
		ApplyTimeout:     10 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewNode %s: %v", nodeID, err)
	}
	t.Cleanup(func() { _ = node.Shutdown() })
	return node
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// applied reports whether db has applied an entry starting with sql.
func applied(db *mockDB, sql string) bool {
	for i := range db.execCount() {
		if strings.HasPrefix(db.logEntry(i), sql) {
			return true
		}
	}
	return false
}

// TestNode_JoinMode_AddedByTheLeader starts two nodes in join mode beside a running
// one-node cluster. Until added they write nothing, hold no configuration, and start
// no election. Once the leader adds them they catch up and vote: with the original
// leader stopped, the two elect a leader and commit. A joined node restarted in join
// mode keeps its state and catches up.
func TestNode_JoinMode_AddedByTheLeader(t *testing.T) {
	tlsCfg := generateTLSConfig(t)
	raft1, fwd1 := pickFreeAddr(t), pickFreeAddr(t)
	raft2, fwd2 := pickFreeAddr(t), pickFreeAddr(t)
	raft3, fwd3 := pickFreeAddr(t), pickFreeAddr(t)
	peers := []string{"n1=" + raft1, "n2=" + raft2, "n3=" + raft3}
	fwdPeers := []string{"n1=" + fwd1, "n2=" + fwd2, "n3=" + fwd3}

	db1 := &mockDB{}
	n1 := joinTestNode(t, "n1", tlsCfg, raft1, fwd1, t.TempDir(), []string{"n1=" + raft1}, fwdPeers, false, db1)
	waitForLeader(t, n1, 10*time.Second)
	if err := n1.Exec("INSERT before"); err != nil {
		t.Fatal(err)
	}

	type joined struct {
		id, raft, fwd, dir string
		db                 *mockDB
		node               *memraft.Node
	}
	nodes := []*joined{{id: "n2", raft: raft2, fwd: fwd2}, {id: "n3", raft: raft3, fwd: fwd3}}
	for _, j := range nodes {
		j.dir, j.db = t.TempDir(), &mockDB{}
		j.node = joinTestNode(t, j.id, tlsCfg, j.raft, j.fwd, j.dir, peers, fwdPeers, true, j.db)
	}

	// Two election timeouts: a node that had bootstrapped would have stood by now.
	time.Sleep(3 * time.Second)
	for _, j := range nodes {
		s := j.node.Stats()
		if s["last_log_index"] != "0" || s["term"] != "0" || s["latest_configuration"] != "[]" || j.node.IsLeader() {
			t.Errorf("%s before it is added: index %s, term %s, configuration %s, leader %v",
				j.id, s["last_log_index"], s["term"], s["latest_configuration"], j.node.IsLeader())
		}
	}

	for _, j := range nodes {
		if err := n1.AddVoter(j.id, j.raft, 10*time.Second); err != nil {
			t.Fatalf("AddVoter %s: %v", j.id, err)
		}
	}
	waitFor(t, "the joined nodes to catch up", 10*time.Second, func() bool {
		return applied(nodes[0].db, "INSERT before") && applied(nodes[1].db, "INSERT before")
	})

	// The joined nodes vote: without n1 they elect a leader and commit.
	if err := n1.Shutdown(); err != nil {
		t.Fatal(err)
	}
	var leader, follower *joined
	waitFor(t, "a leader among the joined nodes", 20*time.Second, func() bool {
		for i, j := range nodes {
			if j.node.IsLeader() {
				leader, follower = j, nodes[1-i]
				return true
			}
		}
		return false
	})
	if err := leader.node.Exec("INSERT after"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "both joined nodes to apply the new write", 10*time.Second, func() bool {
		return applied(nodes[0].db, "INSERT after") && applied(nodes[1].db, "INSERT after")
	})

	// A joined node restarted in join mode keeps its state: it does not wait to be
	// added again, and catches up with a write made while it was down.
	if err := follower.node.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if err := leader.node.Exec("INSERT while down"); err == nil {
		t.Fatal("one voter of three committed alone")
	}
	again := &mockDB{}
	restarted := joinTestNode(t, follower.id, tlsCfg, follower.raft, follower.fwd, follower.dir, peers, fwdPeers, true, again)
	waitFor(t, "the restarted node to rejoin and the cluster to commit", 30*time.Second, func() bool {
		return leaderOf(leader.node, restarted) != nil
	})
	if err := leaderOf(leader.node, restarted).Exec("INSERT after restart"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the restarted node to catch up", 20*time.Second, func() bool {
		return applied(again, "INSERT after restart")
	})
	if s := restarted.Stats(); s["latest_configuration"] == "[]" {
		t.Errorf("the restarted node lost its configuration: %v", s)
	}
}

// leaderOf returns whichever of nodes leads, or nil.
func leaderOf(nodes ...*memraft.Node) *memraft.Node {
	for _, n := range nodes {
		if n.IsLeader() {
			return n
		}
	}
	return nil
}

// TestNode_JoinMode_NeverAdded checks a node in join mode that nobody adds stays out:
// it starts no election and accepts no write.
func TestNode_JoinMode_NeverAdded(t *testing.T) {
	tlsCfg := generateTLSConfig(t)
	raft, fwd := pickFreeAddr(t), pickFreeAddr(t)
	other := pickFreeAddr(t)
	n := joinTestNode(t, "n4", tlsCfg, raft, fwd, t.TempDir(),
		[]string{"n4=" + raft, "n5=" + other}, []string{"n4=" + fwd}, true, &mockDB{})
	time.Sleep(3 * time.Second)
	if s := n.Stats(); n.IsLeader() || s["term"] != "0" || s["last_log_index"] != "0" {
		t.Errorf("a node nobody added: leader %v, term %s, index %s", n.IsLeader(), s["term"], s["last_log_index"])
	}
	if err := n.Exec("INSERT nowhere"); err == nil {
		t.Error("a node nobody added accepted a write")
	}
}

// TestNode_JoinMode_EvenPeersAllowed checks the odd-size rule is a bootstrap rule: a
// node in join mode with an even peer list starts, and without join mode it does not.
func TestNode_JoinMode_EvenPeersAllowed(t *testing.T) {
	tlsCfg := generateTLSConfig(t)
	raft, other := pickFreeAddr(t), pickFreeAddr(t)
	peers := []string{"n6=" + raft, "n7=" + other}
	joinTestNode(t, "n6", tlsCfg, raft, "", t.TempDir(), peers, nil, true, &mockDB{})

	_, err := memraft.NewNode(&mockDB{}, memraft.NodeConfig{
		NodeID: "n8", BindAddr: pickFreeAddr(t), Peers: []string{"n8=" + pickFreeAddr(t), "n9=" + other},
		DataDir: t.TempDir(), TLSConfig: tlsCfg,
	})
	if err == nil || !strings.Contains(err.Error(), "must be odd") {
		t.Errorf("an even peer list without join mode: %v", err)
	}
}
