package raft_test

import (
	"crypto/tls"
	"errors"
	"log/slog"
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

// TestNode_JoinMode_PeersOptionalAndIdleWarned starts a node in join mode with
// no Peers at all: it takes its membership from the leader that adds it and
// never reads them, so none are required. With no state it logs, at Warn, that
// it stays idle until AddVoter, since nothing else tells an operator why a node
// left in join mode by mistake does nothing. Without join mode Peers is still
// required, and the error says what to set instead.
func TestNode_JoinMode_PeersOptionalAndIdleWarned(t *testing.T) {
	tlsCfg := generateTLSConfig(t)
	var logs syncLog
	raftAddr, dir := pickFreeAddr(t), t.TempDir()
	start := func() *memraft.Node {
		t.Helper()
		node, err := memraft.NewNode(&mockDB{}, memraft.NodeConfig{
			NodeID: "n10", BindAddr: raftAddr, DataDir: dir, TLSConfig: tlsCfg, Join: true,
			HeartbeatTimeout: 1500 * time.Millisecond, ElectionTimeout: 1500 * time.Millisecond,
			// Warn and above only: the message must not be an Info one.
			Logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn})),
		})
		if err != nil {
			t.Fatalf("NewNode in join mode with no Peers: %v", err)
		}
		t.Cleanup(func() { _ = node.Shutdown() })
		return node
	}
	node := start()
	out := logs.String()
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "join mode") || !strings.Contains(out, "AddVoter") {
		t.Errorf("no warning that a node in join mode with no state is idle until AddVoter: %q", out)
	}
	if err := node.Exec("INSERT nowhere"); err == nil {
		t.Error("a node nobody added accepted a write")
	}

	_, err := memraft.NewNode(&mockDB{}, memraft.NodeConfig{
		NodeID: "n11", BindAddr: pickFreeAddr(t), DataDir: t.TempDir(), TLSConfig: tlsCfg,
	})
	if err == nil || !strings.Contains(err.Error(), "Peers must not be empty") || !strings.Contains(err.Error(), "Join") {
		t.Errorf("no Peers and no join mode: %v", err)
	}
	// A malformed peer is refused before anything is started.
	_, err = memraft.NewNode(&mockDB{}, memraft.NodeConfig{
		NodeID: "n12", BindAddr: pickFreeAddr(t), DataDir: t.TempDir(), TLSConfig: tlsCfg,
		Peers: []string{"n12-without-an-address"},
	})
	if err == nil || !strings.Contains(err.Error(), "parse peers") {
		t.Errorf("a malformed peer: %v", err)
	}
}

// TestNode_JoinMode_ForwardPeerSetAtRuntime is the regression test for a node
// that could not forward to a leader that joined after it started. ForwardPeers
// is read once, so n1, started knowing only itself, has no forward address for
// n2. Once n2 leads, which a handover brings about, n1's writes fail, with an
// error that names the remedy; after SetForwardPeer they are forwarded, and
// after RemoveForwardPeer they fail again.
func TestNode_JoinMode_ForwardPeerSetAtRuntime(t *testing.T) {
	tlsCfg := generateTLSConfig(t)
	raft1, fwd1 := pickFreeAddr(t), pickFreeAddr(t)
	raft2, fwd2 := pickFreeAddr(t), pickFreeAddr(t)
	raft3, fwd3 := pickFreeAddr(t), pickFreeAddr(t)
	fwdPeers := []string{"n1=" + fwd1, "n2=" + fwd2, "n3=" + fwd3}

	db1, db2, db3 := &mockDB{}, &mockDB{}, &mockDB{}
	// n1 starts alone, as a node does that was running before the others existed.
	n1 := joinTestNode(t, "n1", tlsCfg, raft1, fwd1, t.TempDir(), []string{"n1=" + raft1}, []string{"n1=" + fwd1}, false, db1)
	waitForLeader(t, n1, 10*time.Second)
	n2 := joinTestNode(t, "n2", tlsCfg, raft2, fwd2, t.TempDir(), nil, fwdPeers, true, db2)
	n3 := joinTestNode(t, "n3", tlsCfg, raft3, fwd3, t.TempDir(), nil, fwdPeers, true, db3)
	if err := n1.AddVoter("n2", raft2, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := n1.AddVoter("n3", raft3, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := n1.Exec("INSERT before"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the joined nodes to catch up", 15*time.Second, func() bool {
		return applied(db2, "INSERT before") && applied(db3, "INSERT before")
	})

	if err := n1.TransferLeadershipTo("n2"); err != nil {
		t.Fatalf("TransferLeadershipTo(n2): %v", err)
	}
	waitFor(t, "n2 to lead and n1 to know it", 15*time.Second, func() bool {
		return n2.IsLeader() && n1.LeaderAddr() == raft2
	})

	err := n1.Exec("INSERT unforwarded")
	if !errors.Is(err, memraft.ErrNotLeader) || !strings.Contains(err.Error(), "n2") || !strings.Contains(err.Error(), "SetForwardPeer") {
		t.Fatalf("a write on a node with no forward address for the leader: %v; "+
			"want ErrNotLeader naming the leader and SetForwardPeer", err)
	}

	if err := n1.SetForwardPeer("n2", fwd2); err != nil {
		t.Fatal(err)
	}
	if err := n1.Exec("INSERT forwarded"); err != nil {
		t.Fatalf("a write on n1 after SetForwardPeer: %v", err)
	}
	waitFor(t, "the forwarded write to reach every node", 15*time.Second, func() bool {
		return applied(db1, "INSERT forwarded") && applied(db2, "INSERT forwarded") && applied(db3, "INSERT forwarded")
	})
	if applied(db2, "INSERT unforwarded") {
		t.Error("the write that failed was applied")
	}
	if got := n1.ForwardPeers(); got["n2"] != fwd2 || got["n1"] != fwd1 || len(got) != 2 {
		t.Errorf("n1's forward peers: %v", got)
	}

	n1.RemoveForwardPeer("n2")
	if err := n1.Exec("INSERT after removal"); !errors.Is(err, memraft.ErrNotLeader) {
		t.Errorf("a write on n1 after RemoveForwardPeer: %v; want ErrNotLeader", err)
	}
	if n3.IsLeader() {
		t.Error("n3 leads, so the writes above were not refused for want of n2's address")
	}
}
