package raft_test

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	memraft "github.com/voicetel/memdb/replication/raft"
)

// cluster3 starts three nodes and returns them by ID.
func cluster3(t *testing.T) map[string]*memraft.Node {
	t.Helper()
	tlsCfg := generateTLSConfig(t)
	ids := []string{"node-1", "node-2", "node-3"}
	raft, fwd := map[string]string{}, map[string]string{}
	var peers, fwdPeers []string
	for _, id := range ids {
		raft[id], fwd[id] = pickFreeAddr(t), pickFreeAddr(t)
		peers = append(peers, id+"="+raft[id])
		fwdPeers = append(fwdPeers, id+"="+fwd[id])
	}
	nodes := map[string]*memraft.Node{}
	for _, id := range ids {
		nodes[id], _ = newTestNodeWithForward(t, id, tlsCfg, raft[id], fwd[id], peers, fwdPeers)
	}
	return nodes
}

// leaderAmong waits for one of the nodes not in skip to lead, and returns its ID.
func leaderAmong(t *testing.T, nodes map[string]*memraft.Node, within time.Duration, skip ...string) string {
	t.Helper()
	for deadline := time.Now().Add(within); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		for id, n := range nodes {
			if !strings.Contains(strings.Join(skip, ","), id) && n.IsLeader() {
				return id
			}
		}
	}
	return ""
}

// TestTransferLeadership hands leadership to another voter, and to a named one,
// and refuses a node that is not a member and a call made off the leader.
func TestTransferLeadership(t *testing.T) {
	t.Parallel()
	nodes := cluster3(t)
	first := leaderAmong(t, nodes, 15*time.Second)
	if first == "" {
		t.Fatal("no leader")
	}
	if err := nodes[first].TransferLeadership(); err != nil {
		t.Fatalf("TransferLeadership: %v", err)
	}
	if nodes[first].IsLeader() {
		t.Error("still leading after the transfer")
	}
	second := leaderAmong(t, nodes, 5*time.Second, first)
	if second == "" {
		t.Fatal("no new leader after the transfer")
	}

	target := first
	if err := nodes[second].TransferLeadershipTo(target); err != nil {
		t.Fatalf("TransferLeadershipTo(%s): %v", target, err)
	}
	if got := leaderAmong(t, nodes, 5*time.Second, second); got != target {
		t.Errorf("leader after a transfer to %s is %q", target, got)
	}
	if err := nodes[target].TransferLeadershipTo("node-9"); err == nil || !strings.Contains(err.Error(), "not a member") {
		t.Errorf("a transfer to a stranger: %v", err)
	}
	if err := nodes[second].TransferLeadership(); err == nil {
		t.Error("a transfer called on a follower succeeded")
	}
}

// TestLeaderOnlyCallsOffTheLeaderAreErrNotLeader makes each call only the leader
// can serve on a follower. Each fails with this package's ErrNotLeader, so one
// errors.Is covers them and a refused write alike, and still with hashicorp/raft's
// own error for a caller that already matches that.
func TestLeaderOnlyCallsOffTheLeaderAreErrNotLeader(t *testing.T) {
	t.Parallel()
	nodes := cluster3(t)
	leader := leaderAmong(t, nodes, 15*time.Second)
	if leader == "" {
		t.Fatal("no leader")
	}
	var follower *memraft.Node
	for id, n := range nodes {
		if id != leader {
			follower = n
			break
		}
	}
	for name, call := range map[string]func() error{
		"TransferLeadership":   follower.TransferLeadership,
		"TransferLeadershipTo": func() error { return follower.TransferLeadershipTo(leader) },
		"AddVoter":             func() error { return follower.AddVoter("node-9", pickFreeAddr(t), time.Second) },
		"RemoveServer":         func() error { return follower.RemoveServer("node-9", time.Second) },
	} {
		err := call()
		if !errors.Is(err, memraft.ErrNotLeader) {
			t.Errorf("%s on a follower: %v; want this package's ErrNotLeader", name, err)
		}
		if !errors.Is(err, hraft.ErrNotLeader) {
			t.Errorf("%s on a follower: %v; want hashicorp/raft's ErrNotLeader as well", name, err)
		}
		if err != nil && !strings.Contains(err.Error(), hraft.ErrNotLeader.Error()) {
			t.Errorf("%s on a follower: %q lost raft's own words", name, err)
		}
	}
}

// TestShutdownHandsOverPastADeadVoter stops the leader of a cluster whose first
// follower in configuration order is down, with nothing written since. That is
// the voter hashicorp/raft picks by itself, since it has as much of the log as
// any and comes first, and it cannot take over. Shutdown then asks the others
// by name, so a live node still leads at once; and it does not take longer
// than its budget of three election timeouts.
func TestShutdownHandsOverPastADeadVoter(t *testing.T) {
	t.Parallel()
	tlsCfg := generateTLSConfig(t)
	ids := []string{"node-1", "node-2", "node-3", "node-4", "node-5"}
	raft, fwd := map[string]string{}, map[string]string{}
	var peers, fwdPeers []string
	for _, id := range ids {
		raft[id], fwd[id] = pickFreeAddr(t), pickFreeAddr(t)
		peers = append(peers, id+"="+raft[id])
		fwdPeers = append(fwdPeers, id+"="+fwd[id])
	}
	nodes, dbs := map[string]*memraft.Node{}, map[string]*mockDB{}
	for _, id := range ids {
		nodes[id], dbs[id] = newTestNodeWithForward(t, id, tlsCfg, raft[id], fwd[id], peers, fwdPeers)
	}
	leader := leaderAmong(t, nodes, 30*time.Second)
	if leader == "" {
		t.Fatal("no leader")
	}
	// Every node holds the whole log, so the pick is decided by order alone.
	if err := nodes[leader].Exec("INSERT everywhere"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "every node to apply the write", 15*time.Second, func() bool {
		for _, db := range dbs {
			if !applied(db, "INSERT everywhere") {
				return false
			}
		}
		return true
	})
	var dead string
	for _, id := range ids {
		if id != leader {
			dead = id
			break
		}
	}
	if err := nodes[dead].Shutdown(); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	if err := nodes[leader].Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	// Three election timeouts of 1.5 s, and room for the rest of Shutdown.
	if took := time.Since(start); took > 6*time.Second {
		t.Errorf("Shutdown took %v, beyond its handover budget", took)
	}
	if got := leaderAmong(t, nodes, 500*time.Millisecond, leader, dead); got == "" {
		t.Error("no leader within 500 ms of the old leader's shutdown: it gave up on the dead voter")
	}
}

// TestShutdownHandsOverLeadership stops the leader, and another node leads at
// once, not an election timeout (1.5 s here) later.
func TestShutdownHandsOverLeadership(t *testing.T) {
	t.Parallel()
	nodes := cluster3(t)
	old := leaderAmong(t, nodes, 15*time.Second)
	if old == "" {
		t.Fatal("no leader")
	}
	if err := nodes[old].Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if got := leaderAmong(t, nodes, 500*time.Millisecond, old); got == "" {
		t.Error("no leader within 500 ms of the old leader's shutdown")
	}
}

// TestShutdownFollowerKeepsTheLeader stops a follower, which hands nothing over.
func TestShutdownFollowerKeepsTheLeader(t *testing.T) {
	t.Parallel()
	nodes := cluster3(t)
	leader := leaderAmong(t, nodes, 15*time.Second)
	if leader == "" {
		t.Fatal("no leader")
	}
	for id, n := range nodes {
		if id != leader {
			if err := n.Shutdown(); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	time.Sleep(200 * time.Millisecond)
	if !nodes[leader].IsLeader() {
		t.Error("a follower's shutdown moved leadership")
	}
}

// syncLog is a log buffer the node can write while a test reads.
type syncLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncLog) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncLog) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// TestShutdownLeaderAlone stops a leader with no voter to hand over to: it
// stops at once and warns of nothing, since nothing is wrong.
func TestShutdownLeaderAlone(t *testing.T) {
	t.Parallel()
	var logs syncLog
	addr := pickFreeAddr(t)
	node, err := memraft.NewNode(&mockDB{}, memraft.NodeConfig{
		NodeID: "node-1", BindAddr: addr, Peers: []string{"node-1=" + addr}, DataDir: t.TempDir(),
		TLSConfig: generateTLSConfig(t), HeartbeatTimeout: 1500 * time.Millisecond, ElectionTimeout: 1500 * time.Millisecond,
		CommitTimeout: 10 * time.Millisecond, ApplyTimeout: 10 * time.Second,
		Logger: slog.New(slog.NewTextHandler(&logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForLeader(t, node, 10*time.Second)
	start := time.Now()
	if err := node.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Errorf("Shutdown took %v with no voter to hand over to", took)
	}
	if strings.Contains(logs.String(), "could not hand leadership over") {
		t.Errorf("a leader with no other voter warned about the handover it cannot make: %s", logs.String())
	}
}
