package raft_test

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

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

// TestShutdownLeaderAlone stops a leader with no voter to hand over to: it says
// so and stops at once.
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
	if !strings.Contains(logs.String(), "could not hand leadership over") {
		t.Errorf("logs: %s", logs.String())
	}
}
