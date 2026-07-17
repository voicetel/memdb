package raft_test

// Tests for the rows-affected count contract: FSM.Apply returns the count
// as the Raft future response (NewResultFSM), ApplyResult/Node.ExecResult
// surface it on the leader, and ForwardResponse carries it back to a
// follower that forwarded its write.

import (
	"crypto/tls"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/voicetel/memdb/replication"
	memraft "github.com/voicetel/memdb/replication/raft"
)

// resultMockDB is mockDB extended with ExecLocalResult so NewNode wires
// the count-carrying FSM (the memraft.ResultDB upgrade path).
type resultMockDB struct {
	mockDB
	rowsAffected int64
}

func (m *resultMockDB) ExecLocalResult(sql string, args ...any) (int64, error) {
	if err := m.ExecLocal(sql, args...); err != nil {
		return 0, err
	}
	return m.rowsAffected, nil
}

func TestFSM_Apply_ReturnsCount(t *testing.T) {
	t.Parallel()

	fsm := memraft.NewResultFSM(
		func(sql string, args ...any) (int64, error) { return 42, nil },
		func() ([]byte, error) { return nil, nil },
		func([]byte) error { return nil },
	)

	entry := replication.WALEntry{Seq: 1, SQL: "UPDATE t SET x = 1"}
	log := &hraft.Log{Data: encodeEntry(t, entry)}
	result := fsm.Apply(log)

	n, ok := result.(int64)
	if !ok || n != 42 {
		t.Errorf("Apply result = %v (%T), want int64(42)", result, result)
	}
}

// newResultNodeWithForward mirrors newTestNodeWithForward but backs the
// node with a resultMockDB so ExecResult counts flow.
func newResultNodeWithForward(
	t testing.TB,
	nodeID string,
	tlsCfg *tls.Config,
	raftAddr, fwdAddr string,
	peers, fwdPeers []string,
	rowsAffected int64,
) (*memraft.Node, *resultMockDB) {
	t.Helper()
	db := &resultMockDB{rowsAffected: rowsAffected}
	cfg := memraft.NodeConfig{
		NodeID:       nodeID,
		BindAddr:     raftAddr,
		ForwardAddr:  fwdAddr,
		Peers:        peers,
		ForwardPeers: fwdPeers,
		DataDir:      t.TempDir(),
		TLSConfig:    tlsCfg,
		// Generous timeouts for -race + parallel clusters; see
		// newTestNodeWithForward for the rationale.
		HeartbeatTimeout: 1500 * time.Millisecond,
		ElectionTimeout:  1500 * time.Millisecond,
		CommitTimeout:    10 * time.Millisecond,
		ApplyTimeout:     10 * time.Second,
	}
	node, err := memraft.NewNode(db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Shutdown() })
	return node, db
}

func TestNode_ExecResult_SingleNode_ReturnsCount(t *testing.T) {
	t.Parallel()

	tlsCfg := generateTLSConfig(t)
	db := &resultMockDB{rowsAffected: 5}

	addr := pickFreeAddr(t)
	cfg := memraft.NodeConfig{
		NodeID:           "node-1",
		BindAddr:         addr,
		Peers:            []string{"node-1=" + addr},
		DataDir:          t.TempDir(),
		TLSConfig:        tlsCfg,
		HeartbeatTimeout: 1500 * time.Millisecond,
		ElectionTimeout:  1500 * time.Millisecond,
		CommitTimeout:    10 * time.Millisecond,
		ApplyTimeout:     10 * time.Second,
	}
	node, err := memraft.NewNode(db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Shutdown() })

	waitForLeader(t, node, 10*time.Second)

	n, err := node.ExecResult("UPDATE kv SET v = 1")
	if err != nil {
		t.Fatalf("ExecResult: %v", err)
	}
	if n != 5 {
		t.Errorf("ExecResult count = %d, want 5", n)
	}
}

// TestNode_ExecResult_ForwardedFromFollower verifies the count crosses the
// write-forwarding RPC: a follower's ExecResult must report the count the
// leader's FSM produced.
func TestNode_ExecResult_ForwardedFromFollower(t *testing.T) {
	t.Parallel()
	tlsCfg := generateTLSConfig(t)

	raft1 := pickFreeAddr(t)
	raft2 := pickFreeAddr(t)
	raft3 := pickFreeAddr(t)
	fwd1 := pickFreeAddr(t)
	fwd2 := pickFreeAddr(t)
	fwd3 := pickFreeAddr(t)

	peers := []string{
		"node-1=" + raft1,
		"node-2=" + raft2,
		"node-3=" + raft3,
	}
	fwdPeers := []string{
		"node-1=" + fwd1,
		"node-2=" + fwd2,
		"node-3=" + fwd3,
	}

	const wantCount = 9
	node1, _ := newResultNodeWithForward(t, "node-1", tlsCfg, raft1, fwd1, peers, fwdPeers, wantCount)
	node2, _ := newResultNodeWithForward(t, "node-2", tlsCfg, raft2, fwd2, peers, fwdPeers, wantCount)
	node3, _ := newResultNodeWithForward(t, "node-3", tlsCfg, raft3, fwd3, peers, fwdPeers, wantCount)

	allNodes := []*memraft.Node{node1, node2, node3}

	// Wait for a leader.
	deadline := time.Now().Add(10 * time.Second)
	var elected *memraft.Node
	for time.Now().Before(deadline) {
		for _, n := range allNodes {
			if n.IsLeader() {
				elected = n
				break
			}
		}
		if elected != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if elected == nil {
		t.Fatal("no node became leader within 10s")
	}

	// Pick a follower and wait until it knows the leader.
	var follower *memraft.Node
	for _, n := range allNodes {
		if n != elected {
			follower = n
			break
		}
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if follower.LeaderAddr() != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if follower.LeaderAddr() == "" {
		t.Fatal("follower does not know the leader address after 5s")
	}

	n, err := follower.ExecResult("DELETE FROM kv WHERE expired = 1")
	if err != nil {
		t.Fatalf("follower.ExecResult: %v", err)
	}
	if n != wantCount {
		t.Errorf("forwarded ExecResult count = %d, want %d", n, wantCount)
	}
}
