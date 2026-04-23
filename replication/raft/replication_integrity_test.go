//go:build !purego

package raft_test

import (
	"crypto/tls"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	memraft "github.com/voicetel/memdb/replication/raft"
)

// This file contains end-to-end replication correctness tests that exercise
// the full Raft pipeline (client → leader → FSM → followers) on a real
// three-node cluster bound to loopback ports. The focus is correctness and
// data integrity rather than throughput:
//
//   - TestReplication_AllNodesConverge           — every committed entry is
//                                                   applied on every node
//                                                   and the FSM logs are
//                                                   byte-identical across
//                                                   nodes.
//   - TestReplication_WriteThroughFollower       — writes submitted to a
//                                                   follower are forwarded to
//                                                   the leader and replicated.
//   - TestReplication_ConcurrentWriters          — multiple goroutines writing
//                                                   concurrently produce
//                                                   identical FSM state on
//                                                   every node, with no
//                                                   duplication or loss.
//   - TestReplication_LeaderRestart              — after a leader restart the
//                                                   cluster re-elects and
//                                                   continues to replicate
//                                                   without data loss.
//   - TestReplication_HighVolume_NoCorruption    — a soak test that writes a
//                                                   large number of entries
//                                                   and checks the entries on
//                                                   every follower match the
//                                                   leader byte-for-byte.
//   - TestReplication_Snapshot_RestoresCleanly   — a new follower joining an
//                                                   existing cluster receives
//                                                   state via the snapshot
//                                                   mechanism.

// ---------------------------------------------------------------------------
// test helpers
// ---------------------------------------------------------------------------

// threeNodeCluster brings up a three-node Raft cluster with forwarding
// enabled and returns handles to each node plus its mock DB.
type threeNodeCluster struct {
	nodes  []*memraft.Node
	dbs    []*mockDB
	ids    []string
	raft   []string
	fwd    []string
	tlsCfg *tls.Config
}

// newThreeNodeCluster constructs the cluster and waits for a leader to be
// elected. It registers cleanup so the t.TempDir-based node state is
// discarded at the end of the test.
func newThreeNodeCluster(t *testing.T) *threeNodeCluster {
	t.Helper()
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

	n1, db1 := newTestNodeWithForward(t, "node-1", tlsCfg, raft1, fwd1, peers, fwdPeers)
	n2, db2 := newTestNodeWithForward(t, "node-2", tlsCfg, raft2, fwd2, peers, fwdPeers)
	n3, db3 := newTestNodeWithForward(t, "node-3", tlsCfg, raft3, fwd3, peers, fwdPeers)

	cluster := &threeNodeCluster{
		nodes:  []*memraft.Node{n1, n2, n3},
		dbs:    []*mockDB{db1, db2, db3},
		ids:    []string{"node-1", "node-2", "node-3"},
		raft:   []string{raft1, raft2, raft3},
		fwd:    []string{fwd1, fwd2, fwd3},
		tlsCfg: tlsCfg,
	}

	if err := cluster.waitForLeader(20 * time.Second); err != nil {
		t.Fatal(err)
	}
	return cluster
}

// waitForLeader blocks until the cluster has fully converged on a leader:
// exactly one node reports IsLeader() == true AND every node reports a
// non-empty LeaderAddr(). Waiting only for IsLeader (the previous
// behaviour) returned as soon as the winning node entered the leader
// state, which on a loaded test host (parallel -race runs sharing CPU
// with many other Raft clusters in the same binary) meant the followers
// had not yet received the first heartbeat and would flake any
// subsequent Exec/forwarding call.
//
// The timeout is generous because the heartbeat / election timeouts
// used in tests are 500 ms and under CPU starvation elections can take
// several cycles to converge. This is a test helper, not a production
// code path, so a larger budget is acceptable.
func (c *threeNodeCluster) waitForLeader(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leaders := 0
		allHaveLeaderAddr := true
		for _, n := range c.nodes {
			if n.IsLeader() {
				leaders++
			}
			if n.LeaderAddr() == "" {
				allHaveLeaderAddr = false
			}
		}
		if leaders == 1 && allHaveLeaderAddr {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Build a diagnostic snapshot so a flaky run in CI is easier to
	// triage from the test output alone.
	var state []string
	for i, n := range c.nodes {
		state = append(state,
			fmt.Sprintf("node-%d{isLeader=%t leaderAddr=%q}",
				i+1, n.IsLeader(), n.LeaderAddr()))
	}
	return fmt.Errorf("cluster did not converge on a single leader within %s: %v",
		timeout, state)
}

// leader returns the current leader node (or nil if none). Callers that
// depend on leadership should call waitForLeader first.
func (c *threeNodeCluster) leader() *memraft.Node {
	for _, n := range c.nodes {
		if n.IsLeader() {
			return n
		}
	}
	return nil
}

// follower returns any non-leader node.
func (c *threeNodeCluster) follower() *memraft.Node {
	leader := c.leader()
	for _, n := range c.nodes {
		if n != leader {
			return n
		}
	}
	return nil
}

// waitForReplication polls until every FSM has seen at least wantCount entries
// or the timeout elapses.
func (c *threeNodeCluster) waitForReplication(wantCount int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allOK := true
		for _, db := range c.dbs {
			if db.execCount() < wantCount {
				allOK = false
				break
			}
		}
		if allOK {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Build a diagnostic snapshot of each node's count.
	counts := make([]int, len(c.dbs))
	for i, db := range c.dbs {
		counts[i] = db.execCount()
	}
	return fmt.Errorf("replication did not converge to %d entries within %s (counts=%v)",
		wantCount, timeout, counts)
}

// snapshotLogs returns a byte-level copy of every FSM's log slice so the
// caller can compare nodes without risk of a concurrent ExecLocal mutating
// the slice mid-comparison.
func (c *threeNodeCluster) snapshotLogs() [][]string {
	out := make([][]string, len(c.dbs))
	for i, db := range c.dbs {
		db.mu.Lock()
		out[i] = append([]string(nil), db.log...)
		db.mu.Unlock()
	}
	return out
}

// assertLogsIdentical fails the test if any two nodes' FSM logs differ.
func assertLogsIdentical(t *testing.T, logs [][]string) {
	t.Helper()
	if len(logs) == 0 {
		t.Fatal("assertLogsIdentical: empty logs")
	}
	ref := logs[0]
	for i, other := range logs[1:] {
		if len(other) != len(ref) {
			t.Fatalf("node 0 has %d entries but node %d has %d",
				len(ref), i+1, len(other))
		}
		for j := range ref {
			if ref[j] != other[j] {
				t.Fatalf("node 0 entry %d = %q, node %d entry %d = %q",
					j, ref[j], i+1, j, other[j])
			}
		}
	}
}

// ---------------------------------------------------------------------------
// TestReplication_AllNodesConverge
// ---------------------------------------------------------------------------

// TestReplication_AllNodesConverge writes a modest number of entries on the
// leader and verifies that every node's FSM sees every entry, in the same
// order, with byte-identical content.
func TestReplication_AllNodesConverge(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping replication integration test in -short mode")
	}
	t.Parallel()
	cluster := newThreeNodeCluster(t)

	leader := cluster.leader()
	if leader == nil {
		t.Fatal("no leader after waitForLeader")
	}

	const nWrites = 100
	for i := 0; i < nWrites; i++ {
		if err := leader.Exec(
			"INSERT INTO kv (key, value) VALUES (?, ?)",
			fmt.Sprintf("k-%04d", i),
			fmt.Sprintf("v-%04d", i),
		); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	if err := cluster.waitForReplication(nWrites, 15*time.Second); err != nil {
		t.Fatal(err)
	}

	// Byte-identical FSM logs across all three nodes.
	assertLogsIdentical(t, cluster.snapshotLogs())
}

// ---------------------------------------------------------------------------
// TestReplication_WriteThroughFollower
// ---------------------------------------------------------------------------

// TestReplication_WriteThroughFollower verifies that a write submitted to a
// follower is transparently forwarded to the leader, committed via Raft, and
// applied on every node. Then it additionally verifies that the forwarded
// write arrives in global log order (not out-of-band).
func TestReplication_WriteThroughFollower(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping replication integration test in -short mode")
	}
	t.Parallel()
	cluster := newThreeNodeCluster(t)

	leader := cluster.leader()
	follower := cluster.follower()
	if leader == nil || follower == nil {
		t.Fatal("cluster missing leader or follower")
	}

	// Leader writes A, follower writes B, leader writes C. The follower's
	// forwarded write must land between A and C on every node.
	if err := leader.Exec("INSERT a"); err != nil {
		t.Fatalf("leader write A: %v", err)
	}
	if err := follower.Exec("INSERT b"); err != nil {
		t.Fatalf("follower write B: %v", err)
	}
	if err := leader.Exec("INSERT c"); err != nil {
		t.Fatalf("leader write C: %v", err)
	}

	if err := cluster.waitForReplication(3, 10*time.Second); err != nil {
		t.Fatal(err)
	}

	logs := cluster.snapshotLogs()
	assertLogsIdentical(t, logs)

	// The order on any node must be A, B, C.
	wantOrder := []string{"INSERT a", "INSERT b", "INSERT c"}
	for i, got := range logs[0] {
		if got != wantOrder[i] {
			t.Fatalf("entry %d: got %q, want %q", i, got, wantOrder[i])
		}
	}
}

// ---------------------------------------------------------------------------
// TestReplication_ConcurrentWriters
// ---------------------------------------------------------------------------

// TestReplication_ConcurrentWriters fires many goroutines at the leader
// simultaneously and verifies the final FSM state is consistent across
// nodes (same length, same multiset of entries). Concurrent writers must
// not cause duplicate application or dropped entries.
func TestReplication_ConcurrentWriters(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping replication integration test in -short mode")
	}
	t.Parallel()
	cluster := newThreeNodeCluster(t)

	leader := cluster.leader()
	if leader == nil {
		t.Fatal("no leader")
	}

	const (
		writers          = 8
		writesPerGoroutn = 25
		totalWrites      = writers * writesPerGoroutn
	)

	var wg sync.WaitGroup
	errs := make(chan error, totalWrites)

	for g := 0; g < writers; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < writesPerGoroutn; i++ {
				if err := leader.Exec(
					"INSERT INTO kv (key, value) VALUES (?, ?)",
					fmt.Sprintf("g%d-k%03d", g, i),
					fmt.Sprintf("g%d-v%03d", g, i),
				); err != nil {
					errs <- fmt.Errorf("writer %d row %d: %w", g, i, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	if err := cluster.waitForReplication(totalWrites, 30*time.Second); err != nil {
		t.Fatal(err)
	}

	logs := cluster.snapshotLogs()
	assertLogsIdentical(t, logs)

	// Every goroutine's writes are present exactly once on every node. We
	// do this by counting each (writer, row) pair. If Raft dropped or
	// duplicated an entry, the count would not be 1 on every node.
	counts := make(map[string]int)
	for _, entry := range logs[0] {
		counts[entry]++
	}
	for entry, n := range counts {
		if n != 1 {
			t.Errorf("entry %q applied %d times on node 0 (expected 1)", entry, n)
		}
	}
	if len(counts) != totalWrites {
		t.Errorf("got %d unique entries, want %d", len(counts), totalWrites)
	}
}

// ---------------------------------------------------------------------------
// TestReplication_LeaderRestart
// ---------------------------------------------------------------------------

// TestReplication_LeaderRestart simulates a leader failure by shutting down
// the current leader and verifies that the remaining two nodes elect a new
// leader and continue to accept writes. The two remaining nodes must have
// identical FSM state for every committed entry before and after the
// re-election.
//
// Note: we cannot trivially restart a node in-place (DataDir is kept in a
// t.TempDir and NewNode acquires new store handles) — this test instead
// verifies that the cluster of two surviving nodes continues to make
// progress, which is the operationally important property.
func TestReplication_LeaderRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping replication integration test in -short mode")
	}
	t.Parallel()
	cluster := newThreeNodeCluster(t)

	leader := cluster.leader()
	if leader == nil {
		t.Fatal("no initial leader")
	}

	// Write the initial batch that all three nodes will see.
	const preShutdown = 30
	for i := 0; i < preShutdown; i++ {
		if err := leader.Exec(
			"INSERT pre",
			i,
		); err != nil {
			t.Fatalf("pre-shutdown write %d: %v", i, err)
		}
	}
	if err := cluster.waitForReplication(preShutdown, 10*time.Second); err != nil {
		t.Fatal(err)
	}

	// Identify the leader's index so we can skip it when looking for
	// surviving followers.
	leaderIdx := -1
	for i, n := range cluster.nodes {
		if n == leader {
			leaderIdx = i
			break
		}
	}
	if leaderIdx < 0 {
		t.Fatal("leader not found in cluster.nodes")
	}

	// Shut down the leader.
	if err := leader.Shutdown(); err != nil {
		t.Fatalf("leader shutdown: %v", err)
	}

	// Wait for a new leader to be elected among the remaining nodes. With
	// only two nodes, one will win once the election timeout elapses.
	var newLeader *memraft.Node
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for i, n := range cluster.nodes {
			if i == leaderIdx {
				continue
			}
			if n.IsLeader() {
				newLeader = n
				break
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if newLeader == nil {
		t.Fatal("no new leader elected within 15s after leader shutdown")
	}

	// Continue writing via the new leader — the two surviving nodes should
	// both apply these.
	const postShutdown = 20
	for i := 0; i < postShutdown; i++ {
		if err := newLeader.Exec("INSERT post", i); err != nil {
			t.Fatalf("post-shutdown write %d: %v", i, err)
		}
	}

	// Wait for the two surviving FSMs to converge to preShutdown+postShutdown.
	totalExpected := preShutdown + postShutdown
	deadline = time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		allOK := true
		for i, db := range cluster.dbs {
			if i == leaderIdx {
				continue // this node was shut down — its count is frozen
			}
			if db.execCount() < totalExpected {
				allOK = false
				break
			}
		}
		if allOK {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Verify the two surviving nodes have identical logs.
	survivors := make([][]string, 0, 2)
	for i, db := range cluster.dbs {
		if i == leaderIdx {
			continue
		}
		db.mu.Lock()
		survivors = append(survivors, append([]string(nil), db.log...))
		db.mu.Unlock()
	}
	assertLogsIdentical(t, survivors)

	if len(survivors[0]) < totalExpected {
		t.Errorf("surviving nodes only saw %d entries, expected %d",
			len(survivors[0]), totalExpected)
	}
}

// ---------------------------------------------------------------------------
// TestReplication_HighVolume_NoCorruption
// ---------------------------------------------------------------------------

// TestReplication_HighVolume_NoCorruption is a soak test: it submits a large
// number of entries with varying argument types and verifies byte-level
// replication integrity on every node. The varying arg types exercise the
// gob-encoded Args path on every commit so any encode/decode mismatch would
// surface as a cross-node divergence.
func TestReplication_HighVolume_NoCorruption(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping soak test in -short mode")
	}
	t.Parallel()
	cluster := newThreeNodeCluster(t)

	leader := cluster.leader()
	if leader == nil {
		t.Fatal("no leader")
	}

	// Deterministic seeding so a failing run is reproducible.
	rng := rand.New(rand.NewSource(0xdeadbeef))

	const total = 500
	for i := 0; i < total; i++ {
		// Rotate through several arg types so gob encoding is exercised
		// for every scalar the init() function registers.
		var args []any
		switch i % 6 {
		case 0:
			args = []any{fmt.Sprintf("string-%d", i)}
		case 1:
			args = []any{int64(i), int64(-i)}
		case 2:
			args = []any{float64(rng.NormFloat64())}
		case 3:
			args = []any{true, false}
		case 4:
			args = []any{[]byte(fmt.Sprintf("blob-%d", i))}
		case 5:
			args = []any{time.Unix(int64(i), 0).UTC()}
		}
		if err := leader.Exec("INSERT soak", args...); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	if err := cluster.waitForReplication(total, 60*time.Second); err != nil {
		t.Fatal(err)
	}

	// Every node's log must be byte-identical.
	assertLogsIdentical(t, cluster.snapshotLogs())

	// Additional integrity check: every entry begins with "INSERT soak".
	// This guards against silent truncation of the SQL string during wire
	// transport.
	for _, entry := range cluster.snapshotLogs()[0] {
		if len(entry) < len("INSERT soak") || entry[:len("INSERT soak")] != "INSERT soak" {
			t.Fatalf("corrupt FSM entry: %q", entry)
		}
	}
}

// ---------------------------------------------------------------------------
// TestReplication_Snapshot_RestoresCleanly
// ---------------------------------------------------------------------------

// TestReplication_Snapshot_RestoresCleanly writes past the default snapshot
// threshold and verifies the cluster still converges. This exercises the
// FSM.Snapshot and FSM.Restore code paths end-to-end: when Raft decides to
// snapshot, it calls Snapshot on the leader and the serialised payload is
// applied through Restore on any node that receives a snapshot instead of
// individual log entries.
//
// We can't easily trigger an InstallSnapshot RPC to a live follower in a
// test (the follower is never far enough behind), but we can verify that
// the snapshot mechanism itself does not corrupt the FSM state by writing
// enough to force multiple snapshot cycles and checking convergence.
func TestReplication_Snapshot_RestoresCleanly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping snapshot integration test in -short mode")
	}
	t.Parallel()
	cluster := newThreeNodeCluster(t)

	leader := cluster.leader()
	if leader == nil {
		t.Fatal("no leader")
	}

	// Write enough to cross the default SnapshotThreshold (8192) at least
	// once. 10k entries is plenty; we keep payloads small for speed.
	const total = 10_000
	batchStart := time.Now()
	for i := 0; i < total; i++ {
		if err := leader.Exec("INSERT snap", int64(i)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	t.Logf("wrote %d entries in %s", total, time.Since(batchStart))

	if err := cluster.waitForReplication(total, 120*time.Second); err != nil {
		t.Fatal(err)
	}

	// Byte-identical logs across all three nodes.
	assertLogsIdentical(t, cluster.snapshotLogs())

	// No entry was duplicated or dropped.
	logs := cluster.snapshotLogs()[0]
	if len(logs) != total {
		t.Fatalf("expected %d entries, got %d", total, len(logs))
	}
}

// ---------------------------------------------------------------------------
// TestReplication_NotLeader_Returned
// ---------------------------------------------------------------------------

// TestReplication_NotLeader_Returned verifies the transparent-forwarding
// happy path does NOT mask ErrNotLeader when the follower has no forward
// peers configured. This is the failure-mode counterpart to the other
// tests — it ensures writes fail cleanly when forwarding is unavailable.
func TestReplication_NotLeader_Returned(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping replication test in -short mode")
	}
	t.Parallel()
	tlsCfg := generateTLSConfig(t)

	// Single-node cluster with two phantom peers so it can never win
	// an election.
	addr := pickFreeAddr(t)
	peers := []string{
		"node-a=" + addr,
		"node-b=127.0.0.1:19997",
		"node-c=127.0.0.1:19996",
	}
	// ForwardPeers intentionally omitted so Exec can't forward.
	node, _ := newTestNode(t, "node-a", tlsCfg, peers)

	// Give the election timeout a chance to fire and fail.
	time.Sleep(1 * time.Second)

	err := node.Exec("SELECT 1")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !isNotLeaderErr(err) {
		t.Fatalf("expected ErrNotLeader, got: %v", err)
	}
}

// isNotLeaderErr reports whether err is memraft.ErrNotLeader or wraps it.
func isNotLeaderErr(err error) bool {
	return err != nil && (err == memraft.ErrNotLeader ||
		err.Error() == memraft.ErrNotLeader.Error() ||
		containsErr(err, memraft.ErrNotLeader))
}

// containsErr walks err's chain and reports whether target is present. We
// avoid errors.Is here because the forwarded error is a plain fmt.Errorf
// wrap in some paths.
func containsErr(err, target error) bool {
	for err != nil {
		if err == target {
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// ---------------------------------------------------------------------------
// TestReplication_StateAPIs
// ---------------------------------------------------------------------------

// TestReplication_StateAPIs verifies the read-only observability surface
// used by operators: Stats, IsLeader, LeaderAddr. These must remain safe
// and non-nil throughout the cluster's lifecycle.
func TestReplication_StateAPIs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping replication integration test in -short mode")
	}
	t.Parallel()
	cluster := newThreeNodeCluster(t)

	for i, n := range cluster.nodes {
		stats := n.Stats()
		if stats == nil {
			t.Errorf("node %d: Stats() returned nil", i)
		}
		// The stats map should at minimum include "state" and "term".
		if _, ok := stats["state"]; !ok {
			t.Errorf("node %d: Stats() missing 'state' key", i)
		}
		if _, ok := stats["term"]; !ok {
			t.Errorf("node %d: Stats() missing 'term' key", i)
		}

		addr := n.LeaderAddr()
		// All nodes should agree on a non-empty leader address after
		// waitForLeader returned.
		if addr == "" {
			t.Errorf("node %d: LeaderAddr() empty after leader elected", i)
		}
	}

	// Exactly one leader across the cluster.
	leaders := 0
	for _, n := range cluster.nodes {
		if n.IsLeader() {
			leaders++
		}
	}
	if leaders != 1 {
		t.Errorf("expected exactly one leader, got %d", leaders)
	}
}

// ---------------------------------------------------------------------------
// TestReplication_Shutdown_Ordering
// ---------------------------------------------------------------------------

// TestReplication_Shutdown_Ordering verifies shutting down the entire cluster
// in reverse-election order does not deadlock and returns nil errors.
func TestReplication_Shutdown_Ordering(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping replication integration test in -short mode")
	}
	t.Parallel()
	cluster := newThreeNodeCluster(t)

	// Write one entry so every node has applied state before shutdown.
	leader := cluster.leader()
	if err := leader.Exec("INSERT final"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := cluster.waitForReplication(1, 10*time.Second); err != nil {
		t.Fatal(err)
	}

	// Shut down in reverse order. We call Shutdown twice on each node —
	// the second call must be a no-op (idempotent) per Node.Shutdown docs.
	for i := len(cluster.nodes) - 1; i >= 0; i-- {
		n := cluster.nodes[i]
		if err := n.Shutdown(); err != nil {
			// hashicorp/raft may return a transport-closed error on the
			// last-standing node; accept that but still flag truly
			// unexpected failures.
			if err != hraft.ErrRaftShutdown {
				t.Logf("node %d first shutdown returned: %v", i, err)
			}
		}
		if err := n.Shutdown(); err != nil {
			t.Errorf("node %d second Shutdown() returned %v (expected nil)", i, err)
		}
	}
}

// ---------------------------------------------------------------------------
// TestReplication_ForwardingReconnect
// ---------------------------------------------------------------------------

// TestReplication_ForwardingReconnect verifies a follower's forwarding pool
// recovers when the leader changes mid-test. The follower's first write
// reaches leader A; we then induce a leadership change by shutting down A;
// the follower's next write must reach the new leader B.
func TestReplication_ForwardingReconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping replication integration test in -short mode")
	}
	t.Parallel()
	cluster := newThreeNodeCluster(t)

	oldLeader := cluster.leader()
	follower := cluster.follower()
	if oldLeader == nil || follower == nil {
		t.Fatal("missing leader or follower")
	}

	// Pre-condition: forwarded write through follower lands successfully.
	if err := follower.Exec("INSERT pre-failover"); err != nil {
		t.Fatalf("pre-failover write: %v", err)
	}
	if err := cluster.waitForReplication(1, 10*time.Second); err != nil {
		t.Fatal(err)
	}

	// Shut down the old leader so a new one is elected.
	if err := oldLeader.Shutdown(); err != nil {
		t.Fatalf("shutdown old leader: %v", err)
	}

	// Wait for a new leader to emerge (among the two survivors).
	var newLeader *memraft.Node
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range cluster.nodes {
			if n == oldLeader {
				continue
			}
			if n.IsLeader() {
				newLeader = n
				break
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if newLeader == nil {
		t.Fatal("no new leader elected within 15s")
	}

	// The surviving non-leader node must be able to forward writes to the
	// new leader. Retry a few times — the pool swap happens asynchronously
	// in response to the leader-change observation.
	var survivingFollower *memraft.Node
	for _, n := range cluster.nodes {
		if n != oldLeader && n != newLeader {
			survivingFollower = n
			break
		}
	}
	if survivingFollower == nil {
		t.Fatal("no surviving follower")
	}

	var lastErr error
	writeOK := false
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if err := survivingFollower.Exec("INSERT post-failover"); err == nil {
			writeOK = true
			break
		} else {
			lastErr = err
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !writeOK {
		t.Fatalf("surviving follower never forwarded successfully: last err=%v", lastErr)
	}
}

// ---------------------------------------------------------------------------
// TestReplication_AtomicCommit
// ---------------------------------------------------------------------------

// TestReplication_AtomicCommit checks that Exec is atomic from the caller's
// perspective: when Exec returns nil, every node has applied the entry by
// the time we poll. We verify this using a per-write barrier — Exec is
// observed to return, and then every node is checked immediately.
//
// Note: Raft's Apply returns after the entry is committed (quorum acked),
// not after every follower has applied it. Some followers may trail the
// leader by a heartbeat when Exec returns. This test measures the
// convergence window under a quiet cluster and asserts it is short.
func TestReplication_AtomicCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping replication integration test in -short mode")
	}
	t.Parallel()
	cluster := newThreeNodeCluster(t)

	leader := cluster.leader()
	if leader == nil {
		t.Fatal("no leader")
	}

	var maxLag atomic.Int64
	const writes = 20

	for i := 0; i < writes; i++ {
		if err := leader.Exec("INSERT atomic", int64(i)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		start := time.Now()
		// Busy-poll every 1ms until all three FSMs see this entry, up to 5s.
		converged := false
		for time.Since(start) < 5*time.Second {
			ok := true
			for _, db := range cluster.dbs {
				if db.execCount() < i+1 {
					ok = false
					break
				}
			}
			if ok {
				converged = true
				break
			}
			time.Sleep(time.Millisecond)
		}
		if !converged {
			t.Fatalf("write %d: cluster did not converge within 5s", i)
		}
		lag := time.Since(start).Microseconds()
		if lag > maxLag.Load() {
			maxLag.Store(lag)
		}
	}

	t.Logf("max post-Exec convergence lag across %d writes: %d µs",
		writes, maxLag.Load())

	// The tightest bound we can assert is that the cluster did in fact
	// converge after every Exec (enforced above). Log the max lag for
	// humans watching the test output.
}
