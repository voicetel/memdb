package raft_test

import (
	"crypto/tls"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/voicetel/memdb"
	"github.com/voicetel/memdb/profiling"
	memraft "github.com/voicetel/memdb/replication/raft"
)

// TestPProf_Raft_MemDB profiles the full integrated path that the
// `memdb serve -raft-*` CLI exercises:
//
//	leaderDB.Exec → OnExec → node.Exec → consensus →
//	    FSM Apply on every node → adapter.ExecLocal → memdb.ExecDirect → SQLite write + WAL append
//
// The existing TestPProf_Raft_Apply uses a mock FSM, so it captures the
// hashicorp/raft and transport cost but leaves the real apply-path cost
// (sql.Stmt execution, WAL fsync, write-generation bump) invisible. This
// test drives sustained INSERTs through a real three-node cluster of
// memdb.DB instances so the profile reflects the cost an operator running
// memdb in clustered mode actually pays per write.
func TestPProf_Raft_MemDB(t *testing.T) {
	dir := raftPprofOutputDir(t)

	cluster := newMemDBCluster(t)
	defer cluster.shutdown()

	leader := cluster.leader()
	if leader == nil {
		t.Fatal("no leader after waitForLeader")
	}
	leaderDB := cluster.leaderDB()

	// Seed the schema on the leader. This goes through OnExec and replicates
	// to every node so all three FSMs have the table before the workload
	// starts.
	if _, err := leaderDB.Exec(
		"CREATE TABLE IF NOT EXISTS kv (k INTEGER PRIMARY KEY, v TEXT)",
	); err != nil {
		t.Fatalf("create table: %v", err)
	}

	cpuPath := filepath.Join(dir, "pprof_raft_memdb.cpu.prof")
	heapPath := filepath.Join(dir, "pprof_raft_memdb.heap.prof")

	const workDuration = 5 * time.Second
	var writes atomic.Int64

	start := time.Now()
	err := profiling.CaptureCPUProfile(cpuPath, func() error {
		deadline := time.Now().Add(workDuration)
		i := 0
		for time.Now().Before(deadline) {
			if _, err := leaderDB.Exec(
				"INSERT INTO kv (k, v) VALUES (?, ?)",
				i, fmt.Sprintf("v-%d", i),
			); err != nil {
				return fmt.Errorf("insert %d: %w", i, err)
			}
			i++
			writes.Add(1)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("CaptureCPUProfile: %v", err)
	}

	if err := cluster.waitForRowCount("kv", int(writes.Load()), 15*time.Second); err != nil {
		t.Fatalf("waitForRowCount: %v", err)
	}

	if err := profiling.CaptureHeapProfile(heapPath, func() error { return nil }); err != nil {
		t.Fatalf("CaptureHeapProfile: %v", err)
	}

	n := writes.Load()
	t.Logf("raft+memdb apply: %d committed writes in %s (%.0f writes/s, 3-node cluster)  cpu=%s  heap=%s",
		n, time.Since(start),
		float64(n)/time.Since(start).Seconds(),
		cpuPath, heapPath)
}

// memDBCluster bundles three real memdb.DB instances and their Raft nodes
// using the same adapter pattern the CLI uses (raftDBAdapter wraps ExecDirect
// as ExecLocal).
type memDBCluster struct {
	nodes []*memraft.Node
	dbs   []*memdb.DB
	ids   []string
}

func newMemDBCluster(t *testing.T) *memDBCluster {
	t.Helper()
	tlsCfg := generateTLSConfig(t)

	raftAddrs := []string{pickFreeAddr(t), pickFreeAddr(t), pickFreeAddr(t)}
	fwdAddrs := []string{pickFreeAddr(t), pickFreeAddr(t), pickFreeAddr(t)}
	ids := []string{"node-1", "node-2", "node-3"}

	peers := []string{
		ids[0] + "=" + raftAddrs[0],
		ids[1] + "=" + raftAddrs[1],
		ids[2] + "=" + raftAddrs[2],
	}
	fwdPeers := []string{
		ids[0] + "=" + fwdAddrs[0],
		ids[1] + "=" + fwdAddrs[1],
		ids[2] + "=" + fwdAddrs[2],
	}

	c := &memDBCluster{ids: ids}
	for i := range ids {
		db, node := openMemDBNode(t, ids[i], tlsCfg, raftAddrs[i], fwdAddrs[i], peers, fwdPeers)
		c.dbs = append(c.dbs, db)
		c.nodes = append(c.nodes, node)
	}

	if err := c.waitForLeader(20 * time.Second); err != nil {
		t.Fatal(err)
	}
	return c
}

// openMemDBNode opens a real memdb.DB with a file backend in t.TempDir() and
// wires its OnExec hook to a freshly created Raft node via the same closure
// pattern as cmd/memdb. The adapter is inlined here to keep the test
// self-contained — cmd/memdb's adapter is in package main.
func openMemDBNode(
	t *testing.T,
	nodeID string,
	tlsCfg *tls.Config,
	raftAddr, fwdAddr string,
	peers, fwdPeers []string,
) (*memdb.DB, *memraft.Node) {
	t.Helper()

	dataDir := t.TempDir()

	var nodeExec func(sql string, args ...any) error
	cfg := memdb.Config{
		FilePath:      filepath.Join(dataDir, "memdb.db"),
		FlushInterval: 0,
		Durability:    memdb.DurabilityWAL,
		OnExec: func(sql string, args []any) error {
			return nodeExec(sql, args...)
		},
	}
	db, err := memdb.Open(cfg)
	if err != nil {
		t.Fatalf("memdb.Open %s: %v", nodeID, err)
	}

	raftCfg := memraft.NodeConfig{
		NodeID:           nodeID,
		BindAddr:         raftAddr,
		ForwardAddr:      fwdAddr,
		Peers:            peers,
		ForwardPeers:     fwdPeers,
		DataDir:          filepath.Join(dataDir, "raft"),
		TLSConfig:        tlsCfg,
		HeartbeatTimeout: 1500 * time.Millisecond,
		ElectionTimeout:  1500 * time.Millisecond,
		CommitTimeout:    10 * time.Millisecond,
		ApplyTimeout:     10 * time.Second,
	}
	node, err := memraft.NewNode(memdbAdapter{db: db}, raftCfg)
	if err != nil {
		_ = db.Close()
		t.Fatalf("NewNode %s: %v", nodeID, err)
	}
	nodeExec = node.Exec
	return db, node
}

type memdbAdapter struct{ db *memdb.DB }

func (a memdbAdapter) ExecLocal(sql string, args ...any) error {
	return a.db.ExecDirect(sql, args...)
}
func (a memdbAdapter) Serialize() ([]byte, error) { return a.db.Serialize() }
func (a memdbAdapter) Restore(data []byte) error  { return a.db.Restore(data) }

func (c *memDBCluster) shutdown() {
	for _, n := range c.nodes {
		_ = n.Shutdown()
	}
	for _, db := range c.dbs {
		_ = db.Close()
	}
}

func (c *memDBCluster) waitForLeader(timeout time.Duration) error {
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
	return fmt.Errorf("cluster did not converge on a leader within %s", timeout)
}

func (c *memDBCluster) leader() *memraft.Node {
	for _, n := range c.nodes {
		if n.IsLeader() {
			return n
		}
	}
	return nil
}

func (c *memDBCluster) leaderDB() *memdb.DB {
	for i, n := range c.nodes {
		if n.IsLeader() {
			return c.dbs[i]
		}
	}
	return nil
}

// waitForRowCount polls every node's local memdb instance until the named
// table contains at least want rows, confirming the FSM apply path drained.
func (c *memDBCluster) waitForRowCount(table string, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	query := "SELECT COUNT(*) FROM " + table
	for time.Now().Before(deadline) {
		allOK := true
		for _, db := range c.dbs {
			var n int
			if err := db.QueryRow(query).Scan(&n); err != nil {
				allOK = false
				break
			}
			if n < want {
				allOK = false
				break
			}
		}
		if allOK {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	counts := make([]int, len(c.dbs))
	for i, db := range c.dbs {
		_ = db.QueryRow(query).Scan(&counts[i])
	}
	return fmt.Errorf("rows in %s did not reach %d within %s (counts=%v)",
		table, want, timeout, counts)
}
