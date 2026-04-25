package raft_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/voicetel/memdb/profiling"
)

// This file captures CPU and heap profiles of the full Raft pipeline —
// client Exec on the leader → log replication → FSM Apply on every node.
// No prior pprof scenario exercised this path; on a write-heavy cluster
// it is usually where the dominant CPU cost lives (log encode, transport
// I/O, FSM dispatch), not in memdb's local writer.
//
// Gated behind MEMDB_PPROF=1 like the other pprof suites so the default
// `go test ./replication/raft/...` run does not write profile artefacts.
//
// Run with:
//
//	MEMDB_PPROF=1 MEMDB_PPROF_DIR=/tmp/memdb-raft-prof \
//	    go test -run TestPProf_Raft_ -v ./replication/raft/...
//
// Analyse with:
//
//	go tool pprof -http=: /tmp/memdb-raft-prof/pprof_raft_apply.cpu.prof
//	go tool pprof -http=: /tmp/memdb-raft-prof/pprof_raft_apply.heap.prof

// raftPprofOutputDir mirrors the gating used by the root-package and
// server-package TestPProf_ suites.
func raftPprofOutputDir(t *testing.T) string {
	t.Helper()
	if os.Getenv("MEMDB_PPROF") == "" {
		t.Skip("set MEMDB_PPROF=1 to enable raft pprof capture tests")
	}
	dir := os.Getenv("MEMDB_PPROF_DIR")
	if dir == "" {
		dir = t.TempDir()
		t.Logf("MEMDB_PPROF_DIR not set — writing to %s", dir)
	} else {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
	return dir
}

// TestPProf_Raft_Apply drives sustained writes through the leader of a
// three-node cluster. Each Exec call goes through the full consensus
// round trip (Propose → fsync log → AppendEntries → quorum ack →
// Apply) so the CPU profile reflects the steady-state cost of running
// memdb in clustered mode. The mockDB FSM is intentionally cheap, so
// the profile is dominated by hashicorp/raft, the TLS transport, and
// the FSM dispatch wiring — exactly the surface most likely to harbour
// per-write overhead worth optimising.
func TestPProf_Raft_Apply(t *testing.T) {
	dir := raftPprofOutputDir(t)

	cluster := newThreeNodeCluster(t)
	leader := cluster.leader()
	if leader == nil {
		t.Fatal("no leader after waitForLeader")
	}

	cpuPath := filepath.Join(dir, "pprof_raft_apply.cpu.prof")
	heapPath := filepath.Join(dir, "pprof_raft_apply.heap.prof")

	const workDuration = 5 * time.Second
	var writes atomic.Int64

	start := time.Now()
	err := profiling.CaptureCPUProfile(cpuPath, func() error {
		deadline := time.Now().Add(workDuration)
		i := 0
		for time.Now().Before(deadline) {
			if err := leader.Exec(
				"INSERT INTO kv (key, value) VALUES (?, ?)",
				fmt.Sprintf("k-%d", i), fmt.Sprintf("v-%d", i),
			); err != nil {
				return fmt.Errorf("apply %d: %w", i, err)
			}
			i++
			writes.Add(1)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("CaptureCPUProfile: %v", err)
	}

	// Wait for the followers to drain their apply queue so the heap
	// snapshot taken below reflects steady-state, not transient
	// in-flight log entries.
	if err := cluster.waitForReplication(int(writes.Load()), 15*time.Second); err != nil {
		t.Fatalf("waitForReplication: %v", err)
	}

	if err := profiling.CaptureHeapProfile(heapPath, func() error { return nil }); err != nil {
		t.Fatalf("CaptureHeapProfile: %v", err)
	}

	n := writes.Load()
	t.Logf("raft apply: %d committed writes in %s (%.0f writes/s, 3-node cluster)  cpu=%s  heap=%s",
		n, time.Since(start),
		float64(n)/time.Since(start).Seconds(),
		cpuPath, heapPath)
}
