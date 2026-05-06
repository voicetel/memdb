package raft_test

import (
	"fmt"
	"testing"
)

// BenchmarkForwarding_PoolReuse measures sustained throughput of forwarded
// writes through a single follower. Each iteration drives one Exec on the
// follower, which serialises a ForwardRequest, ships it over the pooled TLS
// connection to the leader, waits for the Raft round-trip, and reads the
// response.
//
// This benchmark exists because (a) there were no perf tests on the
// forwarding path, and (b) the v1.8.1 fix that made handleConn loop on a
// pooled conn instead of single-shotting it is the kind of change that is
// easy to silently regress (e.g. someone re-introducing per-iteration
// connection setup). With the loop in place this benchmark sustains the
// per-Raft-apply cost; without it, the pool would dial fresh on every
// iteration (or, worse, fail with EOF as the original bug did).
//
// Numbers are dominated by the Raft consensus round-trip on loopback. The
// signal to watch for in future runs is per-op allocations, not absolute
// ns/op — handshake regressions show up as a large allocs/op jump.
func BenchmarkForwarding_PoolReuse(b *testing.B) {
	cluster := newThreeNodeCluster(b)
	follower := cluster.follower()
	if follower == nil {
		b.Fatal("missing follower")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := follower.Exec(fmt.Sprintf("INSERT bench-%d", i)); err != nil {
			b.Fatalf("forward #%d: %v", i, err)
		}
	}
}
