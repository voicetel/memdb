package tuning_test

import (
	"fmt"
	"time"

	"github.com/voicetel/memdb/tuning"
)

// ExampleRecommend demonstrates the intended usage pattern: call
// tuning.Recommend once at startup with the operator's workload inputs,
// then copy the returned fields straight into memdb.Config. The Rationale
// string is suitable for a one-line slog.Info entry so the binding
// constraint (memory, cores, or caller cap) is visible at a glance.
//
// The scenario below matches the "small app session store" case from
// BENCHMARKS.md: a 10 MiB database, 200 MiB replica budget, 4 cores,
// read-heavy workload. The recommender should:
//
//   - bind ReadPoolSize to GOMAXPROCS (4) because the memory cap (200/10 = 20)
//     is looser than the core cap,
//   - return the default 50 ms refresh interval (StalenessTolerance unset),
//   - report ~40 MiB of estimated replica memory (4 replicas × 10 MiB),
//   - report a read speedup around 2.6× (AlphaRead × 4 = 0.65 × 4 = 2.6).
//
// CPUCores is pinned to 4 in this example so the output is deterministic
// regardless of the host the doc test runs on.
func ExampleRecommend() {
	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize: 10 << 20,  // 10 MiB in-memory DB
		MemoryBudget: 200 << 20, // 200 MiB budget for the replica pool
		CPUCores:     4,         // pinned for a reproducible example
		Workload:     tuning.WorkloadReadHeavy,
	})

	fmt.Printf("ReadPoolSize=%d\n", rec.ReadPoolSize)
	fmt.Printf("ReplicaRefreshInterval=%s\n", rec.ReplicaRefreshInterval)
	fmt.Printf("EstimatedReplicaBytes=%d\n", rec.EstimatedReplicaBytes)
	fmt.Printf("EstimatedReadSpeedup=%.2fx\n", rec.EstimatedReadSpeedup)

	// Output:
	// ReadPoolSize=4
	// ReplicaRefreshInterval=50ms
	// EstimatedReplicaBytes=41943040
	// EstimatedReadSpeedup=2.60x
}

// ExampleRecommend_tightMemory shows the memory-bound branch of the
// recommender. With a 100 MiB database and only a 250 MiB replica
// budget on an 8-core box, the recommender picks 2 replicas (floor of
// 250/100) rather than 8 — memory binds the decision and the Rationale
// says so explicitly.
func ExampleRecommend_tightMemory() {
	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize: 100 << 20, // 100 MiB DB
		MemoryBudget: 250 << 20, // budget only covers 2 replicas
		CPUCores:     8,
		Workload:     tuning.WorkloadReadHeavy,
	})

	fmt.Printf("ReadPoolSize=%d\n", rec.ReadPoolSize)
	fmt.Printf("EstimatedReplicaBytes=%d\n", rec.EstimatedReplicaBytes)

	// Output:
	// ReadPoolSize=2
	// EstimatedReplicaBytes=209715200
}

// ExampleRecommend_writeHeavy shows the write-heavy halving rule. The
// recommender assumes every refresh tick will do real work under a
// write-heavy workload, so it keeps replica count smaller to leave
// CPU headroom for actual query work rather than refresh churn.
//
// Same inputs as ExampleRecommend except for the workload hint:
// read-heavy → 4 replicas, write-heavy → 2 replicas (halved).
func ExampleRecommend_writeHeavy() {
	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize: 10 << 20,
		MemoryBudget: 200 << 20,
		CPUCores:     4,
		Workload:     tuning.WorkloadWriteHeavy,
	})

	fmt.Printf("ReadPoolSize=%d\n", rec.ReadPoolSize)

	// Output:
	// ReadPoolSize=2
}

// ExampleRecommend_noPoolFallback shows the safe fallback: when the
// memory budget cannot hold even one full replica, the recommender
// returns ReadPoolSize=0 and a Rationale explaining why. A caller who
// passes the returned struct straight into memdb.Config gets the
// single-connection default rather than a broken pool configuration.
func ExampleRecommend_noPoolFallback() {
	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize: 100 << 20, // 100 MiB DB
		MemoryBudget: 50 << 20,  // only 50 MiB — cannot hold one replica
		CPUCores:     8,
	})

	fmt.Printf("ReadPoolSize=%d\n", rec.ReadPoolSize)
	fmt.Printf("ReplicaRefreshInterval=%s\n", rec.ReplicaRefreshInterval)

	// Output:
	// ReadPoolSize=0
	// ReplicaRefreshInterval=0s
}

// ExampleRecommend_stalenessBand shows the three clamp paths on
// ReplicaRefreshInterval: a value below the 5ms floor is raised, a
// value above the 500ms ceiling is lowered, and an in-band value is
// used verbatim. The recommender never produces a refresh interval
// that pprof has shown to dominate CPU (below 5ms) and never produces
// one so long that the "replica" abstraction becomes misleading
// (above 500ms).
func ExampleRecommend_stalenessBand() {
	base := tuning.Workload{
		DatabaseSize: 10 << 20,
		MemoryBudget: 200 << 20,
		CPUCores:     4,
		Workload:     tuning.WorkloadReadHeavy,
	}

	for _, tol := range []time.Duration{
		1 * time.Millisecond,   // below floor
		100 * time.Millisecond, // in-band
		10 * time.Second,       // above ceiling
	} {
		w := base
		w.StalenessTolerance = tol
		rec := tuning.Recommend(w)
		fmt.Printf("tolerance=%s -> interval=%s\n", tol, rec.ReplicaRefreshInterval)
	}

	// Output:
	// tolerance=1ms -> interval=5ms
	// tolerance=100ms -> interval=100ms
	// tolerance=10s -> interval=500ms
}
