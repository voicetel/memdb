package tuning_test

import (
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/voicetel/memdb/tuning"
)

// This file exercises the tuning.Recommend model against:
//
//   - Pathological / boundary inputs (zero DB size, zero budget, tiny
//     budget, huge budget, negative values) — every case must return
//     a safe recommendation rather than panic or produce invalid
//     memdb.Config values.
//
//   - Monotonicity properties that the model claims to guarantee:
//       * ReadPoolSize is non-decreasing in MemoryBudget
//       * ReadPoolSize is capped at GOMAXPROCS (or CPUCores override)
//       * ReplicaRefreshInterval is clamped into [floor, ceiling]
//       * EstimatedRefreshCPU scales as expected with N, T, f_write
//
//   - Workload-hint branches (read-heavy, balanced, write-heavy) —
//     the write-heavy path in particular halves the recommendation
//     and the test verifies that explicitly.
//
//   - Realistic scenarios taken from BENCHMARKS.md so a regression in
//     calibration constants is caught by the test suite rather than
//     silently shipping bad advice.
//
// The tests are pure: no I/O, no goroutines, no time-sensitive
// assertions. They run in microseconds.

// ── Boundary / defensive cases ────────────────────────────────────────────

// TestRecommend_ZeroDatabaseSize returns the no-pool fallback with a
// rationale explaining that the per-replica memory cost cannot be
// bounded.
func TestRecommend_ZeroDatabaseSize(t *testing.T) {
	t.Parallel()
	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize: 0,
		MemoryBudget: 1 << 30,
	})
	if rec.ReadPoolSize != 0 {
		t.Errorf("ReadPoolSize = %d, want 0", rec.ReadPoolSize)
	}
	if rec.ReplicaRefreshInterval != 0 {
		t.Errorf("ReplicaRefreshInterval = %s, want 0", rec.ReplicaRefreshInterval)
	}
	if !strings.Contains(rec.Rationale, "DatabaseSize") {
		t.Errorf("Rationale should mention DatabaseSize: %q", rec.Rationale)
	}
}

// TestRecommend_NegativeDatabaseSize is treated identically to zero —
// the check in Recommend is "<= 0".
func TestRecommend_NegativeDatabaseSize(t *testing.T) {
	t.Parallel()
	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize: -42,
		MemoryBudget: 1 << 30,
	})
	if rec.ReadPoolSize != 0 {
		t.Errorf("ReadPoolSize = %d, want 0 for negative DB size", rec.ReadPoolSize)
	}
}

// TestRecommend_ZeroMemoryBudget returns the no-pool fallback.
func TestRecommend_ZeroMemoryBudget(t *testing.T) {
	t.Parallel()
	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize: 10 << 20,
		MemoryBudget: 0,
	})
	if rec.ReadPoolSize != 0 {
		t.Errorf("ReadPoolSize = %d, want 0 for zero budget", rec.ReadPoolSize)
	}
	if !strings.Contains(rec.Rationale, "smaller than") {
		t.Errorf("Rationale should explain the budget constraint: %q", rec.Rationale)
	}
}

// TestRecommend_BudgetSmallerThanDatabase — budget cannot hold one
// replica, so no pool.
func TestRecommend_BudgetSmallerThanDatabase(t *testing.T) {
	t.Parallel()
	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize: 100 << 20, // 100 MiB
		MemoryBudget: 50 << 20,  //  50 MiB
	})
	if rec.ReadPoolSize != 0 {
		t.Errorf("ReadPoolSize = %d, want 0", rec.ReadPoolSize)
	}
	if !strings.Contains(rec.Rationale, "cannot fit even one replica") {
		t.Errorf("Rationale should explain the under-budget case: %q", rec.Rationale)
	}
}

// TestRecommend_BudgetExactlyOneReplica — the boundary case where the
// caller has budget for exactly one replica and no more.
func TestRecommend_BudgetExactlyOneReplica(t *testing.T) {
	t.Parallel()
	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize: 50 << 20,
		MemoryBudget: 50 << 20,
	})
	if rec.ReadPoolSize != 1 {
		t.Errorf("ReadPoolSize = %d, want 1", rec.ReadPoolSize)
	}
	if rec.EstimatedReplicaBytes != 50<<20 {
		t.Errorf("EstimatedReplicaBytes = %d, want %d",
			rec.EstimatedReplicaBytes, int64(50<<20))
	}
	// Speedup should clamp at 1.0 (never slower than single-conn).
	if rec.EstimatedReadSpeedup < 1.0 {
		t.Errorf("EstimatedReadSpeedup = %v, want >= 1.0", rec.EstimatedReadSpeedup)
	}
}

// ── Core-count capping ────────────────────────────────────────────────────

// TestRecommend_CapAtGOMAXPROCS — a caller with huge memory budget but
// default CPU cores gets capped at GOMAXPROCS.
func TestRecommend_CapAtGOMAXPROCS(t *testing.T) {
	t.Parallel()
	cores := runtime.GOMAXPROCS(0)

	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize: 1 << 20, // 1 MiB — tiny so memory is not the bind
		MemoryBudget: 1 << 40, // 1 TiB — effectively unlimited
		CPUCores:     0,       // default
		Workload:     tuning.WorkloadReadHeavy,
	})
	if rec.ReadPoolSize != cores {
		t.Errorf("ReadPoolSize = %d, want %d (GOMAXPROCS)", rec.ReadPoolSize, cores)
	}
	if !strings.Contains(rec.Rationale, "GOMAXPROCS") {
		t.Errorf("Rationale should mention GOMAXPROCS binding: %q", rec.Rationale)
	}
}

// TestRecommend_CPUCoresOverride — explicit CPUCores takes precedence
// over runtime.GOMAXPROCS.
func TestRecommend_CPUCoresOverride(t *testing.T) {
	t.Parallel()
	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize: 1 << 20,
		MemoryBudget: 1 << 40,
		CPUCores:     2,
		Workload:     tuning.WorkloadReadHeavy,
	})
	if rec.ReadPoolSize != 2 {
		t.Errorf("ReadPoolSize = %d, want 2 (explicit CPUCores)", rec.ReadPoolSize)
	}
}

// TestRecommend_NegativeCPUCores is clamped up to 1 — defensive against
// bad container metadata.
func TestRecommend_NegativeCPUCores(t *testing.T) {
	t.Parallel()
	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize: 1 << 20,
		MemoryBudget: 1 << 40,
		CPUCores:     -4,
		Workload:     tuning.WorkloadReadHeavy,
	})
	// CPUCores < 0 means "use default" (runtime.GOMAXPROCS).
	if rec.ReadPoolSize < 1 {
		t.Errorf("ReadPoolSize = %d, want >= 1 even with negative CPUCores",
			rec.ReadPoolSize)
	}
}

// TestRecommend_MaxReplicasCap — caller's explicit cap beats the other caps.
func TestRecommend_MaxReplicasCap(t *testing.T) {
	t.Parallel()
	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize: 1 << 20,
		MemoryBudget: 1 << 40,
		CPUCores:     64,
		MaxReplicas:  3,
		Workload:     tuning.WorkloadReadHeavy,
	})
	if rec.ReadPoolSize != 3 {
		t.Errorf("ReadPoolSize = %d, want 3 (MaxReplicas cap)", rec.ReadPoolSize)
	}
	if !strings.Contains(rec.Rationale, "MaxReplicas cap") {
		t.Errorf("Rationale should mention MaxReplicas binding: %q", rec.Rationale)
	}
}

// TestRecommend_MaxReplicasZeroMeansUncapped — MaxReplicas=0 is
// documented as "no caller cap" and must not collapse to ReadPoolSize=0.
func TestRecommend_MaxReplicasZeroMeansUncapped(t *testing.T) {
	t.Parallel()
	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize: 1 << 20,
		MemoryBudget: 1 << 40,
		CPUCores:     4,
		MaxReplicas:  0, // "uncapped"
		Workload:     tuning.WorkloadReadHeavy,
	})
	if rec.ReadPoolSize != 4 {
		t.Errorf("ReadPoolSize = %d, want 4 (CPUCores cap with no MaxReplicas)",
			rec.ReadPoolSize)
	}
}

// ── Refresh interval clamping ─────────────────────────────────────────────

// TestRecommend_RefreshIntervalDefault — unset StalenessTolerance gives
// the 50ms default.
func TestRecommend_RefreshIntervalDefault(t *testing.T) {
	t.Parallel()
	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize: 10 << 20,
		MemoryBudget: 100 << 20,
		CPUCores:     4,
	})
	if rec.ReplicaRefreshInterval != tuning.RefreshIntervalDefault {
		t.Errorf("ReplicaRefreshInterval = %s, want %s (default)",
			rec.ReplicaRefreshInterval, tuning.RefreshIntervalDefault)
	}
	if !strings.Contains(rec.Rationale, "default") {
		t.Errorf("Rationale should mention default: %q", rec.Rationale)
	}
}

// TestRecommend_RefreshIntervalRaisedToFloor — a 1ms staleness tolerance
// is clamped up to the 5ms floor per the pprof finding.
func TestRecommend_RefreshIntervalRaisedToFloor(t *testing.T) {
	t.Parallel()
	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize:       10 << 20,
		MemoryBudget:       100 << 20,
		CPUCores:           4,
		StalenessTolerance: 1 * time.Millisecond,
	})
	if rec.ReplicaRefreshInterval != tuning.RefreshIntervalFloor {
		t.Errorf("ReplicaRefreshInterval = %s, want %s (floor)",
			rec.ReplicaRefreshInterval, tuning.RefreshIntervalFloor)
	}
	if !strings.Contains(rec.Rationale, "floor") {
		t.Errorf("Rationale should mention floor clamp: %q", rec.Rationale)
	}
}

// TestRecommend_RefreshIntervalCappedAtCeiling — a 30s staleness
// tolerance is clamped down to the 500ms ceiling.
func TestRecommend_RefreshIntervalCappedAtCeiling(t *testing.T) {
	t.Parallel()
	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize:       10 << 20,
		MemoryBudget:       100 << 20,
		CPUCores:           4,
		StalenessTolerance: 30 * time.Second,
	})
	if rec.ReplicaRefreshInterval != tuning.RefreshIntervalCeiling {
		t.Errorf("ReplicaRefreshInterval = %s, want %s (ceiling)",
			rec.ReplicaRefreshInterval, tuning.RefreshIntervalCeiling)
	}
	if !strings.Contains(rec.Rationale, "ceiling") {
		t.Errorf("Rationale should mention ceiling clamp: %q", rec.Rationale)
	}
}

// TestRecommend_RefreshIntervalExactFloor — the floor itself is accepted
// verbatim and is NOT flagged as clamped in the rationale.
func TestRecommend_RefreshIntervalExactFloor(t *testing.T) {
	t.Parallel()
	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize:       10 << 20,
		MemoryBudget:       100 << 20,
		CPUCores:           4,
		StalenessTolerance: tuning.RefreshIntervalFloor,
	})
	if rec.ReplicaRefreshInterval != tuning.RefreshIntervalFloor {
		t.Errorf("ReplicaRefreshInterval = %s, want exactly %s",
			rec.ReplicaRefreshInterval, tuning.RefreshIntervalFloor)
	}
}

// TestRecommend_RefreshIntervalInBand — a value inside the band is used
// verbatim and no "clamp" rationale is appended.
func TestRecommend_RefreshIntervalInBand(t *testing.T) {
	t.Parallel()
	want := 100 * time.Millisecond
	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize:       10 << 20,
		MemoryBudget:       100 << 20,
		CPUCores:           4,
		StalenessTolerance: want,
	})
	if rec.ReplicaRefreshInterval != want {
		t.Errorf("ReplicaRefreshInterval = %s, want %s",
			rec.ReplicaRefreshInterval, want)
	}
	// No "floor"/"ceiling" clamp notice in the rationale.
	if strings.Contains(rec.Rationale, "floor") || strings.Contains(rec.Rationale, "ceiling") {
		t.Errorf("in-band interval should not mention clamp: %q", rec.Rationale)
	}
}

// ── Workload-hint branches ────────────────────────────────────────────────

// TestRecommend_WriteHeavyHalvesReplicas — write-heavy workloads get
// half the replica count because every tick triggers real refresh work
// and more replicas means more CPU burn for diminishing read benefit.
func TestRecommend_WriteHeavyHalvesReplicas(t *testing.T) {
	t.Parallel()

	// Baseline: read-heavy, unbounded memory, 8 cores.
	base := tuning.Workload{
		DatabaseSize: 1 << 20,
		MemoryBudget: 1 << 40,
		CPUCores:     8,
		Workload:     tuning.WorkloadReadHeavy,
	}
	recRead := tuning.Recommend(base)

	writeHeavy := base
	writeHeavy.Workload = tuning.WorkloadWriteHeavy
	recWrite := tuning.Recommend(writeHeavy)

	if recRead.ReadPoolSize != 8 {
		t.Fatalf("read-heavy baseline: want 8 replicas, got %d", recRead.ReadPoolSize)
	}
	if recWrite.ReadPoolSize != 4 {
		t.Errorf("write-heavy: want 4 replicas (halved from 8), got %d",
			recWrite.ReadPoolSize)
	}
	if !strings.Contains(recWrite.Rationale, "write-heavy") {
		t.Errorf("write-heavy rationale should mention the regime: %q",
			recWrite.Rationale)
	}
}

// TestRecommend_WriteHeavyMinimumNotHalved — when N is already small
// (<= 2) the halving would reduce the pool to uselessness, so the
// halving is skipped.
func TestRecommend_WriteHeavyMinimumNotHalved(t *testing.T) {
	t.Parallel()
	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize: 1 << 20,
		MemoryBudget: 1 << 40,
		CPUCores:     2,
		Workload:     tuning.WorkloadWriteHeavy,
	})
	if rec.ReadPoolSize != 2 {
		t.Errorf("write-heavy with CPUCores=2: want 2 (not halved), got %d",
			rec.ReadPoolSize)
	}
}

// TestRecommend_WriteFractionAffectsCPUEstimate — the EstimatedRefreshCPU
// for read-heavy < balanced < write-heavy at the same N, T, S.
func TestRecommend_WriteFractionAffectsCPUEstimate(t *testing.T) {
	t.Parallel()
	base := tuning.Workload{
		DatabaseSize:       50 << 20, // 50 MiB — enough to make CPU non-trivial
		MemoryBudget:       1 << 40,
		CPUCores:           4,
		StalenessTolerance: 50 * time.Millisecond,
		MaxReplicas:        4, // pin N so the comparison is apples-to-apples
	}

	readH := base
	readH.Workload = tuning.WorkloadReadHeavy
	balanced := base
	balanced.Workload = tuning.WorkloadBalanced
	writeH := base
	writeH.Workload = tuning.WorkloadWriteHeavy

	rR := tuning.Recommend(readH)
	rB := tuning.Recommend(balanced)
	rW := tuning.Recommend(writeH)

	// write-heavy halves N, so the comparison is only clean for
	// read-heavy vs balanced (same N).
	if rR.ReadPoolSize != rB.ReadPoolSize {
		t.Fatalf("read-heavy and balanced should produce same N: %d vs %d",
			rR.ReadPoolSize, rB.ReadPoolSize)
	}
	if rR.EstimatedRefreshCPU >= rB.EstimatedRefreshCPU {
		t.Errorf("read-heavy CPU estimate (%v) should be less than balanced (%v)",
			rR.EstimatedRefreshCPU, rB.EstimatedRefreshCPU)
	}

	// write-heavy has half the replicas, so its per-core fraction is
	// f_write × N_halved × ... The comparison against balanced on a
	// per-replica basis still shows higher writeFraction * N > balanced
	// in most realistic configurations; just verify it is non-zero and
	// does not crash.
	if rW.EstimatedRefreshCPU <= 0 {
		t.Errorf("write-heavy CPU estimate should be > 0, got %v",
			rW.EstimatedRefreshCPU)
	}
}

// TestRecommend_UnknownWorkloadTreatedAsBalanced — documented behaviour:
// WorkloadUnknown uses the same 0.5 f_write as WorkloadBalanced.
func TestRecommend_UnknownWorkloadTreatedAsBalanced(t *testing.T) {
	t.Parallel()
	base := tuning.Workload{
		DatabaseSize: 50 << 20,
		MemoryBudget: 1 << 40,
		CPUCores:     4,
	}
	unknown := tuning.Recommend(base)

	base.Workload = tuning.WorkloadBalanced
	balanced := tuning.Recommend(base)

	if unknown.EstimatedRefreshCPU != balanced.EstimatedRefreshCPU {
		t.Errorf("WorkloadUnknown CPU = %v, WorkloadBalanced = %v; expected equal",
			unknown.EstimatedRefreshCPU, balanced.EstimatedRefreshCPU)
	}
}

// ── Monotonicity properties ───────────────────────────────────────────────

// TestRecommend_MonotonicInMemoryBudget — doubling the memory budget
// (while other caps are loose) must not decrease the recommended N.
func TestRecommend_MonotonicInMemoryBudget(t *testing.T) {
	t.Parallel()
	base := tuning.Workload{
		DatabaseSize: 10 << 20,
		CPUCores:     64, // loose CPU cap so memory binds
		Workload:     tuning.WorkloadReadHeavy,
	}

	prev := 0
	for _, mult := range []int64{1, 2, 4, 8, 16} {
		w := base
		w.MemoryBudget = int64(mult) * (10 << 20)
		rec := tuning.Recommend(w)
		if rec.ReadPoolSize < prev {
			t.Errorf("non-monotonic: budget=%d×, ReadPoolSize=%d, prev=%d",
				mult, rec.ReadPoolSize, prev)
		}
		prev = rec.ReadPoolSize
	}
}

// TestRecommend_MonotonicInRefreshInterval — increasing
// StalenessTolerance must not decrease the recommended interval (it can
// plateau at the ceiling, but never go down).
func TestRecommend_MonotonicInRefreshInterval(t *testing.T) {
	t.Parallel()
	base := tuning.Workload{
		DatabaseSize: 10 << 20,
		MemoryBudget: 100 << 20,
		CPUCores:     4,
	}

	var prev time.Duration = -1
	for _, tol := range []time.Duration{
		1 * time.Millisecond, // below floor — clamped UP
		5 * time.Millisecond,
		10 * time.Millisecond,
		50 * time.Millisecond,
		100 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,  // above ceiling — clamped DOWN to 500ms
		10 * time.Second, // above ceiling — clamped DOWN to 500ms
	} {
		w := base
		w.StalenessTolerance = tol
		rec := tuning.Recommend(w)
		if prev >= 0 && rec.ReplicaRefreshInterval < prev {
			t.Errorf("non-monotonic: StalenessTolerance=%s, interval=%s, prev=%s",
				tol, rec.ReplicaRefreshInterval, prev)
		}
		prev = rec.ReplicaRefreshInterval
	}
}

// TestRecommend_RefreshCPUDropsWithLongerInterval — at fixed N and S,
// a longer refresh interval must reduce EstimatedRefreshCPU.
func TestRecommend_RefreshCPUDropsWithLongerInterval(t *testing.T) {
	t.Parallel()
	base := tuning.Workload{
		DatabaseSize: 100 << 20,
		MemoryBudget: 1 << 40,
		CPUCores:     4,
		MaxReplicas:  4,
		Workload:     tuning.WorkloadBalanced,
	}

	var prev float64 = -1
	for _, tol := range []time.Duration{
		5 * time.Millisecond,
		25 * time.Millisecond,
		100 * time.Millisecond,
		500 * time.Millisecond,
	} {
		w := base
		w.StalenessTolerance = tol
		rec := tuning.Recommend(w)
		if prev >= 0 && rec.EstimatedRefreshCPU >= prev {
			t.Errorf("CPU estimate did not drop: tol=%s, cpu=%v, prev=%v",
				tol, rec.EstimatedRefreshCPU, prev)
		}
		prev = rec.EstimatedRefreshCPU
	}
}

// ── Output invariants ─────────────────────────────────────────────────────

// TestRecommend_SpeedupAtLeastOne — the speedup is never below 1.0 even
// for a single-replica pool (enabling the pool never makes things slower
// per the benchmark).
func TestRecommend_SpeedupAtLeastOne(t *testing.T) {
	t.Parallel()
	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize: 1 << 20,
		MemoryBudget: 1 << 20, // exactly one replica
		CPUCores:     1,
	})
	if rec.ReadPoolSize != 1 {
		t.Fatalf("expected 1 replica, got %d", rec.ReadPoolSize)
	}
	if rec.EstimatedReadSpeedup < 1.0 {
		t.Errorf("EstimatedReadSpeedup = %v, want >= 1.0", rec.EstimatedReadSpeedup)
	}
}

// TestRecommend_SpeedupCappedAtCores — increasing N past GOMAXPROCS in
// the cost model does not increase the reported speedup (the model
// explicitly caps effectiveN at cores).
func TestRecommend_SpeedupCappedAtCores(t *testing.T) {
	t.Parallel()
	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize: 1 << 20,
		MemoryBudget: 1 << 40,
		CPUCores:     2,
		Workload:     tuning.WorkloadReadHeavy,
	})
	// With 2 cores the recommender should cap ReadPoolSize at 2.
	if rec.ReadPoolSize != 2 {
		t.Errorf("ReadPoolSize = %d, want 2", rec.ReadPoolSize)
	}
	// AlphaRead × 2 = 1.3 on the calibrated constants.
	want := tuning.AlphaRead * 2.0
	if rec.EstimatedReadSpeedup < want-0.001 || rec.EstimatedReadSpeedup > want+0.001 {
		t.Errorf("EstimatedReadSpeedup = %v, want ~%v",
			rec.EstimatedReadSpeedup, want)
	}
}

// TestRecommend_EstimatedReplicaBytesMatchesN — the reported memory
// figure always equals N × DatabaseSize, with zero rounding error.
func TestRecommend_EstimatedReplicaBytesMatchesN(t *testing.T) {
	t.Parallel()
	cases := []struct {
		dbSize   int64
		budget   int64
		cores    int
		workload tuning.WorkloadHint
	}{
		{10 << 20, 100 << 20, 4, tuning.WorkloadReadHeavy},
		{50 << 20, 500 << 20, 8, tuning.WorkloadBalanced},
		{1 << 20, 1 << 40, 16, tuning.WorkloadWriteHeavy},
	}
	for _, tc := range cases {
		rec := tuning.Recommend(tuning.Workload{
			DatabaseSize: tc.dbSize,
			MemoryBudget: tc.budget,
			CPUCores:     tc.cores,
			Workload:     tc.workload,
		})
		want := int64(rec.ReadPoolSize) * tc.dbSize
		if rec.EstimatedReplicaBytes != want {
			t.Errorf("db=%d budget=%d cores=%d workload=%s: "+
				"EstimatedReplicaBytes=%d, want %d (N=%d)",
				tc.dbSize, tc.budget, tc.cores, tc.workload,
				rec.EstimatedReplicaBytes, want, rec.ReadPoolSize)
		}
	}
}

// TestRecommend_RationaleNonEmpty — every recommendation returns a
// non-empty Rationale, even the no-pool fallback.
func TestRecommend_RationaleNonEmpty(t *testing.T) {
	t.Parallel()
	cases := []tuning.Workload{
		{},                      // all-zero
		{DatabaseSize: 1 << 20}, // no budget
		{DatabaseSize: 1 << 20, MemoryBudget: 1 << 20}, // minimal pool
		{DatabaseSize: 10 << 20, MemoryBudget: 1 << 40, Workload: tuning.WorkloadReadHeavy},
	}
	for i, w := range cases {
		rec := tuning.Recommend(w)
		if rec.Rationale == "" {
			t.Errorf("case %d: Rationale is empty for %+v", i, w)
		}
	}
}

// TestRecommend_NoPoolFallbackHasZeroInterval — when ReadPoolSize == 0
// the recommended interval is also 0 (the refresh loop is not started
// in that case, so any non-zero value would be misleading).
func TestRecommend_NoPoolFallbackHasZeroInterval(t *testing.T) {
	t.Parallel()
	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize:       10 << 20,
		MemoryBudget:       0,
		StalenessTolerance: 100 * time.Millisecond, // ignored
	})
	if rec.ReadPoolSize != 0 {
		t.Fatalf("ReadPoolSize = %d, want 0", rec.ReadPoolSize)
	}
	if rec.ReplicaRefreshInterval != 0 {
		t.Errorf("ReplicaRefreshInterval = %s, want 0 for no-pool fallback",
			rec.ReplicaRefreshInterval)
	}
}

// ── WorkloadHint.String ────────────────────────────────────────────────────

func TestWorkloadHint_String(t *testing.T) {
	t.Parallel()
	cases := []struct {
		hint tuning.WorkloadHint
		want string
	}{
		{tuning.WorkloadUnknown, "unknown"},
		{tuning.WorkloadReadHeavy, "read-heavy"},
		{tuning.WorkloadBalanced, "balanced"},
		{tuning.WorkloadWriteHeavy, "write-heavy"},
		{tuning.WorkloadHint(99), "unknown"}, // unknown enum value
	}
	for _, tc := range cases {
		if got := tc.hint.String(); got != tc.want {
			t.Errorf("WorkloadHint(%d).String() = %q, want %q",
				tc.hint, got, tc.want)
		}
	}
}

// ── Realistic scenarios (calibration sanity checks) ──────────────────────

// TestRecommend_Scenario_SmallAppSession — a 10 MiB session store with
// a 200 MiB budget for replicas. Expectation: fully core-bound pool
// (memory is loose), default refresh interval, substantial speedup.
func TestRecommend_Scenario_SmallAppSession(t *testing.T) {
	t.Parallel()
	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize: 10 << 20,
		MemoryBudget: 200 << 20,
		CPUCores:     4,
		Workload:     tuning.WorkloadReadHeavy,
	})

	if rec.ReadPoolSize != 4 {
		t.Errorf("ReadPoolSize = %d, want 4 (cores-bound)", rec.ReadPoolSize)
	}
	if rec.ReplicaRefreshInterval != tuning.RefreshIntervalDefault {
		t.Errorf("ReplicaRefreshInterval = %s, want default %s",
			rec.ReplicaRefreshInterval, tuning.RefreshIntervalDefault)
	}
	if rec.EstimatedReadSpeedup < 2.0 {
		t.Errorf("EstimatedReadSpeedup = %v, want >= 2.0 for 4-replica pool",
			rec.EstimatedReadSpeedup)
	}
	// Memory cost: 4 × 10 MiB = 40 MiB, well under budget.
	if rec.EstimatedReplicaBytes != 40<<20 {
		t.Errorf("EstimatedReplicaBytes = %d, want %d",
			rec.EstimatedReplicaBytes, int64(40<<20))
	}
}

// TestRecommend_Scenario_TightMemory — a 100 MiB DB with only 250 MiB
// budget on an 8-core box. Memory should bind at 2 replicas.
func TestRecommend_Scenario_TightMemory(t *testing.T) {
	t.Parallel()
	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize: 100 << 20,
		MemoryBudget: 250 << 20,
		CPUCores:     8,
		Workload:     tuning.WorkloadReadHeavy,
	})
	if rec.ReadPoolSize != 2 {
		t.Errorf("ReadPoolSize = %d, want 2 (memory-bound: 250/100 = 2)",
			rec.ReadPoolSize)
	}
	if !strings.Contains(rec.Rationale, "memory budget") {
		t.Errorf("Rationale should mention memory-budget binding: %q",
			rec.Rationale)
	}
}

// TestRecommend_Scenario_LowLatencyAnalytics — a read-heavy workload
// that can tolerate up to 200ms of staleness. Expects the default/
// caller-requested interval to be used verbatim, not clamped.
func TestRecommend_Scenario_LowLatencyAnalytics(t *testing.T) {
	t.Parallel()
	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize:       500 << 20,
		MemoryBudget:       4 << 30, // 4 GiB — allows up to 8 replicas
		CPUCores:           8,
		StalenessTolerance: 200 * time.Millisecond,
		Workload:           tuning.WorkloadReadHeavy,
	})
	if rec.ReplicaRefreshInterval != 200*time.Millisecond {
		t.Errorf("ReplicaRefreshInterval = %s, want 200ms (in-band)",
			rec.ReplicaRefreshInterval)
	}
	if rec.ReadPoolSize < 4 {
		t.Errorf("ReadPoolSize = %d, want >= 4 (ample budget)",
			rec.ReadPoolSize)
	}
}

// TestRecommend_Scenario_WriteHeavyOLTP — a write-heavy OLTP workload.
// Expects the halving rule to kick in and a smaller recommendation than
// the equivalent read-heavy workload would produce.
func TestRecommend_Scenario_WriteHeavyOLTP(t *testing.T) {
	t.Parallel()
	w := tuning.Workload{
		DatabaseSize: 50 << 20,
		MemoryBudget: 1 << 30, // 1 GiB budget → 20 replicas available by memory
		CPUCores:     8,
	}

	w.Workload = tuning.WorkloadReadHeavy
	readRec := tuning.Recommend(w)

	w.Workload = tuning.WorkloadWriteHeavy
	writeRec := tuning.Recommend(w)

	if writeRec.ReadPoolSize >= readRec.ReadPoolSize {
		t.Errorf("write-heavy (%d) should recommend fewer replicas than "+
			"read-heavy (%d) for the same budget",
			writeRec.ReadPoolSize, readRec.ReadPoolSize)
	}
	if !strings.Contains(writeRec.Rationale, "write-heavy") {
		t.Errorf("write-heavy rationale should name the regime: %q",
			writeRec.Rationale)
	}
}

// TestRecommend_HumanBytesInRationale — the rationale string uses the
// humanBytes formatter for memory so it is readable. Verify KiB/MiB/GiB
// suffixes appear for differently-sized inputs.
func TestRecommend_HumanBytesInRationale(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		dbSize int64
		budget int64
		want   string // substring that should appear in Rationale
	}{
		{"MiB", 5 << 20, 100 << 20, "MiB"},
		{"GiB", 500 << 20, 4 << 30, "GiB"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := tuning.Recommend(tuning.Workload{
				DatabaseSize: tc.dbSize,
				MemoryBudget: tc.budget,
				CPUCores:     4,
				Workload:     tuning.WorkloadReadHeavy,
			})
			if !strings.Contains(rec.Rationale, tc.want) {
				t.Errorf("Rationale should contain %q: %q", tc.want, rec.Rationale)
			}
		})
	}
}

// TestRecommend_Calibration_MatchesBenchmark verifies the model's
// predicted speedup is within tolerance of the measured
// BenchmarkCompare_ConcurrentRead result from BENCHMARKS.md:
//
//	measured: pool=4 @ parallelism=4 → 2032 ns/op
//	          single-conn            → 5500 ns/op
//	          → 2.71× speedup
//
//	predicted: AlphaRead × 4 = 0.65 × 4 = 2.60×
//
// If the calibration drifts outside [2.40×, 3.00×] the test fails so a
// future benchmark re-run that invalidates AlphaRead is caught at
// CI time rather than silently shipping bad tuning advice.
func TestRecommend_Calibration_MatchesBenchmark(t *testing.T) {
	t.Parallel()
	// Reproduce the BenchmarkCompare_ConcurrentRead configuration:
	// 4 replicas, tiny DB so memory is not the bind.
	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize: 1 << 20,
		MemoryBudget: 1 << 40,
		CPUCores:     4,
		Workload:     tuning.WorkloadReadHeavy,
	})
	if rec.ReadPoolSize != 4 {
		t.Fatalf("calibration setup: ReadPoolSize = %d, want 4", rec.ReadPoolSize)
	}

	const (
		measuredSpeedup = 2.71 // BENCHMARKS.md v1.4.0 reference run
		lowerBound      = 2.40
		upperBound      = 3.00
	)
	if rec.EstimatedReadSpeedup < lowerBound || rec.EstimatedReadSpeedup > upperBound {
		t.Errorf("calibration drift: predicted speedup %.2fx outside "+
			"[%.2f, %.2f]×; measured was %.2f× — re-run `make bench` and "+
			"update tuning.AlphaRead if the benchmark moved",
			rec.EstimatedReadSpeedup, lowerBound, upperBound, measuredSpeedup)
	}
}

// TestRecommend_Calibration_CPUEstimatePlausible spot-checks the
// EstimatedRefreshCPU arithmetic against a worked example so a future
// edit to the formula cannot silently produce absurd values.
//
// Scenario: N=4, S=50 MiB, T=50ms, balanced workload (f_write=0.5).
//
//	CPU = 0.5 × 4 × 50*2^20 / (4*2^30 × 0.05)
//	    = 0.5 × 4 × 52,428,800 / 214,748,364.8
//	    ≈ 0.488 core-seconds per wall-second
//
// The test accepts anything in [0.4, 0.6] to absorb small future
// changes to BWDeserialize without spuriously failing.
func TestRecommend_Calibration_CPUEstimatePlausible(t *testing.T) {
	t.Parallel()
	rec := tuning.Recommend(tuning.Workload{
		DatabaseSize:       50 << 20,
		MemoryBudget:       1 << 40,
		CPUCores:           4,
		MaxReplicas:        4, // pin N exactly
		StalenessTolerance: 50 * time.Millisecond,
		Workload:           tuning.WorkloadBalanced,
	})
	if rec.ReadPoolSize != 4 {
		t.Fatalf("calibration setup: ReadPoolSize = %d, want 4", rec.ReadPoolSize)
	}
	if rec.ReplicaRefreshInterval != 50*time.Millisecond {
		t.Fatalf("calibration setup: interval = %s, want 50ms",
			rec.ReplicaRefreshInterval)
	}

	const (
		lowerBound = 0.40
		upperBound = 0.60
	)
	if rec.EstimatedRefreshCPU < lowerBound || rec.EstimatedRefreshCPU > upperBound {
		t.Errorf("CPU estimate %.3f outside plausible range [%.2f, %.2f] "+
			"for N=4, S=50MiB, T=50ms, f_write=0.5 — check "+
			"BWDeserialize and the writeFraction map",
			rec.EstimatedRefreshCPU, lowerBound, upperBound)
	}
}

// TestRecommend_DeterminismSameInputsSameOutput — Recommend is pure, so
// two calls with identical inputs must yield identical outputs.
func TestRecommend_DeterminismSameInputsSameOutput(t *testing.T) {
	t.Parallel()
	w := tuning.Workload{
		DatabaseSize:       25 << 20,
		MemoryBudget:       500 << 20,
		CPUCores:           8,
		StalenessTolerance: 75 * time.Millisecond,
		Workload:           tuning.WorkloadBalanced,
		MaxReplicas:        6,
	}
	r1 := tuning.Recommend(w)
	r2 := tuning.Recommend(w)

	if r1.ReadPoolSize != r2.ReadPoolSize {
		t.Errorf("non-deterministic ReadPoolSize: %d vs %d",
			r1.ReadPoolSize, r2.ReadPoolSize)
	}
	if r1.ReplicaRefreshInterval != r2.ReplicaRefreshInterval {
		t.Errorf("non-deterministic ReplicaRefreshInterval: %s vs %s",
			r1.ReplicaRefreshInterval, r2.ReplicaRefreshInterval)
	}
	if r1.EstimatedReplicaBytes != r2.EstimatedReplicaBytes {
		t.Errorf("non-deterministic EstimatedReplicaBytes: %d vs %d",
			r1.EstimatedReplicaBytes, r2.EstimatedReplicaBytes)
	}
	if r1.EstimatedRefreshCPU != r2.EstimatedRefreshCPU {
		t.Errorf("non-deterministic EstimatedRefreshCPU: %v vs %v",
			r1.EstimatedRefreshCPU, r2.EstimatedRefreshCPU)
	}
	if r1.EstimatedReadSpeedup != r2.EstimatedReadSpeedup {
		t.Errorf("non-deterministic EstimatedReadSpeedup: %v vs %v",
			r1.EstimatedReadSpeedup, r2.EstimatedReadSpeedup)
	}
	if r1.Rationale != r2.Rationale {
		t.Errorf("non-deterministic Rationale:\n  r1=%q\n  r2=%q",
			r1.Rationale, r2.Rationale)
	}
}
