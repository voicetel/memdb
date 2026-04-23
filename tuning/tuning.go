// Package tuning provides a closed-form mathematical model for recommending
// memdb replica-pool settings (ReadPoolSize, ReplicaRefreshInterval) from
// the caller's runtime environment and workload characteristics.
//
// # Why this exists
//
// The two knobs most directly controlling memdb's concurrent-read scaling —
// Config.ReadPoolSize and Config.ReplicaRefreshInterval — trade off read
// throughput against memory cost, refresh CPU, and read staleness. The
// v1.4 benchmark sweep quantified these relationships; this package turns
// those measurements into a reusable recommendation function so callers
// do not have to rediscover the knee of the curve on their own hardware.
//
// The model is deliberately closed-form (no iterative solver, no training
// data, no runtime sampling) so it can be called from Open() or from a
// startup log line without adding latency. Every constant comes directly
// from the v1.4 pprof run documented in BENCHMARKS.md and is revisited
// whenever that benchmark is re-run.
//
// # Model in brief
//
// Let:
//
//	N           = ReadPoolSize (replicas)
//	S           = database size in bytes
//	T           = refresh interval in seconds
//	P           = runtime.GOMAXPROCS
//	M_max       = caller's replica memory budget (bytes)
//	tau_max     = caller's staleness tolerance (seconds)
//	bw_deserial = measured sqlite3_deserialize bandwidth (bytes / second)
//	alpha_read  = scaling efficiency coefficient (0..1)
//
// Costs:
//
//	Memory(N)           = N * S
//	CPU_refresh(N,T,S)  = f_write * N * S / (bw_deserial * T)
//	                      where f_write is the probability a tick has work.
//	                      Under the v1.4 write-generation short-circuit this
//	                      is near 0 for read-only workloads and near 1
//	                      under continuous writes.
//
// Throughput model (empirical):
//
//	throughput_reads(N, P) = throughput_single * min(N, P) * alpha_read
//
// Alpha models the diminishing returns observed in pprof as cgocall
// contention rises with more concurrent readers. v1.4 BENCHMARKS.md
// measured alpha_read ~= 0.68 across the 1..20 thread sweep on
// reference hardware (file/sync=off comparator baseline).
//
// Recommendation (constrained optimisation, maximise min(N, P)):
//
//	N_mem   = floor(M_max / S)                           # memory cap
//	N_cores = P                                           # diminishing returns past cores
//	N_rec   = max(1, min(N_mem, N_cores))
//
//	T_rec   = clamp(tau_max, T_min, T_max)
//	          where T_min = 5ms   (pprof floor: below this refresh dominates)
//	                T_max = 500ms (staleness ceiling before "replica" semantics
//	                               become a confusing user experience)
//
// Both recommendations are independently sound — the memory cap and the
// staleness cap cannot conflict because they constrain orthogonal
// quantities (count vs. interval).
//
// # How to use
//
// Recommend(Workload{...}) returns a Recommendation struct whose fields
// are directly assignable to memdb.Config. Example:
//
//	rec := tuning.Recommend(tuning.Workload{
//	    DatabaseSize:        50 << 20,            // 50 MiB
//	    MemoryBudget:        512 << 20,           // 512 MiB budget for replicas
//	    StalenessTolerance:  100 * time.Millisecond,
//	    Workload:            tuning.WorkloadReadHeavy,
//	})
//	cfg := memdb.Config{
//	    FilePath:               "/var/lib/app/data.db",
//	    ReadPoolSize:           rec.ReadPoolSize,
//	    ReplicaRefreshInterval: rec.ReplicaRefreshInterval,
//	    // ...
//	}
//
// Recommend never fails; pathological inputs (zero budget, zero size, etc.)
// collapse to the safe "single-connection" fallback of ReadPoolSize=0 with
// a rationale string explaining why.
//
// # Constants
//
// The constants in this file are calibrated against the v1.4.0 reference
// run (i7-1280P, Linux, Go 1.24, mattn/go-sqlite3). Re-run the benchmark
// suite (`make bench`) on new hardware and update these values if the
// ratios shift materially. Every constant has a doc comment citing its
// source so a future audit can trace the number back to a specific
// measurement.
package tuning

import (
	"fmt"
	"runtime"
	"strings"
	"time"
)

// ── Calibration constants (from BENCHMARKS.md, v1.4.0 reference run) ────────

const (
	// BWDeserialize is the empirically-measured throughput of
	// sqlite3_deserialize on reference hardware, in bytes per second.
	//
	// Derivation: BenchmarkFlush/rows=10000 copies a ~4 MiB image in
	// ~527 µs wall time (see coverage/bench.txt, v1.4.0). The flush path
	// uses the backup API which is strictly slower than sqlite3_deserialize,
	// so we take the flush bandwidth (~7.6 GiB/s) as a conservative floor
	// and document it here. A separate microbench would pin this tighter,
	// but for recommendation purposes a conservative estimate biases us
	// toward recommending FEWER replicas / LONGER intervals — the
	// safe direction.
	//
	// If you update this, note that memory bandwidth on target hardware
	// is the ultimate ceiling (commodity DDR4/5 at 25-50 GB/s peak,
	// ~8 GB/s achievable for Go-triggered memcpy).
	BWDeserialize = 4 * 1024 * 1024 * 1024 // 4 GiB/s, conservative

	// AlphaRead is the scaling efficiency of the replica pool — the
	// factor by which N replicas yield less than N× speedup because
	// cgocall contention and database/sql driver locks grow with
	// concurrency.
	//
	// Derivation: BenchmarkCompare_ConcurrentRead on 4 goroutines shows
	// pool=4 at 2032 ns/op vs single-conn at 5500 ns/op — a 2.71×
	// speedup on 4 replicas, giving alpha = 2.71/4 = 0.6775. Rounded
	// conservatively to 0.65 to account for the diminishing-returns
	// curve past GOMAXPROCS/2 that pprof shows but the microbench
	// does not fully exercise at parallelism=4.
	AlphaRead = 0.65

	// RefreshIntervalFloor is the shortest interval we will recommend.
	// pprof on the reference run showed the refresh goroutine
	// dominating CPU (>40% of samples) at intervals below this threshold,
	// regardless of dataset size, because of the fixed per-tick overhead
	// of the serialise + fan-out deserialise path.
	//
	// Config.applyDefaults separately emits a slog.Warn when the caller
	// sets a value below this — this package simply never recommends
	// anything lower.
	RefreshIntervalFloor = 5 * time.Millisecond

	// RefreshIntervalCeiling is the longest interval we will recommend.
	// Past this, the "read replica" semantics become misleading to
	// users (a row written 1 second ago may still be invisible to
	// reads) and most applications' implicit expectations break.
	//
	// Callers who genuinely want very stale replicas can set the field
	// directly — this package just will not steer them there.
	RefreshIntervalCeiling = 500 * time.Millisecond

	// RefreshIntervalDefault is the recommendation when the caller does
	// not express a staleness tolerance. Matches the package-level
	// default in config.go (50ms) so Recommend + Open produce the
	// same runtime behaviour when called without explicit tuning.
	RefreshIntervalDefault = 50 * time.Millisecond

	// DiminishingReturnsCap caps the recommended replica count at
	// GOMAXPROCS. pprof showed alpha_read dropping sharply past this
	// point because the single writer's connection becomes the next
	// contention bottleneck and additional replicas cannot help.
	DiminishingReturnsCap = -1 // sentinel: resolved at runtime via runtime.GOMAXPROCS(0)
)

// ── Inputs ──────────────────────────────────────────────────────────────────

// WorkloadHint categorises the expected read/write mix. It influences the
// f_write fraction in the CPU-cost constraint and whether the recommender
// leans toward more replicas (read-heavy) or fewer (write-heavy, where
// the replica pool is mostly overhead because every tick triggers work).
type WorkloadHint int

const (
	// WorkloadUnknown — no preference. The recommender assumes a balanced
	// 50/50 mix.
	WorkloadUnknown WorkloadHint = iota

	// WorkloadReadHeavy — reads dominate, writes are infrequent. The
	// write-generation short-circuit will skip most refresh ticks, so
	// refresh CPU is near zero and the recommender can be aggressive
	// about adding replicas.
	WorkloadReadHeavy

	// WorkloadBalanced — roughly 50/50 mix.
	WorkloadBalanced

	// WorkloadWriteHeavy — writes dominate. Every refresh tick will
	// almost certainly have work to do, so the recommender is more
	// conservative about replica count to keep refresh CPU bounded.
	WorkloadWriteHeavy
)

// String returns a short human-readable name for use in Rationale.
func (w WorkloadHint) String() string {
	switch w {
	case WorkloadReadHeavy:
		return "read-heavy"
	case WorkloadBalanced:
		return "balanced"
	case WorkloadWriteHeavy:
		return "write-heavy"
	default:
		return "unknown"
	}
}

// writeFraction returns the fraction of refresh ticks we assume will do
// real work, for CPU-cost estimation. These values are heuristic — the
// write-gen short-circuit means the actual fraction tracks the
// application's write rate, but we do not know that in advance.
func (w WorkloadHint) writeFraction() float64 {
	switch w {
	case WorkloadReadHeavy:
		return 0.05
	case WorkloadWriteHeavy:
		return 0.95
	case WorkloadBalanced, WorkloadUnknown:
		return 0.50
	default:
		return 0.50
	}
}

// Workload bundles the information the recommender needs. Zero values for
// every field produce a conservative "single-connection" recommendation
// rather than an error — the package is designed to be safe to call even
// from code that does not yet have all the inputs.
type Workload struct {
	// DatabaseSize is the expected steady-state size of the in-memory DB
	// in bytes. If unknown, leave zero — the recommender will decline
	// to recommend a pool at all because the memory cost cannot be
	// bounded.
	DatabaseSize int64

	// MemoryBudget is the number of bytes the caller is willing to
	// spend on replica storage. A value of zero means "no budget for
	// replicas" and collapses the recommendation to ReadPoolSize=0.
	// Typical values: 2-10× DatabaseSize for a modest pool, or
	// runtime.GOMAXPROCS(0) × DatabaseSize for a full per-core pool.
	MemoryBudget int64

	// StalenessTolerance is the maximum acceptable lag between a
	// committed write and that write becoming visible to reads.
	// Zero → RefreshIntervalDefault (50ms). Values below
	// RefreshIntervalFloor are clamped UP to the floor — the
	// recommender will never produce a setting that pprof has shown
	// to dominate CPU.
	StalenessTolerance time.Duration

	// CPUCores, when > 0, overrides runtime.GOMAXPROCS(0). Useful when
	// the caller knows the effective core count is not the GOMAXPROCS
	// value (containers with cpu.max set below GOMAXPROCS, for example).
	// Most callers should leave this zero.
	CPUCores int

	// Workload is the read/write mix hint. Influences how aggressive
	// the recommender is about replica count.
	Workload WorkloadHint

	// MaxReplicas caps the recommendation regardless of what the cost
	// model would otherwise allow. Zero means "no caller cap" (the
	// model's own DiminishingReturnsCap still applies).
	MaxReplicas int
}

// ── Outputs ─────────────────────────────────────────────────────────────────

// Recommendation bundles the recommended settings plus a human-readable
// Rationale explaining which constraint was binding. The Rationale is
// intended for a one-line startup log entry so an operator can see at a
// glance why a given pool size was chosen.
type Recommendation struct {
	// ReadPoolSize is the recommended memdb.Config.ReadPoolSize.
	// 0 means "do not use a replica pool" and is returned when the
	// memory budget is too small to hold even one replica, or when
	// DatabaseSize is zero.
	ReadPoolSize int

	// ReplicaRefreshInterval is the recommended
	// memdb.Config.ReplicaRefreshInterval. Always within
	// [RefreshIntervalFloor, RefreshIntervalCeiling] when
	// ReadPoolSize > 0. Zero when ReadPoolSize == 0 (the refresh loop
	// is not started in that case so the value is meaningless).
	ReplicaRefreshInterval time.Duration

	// EstimatedReplicaBytes is the memory the recommended pool will
	// consume (N * DatabaseSize). Exposed so callers can log or
	// further constrain based on whole-process memory budgets.
	EstimatedReplicaBytes int64

	// EstimatedRefreshCPU is the expected fraction of one core that
	// the refresh goroutine will consume under the workload hint.
	// Derived from f_write × N × S / (BWDeserialize × T).
	// Values close to 1.0 or above indicate the recommendation is
	// CPU-constrained and the caller may want to raise T.
	EstimatedRefreshCPU float64

	// EstimatedReadSpeedup is the expected multiplier over the
	// single-connection baseline for concurrent reads, from the
	// alpha_read × min(N, P) model. Useful for deciding whether
	// enabling the pool is worth the memory cost at all.
	EstimatedReadSpeedup float64

	// Rationale is a short human-readable string describing which
	// constraint was binding (memory, cores, staleness, or no-pool
	// fallback). Suitable for a one-line slog.Info at startup.
	Rationale string
}

// ── Recommender ─────────────────────────────────────────────────────────────

// Recommend returns a Recommendation for the given Workload. It never fails
// and never panics — invalid or zero inputs collapse to the safe
// ReadPoolSize=0 fallback with a Rationale describing why.
//
// The function is pure (no goroutines, no I/O, no allocations beyond the
// returned string) and safe to call from a hot path, though in practice
// it is intended to be called once at startup.
func Recommend(w Workload) Recommendation {
	// ── Fallback: caller cannot afford a pool ──────────────────────────
	if w.DatabaseSize <= 0 {
		return Recommendation{
			Rationale: "ReadPoolSize=0: DatabaseSize is zero or unknown, " +
				"so per-replica memory cost cannot be bounded",
		}
	}
	if w.MemoryBudget < w.DatabaseSize {
		return Recommendation{
			Rationale: fmt.Sprintf(
				"ReadPoolSize=0: MemoryBudget (%d bytes) is smaller than "+
					"DatabaseSize (%d bytes) — cannot fit even one replica",
				w.MemoryBudget, w.DatabaseSize),
		}
	}

	// ── Resolve CPU-cores input ────────────────────────────────────────
	cores := w.CPUCores
	if cores <= 0 {
		cores = runtime.GOMAXPROCS(0)
	}
	if cores < 1 {
		cores = 1
	}

	// ── Recommend N (replica count) ────────────────────────────────────
	//
	// Three independent caps:
	//   - memory cap:    floor(MemoryBudget / DatabaseSize)
	//   - cores cap:     GOMAXPROCS (diminishing returns past this)
	//   - caller cap:    MaxReplicas (zero = no cap)
	nMem := int(w.MemoryBudget / w.DatabaseSize)
	nCores := cores
	n := nMem
	binding := "memory budget"
	if nCores < n {
		n = nCores
		binding = "GOMAXPROCS"
	}
	if w.MaxReplicas > 0 && w.MaxReplicas < n {
		n = w.MaxReplicas
		binding = "MaxReplicas cap"
	}
	if n < 1 {
		// Defensive: if arithmetic ever produces < 1 (shouldn't, given
		// the MemoryBudget >= DatabaseSize check above) fall back to
		// the no-pool recommendation rather than producing invalid
		// memdb.Config.
		return Recommendation{
			Rationale: "ReadPoolSize=0: derived pool size was < 1 " +
				"(check MaxReplicas and CPUCores inputs)",
		}
	}

	// Write-heavy workloads see diminishing returns even faster because
	// the refresh goroutine cannot keep up with writes at high N. Halve
	// the recommendation in that regime to leave CPU headroom for
	// useful work rather than refresh churn.
	if w.Workload == WorkloadWriteHeavy && n > 2 {
		halved := n / 2
		n = halved
		binding = "write-heavy regime (halved from cores/memory cap)"
	}

	// ── Recommend T (refresh interval) ─────────────────────────────────
	//
	// The caller's staleness tolerance is the primary input, clamped to
	// [RefreshIntervalFloor, RefreshIntervalCeiling]. Zero → default.
	t := w.StalenessTolerance
	tRationale := ""
	if t <= 0 {
		t = RefreshIntervalDefault
		tRationale = "StalenessTolerance unset, using default 50ms"
	}
	if t < RefreshIntervalFloor {
		tRationale = fmt.Sprintf(
			"StalenessTolerance %s raised to 5ms floor (below this pprof "+
				"shows refresh loop dominating CPU)",
			w.StalenessTolerance)
		t = RefreshIntervalFloor
	} else if t > RefreshIntervalCeiling {
		tRationale = fmt.Sprintf(
			"StalenessTolerance %s capped at 500ms ceiling (past this the "+
				"'replica' semantics become confusing for applications)",
			w.StalenessTolerance)
		t = RefreshIntervalCeiling
	}

	// ── Cost estimates for reporting ───────────────────────────────────
	memBytes := int64(n) * w.DatabaseSize

	// Expected refresh CPU as a fraction of ONE core, assuming f_write
	// ticks actually do work. A value > 1 means the refresh goroutine
	// would saturate a full core at this N/T — we don't bound by this
	// automatically (the caller's staleness tolerance already bounds T),
	// but we report it so a caller can raise T if they see 0.8+.
	fWrite := w.Workload.writeFraction()
	tSec := t.Seconds()
	cpuRefresh := fWrite * float64(n) * float64(w.DatabaseSize) /
		(BWDeserialize * tSec)

	// Expected read speedup vs single-connection baseline.
	effectiveN := n
	if effectiveN > cores {
		effectiveN = cores
	}
	speedup := AlphaRead * float64(effectiveN)
	if speedup < 1.0 {
		speedup = 1.0 // enabling the pool never makes things slower per the benchmark
	}

	// ── Build Rationale ────────────────────────────────────────────────
	parts := []string{
		fmt.Sprintf("ReadPoolSize=%d (bound by %s)", n, binding),
		fmt.Sprintf("ReplicaRefreshInterval=%s", t),
		fmt.Sprintf("workload=%s", w.Workload),
		fmt.Sprintf("est-mem=%s", humanBytes(memBytes)),
		fmt.Sprintf("est-refresh-cpu=%.1f%%", cpuRefresh*100),
		fmt.Sprintf("est-read-speedup=%.2fx", speedup),
	}
	if tRationale != "" {
		parts = append(parts, tRationale)
	}

	return Recommendation{
		ReadPoolSize:           n,
		ReplicaRefreshInterval: t,
		EstimatedReplicaBytes:  memBytes,
		EstimatedRefreshCPU:    cpuRefresh,
		EstimatedReadSpeedup:   speedup,
		Rationale:              strings.Join(parts, "; "),
	}
}

// ── Helpers ─────────────────────────────────────────────────────────────────

// humanBytes formats n using the closest binary-prefixed unit. Kept local
// to avoid pulling in a dependency; the recommendation output is meant to
// be human-eyeballed, not parsed, so an approximate rendering is fine.
func humanBytes(n int64) string {
	if n < 0 {
		return fmt.Sprintf("%d", n)
	}
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
