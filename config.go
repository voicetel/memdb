package memdb

import (
	"fmt"
	"log/slog"
	"time"
)

const (
	defaultFlushInterval = 30 * time.Second
	defaultBusyTimeout   = 5000
	defaultCacheSize     = -64000 // 64MB in kibibytes

	// DefaultRestoreMaxBytesFallback is the last-resort static value
	// used by defaultRestoreMaxBytes when neither GOMEMLIMIT nor the
	// host-physical-memory probe yields a number (4 GiB). Most callers
	// will never see this value because Linux/Darwin both expose host
	// memory; it exists for platforms without a stdlib probe.
	DefaultRestoreMaxBytesFallback int64 = 4 << 30
)

// DurabilityMode controls the write durability guarantee.
type DurabilityMode int

const (
	// DurabilityNone — periodic snapshot only.
	// Fastest writes. Loss window = FlushInterval.
	DurabilityNone DurabilityMode = iota

	// DurabilityWAL — writes appended to an on-disk WAL after each Exec.
	// Near-zero loss window. Small per-write latency cost.
	DurabilityWAL

	// DurabilitySync — every write flushed to disk immediately.
	// Equivalent to a regular file-backed SQLite. Slowest.
	DurabilitySync
)

// ChangeEvent is emitted for every INSERT, UPDATE, or DELETE.
type ChangeEvent struct {
	Op    string // "INSERT", "UPDATE", "DELETE"
	Table string
	RowID int64
}

// FlushMetrics is passed to OnFlushComplete after each flush.
type FlushMetrics struct {
	Duration time.Duration
	Error    error
}

// FlushErrorHandler is called when an async background flush fails.
type FlushErrorHandler func(err error)

// ChangeHandler is called synchronously on every write to the memory DB.
type ChangeHandler func(ChangeEvent)

// MetricsHandler is called after each completed flush.
type MetricsHandler func(FlushMetrics)

// Config holds all options for opening a memdb database.
type Config struct {
	// Path to the SQLite file on disk. Required unless a custom Backend is set.
	FilePath string

	// DisableForeignKeys opts out of SQLite foreign-key constraint
	// enforcement. By default memdb runs PRAGMA foreign_keys=ON on every
	// new connection so that INSERT/UPDATE/DELETE operations that would
	// violate a declared FK constraint are rejected with an error rather
	// than silently accepted (SQLite's built-in default is OFF, which
	// surprises most applications that declare FK constraints).
	//
	// Set DisableForeignKeys = true only when you deliberately need
	// SQLite's default FK-off behaviour — for example to bulk-load data
	// without constraint checking, or to work with a legacy schema that
	// relies on unenforced foreign keys.
	//
	// Using an inverted name ("Disable…") lets the zero value of the
	// Config struct mean "enforce foreign keys", which is the safe
	// default without requiring a pointer or sentinel.
	//
	// Default: false (foreign-key enforcement is ON).
	DisableForeignKeys bool

	// How often the background goroutine flushes memory to disk.
	// Default: 30s. Set to 0 to disable background flushing.
	FlushInterval time.Duration

	// Durability mode. Default: DurabilityNone.
	Durability DurabilityMode

	// SQLite busy timeout in milliseconds for the file DB during flush.
	// Default: 5000.
	BusyTimeout int

	// SQLite page cache size for the in-memory DB. Negative = kibibytes.
	// Default: -64000 (64MB).
	CacheSize int

	// Pages to copy per backup step. -1 = all at once.
	// Tune for large DBs to reduce latency spikes during flush.
	BackupStepPages int

	// RestoreMaxBytes is the upper bound on how far the deserialized
	// in-memory database (writer and every replica) is allowed to grow
	// after Restore. memdb calls sqlite3_deserialize with
	// SQLITE_DESERIALIZE_RESIZEABLE so the database can grow past the
	// snapshot size; the cap is enforced after deserialization by
	// PRAGMA max_page_count = RestoreMaxBytes / page_size on every
	// copy (writer + N replicas).
	//
	// Default: 0 → host-aware computation in defaultRestoreMaxBytes:
	//
	//   budget = GOMEMLIMIT/2 if set, else host physical RAM / 2
	//   budget = clamp(budget, 256 MiB, 16 GiB)
	//   RestoreMaxBytes = clamp(budget / (1 + ReadPoolSize), 256 MiB, 16 GiB)
	//
	// Dividing by (1 + ReadPoolSize) keeps the total memory footprint
	// across writer + replicas bounded; a 16 GiB host with
	// ReadPoolSize=4 lands at ~1.6 GiB per copy instead of 8 GiB per
	// copy. The 256 MiB floor wins on tiny VMs even if division would
	// otherwise drive the cap below it. Platforms without a memory
	// probe fall through to DefaultRestoreMaxBytesFallback (4 GiB).
	//
	// Growth is on-demand via sqlite3_realloc64, so the cap value
	// itself does not pre-allocate; it only defines the level at
	// which SQLITE_FULL would re-appear.
	//
	// Set explicitly in memory-constrained environments to fail closed
	// rather than relying on the auto-scaled default. A value below
	// the snapshot's already-occupied size is honoured exactly (the
	// snapshot loads but no growth is permitted past current usage).
	//
	// Background: before this knob existed (memdb ≤ v1.8.3),
	// sqlite3_deserialize was called without RESIZEABLE, freezing the
	// page capacity at len(snapshot) and producing SQLITE_FULL on
	// every post-Restore write. See the v1.9.0 release notes for the
	// full diagnosis.
	RestoreMaxBytes int64

	// ReadPoolSize is the number of independent in-memory replica databases
	// to maintain for read operations. When > 0, Query and QueryRow are
	// served from these replicas in round-robin order, allowing multiple
	// goroutines to read concurrently without contending on the single writer
	// connection. Each replica is a full copy of the writer's state, refreshed
	// in the background on ReplicaRefreshInterval.
	//
	// Reads may observe data that is at most ReplicaRefreshInterval stale.
	// Writes (Exec, Begin, BeginTx) always go to the single writer connection.
	//
	// Enabling the pool is the single biggest read-throughput lever memdb
	// exposes: BenchmarkCompare_ConcurrentRead measured 2.71× more
	// throughput on a 4-replica pool vs the single-connection default,
	// and the pool is ~22% faster than file-backed SQLite on concurrent
	// reads (see BENCHMARKS.md). The cost is N × DatabaseSize memory and
	// up to ReplicaRefreshInterval of read staleness.
	//
	// Recommended values: 0 (default) to runtime.GOMAXPROCS(0).
	// Setting this above GOMAXPROCS yields diminishing returns.
	//
	// For a closed-form recommendation based on DB size, memory budget,
	// staleness tolerance, and workload mix, use the memdb/tuning package:
	//
	//	rec := tuning.Recommend(tuning.Workload{
	//	    DatabaseSize:       50 << 20,
	//	    MemoryBudget:       512 << 20,
	//	    StalenessTolerance: 100 * time.Millisecond,
	//	    Workload:           tuning.WorkloadReadHeavy,
	//	})
	//	cfg.ReadPoolSize = rec.ReadPoolSize
	//	cfg.ReplicaRefreshInterval = rec.ReplicaRefreshInterval
	//
	// Default: 0 (disabled — all operations share one connection).
	ReadPoolSize int

	// ReplicaRefreshInterval controls how often the background goroutine
	// re-serializes the writer and deserializes into every replica.
	// Shorter intervals mean fresher reads but more CPU overhead —
	// sqlite3_deserialize copies the entire database into each replica on
	// every tick.
	//
	// Empirical guidance (from BenchmarkReplicaRefreshInterval on a
	// 1 000-row dataset with 8 concurrent readers, measured on a
	// 20-thread x86_64 box; v1.4.0 reference run, see BENCHMARKS.md):
	//
	//	refresh=250µs   ~90 µs/write   34 KB/op   (refresh dominates CPU)
	//	refresh=1ms     ~84 µs/write   29 KB/op   (previous default — 8× slower than 100ms)
	//	refresh=5ms     ~79 µs/write   27 KB/op   (marginal improvement)
	//	refresh=25ms    ~53 µs/write   20 KB/op   (knee of the curve)
	//	refresh=100ms   ~11 µs/write   3.8 KB/op  (writes at full speed)
	//
	// The CPU cost scales with database size because the entire serialised
	// image is memmoved into every replica per tick. On larger datasets
	// the knee shifts further right — a 100 MB database at refresh=1ms
	// would saturate a core just copying bytes.
	//
	// Only used when ReadPoolSize > 0.
	//
	// Default: 50 ms. This keeps the read-staleness window small enough
	// for typical "eventual consistency" expectations while costing writes
	// almost nothing. Callers who need lower staleness at the cost of
	// write throughput can set this explicitly; values below 5 ms emit a
	// warning at Open time because they were observed to dominate CPU in
	// pprof traces.
	ReplicaRefreshInterval time.Duration

	// Called when a background flush fails.
	// If nil, errors are silently dropped.
	OnFlushError FlushErrorHandler

	// Called on every INSERT, UPDATE, or DELETE.
	OnChange ChangeHandler

	// Called after each completed flush.
	OnFlushComplete MetricsHandler

	// OnExec is called synchronously after every successful Exec, with the
	// SQL statement and its arguments. Use this to hook Raft replication:
	// submit the entry to the cluster and block until consensus is reached
	// before returning. Return a non-nil error to propagate back to the caller.
	// If nil, Exec operates locally only.
	//
	// With this hook, Exec's sql.Result always reports 0 rows affected —
	// the func signature cannot carry the count back. Callers that gate on
	// the affected-row count (including PostgreSQL wire-protocol clients
	// served by the server package) should wire OnExecResult instead.
	OnExec func(sql string, args []any) error

	// OnExecResult is OnExec returning the number of rows the statement
	// affected once the write is committed, so Exec can report the real
	// count through sql.Result. Wire it to raft.Node.ExecResult; the count
	// originates from the FSM's ExecDirectResult on the leader and travels
	// back through the consensus (or write-forwarding) response.
	//
	// Setting both OnExec and OnExecResult is a configuration error.
	OnExecResult func(sql string, args []any) (int64, error)

	// Executed once against the memory DB after restore or on first open.
	// Use for CREATE TABLE IF NOT EXISTS statements.
	InitSchema func(db *DB) error

	// Storage backend. Defaults to LocalBackend{Path: FilePath}.
	Backend Backend

	// Logger is used for internal structured log output (flush events, WAL
	// replay, restore, replica refresh errors). If nil, slog.Default() is used.
	// Use logging.NewSyslogHandler, logging.NewJSONHandler, or
	// logging.NewTextHandler to construct a suitable logger.
	Logger *slog.Logger
}

func (c *Config) validate() error {
	if c.FilePath == "" && c.Backend == nil {
		return fmt.Errorf("memdb: FilePath or Backend is required")
	}
	if c.RestoreMaxBytes < 0 {
		return fmt.Errorf("memdb: RestoreMaxBytes must be >= 0 (got %d)", c.RestoreMaxBytes)
	}
	if c.OnExec != nil && c.OnExecResult != nil {
		return fmt.Errorf("memdb: OnExec and OnExecResult are mutually exclusive; set only OnExecResult")
	}
	return nil
}

func (c *Config) applyDefaults() {
	// DisableForeignKeys: zero value false = FK enforcement ON, which is
	// the correct default. No sentinel needed — the inverted name means
	// the zero value is already the safe behaviour.
	if c.FlushInterval == 0 {
		c.FlushInterval = defaultFlushInterval
	}
	// A negative FlushInterval disables background flushing.
	// Normalise it so Open() can simply check > 0.
	if c.FlushInterval < 0 {
		c.FlushInterval = 0
	}
	if c.BusyTimeout == 0 {
		c.BusyTimeout = defaultBusyTimeout
	}
	if c.CacheSize == 0 {
		c.CacheSize = defaultCacheSize
	}
	if c.BackupStepPages == 0 {
		c.BackupStepPages = -1
	}
	if c.ReadPoolSize < 0 {
		c.ReadPoolSize = 0
	}
	if c.RestoreMaxBytes == 0 {
		// Divide the host-aware budget across writer + replicas so the
		// total cap stays bounded regardless of ReadPoolSize. Floor at
		// restoreMaxBytesFloor (256 MiB) so the writer never gets a
		// useless cap even if N is large.
		base := defaultRestoreMaxBytes()
		copies := int64(1 + c.ReadPoolSize)
		perDB := base / copies
		if perDB < restoreMaxBytesFloor {
			perDB = restoreMaxBytesFloor
		}
		c.RestoreMaxBytes = perDB
	}
	if c.ReadPoolSize > 0 && c.ReplicaRefreshInterval <= 0 {
		// 50 ms balances read staleness against writer CPU cost. See the
		// BenchmarkReplicaRefreshInterval sweep and the pprof analysis in
		// the field doc above for the empirical justification. Operators
		// who need lower staleness must set this explicitly and accept
		// the write-throughput trade-off it implies.
		c.ReplicaRefreshInterval = 50 * time.Millisecond
	}
	if c.ReadPoolSize > 0 && c.ReplicaRefreshInterval > 0 && c.ReplicaRefreshInterval < 5*time.Millisecond {
		logger := c.Logger
		if logger == nil {
			logger = slog.Default()
		}
		logger.Warn("memdb: ReplicaRefreshInterval below 5ms was observed in pprof "+
			"to dominate CPU via sqlite3_deserialize memmoves on every tick; "+
			"consider 25ms–100ms unless sub-5ms read staleness is a hard requirement",
			"interval", c.ReplicaRefreshInterval,
			"readPoolSize", c.ReadPoolSize,
		)
	}
	if c.Backend == nil && c.FilePath != "" {
		c.Backend = &LocalBackend{Path: c.FilePath}
	}
}
