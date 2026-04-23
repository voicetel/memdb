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
	// Recommended values: 0 (default) to runtime.GOMAXPROCS(0).
	// Setting this above GOMAXPROCS yields diminishing returns.
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
	// 20-thread x86_64 box):
	//
	//	refresh=250µs   ~83 µs/write   34 KB/op   (refresh dominates CPU)
	//	refresh=1ms     ~75 µs/write   30 KB/op   (still 8× slower than 100ms)
	//	refresh=5ms     ~72 µs/write   26 KB/op   (marginal improvement)
	//	refresh=25ms    ~35 µs/write   14 KB/op   (knee of the curve)
	//	refresh=100ms   ~10 µs/write   3.5 KB/op  (writes at full speed)
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
	OnExec func(sql string, args []any) error

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
	return nil
}

func (c *Config) applyDefaults() {
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
