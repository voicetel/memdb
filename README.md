# memdb

A high-performance, embedded Go database library built on SQLite. All reads and writes operate against an in-memory SQLite instance. Persistence is handled via the SQLite Online Backup API, an optional write-ahead log (WAL), and pluggable storage backends.

Think Redis RDB+AOF semantics with full SQL query power — in a single Go import.

**3.16× faster writes than file SQLite. ~34% faster concurrent reads than file SQLite with `ReadPoolSize > 0`.** See [BENCHMARKS.md](./BENCHMARKS.md) for the current v1.6.1 report.

---

## Features

- **Full SQL** — joins, indexes, transactions, aggregates via SQLite
- **3.16× faster writes** — all writes hit memory; no VFS, no page-cache overhead
- **Sub-millisecond reads** — all queries hit memory, no disk I/O on the hot path
- **Concurrent reads** — channel-based replica pool (`ReadPoolSize`) is ~34% faster than file SQLite at 4 goroutines (and 2.18× faster than memdb without the pool)
- **Prepared-statement cache** — repeated `Exec`/`Query` against the writer skip per-call Prepare/Close; ~1.4× contended write throughput from a single `sync.Map[string]*sql.Stmt`
- **Configurable durability** — periodic snapshot only, WAL-backed near-zero loss, or fully synchronous
- **Atomic snapshots** — write-then-rename prevents corrupt state on crash
- **Pluggable backends** — local disk or any custom `Backend` implementation; AEAD backends opt out of the redundant SHA-256 snapshot header via `AuthenticatedBackend`
- **Snapshot compression** — zstd at `SpeedFastest` built in, ~50-60% size reduction on typical schemas, ~2× faster encode than the default level
- **Change notifications** — `OnChange` hook via SQLite update hook for cache invalidation and audit
- **Panic-safe callbacks** — `OnChange`, `OnFlushError`, `OnFlushComplete`, and background goroutines recover panics so a misbehaving callback cannot crash the process.
- **Raft replication** — strong-consistency multi-node replication via `hashicorp/raft` over mutual TLS; any node accepts writes and transparently forwards to the leader
- **Structured logging** — `log/slog` throughout with syslog, JSON, and text handlers; `hclog` bridge routes Raft internals through the same pipeline
- **PostgreSQL wire protocol** — optional server mode accepts any Postgres client or ORM; SSL negotiation, correct `ErrorResponse` severity field
- **ORM compatible** — exposes `*sql.DB` for use with `sqlx`, `bun`, `ent`, `sqlc`, GORM, and others

---

## Installation

### As a library

```bash
go get github.com/voicetel/memdb
```

memdb uses `mattn/go-sqlite3` under the hood, which requires CGo. A C compiler
must be present at build time:

```bash
# Linux (Debian/Ubuntu)
apt install gcc

# Linux (Alpine / musl)
apk add gcc musl-dev

# macOS
xcode-select --install

# Windows
# Install MinGW-w64, e.g. via MSYS2: pacman -S mingw-w64-x86_64-gcc
```

`CGO_ENABLED=1` is the Go default on Linux and macOS for native builds, so no
extra environment variable is required unless you have explicitly disabled it.
Cross-compiling to a different OS/arch requires a matching C toolchain — see
the [Cross-Compilation](#cross-compilation) section below.

### As a CLI binary (from source)

A `Makefile` is provided for common development and build tasks.

```bash
git clone https://github.com/voicetel/memdb
cd memdb

make build          # build ./build/memdb
make install        # install to /usr/local/bin
```

For all supported platforms at once:

```bash
make build-all      # linux/amd64, linux/arm64, darwin/amd64, darwin/arm64
```

Run `make help` to see every available target.

---

## Quick Start

```go
package main

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/voicetel/memdb"
)

func main() {
	db, err := memdb.Open(memdb.Config{
		FilePath:      "/var/lib/myapp/data.db",
		FlushInterval: 10 * time.Second,
		Durability:    memdb.DurabilityWAL,
		OnFlushError: func(err error) {
			log.Printf("flush error: %v", err)
		},
		InitSchema: func(db *memdb.DB) error {
			_, err := db.Exec(`
				CREATE TABLE IF NOT EXISTS sessions (
					id    TEXT PRIMARY KEY,
					data  BLOB NOT NULL,
					ts    INTEGER NOT NULL
				)
			`)
			return err
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// All writes hit memory — microsecond latency
	_, err = db.Exec(
		`INSERT OR REPLACE INTO sessions (id, data, ts) VALUES (?, ?, ?)`,
		"abc123", []byte(`{"user":1}`), time.Now().Unix(),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Reads are equally fast
	var data []byte
	err = db.QueryRow(
		`SELECT data FROM sessions WHERE id = ?`, "abc123",
	).Scan(&data)
	if err == sql.ErrNoRows {
		log.Println("not found")
	} else if err != nil {
		log.Fatal(err)
	}
	log.Printf("session: %s", data)

	// Force a flush when you need guaranteed durability
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.Flush(ctx); err != nil {
		log.Fatal(err)
	}
}
```

---

## Configuration

```go
type Config struct {
	// Path to the SQLite file on disk. Required.
	FilePath string

	// Background flush interval. Default: 30s.
	// Set to 0 to disable background flushing (manual Flush() only).
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
	// Tune for large DBs to reduce latency spikes. Default: -1.
	BackupStepPages int

	// Set true to disable SQLite foreign-key enforcement.
	// The zero value (false) means enforcement is ON.
	DisableForeignKeys bool

	// Number of independent in-memory read replicas to maintain.
	// When > 0, Query and QueryRow are load-balanced across replicas,
	// allowing concurrent reads without contending on the writer connection.
	// Reads may be at most ReplicaRefreshInterval stale.
	// Writes always go to the single writer connection.
	// Recommended: 0 (default) or runtime.GOMAXPROCS(0).
	ReadPoolSize int

	// How often the background goroutine refreshes replicas from the writer
	// via sqlite3_serialize / sqlite3_deserialize. Shorter intervals yield
	// fresher reads but copy the entire database into every replica on
	// every tick — the cost scales with database size × ReadPoolSize.
	//
	// Write-generation short-circuit: each refresh tick first checks a
	// monotonic write counter that bumps on every successful Exec /
	// ExecContext / execDirect and on every Begin / BeginTx. When the
	// counter has not advanced since the last successful refresh, the
	// tick returns immediately without serialising or deserialising
	// anything — so a read-only workload imposes no per-tick CPU cost
	// regardless of how short this interval is. Measured in pprof on
	// the v1.4.0 reference run, two successive optimisations took
	// read-only throughput from 519k → 727k → 812k ops/s (+56% total):
	// first the write-generation short-circuit in replicaPool.refresh
	// (+40%), then the fast-path tick in replicaRefreshLoop that skips
	// the per-tick goroutine/channel/context allocation when the
	// short-circuit fires (+16% on top). replicaRefreshLoop now drops
	// below the top-10 CPU sampling threshold entirely for read-only
	// workloads. See BENCHMARKS.md for the full before/after profiles.
	//
	// Empirical sweep (1 000-row dataset, 8 concurrent readers with a
	// continuously active writer, BenchmarkReplicaRefreshInterval on a
	// 20-thread x86_64 box — the writer-active case that the
	// short-circuit cannot help; v1.4.0 reference run, see
	// BENCHMARKS.md):
	//
	//   refresh=250µs  ~90 µs/write  34 KB/op  (refresh dominates CPU)
	//   refresh=1ms    ~84 µs/write  29 KB/op  (previous default — 8× slower than 100ms)
	//   refresh=5ms    ~79 µs/write  27 KB/op  (marginal improvement)
	//   refresh=25ms   ~53 µs/write  20 KB/op  (knee of the curve)
	//   refresh=100ms  ~11 µs/write  3.8 KB/op (writes at full speed)
	//
	// Only used when ReadPoolSize > 0. Default: 50ms. Values below 5ms
	// emit a warning at Open time because pprof traces attribute the
	// majority of CPU to the refresh loop at those intervals when the
	// writer is active.
	ReplicaRefreshInterval time.Duration

	// Called when a background flush fails.
	// If nil, errors are silently dropped.
	OnFlushError FlushErrorHandler

	// Called on every INSERT, UPDATE, or DELETE against the memory DB.
	OnChange ChangeHandler

	// Called after each completed flush.
	OnFlushComplete MetricsHandler

	// Executed once after restore on startup.
	// Use for CREATE TABLE IF NOT EXISTS statements.
	InitSchema func(db *DB) error

	// Storage backend. Default: LocalBackend{Path: FilePath}.
	Backend Backend

	// Called after every successful Exec with the SQL and its arguments.
	// When set, Exec no longer writes locally — the write only happens when
	// the Raft FSM calls ExecDirect after consensus. Use this to route all
	// writes through a raft.Node for cluster replication.
	// If nil, Exec operates locally (standalone mode).
	OnExec func(sql string, args ...any) error
}
```

---

## Durability Modes

```go
const (
	// DurabilityNone — periodic snapshot only.
	// Fastest writes. Loss window = FlushInterval.
	DurabilityNone DurabilityMode = iota

	// DurabilityWAL — writes appended to an on-disk WAL after each Exec.
	// Near-zero loss window. Small per-write latency cost.
	// On startup: snapshot is loaded, then WAL is replayed.
	DurabilityWAL

	// DurabilitySync — every write appended to the WAL AND a full
	// backend snapshot is flushed before Exec returns. Slowest mode;
	// provides zero-loss durability equivalent to a regular file-backed
	// SQLite configured with synchronous=FULL.
	DurabilitySync
)
```

| Mode | Loss Window | Write Latency | Use Case |
|---|---|---|---|
| `DurabilityNone` | Up to `FlushInterval` | Lowest | Caches, rate limiters, ephemeral state |
| `DurabilityWAL` | Milliseconds (fsync per write, WAL replay on restart) | Low | Session stores, feature flags, analytics |
| `DurabilitySync` | None (WAL append + full backend flush per write) | Highest | Audit logs, financial state |

Context-aware variants are available for all three modes: `ExecContext`,
`QueryContext`, and `QueryRowContext` thread a `context.Context` into the
underlying `database/sql` call and — for `DurabilitySync` — bound the
worst-case latency of the synchronous backend flush. Prefer the context
variants in any code path where a caller-supplied deadline should be
honoured.

---

## Storage Backends

Third-party backends (`CompressedBackend`, `EncryptedBackend`, and custom
implementations) implement `memdb.ExternalBackend` — the public stream-oriented
interface — and must be wrapped with `memdb.WrapBackend` before being assigned to
`Config.Backend`. This keeps the backup-API internals private while still allowing
arbitrary backend composition.

### Local (default)

```go
cfg.Backend = &memdb.LocalBackend{
	Path: "/var/lib/myapp/data.db",
}
```

Snapshots are written atomically via a temp file + `rename(2)`. The file on disk is
always a valid, complete SQLite database. `LocalBackend` implements `memdb.Backend`
directly and does not need `WrapBackend`.

### Compressed (wraps any backend)

```go
import "github.com/voicetel/memdb/backends"

cfg.Backend = memdb.WrapBackend(&backends.CompressedBackend{
    Inner: &backends.LocalBackend{Path: "/var/lib/myapp/data.db.zst"},
})
```

Uses zstd compression at `SpeedFastest` — pprof showed the default level
dominating CPU on a frequently-flushed hot path; `SpeedFastest` halves
encoder CPU for ~10% larger snapshots, the right trade for snapshots that
get rewritten often. Typical SQLite files still compress 50–60%.

### Encrypted (wraps any backend)

```go
var key [32]byte
copy(key[:], os.Getenv("MEMDB_KEY"))

cfg.Backend = memdb.WrapBackend(&backends.EncryptedBackend{
    Inner: &backends.LocalBackend{Path: "/var/lib/myapp/data.db.enc"},
    Key:   key,
})
```

AES-256-GCM encryption. The nonce is prepended to the ciphertext; each flush
generates a fresh random nonce. `EncryptedBackend` implements
`memdb.AuthenticatedBackend`, so the adapter skips its 40-byte SHA-256
snapshot header — the GCM tag already provides authenticated integrity, and
SHA-256 was measured at ~17% of the encrypted-flush CPU profile.

Backends compose freely — compression and encryption are independent layers that
stack in any order:

```go
// Compress then encrypt to local disk
cfg.Backend = memdb.WrapBackend(&backends.CompressedBackend{
    Inner: &backends.EncryptedBackend{
        Inner: &backends.LocalBackend{Path: "/var/lib/myapp/data.db.zst.enc"},
        Key:   encKey,
    },
})
```

### Custom Backend

Implement `memdb.ExternalBackend` and pass it to `memdb.WrapBackend`:

```go
type ExternalBackend interface {
    Exists(ctx context.Context) (bool, error)
    Write(ctx context.Context, r io.Reader) error
    Read(ctx context.Context) (io.ReadCloser, error)
}

cfg.Backend = memdb.WrapBackend(myCustomBackend)
```

`Write` receives a stream of the raw SQLite database bytes. `Read` must return
those exact bytes. `Exists` should return `false, nil` (not an error) when no
snapshot has been written yet.

A custom backend that already authenticates its payload (AEAD encryption,
HMAC, signed object stores, etc.) can opt out of the adapter's redundant
SHA-256 snapshot header by additionally implementing
`memdb.AuthenticatedBackend`:

```go
type AuthenticatedBackend interface {
    Authenticated() bool
}
```

Returning `true` causes the adapter to write the SQLite payload directly
and to suppress the legacy-snapshot warning on restore when the bytes
do not start with the `MDBK` header.

---

## ORM Integration

The `DB()` method returns the underlying `*sql.DB`:

```go
// sqlx
sqlxDB := sqlx.NewDb(db.DB(), "sqlite3")

// bun
bunDB := bun.NewDB(db.DB(), sqlitedialect.New())

// sqlc — pass db.DB() wherever a *sql.DB querier is accepted

// ent
client, err := ent.NewClient(ent.Driver(sql.OpenDB(db.DB())))
```

---

## Change Notifications

```go
cfg.OnChange = func(e memdb.ChangeEvent) {
	// e.Op    → "INSERT", "UPDATE", "DELETE"
	// e.Table → table name
	// e.RowID → SQLite rowid of affected row
	if e.Table == "sessions" {
		sessionCache.Invalidate(e.RowID)
	}
}
```

Powered by `sqlite3_update_hook`. Fires synchronously in the calling goroutine before `Exec` returns.

---

## Metrics

```go
cfg.OnFlushComplete = func(m memdb.FlushMetrics) {
	// Prometheus example
	flushDuration.Observe(m.Duration.Seconds())
	if m.Error != nil {
		flushErrors.Inc()
	}
}
```

`FlushMetrics` contains `Duration time.Duration` and `Error error`.

---

## Logging

memdb uses [`log/slog`](https://pkg.go.dev/log/slog) as its single structured
logging interface. All components — the core DB, WAL, replica pool, Raft node,
and CLI — write through the same pipeline. Three handler constructors are
provided in the `logging` sub-package; no new dependencies are required.

### Handlers

```go
import (
    "log/slog"
    "os"
    "github.com/voicetel/memdb/logging"
)

// Human-readable key=value output — good for development
logger := logging.NewTextHandler(os.Stderr, slog.LevelDebug)

// Structured JSON — good for log aggregators (Datadog, Splunk, Loki)
logger := logging.NewJSONHandler(os.Stderr, slog.LevelInfo)

// Syslog via /dev/log — recommended for production Linux deployments.
// Falls back gracefully if syslogd is unavailable (macOS, containers).
logger, err := logging.NewSyslogHandler("memdb", slog.LevelInfo)
if err != nil {
    logger = logging.NewTextHandler(os.Stderr, slog.LevelInfo)
}
```

### Levels

| `slog` level | When it fires | Syslog priority |
|---|---|---|
| `Debug` | Forwarded writes | `LOG_DEBUG` |
| `Info` | Flush complete, WAL replay, restore, leader election, bootstrap | `LOG_INFO` |
| `Warn` | Single-node cluster warning | `LOG_WARNING` |
| `Error` | Flush failure, replica refresh error, FSM apply error | `LOG_ERR` |

### Wiring the logger

Pass the logger in `Config` for the core DB and in `NodeConfig` for the Raft
node. Both default to `slog.Default()` when nil.

```go
logger, _ := logging.NewSyslogHandler("memdb", slog.LevelInfo)

db, err := memdb.Open(memdb.Config{
    FilePath: "/var/lib/myapp/data.db",
    Logger:   logger,
    // ...
})

node, err := mraft.NewNode(adapter, mraft.NodeConfig{
    Logger: logger, // bridges to hclog internally for hashicorp/raft
    // ...
})
```

### Application-wide default

Set `slog.Default` once at startup and all memdb components that receive a nil
`Logger` will automatically use it:

```go
slog.SetDefault(logging.NewJSONHandler(os.Stderr, slog.LevelInfo))

// Now Open with Logger: nil — uses slog.Default() automatically
db, err := memdb.Open(memdb.Config{FilePath: "..."})
```

### hclog bridge

`hashicorp/raft` uses its own `hclog.Logger` interface internally. The
`logging.NewHCLogAdapter` function wraps any `*slog.Logger` as an `hclog.Logger`
so Raft's internal events (elections, heartbeats, log compaction, snapshot
installs) flow through the same slog pipeline:

```go
// Used automatically by NodeConfig — no manual wiring needed.
// Available for custom integrations:
hclogLogger := logging.NewHCLogAdapter(logger, "raft")
```

Level mapping: `hclog.Trace` → `slog.Debug`, `hclog.Debug` → `slog.Debug`,
`hclog.Info` → `slog.Info`, `hclog.Warn` → `slog.Warn`,
`hclog.Error` → `slog.Error`.

---

## Replication

memdb uses **Raft consensus** (`hashicorp/raft`) for multi-node replication. All
nodes are peers — there is no separate leader/follower configuration. Any node
accepts writes and transparently forwards them to the current leader over a
dedicated TLS RPC connection backed by a channel-based connection pool. Once the
leader commits the entry through Raft, every node's FSM applies it locally.

### Cluster sizing

Raft requires an **odd** number of nodes. Quorum is `⌊N/2⌋ + 1`.

| Nodes | Quorum | Survives |
|---|---|---|
| 1 | 1 | No failures (development only) |
| 3 | 2 | 1 failure |
| 5 | 3 | 2 failures |
| 7 | 4 | 3 failures |

Even-sized clusters are rejected at startup — they offer no fault-tolerance
advantage over the next smaller odd size.

### Network ports

Each node uses **two TLS ports**:

| Port | Purpose |
|---|---|
| `BindAddr` | Raft consensus RPCs (AppendEntries, RequestVote, InstallSnapshot) |
| `ForwardAddr` | Write-forwarding RPC — followers dial the leader here |

### Wiring a node to a memdb.DB

The `replication/raft` package's `Node` is decoupled from `memdb.DB` via the
`DB` interface (`ExecLocal`, `Serialize`, `Restore`). Wire them together with
a thin adapter and the `OnExec` hook:

```go
import (
    "crypto/tls"

    memdb  "github.com/voicetel/memdb"
    mraft  "github.com/voicetel/memdb/replication/raft"
)

// adapter bridges memdb.DB to the raft.DB interface.
type adapter struct{ db *memdb.DB }

func (a *adapter) ExecLocal(sql string, args ...any) error {
    return a.db.ExecDirect(sql, args...)   // bypasses OnExec — no Raft loop
}
func (a *adapter) Serialize() ([]byte, error) { return a.db.Serialize() }
func (a *adapter) Restore(data []byte) error  { return a.db.Restore(data) }

// openClusterNode opens a memdb.DB and a Raft node together.
// OnExec is set in Config so that every db.Exec routes through Raft consensus.
// ExecDirect (called by the FSM on every node after commit) bypasses OnExec
// to avoid an infinite Raft → Exec → Raft loop.
func openClusterNode(tlsCfg *tls.Config, nodeID string) (*memdb.DB, *mraft.Node, error) {
    peers := []string{
        "node-1=10.0.0.1:7000",
        "node-2=10.0.0.2:7000",
        "node-3=10.0.0.3:7000",
    }
    fwdPeers := []string{
        "node-1=10.0.0.1:7001",
        "node-2=10.0.0.2:7001",
        "node-3=10.0.0.3:7001",
    }

    // node is captured in the OnExec closure below; declare it first.
    var node *mraft.Node

    db, err := memdb.Open(memdb.Config{
        FilePath:      "/var/lib/myapp/data.db",
        FlushInterval: 30 * time.Second,
        InitSchema: func(db *memdb.DB) error {
            _, err := db.ExecDirect(`CREATE TABLE IF NOT EXISTS kv (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )`)
            return err
        },
        // Route every Exec through Raft. ExecDirect (the FSM path) bypasses
        // this hook so committed entries are applied without looping back.
        OnExec: func(sql string, args []any) error {
            return node.Exec(sql, args...)
        },
    })
    if err != nil {
        return nil, nil, err
    }

    node, err = mraft.NewNode(&adapter{db}, mraft.NodeConfig{
        NodeID:        nodeID,
        BindAddr:      "0.0.0.0:7000",    // Raft consensus port
        AdvertiseAddr: "10.0.0.1:7000",   // address announced to peers
        ForwardAddr:   "0.0.0.0:7001",    // write-forwarding port
        Peers:         peers,
        ForwardPeers:  fwdPeers,
        DataDir:       "/var/lib/myapp/raft",
        TLSConfig:     tlsCfg,            // mutual TLS — required
        OnLeaderChange: func(isLeader bool) {
            log.Printf("leadership changed: isLeader=%v", isLeader)
        },
    })
    if err != nil {
        db.Close()
        return nil, nil, err
    }

    return db, node, nil
}
```

> **Note:** When `OnExec` is set, `db.Exec` no longer writes locally —
> the write only happens when the Raft FSM calls `ExecDirect` after consensus
> is reached. Transactions (`Begin`, `BeginTx`, `WithTx`) return
> `ErrTransactionNotSupported` when replication is enabled; use `node.Exec`
> (or `db.Exec` which routes through it via `OnExec`) for all writes.
> Use `db.ExecDirect` only inside `InitSchema` and the FSM adapter, never in
> application code.

### Write flow

```
Any node receives db.Exec(sql)
  → OnExec fires → node.Exec(sql)
  → IsLeader?
      Yes → hraft.Apply → consensus → FSM.Apply on all nodes → ExecDirect
      No  → dial leader ForwardAddr (TLS) → ForwardRequest
           → leader: hraft.Apply → consensus → FSM.Apply on all nodes
           → ForwardResponse → return to caller
```

Writes block until a quorum of nodes commits the entry. The caller always
gets a consistent result regardless of which node it called.

### Connection pool

Forwarded writes reuse TLS connections from a per-leader channel-based
connection pool, eliminating the TLS handshake cost on every write when
the leader is stable.

```
pool (buffered channel of idle *tls.Conn, capacity 8)

Get()  → non-blocking receive from channel
             hit  → return warm connection (no handshake)
             miss → tls.DialWithDialer (new handshake)

Put()  → non-blocking send to channel
             channel not full → connection returned for reuse
             channel full     → connection closed (pool bounded)

Close() → drain channel, close every idle connection
```

The pool is keyed to the current leader. When the leader changes, the
`LeaderObservation` goroutine atomically swaps the pool pointer and closes
the old pool — idle connections to the previous leader are discarded and
new connections are dialled to the new leader on the next write.

### Leadership changes and consistency

When the leader changes mid-write, one of two outcomes is possible:

| Situation | Raft's guarantee | Result |
|---|---|---|
| Entry committed before leader stepped down | Entry in all quorum logs | Applied on every node — not lost |
| Entry not committed before leader stepped down | Entry rolled back | Not applied anywhere — not lost |

In both cases the cluster is consistent. The caller receives an error
(`ErrLeadershipLost` or a forwarding error) and can retry safely.

**Raft guarantees the cluster is always consistent.** A caller that retries
on error may send the same write twice — the cluster will apply it twice
unless the SQL is idempotent. Use retry-safe SQL patterns:

```sql
-- Safe to retry: duplicate key is a no-op
INSERT OR REPLACE INTO sessions (id, data, ts) VALUES (?, ?, ?)

-- Safe to retry: conflict is silently ignored
INSERT INTO events (id, payload) VALUES (?, ?)
ON CONFLICT (id) DO NOTHING
```

This is the standard recommendation for all distributed databases that
use Raft (etcd, CockroachDB, rqlite, Consul) — push idempotency into the
data model rather than building a distributed deduplication layer.

### Membership changes

```go
// Add a new node (call on the leader)
if err := node.AddVoter("node-4", "10.0.0.4:7000", 10*time.Second); err != nil {
    log.Fatal(err)
}

// Remove a node (call on the leader)
if err := node.RemoveServer("node-2", 10*time.Second); err != nil {
    log.Fatal(err)
}

// Check leadership and stats
log.Printf("leader: %v  addr: %s", node.IsLeader(), node.LeaderAddr())
log.Printf("stats: %v", node.Stats())
```

### Shutdown

```go
if err := node.Shutdown(); err != nil {
    log.Printf("raft shutdown: %v", err)
}
```

### Replication Testing

The `replication/raft` package ships with an extensive end-to-end integrity
suite (`replication_integrity_test.go`) that brings up a real three-node
cluster on loopback ports for every test. Each test asserts byte-level
equality of every node's FSM log — the same property Raft guarantees on
paper — so any corruption, duplication, or reordering in the wire, log
store, or FSM path is caught immediately.

| Test | What it verifies |
|---|---|
| `TestReplication_AllNodesConverge` | Every committed entry is applied on every node in the same order |
| `TestReplication_WriteThroughFollower` | Follower-submitted writes are forwarded and preserve global log order |
| `TestReplication_ConcurrentWriters` | 8 goroutines × 25 writes produce identical FSM state on every node with no duplicates or drops |
| `TestReplication_LeaderRestart` | Cluster re-elects and keeps replicating after the leader is shut down |
| `TestReplication_HighVolume_NoCorruption` | 500-entry soak with 6 argument types exercising every binary-codec scalar |
| `TestReplication_Snapshot_RestoresCleanly` | 10 000 entries force multiple snapshot cycles without divergence |
| `TestReplication_NotLeader_Returned` | Writes fail cleanly with `ErrNotLeader` when forwarding is unavailable |
| `TestReplication_StateAPIs` | `Stats`, `IsLeader`, `LeaderAddr` remain safe and consistent |
| `TestReplication_Shutdown_Ordering` | Reverse-order cluster shutdown is idempotent and does not deadlock |
| `TestReplication_ForwardingReconnect` | Follower's forwarding pool recovers across a leadership change |
| `TestReplication_AtomicCommit` | Post-`Exec` convergence window is bounded and logged |

Run them with the race detector:

```bash
make test-replication-integrity   # full suite (~10 s with -race)
make test-replication-soak        # high-volume soak + 10k-entry snapshot
```

---

## Server Mode (PostgreSQL Wire Protocol)

Run memdb as a standalone server that any PostgreSQL client can connect to:

```go
import "github.com/voicetel/memdb/server"

srv := server.New(db, server.Config{
	ListenAddr: "127.0.0.1:5433",
	TLSConfig:  tlsCfg,
	Auth: server.BasicAuth{
		Username: "memdb",
		Password: os.Getenv("MEMDB_PASSWORD"),
	},
})
srv.ListenAndServe()
```

Connect with any standard client:

```bash
psql -h 127.0.0.1 -p 5433 -U memdb
```

```python
import psycopg2
conn = psycopg2.connect(host="127.0.0.1", port=5433, user="memdb", password="...")
```

```javascript
const { Pool } = require('pg')
const pool = new Pool({ host: '127.0.0.1', port: 5433, user: 'memdb' })
```

For same-host multi-process access with minimal overhead, use a Unix domain socket:

```go
srv := server.New(db, server.Config{
	ListenAddr: "unix:///var/run/memdb/memdb.sock",
})
```

`Config.Logger` accepts a `*slog.Logger` for structured logging from the server, including panics recovered from connection handlers (logged at `ERROR` level).

`BasicAuth` uses constant-time comparison (`crypto/subtle.ConstantTimeCompare`) to prevent timing attacks.

### Protocol compatibility

The server implements the PostgreSQL Simple Query protocol. All DML and DDL
that application code needs works out of the box:

| Statement | Supported |
|---|---|
| `SELECT` / `WITH` / `EXPLAIN` | ✅ |
| `INSERT` / `UPDATE` / `DELETE` | ✅ |
| `CREATE TABLE` / `CREATE INDEX` / `DROP` | ✅ |
| `BEGIN` / `COMMIT` / `ROLLBACK` | ✅ via the underlying SQLite connection |
| `PRAGMA` | ✅ |

**Administrator schema-browsing commands are not supported.** When an
operator connects with `psql` and types `\d tablename`, psql internally
generates a sequence of `pg_catalog` queries that contain PostgreSQL-specific
syntax (`OPERATOR(pg_catalog.~)`, `::regtype::text` casts) that SQLite cannot
parse. These are issued by the `psql` client itself, not by application code,
and the server returns an error for them.

**For administrator introspection use the CLI instead:**

```bash
# List tables in a snapshot
memdb snapshot --file /var/lib/myapp/data.db   # ensure snapshot is current
dd if=/var/lib/myapp/data.db bs=1 skip=40 of=/tmp/inspect.db
sqlite3 /tmp/inspect.db ".tables"
sqlite3 /tmp/inspect.db ".schema users"

# Query live data while the server is running
psql -h 127.0.0.1 -p 5433 -U memdb -c "SELECT * FROM users LIMIT 10"
psql -h 127.0.0.1 -p 5433 -U memdb -c "SELECT name FROM sqlite_master WHERE type='table'"
```

`SELECT name FROM sqlite_master WHERE type='table'` is the portable
alternative to `\dt` and works correctly through the memdb server because it
is plain SQL, not a psql client command.

> **Note:** Extended Query protocol (prepared statements with `$1` placeholders,
> `Parse`/`Bind`/`Execute` message flow) is also not implemented. Applications
> that require extended protocol should use the library API directly rather
> than the wire-protocol server.

---

## Profiling (pprof)

memdb ships with a small `profiling` package that wraps `net/http/pprof` and
provides helpers for capturing CPU, heap, mutex, and block profiles from
inside tests or benchmarks.

### HTTP endpoint from the CLI

The `serve` sub-command exposes all standard pprof endpoints when started
with the `--pprof` flag:

```bash
memdb serve \
  --file /var/lib/myapp/data.db \
  --addr 127.0.0.1:5433 \
  --pprof 127.0.0.1:6060 \
  --pprof-mutex-fraction 100 \
  --pprof-block-rate 10000
```

Then point `go tool pprof` at the server:

```bash
go tool pprof -http=: http://127.0.0.1:6060/debug/pprof/heap
go tool pprof -http=: http://127.0.0.1:6060/debug/pprof/profile?seconds=30
go tool pprof -http=: http://127.0.0.1:6060/debug/pprof/mutex
go tool pprof -http=: http://127.0.0.1:6060/debug/pprof/block
```

The listener defaults to **loopback only** — pprof exposes full heap contents
and should never be bound to a public address without an explicit threat
model.

### Programmatic use

```go
import "github.com/voicetel/memdb/profiling"

srv, err := profiling.StartServer(profiling.Config{
    Addr:                 "127.0.0.1:6060",
    MutexProfileFraction: 100,   // sample ~1% of mutex contention
    BlockProfileRate:     10000, // 10 µs blocking threshold
})
if err != nil { log.Fatal(err) }
defer srv.Close()
```

`profiling.CaptureCPUProfile`, `profiling.CaptureHeapProfile`, and
`profiling.CaptureNamedProfile` write profiles to disk for the duration of a
single function call — intended for use in integration tests and long-running
benchmarks.

### Capture targets

A set of `make` targets runs representative workloads and writes profiles to
`./coverage/pprof/`:

```bash
make pprof              # run all pprof-capturing scenarios in package memdb
make pprof-writes       # single-writer INSERT workload
make pprof-reads        # concurrent read workload (replica pool)
make pprof-mixed        # mixed read/write workload (2 ms and default refresh)
make pprof-flush        # flush path at 50 000 rows (bare; compressed and
                        # encrypted variants run via the full pprof target)
make pprof-wal          # DurabilityWAL writes
make pprof-server       # PostgreSQL wire-protocol server scenarios
make pprof-raft         # Raft Apply on a 3-node cluster
make bench-pprof        # benchmarks with -cpuprofile / -memprofile / -mutex / -block
make pprof-view PROF=./coverage/pprof/pprof_writes.cpu.prof
```

The underlying tests live in `memdb_pprof_test.go`, `server/server_pprof_test.go`,
and `replication/raft/raft_pprof_test.go`, all gated behind the `MEMDB_PPROF=1`
environment variable so the default `go test ./...` run never writes profile
artefacts. Coverage includes Raft FSM Apply, writer-pool contention (mutex +
block), WAL cold-start replay, compressed and encrypted flush paths, TLS
handshake cost, and a Postgres-wire allocations diff.

---

## CLI

Build and install the CLI with `make install`, then:

```bash
# Start the PostgreSQL wire-protocol server
memdb serve --file /var/lib/myapp/data.db --addr 127.0.0.1:5433 --flush 30s

# Start the server with pprof on loopback (:6060)
memdb serve --file /var/lib/myapp/data.db --pprof 127.0.0.1:6060

# Force a snapshot flush to disk
memdb snapshot --file /var/lib/myapp/data.db

# Restore a snapshot file to a new location
memdb restore --from /var/lib/myapp/data.db --to /var/lib/myapp/data-copy.db
```

Each sub-command accepts `-h` for flag details:

```bash
memdb serve -h
memdb snapshot -h
memdb restore -h
```

### Inspecting snapshots with sqlite3

Snapshot files are not directly openable by the `sqlite3` CLI because they
start with a 40-byte `MDBK` integrity header rather than the SQLite magic
bytes. Strip the header first:

```bash
# Force a flush so the snapshot is up to date
memdb snapshot --file /var/lib/myapp/data.db

# Strip the MDBK header and open with sqlite3
dd if=/var/lib/myapp/data.db bs=1 skip=40 of=/tmp/inspect.db
sqlite3 /tmp/inspect.db ".tables"
sqlite3 /tmp/inspect.db "SELECT COUNT(*) FROM sessions"
```

For live data inspection without touching the snapshot file at all, use the
PostgreSQL wire protocol — it always reads the in-memory state:

```bash
# Requires the server to be running
psql -h 127.0.0.1 -p 5433 -U memdb -c "SELECT * FROM sessions LIMIT 10"
```

See the [Snapshot integrity](#snapshot-integrity) section for a full
explanation of the `MDBK` header format.

### Makefile reference

```bash
make help               # list all targets

# Build
make build              # memdb binary → ./build/memdb
make build-prod         # optimised production binary (stripped)
make build-all          # all platforms (linux/darwin × amd64/arm64)

# Test
make test               # run all tests
make test-race          # run all tests with race detector
make test-coverage      # generate HTML coverage report → ./coverage/coverage.html
make test-ci            # vet + lint + race + coverage threshold (CI pipeline)
make test-smoke         # quick open/write/flush/restore sanity check
make test-specific TEST=TestFlushAndRestore  # run one test

# Replication integrity & profiling
make test-replication-integrity  # 3-node end-to-end Raft convergence/failover
make test-replication-soak       # high-volume soak + 10k-entry snapshot
make test-profiling              # profiling package unit tests

# Benchmarks
make bench              # all benchmarks, 5 s each
make bench-compare      # memdb vs file SQLite comparison
make bench-concurrency  # concurrent reads at -cpu=1,4,8
make bench-pprof        # benchmarks with CPU/mem/mutex/block pprof capture

# Profiling — see the Profiling (pprof) section above for the full list
# of per-scenario capture targets (pprof-writes / pprof-reads /
# pprof-mixed / pprof-flush / pprof-wal) and the pprof-view entry point.
make pprof              # capture all pprof scenarios → ./coverage/pprof
make pprof-view PROF=<file>  # open a profile in the pprof web UI

# Code quality
make check              # fmt-check + vet + lint
make lint               # golangci-lint
make fmt                # gofmt all files
make security           # gosec scan

# Release
make release            # clean → check → test → build-all → tarballs
make tag VERSION_TAG=v1.2.0
```

---

## Compatibility

### Go

| Version | Status |
|---|---|
| 1.24 | Minimum (required by dependency graph) |
| 1.24+ | Fully supported |

### Operating Systems

| OS | Status |
|---|---|
| Linux (glibc) | ✅ |
| Linux (musl/Alpine) | ✅ — needs `gcc musl-dev` |
| macOS | ✅ |
| Windows | ✅ — needs MinGW-w64 |
| FreeBSD | ✅ |
| WASM / wasip1 | ❌ — CGo and `mattn/go-sqlite3` are not available on `wasip1` |

### Architectures

`amd64`, `arm64`, `arm/v7`, `386` — all fully supported. `riscv64`, `s390x`, `ppc64` build but have limited CI coverage.

### Cross-Compilation

CGo requires a cross C toolchain that matches the target OS/arch. The
recommended approach for CI is Docker with `tonistiigi/xx`, which installs
the correct cross toolchain automatically:

```dockerfile
FROM --platform=$BUILDPLATFORM golang:1.24 AS builder
COPY --from=tonistiigi/xx / /
ARG TARGETPLATFORM
RUN xx-apt install -y gcc libc6-dev
RUN CGO_ENABLED=1 xx-go build -o /app ./...
```

For local development across architectures, native builds on the target host
are simplest. `make build-all` already covers the four most common targets
(linux/amd64, linux/arm64, darwin/amd64, darwin/arm64) and expects the
matching C toolchain to be available on the build host.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                          Caller                               │
│                 Exec / Query / Begin / WithTx                 │
└───────┬──────────────────────────────┬────────────────────────┘
        │ writes (OnExec set)          │ reads (ReadPoolSize > 0)
        ▼                              ▼
  ┌───────────┐              ┌──────────────────────────────────┐
  │ OnExec()  │              │        replicaPool (×N)          │
  │ node.Exec │              │  channel-based exclusive checkout│
  └─────┬─────┘              │  sqlite3_serialize/deserialize   │
        │                    │  N independent :memory: DBs      │
        │ IsLeader?          │  refreshed every 50 ms (default) │
        │                    └──────────────────────────────────┘
        ├─ Yes ──────────────────────────────────────┐
        │                                            │
        │  No: dial leader ForwardAddr (TLS)         │
        │  → ForwardRequest{SQL, Args}               │
        │  ← ForwardResponse{Err}                    │
        │                                            │
        └────────────────────────────────────────────┤
                                                     ▼
                                           hraft.Apply (consensus)
                                                     │
                                    quorum of nodes commit
                                                     │
                                           FSM.Apply on ALL nodes
                                                     │
                                                     ▼
                                    ┌────────────────────────────┐
                                    │   Writer (×1) per node     │
                                    │   SQLite :memory:          │
                                    │   ExecDirect (no OnExec)   │
                                    └──────────┬─────────────────┘
                                               │
                           ┌───────────────────┤
                           │                   │
                    OnChange hook        DurabilityWAL path
                    (UPDATE/INSERT/      ┌─────────────┐
                     DELETE)            │  WAL Writer │ fsync/write
                                        └──────┬──────┘
                                               │
                                    Periodic snapshot (Backup API)
                                               │
                                        ┌──────▼──────┐
                                        │   Backend   │
                                        │ Local / Enc │
                                        │ / Compr /   │
                                        │ Custom      │
                                        └─────────────┘

┌─────────────────────┐   ┌────────────────────────────────────┐
│  server/            │   │  replication/raft/                 │
│  Postgres wire      │   │  Node — Raft consensus over TLS    │
│  Unix socket        │   │  BindAddr: Raft RPCs               │
└─────────────────────┘   │  ForwardAddr: write forwarding RPC │
                          └────────────────────────────────────┘
```

---

## Benchmarks

> **For the current v1.6.1 report see [BENCHMARKS.md](./BENCHMARKS.md).**
> It contains the full throughput table (core DB + Postgres wire server),
> the `ReplicaRefreshInterval` parameter sweep, the side-by-side vs
> file-SQLite comparison, and a section-by-section pprof analysis of
> where the remaining CPU is spent. Re-run the numbers locally with
> `make bench`, `make pprof`, and `make pprof-server`.

The section below is a short summary of the most frequently-cited
microbenchmarks for quick reference.

Benchmarks run on a 12th Gen Intel Core i7-1280P (20 threads), Linux, Go 1.24,
`github.com/mattn/go-sqlite3`. All file-SQLite numbers use WAL journal mode and
`cache_size=-64000` (64 MB), identical to memdb's in-memory configuration. Run
with `-benchtime=5s -benchmem`. Numbers below are from the v1.6.1 reference run
— see [BENCHMARKS.md](./BENCHMARKS.md) for the full table and the raw
`coverage/bench.txt` output.

### Single-goroutine throughput vs file SQLite

| Operation | memdb | memdb + WAL | file/sync=off | file/sync=full | vs file/off |
|---|---|---|---|---|---|
| INSERT | **2,359 ns** | 3,104 ns | 7,446 ns | 7,465 ns | **3.16× faster** |
| UPDATE | **1,327 ns** | — | 4,141 ns | 4,153 ns | **3.12× faster** |
| QueryRow | **2,017 ns** | — | 4,271 ns | 4,304 ns | **2.12× faster** |
| RangeScan (100 rows) | **35,865 ns** | — | 38,633 ns | 38,834 ns | **1.08× faster** |
| BatchInsert (50 rows/tx) | **164,146 ns** | — | 169,091 ns | 167,730 ns | **1.03× faster** |

memdb's advantage is proportional to how much time each operation spends in I/O vs pure SQL
work. Single-row writes spend most of their time flushing pages through the OS VFS stack —
memdb skips all of that, giving a **2.1–3.2× edge**. Point reads see a smaller gain because
both backends serve hot pages from RAM; the difference is the VFS abstraction cost alone.
For bulk operations and range scans the bottleneck shifts to SQLite's B-tree and cursor
machinery, which is identical for both backends, so the gap narrows to **~3–8%**.

`memdb + WAL` appends a binary-encoded entry and calls `fsync` after every `Exec`. The
WAL hot path uses a hand-rolled binary wire format (defined in package `replication`
as `EncodeEntry` / `DecodeEntry`, shared with the Raft FSM) in place of `encoding/gob`,
which previously accounted for ~25% of total CPU under `DurabilityWAL` in pprof traces.
A zero-alloc optimisation pre-reserves the 4-byte length prefix in the pool buffer,
eliminating a `make+copy` per write. `BenchmarkWAL_Append` measures **~560 ns/op,
0 B/op, 0 allocs/op** (down from 2,809 ns/op, 1,602 B/op, 20 allocs/op pre-binary-codec).
End-to-end WAL-durability insert throughput in `TestPProf_Writes_WAL` reaches 183k
writes/s. Even with the per-write `fsync` included, `DurabilityWAL` (3,104 ns/op) remains
**2.40× faster than `file/sync=off`** (7,446 ns/op) on INSERT — you get near-zero data
loss durability while still beating an unprotected file database.

The `synchronous` pragma barely matters on fast NVMe storage (`off` vs `full` is only 5%).
On spinning disk or network-attached storage the gap would be many milliseconds per commit,
making memdb's advantage even larger in practice.

### Concurrent read throughput (`-cpu=1,4,8`)

By default memdb pins all operations to a single connection. When `ReadPoolSize > 0`, `Query`
and `QueryRow` are served from a channel-based pool of N independent in-memory replica
databases, each refreshed via `sqlite3_serialize`/`sqlite3_deserialize` on a background
ticker (`ReplicaRefreshInterval`, default **50 ms** — see the pprof-derived guidance in the
`Config.ReplicaRefreshInterval` field doc; values below 5 ms emit a warning at Open time).
Each replica is checked out exclusively for the duration of a query — `refresh()` blocks
until all checked-out replicas are returned before calling `sqlite3_deserialize`,
eliminating the open-cursor race. Reads may be at most one interval stale; writes and
transactions always use the writer connection.

From `BenchmarkCompare_ConcurrentRead` at parallelism=4:

| Backend | ns/op | vs `memdb/pool` |
|---|---:|---:|
| **memdb + ReadPoolSize=4** | **1,783** | **1.00×** |
| file SQLite (sync=off, WAL) | 2,396 | 1.34× |
| file SQLite (sync=normal, WAL) | 2,463 | 1.38× |
| file SQLite (sync=full, WAL) | 2,456 | 1.38× |
| memdb (default, 1 connection) | 3,895 | 2.18× |

Without the pool, memdb serialises all readers through one connection and degrades under
concurrency while file SQLite fans out across independent connections. With
`ReadPoolSize=4` and the channel-based pool, memdb is **~34% faster than file SQLite at
4 goroutines** (1,783 ns vs 2,396 ns) and **2.18× faster than memdb without the pool**,
while retaining the full 3.16× write-speed advantage over file SQLite. At higher goroutine
counts, any goroutine that cannot immediately check out a replica falls back to the writer
connection; raise `ReadPoolSize` to match the goroutine count to recover the throughput.
See the [Tuning](#tuning) section for a closed-form recommender.

### Flush cost vs table size

| Rows | Flush latency | B/op | Allocs |
|---|---|---|---|
| 100 | ~118 µs | ~49 KB | 88 |
| 1,000 | ~283 µs | ~266 KB | 90 |
| 10,000 | ~1,455 µs | ~2.1 MB | 97 |

Starting in v1.5.0, every flush buffers the full SQLite snapshot in memory to compute its
SHA-256 integrity checksum before writing to the backend. Flush time and memory now scale
approximately linearly with database size — 100× more rows takes ~11.8× longer and ~43×
more memory. Allocations remain nearly flat because the snapshot is a single `bytes.Buffer`
growth, not per-row allocation. The default 30 s flush interval is appropriate for most
workloads; avoid flushing more frequently than every few seconds on a hot write path.

### WAL primitives

| Operation | Latency | Allocs |
|---|---|---|
| WAL append (binary encode + fsync) | ~560 ns | 0 |
| WAL replay — 100 entries | ~74 µs | 609 |
| WAL replay — 1,000 entries | ~736 µs | 6,012 |
| WAL replay — 10,000 entries | ~8,851 µs | 60,024 |

WAL append costs ~560 ns including `fsync` on a warm buffer (benchmarked on a 20-thread
x86_64 box; dominated by the `write(2)` + `fsync` syscall pair, not the encoder itself —
see the WAL on-disk format section below for the binary v1 layout), making
`DurabilityWAL` practical for most workloads. Replay is perfectly linear (10× entries
= ~10× time). A 10,000-entry WAL replays in ~9 ms; the prepared-statement cache on the
writer brings end-to-end replay throughput on `TestPProf_WAL_Replay` to ~99k entries/s
by reusing one `*sql.Stmt` per distinct SQL string instead of preparing on every entry.

## WAL on-disk format

The WAL file is a simple append-only sequence of length-prefixed records:

```
[4-byte big-endian length][record body]
```

Each record body is the binary v1 format defined in package `replication`: it
begins with the 4-byte magic `"MDBW"` followed by a 1-byte version tag, then
`uint64 seq`, `int64 timestamp`, a length-prefixed SQL string, and a
length-prefixed tagged argument list. The same codec is used by the Raft FSM
apply path so a single hand-rolled encoder serves both the WAL hot path and
inter-node replication. `encoding/gob` was measured at ~25% of total CPU on
the WAL hot path and ~31% on the FSM apply path before the swap.

Supported argument types: `nil`, all signed integers (widened to `int64` on
decode), all unsigned integers (widened to `uint64`), `float32`/`float64`
(widened to `float64`), `bool`, `string`, `[]byte`, and `time.Time` (stored
as `UnixNano` and decoded in UTC). Arguments of any other Go type are
rejected with `memdb.ErrWALUnsupportedArgType` (aliased to
`replication.ErrUnsupportedArgType`) so callers see the failure at `Exec`
time rather than losing the entry silently.

Records with a corrupt header, truncated body, or unknown binary version
tag cause `Replay` to stop at that record and return every prior valid
entry — matching the behaviour expected after an unclean shutdown.

### Snapshot integrity

Snapshots include a 40-byte integrity header prepended to the raw SQLite
bytes:

```
Offset  Length  Content
     0       4  "MDBK" magic
     4       4  version = 1 (uint32 big-endian)
     8      32  SHA-256 of the raw SQLite payload that follows
    40       ∞  Raw SQLite database bytes
```

On restore the checksum is verified; a mismatch returns `ErrSnapshotCorrupt`
so a corrupt or partially-written snapshot is never silently loaded. Legacy
snapshots written without the header are accepted with a warning and are
upgraded (header prepended) on the next flush.

**AEAD backends skip the header.** External backends that implement
`memdb.AuthenticatedBackend` with a `true` return advertise that they
already authenticate their payload (e.g. `backends.EncryptedBackend`'s
AES-256-GCM tag). For these the adapter writes the SQLite bytes directly
— the redundant SHA-256 pass would double the integrity work without
adding security. On restore, an absent `MDBK` header is treated as the
new headerless format (no warning) when the inner backend is
authenticated; otherwise it is treated as legacy as before.

#### Direct access with the sqlite3 CLI

Because snapshot files start with `MDBK` rather than the SQLite magic bytes
(`SQLite format 3\0`), the `sqlite3` command-line tool cannot open them
directly:

```bash
$ sqlite3 /var/lib/myapp/data.db ".tables"
Error: file is not a database
```

To inspect a snapshot with `sqlite3`, strip the 40-byte header first:

```bash
# Strip the MDBK header — the remainder is a valid SQLite 3 database.
dd if=/var/lib/myapp/data.db bs=1 skip=40 of=/tmp/inspect.db

sqlite3 /tmp/inspect.db ".tables"
sqlite3 /tmp/inspect.db "SELECT * FROM your_table LIMIT 10"
```

> **Note:** the stripped file is a read-only copy for inspection. Write
> changes to it and then try to load it via memdb — the missing integrity
> header means memdb will treat it as a legacy snapshot and accept it, but
> you lose checksum protection. For production inspection prefer connecting
> via the PostgreSQL wire protocol instead (`psql -h 127.0.0.1 -p 5433`),
> which always reads the live in-memory state rather than the on-disk
> snapshot.

The `memdb restore` subcommand can copy a snapshot to a new location while
preserving the header, or you can use `dd` to produce a header-free file
for archiving and third-party tooling.

### Lifecycle

| Operation | Latency |
|---|---|
| Open (fresh, no snapshot) | ~81 µs |
| Open + restore (1,000-row snapshot) | ~271 µs |

Open cost covers driver registration, pragma application via the connect hook, and
`InitSchema` execution. Restore adds a full SQLite backup-API round-trip (file → temp →
memory) plus SHA-256 verification of the snapshot payload on top — both scale linearly
with database size.

### Reproduce

```bash
# All benchmarks
go test -bench=. -benchtime=5s -benchmem .

# memdb vs file SQLite comparison only
go test -bench='^BenchmarkCompare' -benchtime=5s -benchmem .

# Concurrency curve (vary goroutine count)
go test -bench='^BenchmarkCompare_ConcurrentRead$' -benchtime=5s -cpu=1,4,8 .
```

---

## Comparison

|  | memdb | Raw SQLite `:memory:` | BoltDB | BadgerDB | Redis | rqlite |
|---|---|---|---|---|---|---|
| Full SQL | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ |
| In-process | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| In-memory primary | ✅ | ✅ | ❌ | Partial | ✅ | ❌ |
| WAL durability | ✅ | ❌ | ✅ | ✅ | ✅ AOF | ✅ |
| Atomic snapshots | ✅ | ❌ | ✅ | ✅ | ✅ RDB | ✅ |
| Pluggable backends | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Change notifications | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ |
| Replication | ✅ Raft | ❌ | ❌ | ❌ | ✅ | ✅ Raft |
| Postgres wire | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |

---

## Tuning

The two knobs most directly controlling concurrent-read scaling —
`Config.ReadPoolSize` and `Config.ReplicaRefreshInterval` — trade off read
throughput against memory cost, refresh CPU, and read staleness. The
`memdb/tuning` sub-package turns the v1.5 benchmark sweep (see
[BENCHMARKS.md](./BENCHMARKS.md)) into a closed-form recommender so you do
not have to rediscover the knee of the curve on your own hardware.

### Model

Given your workload characteristics:

```
N         = ReadPoolSize (replicas)
S         = database size in bytes
T         = refresh interval (seconds)
P         = GOMAXPROCS
M_max     = memory budget for replicas (bytes)
tau_max   = staleness tolerance (seconds)
f_write   = expected fraction of refresh ticks that do work
```

The recommender maximises `min(N, P)` subject to:

```
N × S                           ≤ M_max             (memory budget)
f_write × N × S / bw_deserial   ≤ 1 core            (refresh CPU)
T                               ∈ [5ms, 500ms]      (staleness band)
N                               ≤ P                 (diminishing returns)
```

Expected throughput follows `speedup ≈ α_read × min(N, P)` where
`α_read ≈ 0.65` on the reference run, matching the measured 2.74× at 4
replicas.

### Usage

```go
import (
    "time"
    "github.com/voicetel/memdb"
    "github.com/voicetel/memdb/tuning"
)

rec := tuning.Recommend(tuning.Workload{
    DatabaseSize:       50 << 20,           // 50 MiB in-memory DB
    MemoryBudget:       512 << 20,          // 512 MiB for the replica pool
    StalenessTolerance: 100 * time.Millisecond,
    Workload:           tuning.WorkloadReadHeavy,
})

log.Printf("memdb tuning: %s", rec.Rationale)

db, err := memdb.Open(memdb.Config{
    FilePath:               "/var/lib/app/data.db",
    ReadPoolSize:           rec.ReadPoolSize,
    ReplicaRefreshInterval: rec.ReplicaRefreshInterval,
    // ... other fields
})
```

The returned `Recommendation` includes:

| Field | Meaning |
|---|---|
| `ReadPoolSize` | drops into `Config.ReadPoolSize` |
| `ReplicaRefreshInterval` | drops into `Config.ReplicaRefreshInterval` |
| `EstimatedReplicaBytes` | memory the pool will consume (`N × DatabaseSize`) |
| `EstimatedRefreshCPU` | expected fraction of one core the refresh loop will burn |
| `EstimatedReadSpeedup` | multiplier vs the single-connection baseline |
| `Rationale` | one-line human-readable explanation of the binding constraint |

`Recommend` never fails. Invalid or zero inputs (e.g. `MemoryBudget <
DatabaseSize`, or `DatabaseSize == 0`) collapse to the safe
`ReadPoolSize=0` fallback with a `Rationale` explaining why — drop the
struct straight into `memdb.Config` and you get the single-connection
default without a broken pool configuration.

### Workload hints

| Hint | Meaning | Effect |
|---|---|---|
| `WorkloadReadHeavy` | reads dominate, writes infrequent | aggressive N, refresh fast-path common |
| `WorkloadBalanced` / `WorkloadUnknown` | roughly 50/50 | default recommendation |
| `WorkloadWriteHeavy` | writes dominate | N halved to leave CPU headroom — every tick does real work |

### Example output (read-heavy, 10 MiB DB, 200 MiB budget, 4 cores)

```
ReadPoolSize=4; ReplicaRefreshInterval=50ms; workload=read-heavy;
est-mem=40.0MiB; est-refresh-cpu=0.1%; est-read-speedup=2.60x
```

### When to recalibrate

The tuning constants (`BWDeserialize`, `AlphaRead`, floor/ceiling/default
intervals) are calibrated against the v1.4.0 reference run. If you re-run
`make bench` on different hardware and the headline numbers shift
materially, update the constants in `tuning/tuning.go` — every constant
has a doc comment citing its source so the audit trail stays clear. The
`TestRecommend_Calibration_*` tests guard against silent drift.

---

## Good Fit / Bad Fit

**Good fit:**
- Session and token stores
- Rate limiters and quota tracking
- Feature flag state
- Local analytics and aggregation scratch space
- Multi-service shared state on the same host (Unix socket)
- Test infrastructure replacing go-sqlmock or dockertest
- Read replicas for a primary relational database
- Any workload where writes are 2× faster than file SQLite matters

**Bad fit:**
- Datasets that do not fit in RAM
- Multi-process write-heavy workloads without the replication layer
- Serverless or ephemeral container environments without persistent volume mounts
- Workflows that require opening the database file directly with the `sqlite3`
  CLI or other SQLite tooling — snapshot files include a 40-byte `MDBK`
  integrity header that those tools do not understand (a `dd skip=40`
  workaround exists; see [Inspecting snapshots with sqlite3](#inspecting-snapshots-with-sqlite3))
- Administrator schema-browsing via `psql` backslash commands (`\d`, `\dt`,
  `\di`) — these generate `pg_catalog` queries with PostgreSQL-specific syntax
  that SQLite cannot execute; use `sqlite3` on the stripped snapshot or
  `SELECT name FROM sqlite_master` instead (see
  [Protocol compatibility](#protocol-compatibility))

---

## Project Structure

```
memdb/
├── memdb.go                # Public API: Open, Exec, Query, Flush, Close, ExecDirect
├── backend.go              # Backend interface + LocalBackend + WrapBackend adapter
├── backup.go               # SQLite Online Backup API; Serialize/Restore
├── replica.go              # Channel-based read replica pool
├── driver.go               # mattn/go-sqlite3 driver registration + raw conn access
├── driver_shared.go        # fnv32 hash used for driver-registration keys
├── config.go               # Config struct, defaults, validation
├── errors.go               # Sentinel errors
├── wal.go                  # Write-ahead log: binary v1 format, replay, truncate
├── memdb_test.go           # Unit and integration tests
├── memdb_extra_test.go     # Additional unit tests
├── memdb_bench_test.go     # Throughput benchmarks
├── memdb_compare_test.go   # memdb vs file SQLite comparison benchmarks
├── memdb_pprof_test.go     # pprof-capturing scenarios (MEMDB_PPROF=1 gated)
├── wal_binary_test.go      # Binary WAL format: round-trip, backwards-compat, corruption
├── logging/
│   ├── logging.go          # NewTextHandler, NewJSONHandler
│   ├── syslog.go           # NewSyslogHandler (Linux/macOS)
│   ├── syslog_stub.go      # NewSyslogHandler stub (Windows/Plan9)
│   └── hclog.go            # NewHCLogAdapter — hclog.Logger → *slog.Logger bridge
├── profiling/
│   ├── profiling.go        # net/http/pprof wrapper + Capture* helpers
│   └── profiling_test.go   # Unit tests for StartServer and capture helpers
├── tuning/
│   ├── tuning.go           # Closed-form Recommend() for ReadPoolSize + interval
│   ├── tuning_test.go      # Boundary, monotonicity, calibration-drift tests
│   └── example_test.go     # Runnable doc examples with golden output
├── server/
│   ├── server.go           # PostgreSQL wire protocol server (TLS, BasicAuth, Unix socket)
│   ├── handler.go          # Simple Query, DML dispatch, SSL negotiation, wire helpers
│   └── server_pprof_test.go # Server pprof-capture scenarios (MEMDB_PPROF=1 gated)
├── replication/
│   ├── replication.go      # WALEntry type (shared between packages)
│   ├── codec.go            # Binary v1 EncodeEntry/DecodeEntry; shared by WAL + Raft FSM
│   └── raft/
│       ├── raft.go         # FSM: Apply (binary v1 decode), Snapshot, Restore; Apply submitter
│       ├── node.go         # Node: NewNode, Exec, forward, AddVoter, Shutdown
│       ├── tls.go          # tlsStreamLayer — TLS StreamLayer for hashicorp/raft
│       ├── forwarder.go    # Write-forwarding RPC server (leader side, WaitGroup tracked)
│       ├── rpc.go          # ForwardRequest/Response wire protocol; channel-based ConnPool
│       ├── store.go        # Crash-safe fileLogStore + fileStableStore
│       ├── raft_pprof_test.go # Raft Apply pprof scenario (MEMDB_PPROF=1 gated)
│       └── replication_integrity_test.go # 3-node end-to-end convergence / failover
├── backends/
│   ├── local.go            # Atomic local file backend
│   ├── compressed.go       # zstd compression wrapper (fixed at SpeedFastest)
│   └── encrypted.go        # AES-256-GCM encryption wrapper (implements AuthenticatedBackend)
├── stmt_cache.go           # Prepared-statement cache (writer); multi-statement bypass
├── BENCHMARKS.md           # v1.6.1 benchmark report with pprof analysis
├── Makefile                # Build, test, lint, benchmark, profiling, release
└── cmd/
    └── memdb/              # CLI: serve [--pprof], snapshot, restore
```

---

## License

MIT — see [LICENSE](LICENSE).
