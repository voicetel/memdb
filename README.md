# memdb

A high-performance, embedded Go database library built on SQLite. All reads and writes operate against an in-memory SQLite instance. Persistence is handled via the SQLite Online Backup API, an optional write-ahead log (WAL), and pluggable storage backends.

Think Redis RDB+AOF semantics with full SQL query power — in a single Go import.

**2.26× faster writes than file SQLite. Concurrent reads match file SQLite with `ReadPoolSize > 0`.**

---

## Features

- **Full SQL** — joins, indexes, transactions, aggregates via SQLite
- **2.26× faster writes** — all writes hit memory; no VFS, no page-cache overhead
- **Sub-millisecond reads** — all queries hit memory, no disk I/O on the hot path
- **Concurrent reads** — channel-based replica pool (`ReadPoolSize`) matches file SQLite throughput at 4+ goroutines
- **Configurable durability** — periodic snapshot only, WAL-backed near-zero loss, or fully synchronous
- **Atomic snapshots** — write-then-rename prevents corrupt state on crash
- **Pluggable backends** — local disk or any custom `Backend` implementation
- **Snapshot compression** — zstd compression built in, ~50-70% size reduction on typical schemas
- **Change notifications** — `OnChange` hook via SQLite update hook for cache invalidation and audit
- **Raft replication** — strong-consistency multi-node replication via `hashicorp/raft` over mutual TLS; any node accepts writes and transparently forwards to the leader
- **Structured logging** — `log/slog` throughout with syslog, JSON, and text handlers; `hclog` bridge routes Raft internals through the same pipeline
- **PostgreSQL wire protocol** — optional server mode accepts any Postgres client or ORM; SSL negotiation, correct `ErrorResponse` severity field
- **Pure Go build tag** — swap `mattn/go-sqlite3` for `modernc.org/sqlite` with `-tags purego`
- **ORM compatible** — exposes `*sql.DB` for use with `sqlx`, `bun`, `ent`, `sqlc`, GORM, and others

---

## Installation

### As a library

```bash
go get github.com/voicetel/memdb
```

CGo is required by default (via `mattn/go-sqlite3`). A C compiler must be present:

```bash
# Linux
apt install gcc

# macOS
xcode-select --install
```

For a pure Go build with no CGo requirement:

```bash
go build -tags purego ./...
```

> **Note:** The `purego` tag uses `modernc.org/sqlite` which does not expose the native backup API.
> Snapshot flush and restore are unavailable in purego builds. All other functionality is identical;
> performance is approximately 10-20% lower.

### As a CLI binary (from source)

A `Makefile` is provided for common development and build tasks.

```bash
git clone https://github.com/voicetel/memdb
cd memdb

make build          # build ./build/memdb
make install        # install to /usr/local/bin
```

For a purego (no CGo) binary:

```bash
make build-purego   # build ./build/memdb-purego
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

	// Number of independent in-memory read replicas to maintain.
	// When > 0, Query and QueryRow are load-balanced across replicas,
	// allowing concurrent reads without contending on the writer connection.
	// Reads may be at most ReplicaRefreshInterval stale.
	// Writes always go to the single writer connection.
	// Recommended: 0 (default) or runtime.GOMAXPROCS(0).
	ReadPoolSize int

	// How often the background goroutine refreshes replicas from the writer
	// via sqlite3_serialize / sqlite3_deserialize.
	// Only used when ReadPoolSize > 0. Default: 1ms.
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

Uses zstd compression. Typical SQLite files compress 50–70% at default settings.

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
generates a fresh random nonce.

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
| `TestReplication_HighVolume_NoCorruption` | 500-entry soak with 6 argument types exercising every gob-registered scalar |
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
make pprof              # run all pprof-capturing scenarios
make pprof-writes       # single-writer INSERT workload
make pprof-reads        # concurrent read workload (replica pool)
make pprof-mixed        # mixed read/write workload
make pprof-flush        # flush path at 50 000 rows
make pprof-wal          # DurabilityWAL writes
make bench-pprof        # benchmarks with -cpuprofile / -memprofile / -mutex / -block
make pprof-view PROF=./coverage/pprof/pprof_writes.cpu.prof
```

The underlying tests live in `memdb_pprof_test.go` and are gated behind the
`MEMDB_PPROF=1` environment variable so the default `go test ./...` run never
writes profile artefacts.

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

### Makefile reference

```bash
make help               # list all targets

# Build
make build              # CGo binary → ./build/memdb
make build-purego       # purego binary → ./build/memdb-purego
make build-prod         # optimised production binary
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

# Profiling
make pprof              # capture all pprof scenarios → ./coverage/pprof
make pprof-writes       # single-writer INSERT workload
make pprof-reads        # concurrent replica-pool reads
make pprof-mixed        # mixed read/write workload
make pprof-flush        # flush path at 50k rows
make pprof-wal          # DurabilityWAL writes
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

| OS | CGo (default) | Pure Go (`-tags purego`) |
|---|---|---|
| Linux (glibc) | ✅ | ✅ |
| Linux (musl/Alpine) | ⚠️ Needs `gcc musl-dev` | ✅ |
| macOS | ✅ | ✅ |
| Windows | ⚠️ Needs MinGW-w64 | ✅ |
| FreeBSD | ✅ | ✅ |
| WASM / wasip1 | ❌ | ✅ |

### Architectures

`amd64`, `arm64`, `arm/v7`, `386` — all fully supported. `riscv64`, `s390x`, `ppc64` build but have limited CI coverage.

### Cross-Compilation

CGo makes cross-compilation require a cross C toolchain. The recommended approach for CI is Docker with `tonistiigi/xx`:

```dockerfile
FROM --platform=$BUILDPLATFORM golang:1.24 AS builder
COPY --from=tonistiigi/xx / /
ARG TARGETPLATFORM
RUN xx-apt install -y gcc libc6-dev
RUN CGO_ENABLED=1 xx-go build -o /app ./...
```

For cross-compilation without Docker, use the `-tags purego` build tag to eliminate the CGo dependency entirely.

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
        │ IsLeader?          │  refreshed every 1 ms (default)  │
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

Benchmarks run on a 12th Gen Intel Core i7-1280P, Linux, Go 1.24, `github.com/mattn/go-sqlite3`.
All file-SQLite numbers use WAL journal mode and `cache_size=-64000` (64 MB), identical to memdb's
in-memory configuration. Run with `-benchtime=5s -benchmem`.

### Single-goroutine throughput vs file SQLite

| Operation | memdb | memdb + WAL | file/sync=off | file/sync=full | vs file/off |
|---|---|---|---|---|---|
| INSERT | **3,627 ns** | 5,224 ns | 8,190 ns | 8,641 ns | **2.26× faster** |
| UPDATE | **3,206 ns** | — | 4,697 ns | 4,703 ns | **1.46× faster** |
| QueryRow | **4,029 ns** | — | 4,903 ns | 4,969 ns | **1.22× faster** |
| RangeScan (100 rows) | **42,590 ns** | — | 44,164 ns | 43,850 ns | **1.04× faster** |
| BatchInsert (50 rows/tx) | **186,175 ns** | — | 199,449 ns | 200,630 ns | **1.07× faster** |

memdb's advantage is proportional to how much time each operation spends in I/O vs pure SQL
work. Single-row writes spend most of their time flushing pages through the OS VFS stack —
memdb skips all of that, giving a **1.5–2.3× edge**. Point reads see a smaller **~1.2×**
gain because both backends serve hot pages from RAM; the difference is the VFS abstraction
cost alone. For bulk operations and range scans the bottleneck shifts to SQLite's B-tree and
cursor machinery, which is identical for both backends, so the gap narrows to **~2–7%**.

`memdb + WAL` appends a gob-encoded entry and calls `fsync` after every `Exec`, adding
~1.6 µs per write. Even so, it remains **1.57× faster** than `file/sync=off` on INSERT —
you get near-zero data loss durability while still beating an unprotected file database.

The `synchronous` pragma barely matters on fast NVMe storage (`off` vs `full` is only 5%).
On spinning disk or network-attached storage the gap would be many milliseconds per commit,
making memdb's advantage even larger in practice.

### Concurrent read throughput (`-cpu=1,4,8`)

By default memdb pins all operations to a single connection. When `ReadPoolSize > 0`, `Query`
and `QueryRow` are served from a channel-based pool of N independent in-memory replica
databases, each refreshed via `sqlite3_serialize`/`sqlite3_deserialize` on a background
ticker (`ReplicaRefreshInterval`, default 1 ms). Each replica is checked out exclusively for
the duration of a query — `refresh()` blocks until all checked-out replicas are returned
before calling `sqlite3_deserialize`, eliminating the open-cursor race. Reads may be at most
one interval stale; writes and transactions always use the writer connection.

| | 1 goroutine | 4 goroutines | 8 goroutines |
|---|---|---|---|
| memdb (default, 1 connection) | 4,759 ns | 5,958 ns | 6,073 ns |
| **memdb + ReadPoolSize=4** | 4,825 ns | **2,630 ns** | 3,603 ns |
| file SQLite (4 connections, WAL) | 5,193 ns | 2,592 ns | 3,038 ns |

Without the pool, memdb serialises all readers through one connection and degrades under
concurrency while file SQLite fans out across 4 independent connections. With
`ReadPoolSize=4` and the channel-based pool, memdb **matches file SQLite at 4 goroutines**
(2,630 ns vs 2,592 ns) — essentially tied — while retaining the full 2.26× write speed
advantage that file SQLite can never match. At 8 goroutines, goroutines that cannot
immediately check out a replica fall back to the writer connection; increasing `ReadPoolSize`
to match the goroutine count recovers the throughput.

### Flush cost vs table size

| Rows | Flush latency | Allocs |
|---|---|---|
| 100 | ~87 µs | 75 |
| 1,000 | ~121 µs | 75 |
| 10,000 | ~513 µs | 75 |

There is an ~87 µs fixed overhead per flush regardless of data volume (SQLite Online Backup
API initialisation). Flush time then scales sub-linearly with page count — 100× more rows
takes only ~6× longer. Allocations are flat because the `database/sql` connection machinery
dominates, not the data being flushed. The default 30 s flush interval is appropriate for
most workloads; avoid flushing more frequently than every few seconds on a hot write path.

### WAL primitives

| Operation | Latency | Allocs |
|---|---|---|
| WAL append (gob encode + fsync) | ~1,263 ns | 1 |
| WAL replay — 100 entries | ~125 µs | 1,288 |
| WAL replay — 1,000 entries | ~1,026 µs | 11,191 |
| WAL replay — 10,000 entries | ~11,290 µs | 110,199 |

WAL append costs ~1.3 µs including `fsync`, making `DurabilityWAL` practical for most
workloads. Replay is perfectly linear (10× entries = ~10× time). The 11 allocs/entry cost
is intrinsic to `gob` decoding `[]any` and is a one-time startup expense — even a 10,000
entry WAL replays in ~11 ms.

### Lifecycle

| Operation | Latency |
|---|---|
| Open (fresh, no snapshot) | ~63 µs |
| Open + restore (1,000-row snapshot) | ~137 µs |

Open cost covers driver registration, pragma application via the connect hook, and
`InitSchema` execution. Restore adds a full SQLite backup-API round-trip (file → temp →
memory) on top and scales linearly with database size.

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
| Pure Go option | ✅ | ✅ | ✅ | ✅ | N/A | ❌ |

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

---

## Project Structure

```
memdb/
├── memdb.go              # Public API: Open, Exec, Query, Flush, Close, ExecDirect
├── backend.go            # Backend interface + LocalBackend + WrapBackend adapter
├── backup.go             # SQLite Online Backup API; Serialize/Restore (cgo)
├── backup_purego.go      # Backup stubs for purego build
├── replica.go            # Channel-based read replica pool (cgo)
├── replica_purego.go     # Replica stubs for purego build
├── driver.go             # mattn driver registration + raw conn access (cgo)
├── driver_purego.go      # modernc driver registration (purego)
├── driver_shared.go      # fnv32 hash shared between cgo and purego builds
├── config.go             # Config struct, defaults, validation
├── errors.go             # Sentinel errors
├── wal.go                # Write-ahead log: append, replay, truncate
├── memdb_test.go         # Unit and integration tests
├── memdb_extra_test.go   # Additional unit tests
├── memdb_bench_test.go   # Throughput benchmarks
├── memdb_compare_test.go # memdb vs file SQLite comparison benchmarks
├── logging/
│   ├── logging.go        # NewTextHandler, NewJSONHandler
│   ├── syslog.go         # NewSyslogHandler (Linux/macOS)
│   ├── syslog_stub.go    # NewSyslogHandler stub (Windows/Plan9)
│   └── hclog.go          # NewHCLogAdapter — hclog.Logger → *slog.Logger bridge
├── server/
│   ├── server.go         # PostgreSQL wire protocol server (TLS, BasicAuth, Unix socket)
│   └── handler.go        # Simple Query, DML dispatch, SSL negotiation, wire helpers
├── replication/
│   ├── replication.go    # WALEntry type (shared between packages)
│   └── raft/
│       ├── raft.go       # FSM: Apply, Snapshot, Restore; gob type registry
│       ├── node.go       # Node: NewNode, Exec, forward, AddVoter, Shutdown
│       ├── tls.go        # tlsStreamLayer — TLS StreamLayer for hashicorp/raft
│       ├── forwarder.go  # Write-forwarding RPC server (leader side, WaitGroup tracked)
│       ├── rpc.go        # ForwardRequest/Response wire protocol; channel-based ConnPool
│       └── store.go      # Crash-safe fileLogStore + fileStableStore
├── backends/
│   ├── local.go          # Atomic local file backend
│   ├── compressed.go     # zstd compression wrapper
│   └── encrypted.go      # AES-256-GCM encryption wrapper
├── Makefile              # Build, test, lint, benchmark, and release targets
└── cmd/
    └── memdb/            # CLI: serve, snapshot, restore
```

---

## License

This project is licensed under the MIT License with the Commons Clause - see the [LICENSE](LICENSE) file for details.

The Commons Clause restricts selling the Software (including hosted or paid support services whose value derives substantially from memdb's functionality). All other MIT rights — use, modify, distribute, embed in your own products — are unaffected.
