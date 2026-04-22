# memdb

A high-performance, embedded Go database library built on SQLite. All reads and writes operate against an in-memory SQLite instance. Persistence is handled via the SQLite Online Backup API, an optional write-ahead log (WAL), and pluggable storage backends.

Think Redis RDB+AOF semantics with full SQL query power — in a single Go import.

---

## Features

- **Full SQL** — joins, indexes, transactions, aggregates via SQLite
- **Sub-millisecond reads** — all queries hit memory, no disk I/O on the hot path
- **Configurable durability** — periodic snapshot only, WAL-backed near-zero loss, or fully synchronous
- **Atomic snapshots** — write-then-rename prevents corrupt state on crash
- **Pluggable backends** — local disk or any custom `Backend` implementation
- **Snapshot compression** — zstd compression built in, ~50-70% size reduction on typical schemas
- **Change notifications** — `OnChange` hook via SQLite update hook for cache invalidation and audit
- **Raft replication** — strong-consistency multi-node replication via `hashicorp/raft` over mutual TLS; any node accepts writes and transparently forwards to the leader
- **PostgreSQL wire protocol** — optional server mode accepts any Postgres client or ORM
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

	// Called after each completed flush with timing and size metrics.
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
	OnExec func(sql string, args []any) error
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

	// DurabilitySync — every write flushed to disk immediately.
	// Equivalent to a regular file-backed SQLite. Slowest.
	DurabilitySync
)
```

| Mode | Loss Window | Write Latency | Use Case |
|---|---|---|---|
| `DurabilityNone` | Up to `FlushInterval` | Lowest | Caches, rate limiters, ephemeral state |
| `DurabilityWAL` | Milliseconds | Low | Session stores, feature flags, analytics |
| `DurabilitySync` | None | Higher | Audit logs, financial state |

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
	flushBytes.Add(float64(m.BytesWritten))
	flushPageCount.Add(float64(m.PageCount))
	walEntriesReplayed.Add(float64(m.WALEntriesReplayed))
	if m.Error != nil {
		flushErrors.Inc()
	}
}
```

---

## Replication

memdb uses **Raft consensus** (`hashicorp/raft`) for multi-node replication. All
nodes are peers — there is no separate leader/follower configuration. Any node
accepts writes and transparently forwards them to the current leader over a
dedicated TLS RPC connection. Once the leader commits the entry through Raft,
every node's FSM applies it locally.

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

---

## Server Mode (PostgreSQL Wire Protocol)

Run memdb as a standalone server that any PostgreSQL client can connect to:

```go
import "github.com/voicetel/memdb/server"

srv := server.New(db, server.Config{
	ListenAddr: "127.0.0.1:5433",
	Protocol:   server.ProtocolPostgres,
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

## CLI

Build and install the CLI with `make install`, then:

```bash
# Start the PostgreSQL wire-protocol server
memdb serve --file /var/lib/myapp/data.db --addr 127.0.0.1:5433 --flush 30s

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

# Benchmarks
make bench              # all benchmarks, 5 s each
make bench-compare      # memdb vs file SQLite comparison
make bench-concurrency  # concurrent reads at -cpu=1,4,8

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
FROM --platform=$BUILDPLATFORM golang:1.22 AS builder
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
  ┌───────────┐              ┌─────────────────────────────────┐
  │ OnExec()  │              │       replicaPool (×N)          │
  │ node.Exec │              │  sqlite3_serialize/deserialize  │
  └─────┬─────┘              │  N independent :memory: DBs     │
        │                    │  refreshed every 1 ms (default) │
        │ IsLeader?          └─────────────────────────────────┘
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

Benchmarks run on a 12th Gen Intel Core i7-1280P, Linux, Go 1.25, `github.com/mattn/go-sqlite3`.
All file-SQLite numbers use WAL journal mode and `cache_size=-64000` (64 MB), identical to memdb's
in-memory configuration. Run with `-benchtime=5s -benchmem`.

### Single-goroutine throughput vs file SQLite

| Operation | memdb | memdb + WAL | file/sync=off | file/sync=full | vs file/off |
|---|---|---|---|---|---|
| INSERT | **4,178 ns** | 5,495 ns | 7,556 ns | 7,713 ns | **1.81× faster** |
| UPDATE | **3,033 ns** | — | 4,568 ns | 4,620 ns | **1.51× faster** |
| QueryRow | **4,086 ns** | — | 4,926 ns | 4,901 ns | **1.21× faster** |
| RangeScan (100 rows) | **44,006 ns** | — | 44,827 ns | 45,057 ns | **1.02× faster** |
| BatchInsert (50 rows/tx) | **181,413 ns** | — | 191,265 ns | 195,922 ns | **1.05× faster** |

memdb's advantage is proportional to how much time each operation spends in I/O vs pure SQL
work. Single-row writes spend most of their time flushing pages through the OS VFS stack —
memdb skips all of that, giving a **1.5–1.8× edge**. Point reads see a smaller **~1.2×**
gain because both backends serve hot pages from RAM; the difference is the VFS abstraction
cost alone. For bulk operations and range scans the bottleneck shifts to SQLite's B-tree and
cursor machinery, which is identical for both backends, so the gap narrows to **~2–5%**.

`memdb + WAL` appends a gob-encoded entry and calls `fsync` after every `Exec`, adding
~1.3 µs per write. Even so, it remains **1.37× faster** than `file/sync=off` on INSERT —
you get near-zero data loss durability while still beating an unprotected file database.

The `synchronous` pragma barely matters on fast NVMe storage (`off` vs `full` is only 2%).
On spinning disk or network-attached storage the gap would be many milliseconds per commit,
making memdb's advantage even larger in practice.

### Concurrent read throughput (`-cpu=1,4,8`)

By default memdb pins all operations to a single connection. When `ReadPoolSize > 0`, `Query`
and `QueryRow` are served from a pool of N independent in-memory replica databases, each
refreshed via `sqlite3_serialize`/`sqlite3_deserialize` on a background ticker
(`ReplicaRefreshInterval`, default 1 ms). Reads may be at most one interval stale;
writes and transactions always use the writer connection and are always consistent.

| | 1 goroutine | 4 goroutines | 8 goroutines |
|---|---|---|---|
| memdb (default, 1 connection) | 4,083 ns | 5,234 ns | 5,744 ns |
| **memdb + ReadPoolSize=4** | 4,302 ns | **3,080 ns** | **3,019 ns** |
| file SQLite (4 connections, WAL) | 5,133 ns | 2,358 ns | 2,668 ns |

Without the pool, memdb serialises all readers through one connection and degrades under
concurrency while file SQLite fans out across 4 independent connections. With
`ReadPoolSize=4`, memdb eliminates the bottleneck: at 4 goroutines it is **1.70× faster**
than the default single-connection mode and is within **~30%** of a 4-connection file
database — while retaining the full write speed advantage that file SQLite can never match.

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
| Replication | ✅ | ❌ | ❌ | ❌ | ✅ | ✅ Raft |
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

**Bad fit:**
- Financial transactions or audit logs requiring strict durability
- Datasets that do not fit in RAM
- Multi-process write-heavy workloads without the replication layer
- Serverless or ephemeral container environments without persistent volume mounts

---

## Project Structure

```
memdb/
├── memdb.go              # Public API: Open, Exec, Query, Flush, Close
├── backend.go            # Backend interface + LocalBackend
├── backup.go             # SQLite Online Backup API (cgo)
├── backup_purego.go      # Backup stubs for purego build
├── replica.go            # Read replica pool via serialize/deserialize (cgo)
├── replica_purego.go     # Replica stubs for purego build
├── driver.go             # mattn driver registration + raw conn access (cgo)
├── driver_purego.go      # modernc driver registration (purego)
├── config.go             # Config struct, defaults, validation
├── errors.go             # Sentinel errors
├── wal.go                # Write-ahead log: append, replay, truncate
├── memdb_test.go         # Unit and integration tests
├── memdb_bench_test.go   # Throughput benchmarks
├── memdb_compare_test.go # memdb vs file SQLite comparison benchmarks
├── server/
│   ├── server.go         # PostgreSQL wire protocol server
│   └── handler.go        # Simple Query, DML dispatch, wire helpers
├── replication/
│   ├── replication.go    # WALEntry type (shared between packages)
│   └── raft/
│       ├── raft.go       # FSM: Apply, Snapshot, Restore; gob type registry
│       ├── node.go       # Node: NewNode, Exec, forward, AddVoter, Shutdown
│       ├── tls.go        # tlsStreamLayer — TLS StreamLayer for hashicorp/raft
│       ├── forwarder.go  # Write-forwarding RPC server (leader side)
│       ├── rpc.go        # ForwardRequest/Response wire protocol (gob framed)
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

MIT — see [LICENSE](LICENSE).
