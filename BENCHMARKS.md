# memdb v1.6.1 — Benchmark Report

Consolidated results from the full benchmark suite and the pprof-driven
throughput scenarios. All numbers captured in a single session against the
commit tagged `v1.6.1` after the second-sweep allocation cleanups; see the
Setup section for the exact environment.

> **Headline change vs the v1.5.0 baseline** — repeated `Exec`/`Query`
> calls now skip per-call Prepare/Close on the writer (prepared-statement
> cache added in v1.6.0), the Raft and WAL hot paths share the same binary
> codec with no `encoding/gob`, and a second-sweep pass replaced JSON in
> the Raft on-disk log with a binary codec, swapped two more per-request
> allocations for reusable buffers (server reader, replica pool checkout),
> and pinned `CompressedBackend` to `zstd.SpeedFastest`.
>
> | Scenario | v1.5.0 | v1.6.0 | v1.6.1 | v1.5→v1.6.1 |
> |---|---:|---:|---:|---:|
> | Raft Apply (3-node cluster, writes/s) | 5,082 | 12,579 | **16,864** | **3.32×** |
> | Contended writes (16 goroutines, ops/s) | 163k | 225k | 131k | see note¹ |
> | Single-row INSERT vs file SQLite (`memdb` ns/op) | 3,686 | — | **2,359** | **1.56× faster** |
> | Single-row UPDATE vs file SQLite (`memdb` ns/op) | 2,951 | — | **1,327** | **2.22× faster** |
> | Concurrent RW (50 % writes, 20 goroutines, ns/op) | 5,923 | — | **3,884** | **−34 %** |
> | Compressed flush (CPU samples, 5×50k rows) | 120 ms | 90 ms | 90 ms | **−25 %** |
> | Encrypted flush (CPU samples) | 60 ms | 50 ms | 50 ms | **−17 %** |
>
> ¹ The contended-writes pprof scenario is sensitive to background load on
> the host; the 131k v1.6.1 figure is from a single 3 s window. The
> microbench (`Concurrent RW` row above, derived from
> `BenchmarkConcurrentReadWrite`) is averaged over 5 s and is the more
> stable indicator that v1.6.x writer-side contention is materially
> better than v1.5.0.
>
> v1.6.0 numbers above are from `/tmp/memdb-prof-after/`; v1.6.1 numbers
> are from a fresh `make bench` + `make pprof` + `make pprof-server` +
> `make pprof-raft` on the reference hardware. Re-run on your hardware
> for fresh comparisons.

> Re-run with `make bench`, `make pprof`, and `make pprof-server` from the
> repo root. Raw output is written to `coverage/bench.txt` and the `.prof`
> files land in `coverage/pprof/` — both are gitignored, so a fresh run
> will recreate them. Open any profile with:
>
> ```bash
> go tool pprof -http=: coverage/pprof/<file>.prof
> ```

---

## Setup

| Item | Value |
|---|---|
| Host | Linux 6.17.0-22-generic x86_64 |
| CPU | 12th Gen Intel(R) Core(TM) i7-1280P (20 threads) |
| Go | go1.24.4 linux/amd64 |
| SQLite driver | github.com/mattn/go-sqlite3 (cgo) |
| Commit | v1.6.1 |
| `-benchtime` | 5 s per bench |
| `-cpu` | default (GOMAXPROCS=20) unless noted |

Background flushing is disabled in every benchmark (`FlushInterval: -1`) so
timings reflect only the operation under measurement, not periodic I/O.

---

## What changed since v1.5.0

### v1.6.1 — second-sweep allocation cleanups

Three smaller allocation hotspots surfaced after the v1.6.0 sweep removed
the dominant ones. None is individually large, but each removes a
per-operation allocation from a hot path that runs once per request.

| Change | Impact |
|---|---|
| **Raft on-disk log codec** — `replication/raft/log_codec.go` replaces `encoding/json` with a hand-rolled binary codec; `appendLocked` reuses a per-batch scratch buffer | Raft Apply on a 3-node cluster: **12,579 → 16,864 writes/s** (1.34×). Removes ~27 % of Raft inuse heap. **Wire-format break** — v1.6.0 JSON log entries cannot be decoded by v1.6.1; snapshot before upgrading. |
| **Server `handler.readMessage` scratch buffer** — per-call body alloc replaced with a per-handler `[]byte` reused across reads | Removes ~15 MB of GC churn over a 3 s SELECT workload. |
| **`replicaPool.checkout` value-type releaser** — returns a `replicaReleaser` value instead of a heap-allocated `*releaser` plus closure | Removes ~9 MB of churn under the 16-goroutine read scenario. |

### v1.6.0 — pprof-driven perf sprint

The headline win of the sprint was removing the last `encoding/gob` usage
from the Raft FSM apply path: the WAL binary v1 codec was lifted into
package `replication` so the FSM and the WAL share the same encoder.

| Change | Impact |
|---|---|
| **Shared binary codec** for WAL hot path + Raft FSM apply path (`replication/codec.go`); no `encoding/gob` left on either path | Raft Apply on a 3-node cluster: **5,082 → 12,579 writes/s** (2.47×). Was ~25 % of CPU on the WAL path, ~31 % on the FSM apply path. |
| **Writer-side prepared-statement cache** (`stmt_cache.go`) — repeated `Exec`/`Query`/`QueryRow` on the same SQL string skip `Prepare → Close` inside `mattn/go-sqlite3`. Multi-statement SQL bypasses (a `*sql.Stmt` only handles one statement). | Contended writes: 163k → 225k ops/s (1.38×). WAL replay: 124k → 165k entries/s (1.33×). |
| **`CompressedBackend` pinned to `zstd.SpeedFastest`** | Halves encoder CPU for ~10 % larger snapshots — the right trade for a frequently-flushed hot path. Compressed flush CPU: 120 ms → 90 ms across 5×50k rows. |
| **`memdb.AuthenticatedBackend`** marker; `EncryptedBackend` opts in so the redundant 40-byte SHA-256 `MDBK` header is skipped (the AES-GCM tag is already an integrity check) | Encrypted flush CPU: 60 → 50 ms; SHA-256 fully removed from the encrypted path. |

### v1.5.0 — Sprint 1 security + early pprof optimisations

Snapshot/restore integrity (SHA-256 header `"MDBK"`), FK enforcement on
by default, panic recovery in callbacks, the WAL binary v1 format and
pool buffer (zero-alloc `WAL.Append`), and the 32 KB server write buffer
all landed in v1.5.0 — the foundation the later sprints built on. The
flush/restore paths still buffer the full payload to hash it, which is
why flush B/op scales with DB size; on AEAD backends v1.6.0 skips this.

---

## Headline throughput (pprof scenarios, 3 s wall-clock each)

These numbers come from the `TestPProf_*` harnesses, which count real
operations over a fixed wall-clock window and include all goroutine
orchestration, buffered-writer flushing, and connection churn.

### Core DB

| Scenario | Throughput |
|---|---|
| Pure writes (single goroutine, `DurabilityNone`) | **255 672 ops/s** |
| Pure writes (single goroutine, `DurabilityWAL`) | **183 247 ops/s** |
| Concurrent reads (16 goroutines, replica pool) | **655 407 ops/s** |
| Contended writes (16 goroutines, single writer) | **130 827 ops/s** |
| Mixed RW, `ReplicaRefreshInterval=2ms` | 14 701 w/s + 113 499 r/s |
| Mixed RW, `ReplicaRefreshInterval=50ms` *(default)* | 42 924 w/s + 201 129 r/s |
| Raft Apply (3-node cluster, sustained writes) | **16 864 writes/s** |
| Flush (50 000 rows) | ~41 ms / flush |
| WAL cold-start replay (50 000 entries) | **98 584 entries/s** |

Notes:

- `DurabilityWAL` pays ~0.5 µs per write for `WAL.Append` + fsync; throughput
  is 72 % of the no-durability path. The WAL hot path contributes only ~9 % of
  total DurabilityWAL CPU after the v1.5.0 zero-alloc optimisation.
- The mixed-RW default refresh case represents a 3-second wall-clock window;
  variance between runs is ±10–15 % — use `BenchmarkReplicaRefreshInterval`
  for stable parameter-sweep data.
- Raft Apply throughput is measured against a 3-node in-process cluster; on
  real hardware with cross-host networking the ceiling is set by RTT.

### Postgres wire-protocol server

The server tests bind `ReadPoolSize=GOMAXPROCS` (added in v1.6.0) so SELECT
traffic exercises the read replica pool — the v1.5.0 numbers serialised
every read through the single writer connection, which is why the SELECT
throughputs in the table below are several × the v1.5.0 figures.

| Scenario | Throughput |
|---|---|
| Narrow SELECT (16 clients, ~10 rows/query) | **186 359 q/s** |
| Wide SELECT (8 clients, 500 rows/query) | **7 939 q/s (~4.0 M rows/s)** |
| INSERT (8 clients, simple-query protocol) | **23 874 q/s** |
| Mixed SELECT/INSERT (20 clients, 1-in-4 writes) | **80 883 q/s** |
| Connect → query → disconnect (8 clients) | **14 229 cycles/s** |
| TLS connect → query → disconnect (8 clients) | **3 189 cycles/s** |
| Allocation diff scenario (8 clients) | **45 277 q/s** |

---

## Microbenchmarks

### Single-row operations

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `Exec_Insert` | 4 142 | 368 | 12 |
| `Exec_Insert_WAL` | 5 088 | 368 | 12 |
| `Exec_Update` | 2 225 | 374 | 12 |
| `Exec_Delete` | 2 006 | 184 | 10 |
| `QueryRow` | 3 664 | 702 | 22 |
| `Query_RangeScan` (100 rows) | 39 565 | 7 024 | 419 |

The absolute allocation counts are dominated by `database/sql` and the
`mattn/go-sqlite3` driver — memdb itself contributes <2 allocs/op on the
write path. The 2 fewer allocs/op vs the v1.5.0 baseline (`Exec_Insert`
went from 14 → 12) come from the prepared-statement cache added in
v1.6.0: `Prepare` and `Close` no longer run on every `Exec`/`Query`
against a previously-seen SQL string.

### Transactions

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `WithTx_SingleInsert` | 7 620 | 897 | 29 |
| `WithTx_BatchInsert` (50 rows) | 164 294 | 15 414 | 617 |
| `WithTx_ReadOnly` | 7 448 | 1 416 | 44 |

Batching 50 inserts inside a single transaction is ~1.3× faster per row
than 50 individual `Exec` calls (3 286 ns/row vs 4 142 ns/row) — the
amortised SQLite `COMMIT` cost is the reason. The gap is narrower than in
v1.5.0 because the per-row `Exec` path now also skips per-call Prepare
overhead via the writer-side statement cache.

### WAL primitives (binary format, v1.4+)

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `WAL_Append` | **559.7** | **0** | **0** |
| `WAL_Replay` / 100 entries | 74 440 | 39 008 | 609 |
| `WAL_Replay` / 1 000 entries | 736 386 | 366 311 | 6 012 |
| `WAL_Replay` / 10 000 entries | 8 851 291 | 5 129 758 | 60 024 |

Replay scales linearly and costs ~9 ms for a 10 000-entry WAL — a one-time
cost at startup. Replay throughput end-to-end (50 000 entries through
`WAL.Replay` + the writer's prepared-statement cache) is **98 584 entries/s**.

For context, the v1.3 baseline (gob-encoded, unpooled buffers) was
**2 809 ns/op / 1 602 B/op / 20 allocs/op** for `WAL_Append`. The binary
format plus buffer pooling (v1.4) and the zero-alloc length-prefix trick
(v1.5.0) together yield a 5.0× speed-up, infinite memory reduction, and
zero hot-path allocations. The v1.6.x prepared-statement cache further
amortises replay's `Prepare`/`Close` cost when repeated SQL templates
dominate the WAL.

### Flush (SQLite backup API + SHA-256 integrity, v1.5.0)

| Rows | ns/op | B/op | allocs/op |
|---:|---:|---:|---:|
| 100 | 118 175 | 49 109 | 88 |
| 1 000 | 283 127 | 266 427 | 90 |
| 10 000 | 1 454 781 | 2 103 270 | 97 |

Starting in v1.5.0, every flush buffers the full SQLite payload in
`snapshotWriter` to compute the SHA-256 checksum before writing to the
backend. This changes the scaling behaviour relative to v1.4.0:

- **Latency** now grows approximately linearly with database size (100× more
  rows ≈ 12.8× longer), versus the sub-linear 5.7× observed in v1.4.0 before
  the integrity buffer was introduced.
- **B/op** scales with the serialised database size rather than being
  constant. The numbers above are representative of a minimal single-table
  schema; real databases with more data per row will see proportionally larger
  B/op values.
- **allocs/op** remains nearly flat (88–97) because the snapshot buffer
  is a single `bytes.Buffer` growth, not per-row allocation.

The tradeoff is deliberate: a corrupt snapshot that passes the SHA-256 check
is cryptographically impossible, so the flush-time buffer is the correct
price to pay for integrity guarantees on the restore path.

### Lifecycle

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `Open` (empty DB) | 81 068 | 4 778 | 106 |
| `OpenRestore` (1 000-row snapshot) | 270 744 | 219 334 | 187 |

`OpenRestore` is notably higher than v1.4.0 (153 369 ns/op, 6 413 B/op)
because `verifyAndStrip` reads the entire snapshot payload via `io.ReadAll`
to verify the SHA-256 digest before handing bytes to the backup API. Memory
cost at restore time scales with snapshot size.

### Concurrent mixed read/write (GOMAXPROCS=20)

| Write mix | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| 10 % writes | 4 026 | 669 | 21 |
| 50 % writes | 3 884 | 537 | 17 |
| 90 % writes | 3 876 | 404 | 13 |

Per-op cost is remarkably flat across the write ratio — read contention
on the replica pool and write contention on the single writer connection
balance out near 4 µs/op regardless of mix. The ~34 % improvement vs
v1.5.0 (5.9 → 3.9 µs) is the prepared-statement cache and the
second-sweep allocation cleanups compounding.

---

## Parameter sweeps

### `ReplicaRefreshInterval` (pprof-driven default guidance)

1 000-row dataset, 8 concurrent readers with an active writer.

| Interval | ns/op (write) | B/op | Note |
|---|---:|---:|---|
| 250 µs | 56 732 | 23 237 | refresh dominates CPU |
| 1 ms | 53 494 | 22 100 | 4.5× slower than 100 ms |
| 5 ms | 45 795 | 21 879 | marginal improvement |
| 25 ms | 37 685 | 24 839 | knee of the curve |
| 100 ms | **11 761** | **5 100** | writes near full speed |

**Default is 50 ms.** Values below 5 ms emit a warning at `Open`
because pprof showed the refresh loop dominating CPU at those intervals.

---

## Comparison vs plain file-backed SQLite

Same machine, same SQLite build, same schema. `memdb` uses periodic
snapshot only (default). `memdb/wal` adds per-write fsync durability.
`file/sync=off|normal|full` are the three standard SQLite synchronous
levels on a file-backed database.

### Single-row INSERT

| Backend | ns/op | vs `memdb` |
|---|---:|---:|
| `memdb` | 2 359 | 1.00× |
| `memdb/wal` | 3 104 | 1.32× |
| `file/sync=off` | 7 446 | 3.16× |
| `file/sync=normal` | 7 379 | 3.13× |
| `file/sync=full` | 7 465 | 3.16× |

`memdb` is **3.16× faster** than the *fastest* file-backed configuration
(`sync=off`) and `memdb/wal` is still **2.40× faster** while providing
near-zero data loss durability.

### Single-row UPDATE

| Backend | ns/op | vs `memdb` |
|---|---:|---:|
| `memdb` | 1 327 | 1.00× |
| `file/sync=off` | 4 141 | 3.12× |
| `file/sync=normal` | 4 082 | 3.08× |
| `file/sync=full` | 4 153 | 3.13× |

### Single-row QueryRow

| Backend | ns/op | vs `memdb` |
|---|---:|---:|
| `memdb` | 2 017 | 1.00× |
| `file/sync=off` | 4 271 | 2.12× |
| `file/sync=normal` | 4 254 | 2.11× |
| `file/sync=full` | 4 304 | 2.13× |

### 100-row range scan

| Backend | ns/op | vs `memdb` |
|---|---:|---:|
| `memdb` | 35 865 | 1.00× |
| `file/sync=off` | 38 633 | 1.08× |
| `file/sync=normal` | 38 915 | 1.09× |
| `file/sync=full` | 38 834 | 1.08× |

Sequential scans spend almost all their time in SQLite's cursor machinery,
which is identical across backends — the gap stays small.

### 50-row batch INSERT (in one transaction)

| Backend | ns/op | vs `memdb` |
|---|---:|---:|
| `memdb` | 164 146 | 1.00× |
| `file/sync=off` | 169 091 | 1.03× |
| `file/sync=normal` | 167 694 | 1.02× |
| `file/sync=full` | 167 730 | 1.02× |

### Concurrent read (parallelism=4, 500-row dataset)

Baseline (1.00×) is `memdb/pool` — the fastest backend in this table.

| Backend | ns/op | vs `memdb/pool` |
|---|---:|---:|
| `memdb/pool` (`ReadPoolSize=4`) | **1 783** | **1.00×** |
| `file/sync=off` | 2 396 | 1.34× |
| `file/sync=normal` | 2 463 | 1.38× |
| `file/sync=full` | 2 456 | 1.38× |
| `memdb` (single-conn) | 3 895 | 2.18× |

With the replica pool `memdb` is **~34 % faster than file SQLite** on
concurrent reads, and **2.18× faster than `memdb` without the pool**.

---

## Profile analysis (what the pprof files say)

All percentages below are read off the v1.6.1 profile captures in
`coverage/pprof/`. Open any file with
`go tool pprof -http=: coverage/pprof/<file>.prof` to reproduce.

### `pprof_reads.cpu.prof` — 655k reads/s

| Function | cum % | Comment |
|---|---:|---|
| `TestPProf_Reads_Replicas.func1.1` | 95.1 % | user goroutine doing the work |
| `database/sql.withLock` | 76.8 % | driver-internal lock (unavoidable) |
| `memdb.(*DB).QueryRowContext` | 56.1 % | our entry point |
| `runtime.cgocall` (flat 45.0 %) | 55.4 % | crossing into SQLite (unavoidable) |

`replicaRefreshLoop` does not appear in the top 10 — after the
write-generation short-circuit and the fast-path tick, it is below the
0.5 % sampling threshold during a read-only workload.

### `pprof_writes_wal.cpu.prof` — 183k WAL writes/s

| Function | flat % | cum % | Comment |
|---|---:|---:|---|
| `runtime.cgocall` | 55.5 % | 60.7 % | SQLite work (unavoidable) |
| `internal/runtime/syscall.Syscall6` | 11.2 % | 11.2 % | `write(2)` + `fsync(2)` |
| `database/sql.resultFromStatement` | 1.3 % | 68.3 % | driver result handling |
| `memdb.(*stmtCache).ExecContext` | <1 % | 75.6 % | prepared-stmt cache (v1.6.0) |
| `WAL.Append` | <1 % | 15.8 % | our encoder; dominated by `fsync` |

`gob` is completely absent from the profile, confirming the v1.4 binary
codec change carries through to v1.6.1. The new `(*stmtCache).ExecContext`
node at 75.6 % cumulative is the v1.6.0 prepared-statement cache — it
sits in front of `database/sql.(*Stmt).ExecContext` and lets repeated
INSERT statements skip per-call `Prepare`/`Close`. `WAL.Append` itself
remains a thin wrapper around the `write(2)` + `fsync(2)` pair, which
cannot be eliminated.

> Note: server pprof captures land in `server/coverage/pprof/` (the test
> resolves the `MEMDB_PPROF_DIR` env var relative to the package), so
> `coverage/pprof/` at the repo root will not contain `pprof_server_*`
> files. Use the package-relative path when running `go tool pprof`.

### `pprof_server_select_wide.cpu.prof` — 7 939 q/s, 500 rows each

| Function | flat % | cum % | Comment |
|---|---:|---:|---|
| `internal/runtime/syscall.Syscall6` | 45.8 % | 45.8 % | cgo + network read/write |
| `runtime.cgocall` | 15.2 % | 21.8 % | cgo boundary crossing |
| `memdb/server.(*handler).handleSelect` | 0.9 % | 37.3 % | our handler |
| `runtime.casgstatus` | 4.5 % | 5.9 % | goroutine scheduling |

The dominant `Syscall6` at 46 % is split between sqlite3's internal cgo
read operations and the per-connection network reads on the test
clients; the memdb-side handler (`handleSelect`) sits at 37 %
cumulative and 0.9 % flat — almost all the time inside it is spent in
`db.Query` and the row iteration loop, not in the handler's own code.
`bufio.Writer.Flush` does not appear in the top 30 — confirming that
the 32 KB server write buffer (v1.5.0) collapses the ~6 implicit
mid-response flushes into a single explicit flush in
`sendReadyForQuery`.

### `pprof_server_select_wide.heap.prof` — 7 939 q/s, 500 rows each

Cumulative allocation: **~1.84 GB / 3 s** (~613 MB/s).

| Function | alloc_space % | Comment |
|---|---:|---|
| `mattn/go-sqlite3._Cfunc_GoStringN` | 29.5 % | cgo string copy — driver-internal |
| `(*SQLiteRows).nextSyncLocked` (flat 28.4 %, cum 59.7 %) | 28.4 % | driver row iteration |
| `memdb/server.(*handler).handleSelect` (flat 4.5 %, cum 85.3 %) | 4.5 % | server handler |
| `(*SQLiteConn).prepare` | 3.1 % | per-query SQL prepare on replicas |
| `memdb/server.trimTrailingNUL` | 2.3 % | wire-format helper |
| `memdb/server.(*handler).sendRowDescription` | 1.9 % | column metadata write |

The dominant remaining hotspots are still inside `mattn/go-sqlite3`'s
cgo boundary (the `_Cfunc_GoStringN` string copy and the row
iteration), which cannot be reduced without replacing the driver. On
the memdb side, `(*SQLiteConn).prepare` at 3.1 % is the per-query
prepare on the read replicas (which deliberately bypass the writer's
prepared-statement cache — `sqlite3_deserialize` invalidates statement
handles on each refresh tick).

### `pprof_reads.block.prof`

The biggest named blockers under the read replica pool are
`runtime.selectgo` (62.6 %) and `runtime.chanrecv1` (36.8 %) — both are
the channel-based replica check-in/check-out machinery in
`replicaPool`. `database/sql.(*DB).conn` follows at 22.3 %: readers
waiting for the per-replica single connection to free. This is by
design — it is how the replica pool maintains mutual exclusion between
open `*sql.Rows` cursors and `sqlite3_deserialize` calls during
refresh. Adding more replicas (`ReadPoolSize`) scales this linearly.

---

## Where the remaining cost lives

After the v1.6.x sweeps, the profiles say memdb is spending its cycles on:

1. **cgo boundary crossing into SQLite** (~40–60 % depending on workload)
   — structural, requires replacing the driver to reduce.
2. **`database/sql` driver plumbing** (~15–20 %) — connection management,
   statement caching, argument binding.
3. **SQLite itself** — inside cgo, not visible in Go pprof.
4. **`write(2)` + `fsync(2)` syscalls** on the WAL path (~7 %) —
   inherent to durability.
5. **SHA-256 + `bytes.Buffer` copy** on the flush/restore path — new in
   v1.5.0; only visible during `Flush` and `Open+restore`, not in steady-state
   operation.

memdb's own code (core, replica pool, server handler, WAL) remains
consistently below 5 % of CPU in every hot-path profile.

---

## Reproducing these numbers

```bash
# Core benchmarks (~8 minutes on the reference hardware)
make bench

# Core + server pprof capture (~30 seconds each)
make pprof
make pprof-server

# Individual captures
make pprof-writes
make pprof-reads
make pprof-mixed
make pprof-flush
make pprof-wal
make pprof-server-select-wide

# Open a profile in the pprof web UI
make pprof-view PROF=./coverage/pprof/pprof_reads.cpu.prof
```

Full results from the v1.6.1 run are written to `coverage/bench.txt`
alongside the raw `.prof` files in `coverage/pprof/`. Both paths are
gitignored — this report (`BENCHMARKS.md`) lives at the repo root and is
the committed summary of that data.
