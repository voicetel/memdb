# memdb v1.10.0 — Benchmark Report

Consolidated results from the full benchmark suite and the pprof-driven
throughput scenarios. The headline throughput tables below were captured
in a single fresh session against the v1.10.0 tree (replica
prepared-statement cache + cache-policy fix — see the v1.10.0 section
under "What changed"); the microbenchmark tables further down are from
the v1.8.0 capture and remain representative for the paths v1.9.x/v1.10.0
did not touch. See the Setup section for the exact environment.

> **Headline change vs the v1.5.0 baseline** — repeated `Exec`/`Query`
> calls now skip per-call Prepare/Close on the writer (prepared-statement
> cache added in v1.6.0), the Raft and WAL hot paths share the same binary
> codec with no `encoding/gob`, a second-sweep pass replaced JSON in the
> Raft on-disk log with a binary codec, swapped two more per-request
> allocations for reusable buffers (server reader, replica pool checkout),
> pinned `CompressedBackend` to `zstd.SpeedFastest`, and v1.6.2 moved the
> snapshot SHA-256 from a header buffer to a streaming footer (B/op
> constant regardless of payload size; **−98 %** at 10k rows, **−39 %**
> wall time vs v1.6.1).
>
> | Scenario | v1.5.0 | v1.6.1 | v1.8.0 | v1.5→v1.8.0 |
> |---|---:|---:|---:|---:|
> | Raft Apply (3-node cluster, writes/s) | 5,082 | 16,864 | **18,203** | **3.58×** |
> | Contended writes (16 goroutines, ops/s) | 163k | 131k¹ | **224,784** | **1.38×** |
> | Single-row INSERT vs file SQLite (`memdb` ns/op) | 3,686 | 2,359 | **2,451** | **1.50× faster** |
> | Single-row UPDATE vs file SQLite (`memdb` ns/op) | 2,951 | 1,327 | **1,356** | **2.18× faster** |
> | Concurrent RW (50 % writes, 20 goroutines, ns/op) | 5,923 | 3,884 | **3,942** | **−33 %** |
> | Flush B/op (10k rows) | 2.05 MiB | 2.05 MiB | **36 KiB** | **−98 %** |
> | Flush wall time (10k rows) | 1.45 ms | 1.37 ms | **0.74 ms** | **−49 %** |
>
> ¹ The v1.6.1 contended-writes figure was from a single noisy 3 s window;
> the v1.8.0 number above is on the same hardware, fresh run, no
> background load. The microbench (`Concurrent RW` row above, derived from
> `BenchmarkConcurrentReadWrite`) is averaged over 5 s and is the more
> stable comparison.
>
> v1.8.0 numbers are from a fresh `make bench` + `make pprof` +
> `make pprof-server` + `make pprof-raft` on the reference hardware.
> v1.6.0 / v1.6.1 figures retained for the per-release narrative are from
> the historical captures referenced in the per-release sections below.
> Re-run on your hardware for fresh comparisons.

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
| Host | Linux 7.0.0-27-generic x86_64 (headline tables); 6.17.0-22 (v1.8.0 microbenches) |
| CPU | 12th Gen Intel(R) Core(TM) i7-1280P (20 threads) |
| Go | go1.26.0 linux/amd64 (headline tables); go1.24.4 (v1.8.0 microbenches) |
| SQLite driver | github.com/mattn/go-sqlite3 (cgo) |
| Commit | v1.10.0 working tree (headline tables); v1.8.0 (microbenches) |
| `-benchtime` | 5 s per bench |
| `-cpu` | default (GOMAXPROCS=20) unless noted |

Background flushing is disabled in every benchmark (`FlushInterval: -1`) so
timings reflect only the operation under measurement, not periodic I/O.

---

## What changed since v1.5.0

### v1.10.0 — replica statement cache + cache-policy fix (pprof round sweep)

A profile → fix → re-profile sweep over every pprof scenario. Two
hot-path changes landed; each was validated with an interleaved A/B run
(5 repetitions per tree per scenario, alternating baseline/current every
run to cancel thermal drift; mean ± stdev):

| Scenario | baseline | v1.10.0 | Δ |
|---|---|---|---|
| replica reads (16 goroutines) | 700 481 ±57k ops/s | 859 007 ±37k ops/s | **+22.6 %** |
| mixed RW, default refresh — reads | 533 167 ±40k /s | 750 618 ±95k /s | **+40.8 %** |
| pg-wire INSERT (literal DML, msilo-style) | 34 023 ±6.9k q/s | 52 162 ±4.1k q/s | **+53.3 %** |
| pg-wire SELECT (literal, replica-served) | 137 321 ±3.3k q/s | 134 729 ±2.1k q/s | −1.9 % (noise) |
| pure writes | 377 503 ±23k ops/s | 366 228 ±82k ops/s | −3.0 % (noise) |
| mixed RW — writes | 144 129 ±7k /s | 136 001 ±18k /s | −5.6 % (σ overlap — watch) |
| large-DB (100 MiB) mixed | — | — | −6…−7 % (σ overlap — watch) |

What landed:

- **Replica prepared-statement cache** (`replica.go`, `stmt_cache.go`).
  Replica reads previously paid a full `sqlite3_prepare` per call — the
  writer has had a stmtCache since v1.7 but `checkout()` handed back a
  bare `*sql.DB`. Round-1 profiles showed replica-read workloads at
  43–52 % `runtime.cgocall` with per-query prepare buried inside; the
  cache cut the reads-scenario cgo share to ~27 % flat. Refresh
  interaction is safe **without a per-tick flush**: an idle prepared
  statement survives `deserializeReadonlyShared` and SQLite lazily
  re-prepares it against the new snapshot (guarded by
  `TestReplicaPreparedStmtSurvivesRefresh`).
- **Zero-argument SQL bypasses the statement cache** (`stmt_cache.go`).
  Literal-inlined SQL (every pg-wire simple-query client: psql, OpenSIPS
  `db_postgres`) synthesises a distinct string per call — caching those
  never hits, held every statement forever (unbounded growth, present
  since v1.7 on the writer), and an intermediate epoch-flush design
  collapsed the wire-SELECT workload to a third of baseline through
  statement-finalize storms on the single connection. Zero-args
  passthrough removes the whole class and is itself the +53 % on
  literal INSERT. Parameterised statements (all library callers,
  extended-protocol wire clients) still cache; a refcount-safe epoch
  flush at 512 entries remains as a backstop for pathological
  parameterised-distinct-string callers.

Also in the sweep, three correctness/infra fixes the rounds surfaced:
`Server.Stop` accept-loop `WaitGroup` race (handler could start after
Stop returned), test `pickFreeAddr` duplicate-ephemeral-port collisions
(raft bootstrap flake), and a single-attempt `checkout()` flake in
`TestReplica_ReadOnlyEnforced`.

**Hot paths measured and deliberately NOT addressed** (see the pprof
round evidence in `coverage/pprof-round1/` for shapes):

1. **SQLite cgo floor** — 72–79 % of pure-write CPU is
   `runtime.cgocall` under `ctxDriverStmtExec`: the workload *is*
   SQLite. One exec crossing per statement is already the minimum.
   `database/sql` machinery adds ~8–10 % on top (retry wrapper, arg
   conversion, result plumbing); bypassing it with raw `driver.Stmt`
   handles could recover part of that but forfeits the `*sql.DB`
   compatibility surface — rejected at this effort/risk for ≤8 % upside.
2. **WAL fsync** — 15 % `Syscall6` in DurabilityWAL writes is the
   durability contract itself (`fdatasync` per write). Configurable via
   `Durability`, not optimisable away.
3. **Refresh copy on large DBs** — 28–29 % flat `runtime.memmove` on
   the 100 MiB scenario: one full-image serialize+copy per tick, linear
   in DB size. Structural in the v1.9 snapshot-refresh architecture.
   The unlock is session-extension v2.0 (row-level changesets, cost
   scales with write rate instead of DB size) — separate decision, not
   attempted here. The direct-cgo serialize shortcut remains a
   documented 40–90× regression; do not retry without new evidence.
4. **Network syscall floor (pg-wire)** — 32–57 % `Syscall6` across
   server scenarios is one read/write syscall pair per protocol round
   trip on loopback TCP, inflated in-profile by the in-process load
   clients sharing the sample space. Responses are already batched into
   a 32 KB buffered writer flushed once per ReadyForQuery.
5. **Raft consensus overhead** — 28 % syscalls + `selectgo`/scheduler
   churn inside hashicorp/raft (network, log fsync, channel plumbing):
   the price of consensus, tuned via raft batching knobs, not memdb
   code. SQLite work is only ~6 % of the raft+memdb pipeline.
6. **Saturation scheduler churn** — 8–12 % futex/procyield/`lock2` on
   fully-saturated mixed workloads: N goroutines contending for one
   writer connection and GOMAXPROCS replicas at 100 % CPU. A property
   of running at saturation, not a fixable code path.

### v1.9.3 — driver-registration race fix

No perf impact: `registerDriver` now registers the sqlite driver before
publishing its name, closing a startup race under concurrent `Open`.

### v1.9.2 — large-DB pprof characterisation + observability + lint cleanup

No hot-path perf changes (the v1.9.1/v1.9.2 headline numbers of the era
are preserved in the per-release notes; the tables above were re-captured
at v1.10.0). What landed:

- `DB.ReplicaRefreshDivergenceCount()` (`memdb.go`) — public counter
  bumped when `replicaPool.refresh` ends a tick with replicas pointing
  at different snapshots (first-refresh partial OR rollback failure).
  Operators should scrape it; in practice it advances only under SQLite
  OOM during `sqlite3_deserialize`.
- `TestPProf_LargeDB_MixedRW` (`memdb_pprof_test.go`) — new pprof
  scenario at ~100 MiB seeded data. Throughput **9 976 writes/s +
  80 393 reads/s** with the default 50 ms refresh interval; pprof
  attributes **62 % cumulative CPU** to `replicaPool.refresh`,
  **29 % flat** to `runtime.memmove`. Confirms that the per-tick
  full-image copy still scales with DB size — the v1.9.1
  shared-READONLY-buffer optimisation eliminated the N-replicas-cost
  but the 1-copy-per-tick remains linear in payload size. **Session-
  extension v2.0** (row-level changeset streaming) is the right next
  lever for large-DB workloads with continuous write activity.
- Two attempts to remove the C→Go→C round trip in refresh (skip
  `mattn.Serialize`'s Go-heap copy by calling `sqlite3_serialize`
  directly from a custom cgo helper) regressed throughput **40–90×**
  on the large-DB scenario. Writer-held time was identical between
  baseline and the direct path, yet the workload became
  single-threaded. Both attempts reverted; the proven `mattn.Serialize
  → allocSharedSnapshotBuffer` chain remains in place.
- `golangci-lint` errcheck across the entire codebase: **50 → 0**.
  Mechanical `_ = X.Close()` / `defer func() { _ = X.Close() }()`
  pattern applied uniformly. No behaviour change.

### v1.9.1 — shared read-only deserialize buffer for replicas

The replica refresh path previously called mattn's `Deserialize` on
each of N replicas, which `sqlite3_malloc64`s and `memcpy`s the full
serialized image **per replica per tick**. pprof showed
`runtime.memmove` at ~19 % of CPU on the
`TestPProf_MixedReadWrite_DefaultRefresh` workload — the
N + 2 full-image copies per refresh (one inside sqlite3_serialize,
one C→Go inside mattn.Serialize, and N Go→C inside mattn.Deserialize)
dominated.

`replicaPool.refresh` now allocates ONE C buffer per tick, copies the
serialized bytes into it once, and deserializes every replica to
point at the same buffer with `SQLITE_DESERIALIZE_READONLY` (no
`FREEONCLOSE`; the pool owns the buffer's lifetime). The cost is
`O(1)` memcpys per refresh regardless of replica count.

The `READONLY` flag also pins `PRAGMA query_only=ON` on every replica
automatically, so a stray direct-replica write attempt is rejected
with `SQLITE_READONLY` instead of silently mutating the in-memory
copy.

| Scenario | v1.8.0 | v1.9.1 | Change |
|---|---:|---:|---:|
| `runtime.memmove` % of CPU (mixed RW) | ~19 % | **2.17 %** | **−89 %** |
| Mixed RW (default refresh) writes/s | 56,438 | **170,717** | **3.03×** |
| Mixed RW (default refresh) reads/s | 273,878 | **596,058** | **2.18×** |
| Concurrent reads (16 goroutines) | 821,756 | **840,522** | within noise |

v1.9.1 captures are from `make pprof` on the same 12th Gen Intel Core
i7-1280P (20 threads) reference hardware as v1.8.0. Reads stayed
roughly the same because the read-only workload already uses the
write-generation short-circuit (refresh fast-path skips deserialize
when writeGen has not advanced); the optimisation only fires when
refresh actually runs, which it does continuously under a mixed RW
workload.

The shared-buffer wins compound with the v1.9.0 fix: writer +
replicas now both go through the cgo-shimmed `sqlite3_deserialize`
path, with the writer using `FREEONCLOSE | RESIZEABLE` and replicas
using `READONLY` against a pool-owned shared buffer. Both paths
share the unsafe-mirror access to mattn's unexported `*C.sqlite3`
handle (`sqliteConnMirror`, layout-guarded by
`TestSQLiteConnMirrorLayout`).

### v1.9.0 — SQLITE_FULL after Restore fix (no perf-impacting changes on the bench)

`DB.Restore` now uses `SQLITE_DESERIALIZE_RESIZEABLE` so the writer
can grow past the snapshot size. Adds `Config.RestoreMaxBytes` with
a host-aware default. No effect on the steady-state benchmarks above
— the change is on the post-restore write path, not the hot path.
See the v1.9.0 release notes for the bug diagnosis.

### v1.7.0 – v1.8.0 — feature releases (no perf-impacting changes)

These tags add user-visible features without touching the measured hot
paths. The v1.8.0 numbers in this report come from a fresh end-to-end
re-run on the reference hardware; per-release shifts vs v1.6.2 are
within run-to-run variance.

| Tag | What landed | Hot-path impact |
|---|---|---|
| v1.7.0 | Raft replication wired into `memdb serve` (`-raft-*` flags); TLS session reuse on the wire-protocol server; `-durability` flag on the CLI | None — Raft Apply uses the same binary v1 codec measured in v1.6.0/v1.6.1; TLS session reuse changes connect-cycle CPU, not steady-state query CPU. |
| v1.7.1 | SCRAM-SHA-256 auth (RFC 5802) via `NewScramAuth` and `-auth-method=scram` | Auth is one round per connection; not on the per-query path. The per-query benches still bind once and run for 5 s. |
| v1.7.2 | Postgres Extended Query Protocol (`Parse`/`Bind`/`Describe`/`Execute`/`Sync`) with server-side prepared statements and binary scalar formats; dual-protocol restapi example | Adds an opt-in code path; Simple Query (what the server benches measure) is unchanged. Extended-protocol clients get a separate amortised-Prepare path that has not yet been benchmarked here. |
| v1.7.3 | 3-node Raft cluster + PG wire example under `examples/cluster` | Docs/examples only. |
| v1.7.4 | Relocated stray `example/` directory to `examples/quickstart` | Docs/examples only. |
| v1.8.0 | `memdb-cli` wire mode (connect to a running `memdb serve` over the Postgres protocol) plus readline history and tab completion | CLI tooling, off the measured hot paths. |

### v1.6.2 — streaming snapshot writer

The flush path previously buffered the entire serialised SQLite payload in
a `bytes.Buffer` so SHA-256 could be computed before writing the integrity
header. For multi-MiB databases this allocated ~2× the payload size on
every flush interval — measured at 2.0 MiB B/op for a 10k-row snapshot
and growing linearly with dataset size, dwarfing all other allocations
on the flush path.

Moved the integrity wrap from header to footer
(`[magic][version][...payload...][payloadLen][sha256]`). The writer
streams payload bytes straight to dst through an incremental
`sha256.New()` (O(1) memory). The reader uses a 40-byte sliding window
via `bufio.Reader.Peek(64KiB)`: every iteration emits everything except
the trailing 40 bytes; the final `Peek` (returning `io.EOF`) holds the
footer in its tail. Restore stays O(1) memory too.

| Scenario | v1.6.1 | v1.6.2 | Change |
|---|---:|---:|---:|
| `BenchmarkFlush/rows=100` time | 113.7 µs | 106.1 µs | **−6.7 %** |
| `BenchmarkFlush/rows=1000` time | 239.2 µs | 178.4 µs | **−25.4 %** |
| `BenchmarkFlush/rows=10000` time | 1369.2 µs | 829.9 µs | **−39.4 %** |
| `BenchmarkFlush/rows=100` B/op | 47.9 KiB | 36.1 KiB | **−24.7 %** |
| `BenchmarkFlush/rows=1000` B/op | 260.0 KiB | 36.1 KiB | **−86.1 %** |
| `BenchmarkFlush/rows=10000` B/op | 2053.0 KiB | 36.1 KiB | **−98.2 %** |

Flush B/op is now **constant** regardless of payload size — the headline
property of the streaming wrap. The wall-time win grows with dataset
size because removing the buffer-and-hash pass also eliminates a
linear-in-N allocation, copy, and GC sweep per flush. **Wire-format
break** — v1.6.1 snapshots cannot be read by v1.6.2; flush before
upgrade is not preserved across this version.

A WAL group-commit prototype was also evaluated this cycle and reverted:
on consumer NVMe (~540 ns fsync) the cond-variable bookkeeping costs
more than the fsyncs it saves (`BenchmarkWAL_Append_Parallel-8` regressed
+25 %). It would still be a win on cloud SSDs and spinning disks, but
without runtime detection of fsync latency the safer default is to keep
the simple `Write+Sync`-under-mu path. `BenchmarkWAL_Append_Parallel`
was retained as a regression guard for any future WAL work.

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

| Scenario | Throughput (v1.10.0) |
|---|---|
| Pure writes (single goroutine, `DurabilityNone`) | **474 833 ops/s** |
| Pure writes (single goroutine, `DurabilityWAL`) | **364 499 ops/s** |
| Concurrent reads (16 goroutines, replica pool) | **1 106 073 ops/s** |
| Contended writes (16 goroutines, single writer) | **244 175 ops/s** |
| Mixed RW, `ReplicaRefreshInterval=2ms` | 29 008 w/s + 275 994 r/s |
| Mixed RW, `ReplicaRefreshInterval=50ms` *(default)* | 165 299 w/s + 898 883 r/s |
| Large-DB mixed RW (100 MiB seed, 20 replicas, default refresh) | 12 198 w/s + 92 324 r/s |
| Raft Apply (3-node cluster, sustained writes) | **18 510 writes/s** |
| Raft+memdb Apply (3-node, real `*memdb.DB` FSMs) | **13 993 writes/s** |
| Flush (50 000 rows) | ~4 ms / flush (compressed ~10 ms, encrypted ~5 ms) |
| WAL cold-start replay (50 000 entries) | **330 791 entries/s** |

Notes:

- `DurabilityWAL` pays ~0.5 µs per write for `WAL.Append` + fsync; throughput
  is ~77 % of the no-durability path. The WAL hot path contributes only ~10 % of
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

| Scenario | Throughput (v1.10.0) |
|---|---|
| Narrow SELECT (16 clients, ~10 rows/query, simple query) | **154 611 q/s** |
| Narrow SELECT (16 clients, Extended Query Protocol) | **164 378 q/s** |
| Wide SELECT (8 clients, 500 rows/query) | **9 574 q/s (~4.8 M rows/s)** |
| INSERT (8 clients, simple-query protocol) | **55 718 q/s** |
| Mixed SELECT/INSERT (20 clients, 1-in-4 writes) | **173 610 q/s** |
| Connect → query → disconnect (8 clients) | **15 210 cycles/s** |
| TLS connect → query → disconnect (8 clients) | **3 776 cycles/s** |
| Allocation diff scenario (8 clients) | **52 906 q/s** |

As of v1.10.0 the Extended Query Protocol edges out Simple Query on the
narrow-SELECT scenario: extended-path parameters are bound (so the
replica statement cache hits), while the simple-query workload inlines
its values and deliberately bypasses the cache. The simple-query INSERT
figure is up ~43 % on the same policy change (no more prepare +
cache-forever on literal DML).

---

## Microbenchmarks

### Single-row operations

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `Exec_Insert` | 4 616 | 368 | 12 |
| `Exec_Insert_WAL` | 5 494 | 368 | 12 |
| `Exec_Update` | 2 414 | 374 | 12 |
| `Exec_Delete` | 2 086 | 184 | 10 |
| `QueryRow` | 3 704 | 702 | 22 |
| `Query_RangeScan` (100 rows) | 38 306 | 7 024 | 419 |

The absolute allocation counts are dominated by `database/sql` and the
`mattn/go-sqlite3` driver — memdb itself contributes <2 allocs/op on the
write path. The 2 fewer allocs/op vs the v1.5.0 baseline (`Exec_Insert`
went from 14 → 12) come from the prepared-statement cache added in
v1.6.0: `Prepare` and `Close` no longer run on every `Exec`/`Query`
against a previously-seen SQL string.

### Transactions

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `WithTx_SingleInsert` | 7 770 | 896 | 29 |
| `WithTx_BatchInsert` (50 rows) | 167 962 | 15 414 | 617 |
| `WithTx_ReadOnly` | 7 621 | 1 416 | 44 |

Batching 50 inserts inside a single transaction is ~1.4× faster per row
than 50 individual `Exec` calls (3 359 ns/row vs 4 616 ns/row) — the
amortised SQLite `COMMIT` cost is the reason. The gap is narrower than in
v1.5.0 because the per-row `Exec` path now also skips per-call Prepare
overhead via the writer-side statement cache.

### WAL primitives (binary format, v1.4+)

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `WAL_Append` | **568.4** | **0** | **0** |
| `WAL_Append_Parallel` (-cpu=20) | 691.9 | 0 | 0 |
| `WAL_Replay` / 100 entries | 82 774 | 39 008 | 609 |
| `WAL_Replay` / 1 000 entries | 751 373 | 366 311 | 6 012 |
| `WAL_Replay` / 10 000 entries | 8 911 571 | 5 129 748 | 60 024 |

Replay scales linearly and costs ~9 ms for a 10 000-entry WAL — a one-time
cost at startup. Replay throughput end-to-end (50 000 entries through
`WAL.Replay` + the writer's prepared-statement cache) is **163 622 entries/s**.

For context, the v1.3 baseline (gob-encoded, unpooled buffers) was
**2 809 ns/op / 1 602 B/op / 20 allocs/op** for `WAL_Append`. The binary
format plus buffer pooling (v1.4) and the zero-alloc length-prefix trick
(v1.5.0) together yield a 5.0× speed-up, infinite memory reduction, and
zero hot-path allocations. The v1.6.x prepared-statement cache further
amortises replay's `Prepare`/`Close` cost when repeated SQL templates
dominate the WAL.

### Flush (SQLite backup API + SHA-256 integrity, streaming v1.6.2+)

| Rows | ns/op | B/op | allocs/op |
|---:|---:|---:|---:|
| 100 | 107 758 | 36 985 | 90 |
| 1 000 | 171 924 | 36 999 | 90 |
| 10 000 | 740 736 | 37 000 | 90 |

Since v1.6.2, the writer streams the SQLite payload through an incremental
`sha256.New()` and `io.MultiWriter` directly to the backend, with the
integrity check moved from a 40-byte header to a `[payloadLen][sha256]`
footer. Three useful properties fall out of this:

- **B/op is constant** at ~37 KiB regardless of payload size — there is
  no longer a per-flush buffer that scales with the serialised database.
  At 10k rows this is a **−98 %** B/op reduction vs the v1.6.1 buffered
  path.
- **Wall time scales sub-linearly** in the small range (100 → 10 000 rows
  is 100× the data but only ~6.9× the time) because the per-flush
  fixed cost dominates at low row counts.
- **allocs/op stays flat at 90** — the streaming writer's small set of
  scratch slices accounts for the entire allocation count, independent
  of payload size.

Restore is also O(1) memory: the reader uses a 40-byte sliding window via
`bufio.Reader.Peek(64 KiB)` and verifies the footer at EOF. AEAD backends
(`backends.EncryptedBackend`) skip the wrap entirely — the AES-GCM tag is
already an integrity check.

### Lifecycle

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `Open` (empty DB) | 81 877 | 4 778 | 106 |
| `OpenRestore` (1 000-row snapshot) | 204 001 | 73 363 | 172 |

`OpenRestore` is **24 % faster** and **66 % leaner** than v1.6.1 (270 744
ns/op, 219 334 B/op) thanks to the v1.6.2 streaming reader: the 40-byte
sliding-window verify avoids the prior `io.ReadAll` of the full snapshot
payload before handing bytes to the backup API. Memory cost at restore
time is now dominated by the SQLite backup buffers rather than by the
integrity check.

### Concurrent mixed read/write (GOMAXPROCS=20)

| Write mix | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| 10 % writes | 3 881 | 669 | 21 |
| 50 % writes | 3 942 | 537 | 17 |
| 90 % writes | 3 987 | 404 | 13 |

Per-op cost is remarkably flat across the write ratio — read contention
on the replica pool and write contention on the single writer connection
balance out near 4 µs/op regardless of mix. The ~33 % improvement vs
v1.5.0 (5.9 → 3.9 µs) is the prepared-statement cache and the
second-sweep allocation cleanups compounding; v1.7.x/v1.8.0 are
unchanged here.

---

## Parameter sweeps

### `ReplicaRefreshInterval` (pprof-driven default guidance)

1 000-row dataset, 8 concurrent readers with an active writer.

| Interval | ns/op (write) | B/op | Note |
|---|---:|---:|---|
| 250 µs | 57 595 | 24 224 | refresh dominates CPU |
| 1 ms | 52 881 | 22 791 | 4.3× slower than 100 ms |
| 5 ms | 49 328 | 24 629 | marginal improvement |
| 25 ms | 38 043 | 26 229 | knee of the curve |
| 100 ms | **12 302** | **5 247** | writes near full speed |

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
| `memdb` | 2 451 | 1.00× |
| `memdb/wal` | 3 144 | 1.28× |
| `file/sync=off` | 7 338 | 2.99× |
| `file/sync=normal` | 7 416 | 3.03× |
| `file/sync=full` | 7 480 | 3.05× |

`memdb` is **2.99× faster** than the *fastest* file-backed configuration
(`sync=off`) and `memdb/wal` is still **2.33× faster** while providing
near-zero data loss durability.

### Single-row UPDATE

| Backend | ns/op | vs `memdb` |
|---|---:|---:|
| `memdb` | 1 356 | 1.00× |
| `file/sync=off` | 4 119 | 3.04× |
| `file/sync=normal` | 4 133 | 3.05× |
| `file/sync=full` | 4 153 | 3.06× |

### Single-row QueryRow

| Backend | ns/op | vs `memdb` |
|---|---:|---:|
| `memdb` | 2 078 | 1.00× |
| `file/sync=off` | 4 357 | 2.10× |
| `file/sync=normal` | 4 312 | 2.07× |
| `file/sync=full` | 4 395 | 2.11× |

### 100-row range scan

| Backend | ns/op | vs `memdb` |
|---|---:|---:|
| `memdb` | 34 548 | 1.00× |
| `file/sync=off` | 39 114 | 1.13× |
| `file/sync=normal` | 38 917 | 1.13× |
| `file/sync=full` | 38 179 | 1.11× |

Sequential scans spend almost all their time in SQLite's cursor machinery,
which is identical across backends — the gap stays small.

### 50-row batch INSERT (in one transaction)

| Backend | ns/op | vs `memdb` |
|---|---:|---:|
| `memdb` | 164 347 | 1.00× |
| `file/sync=off` | 172 379 | 1.05× |
| `file/sync=normal` | 169 288 | 1.03× |
| `file/sync=full` | 171 021 | 1.04× |

### Concurrent read (parallelism=4, 500-row dataset)

Baseline (1.00×) is `memdb/pool` — the fastest backend in this table.

| Backend | ns/op | vs `memdb/pool` |
|---|---:|---:|
| `memdb/pool` (`ReadPoolSize=4`) | **1 821** | **1.00×** |
| `file/sync=off` | 2 488 | 1.37× |
| `file/sync=normal` | 2 468 | 1.36× |
| `file/sync=full` | 2 511 | 1.38× |
| `memdb` (single-conn) | 3 887 | 2.13× |

With the replica pool `memdb` is **~37 % faster than file SQLite** on
concurrent reads, and **2.13× faster than `memdb` without the pool**.

---

## Profile analysis (what the pprof files say)

All percentages below are read off the v1.8.0 profile captures in
`coverage/pprof/`. Open any file with
`go tool pprof -http=: coverage/pprof/<file>.prof` to reproduce.

### `pprof_reads.cpu.prof` — 822k reads/s

| Function | cum % | Comment |
|---|---:|---|
| `TestPProf_Reads_Replicas.func1.1` | 93.2 % | user goroutine doing the work |
| `database/sql.withLock` | 69.9 % | driver-internal lock (unavoidable) |
| `memdb.(*DB).QueryRowContext` | 56.6 % | our entry point |
| `runtime.cgocall` (flat 40.1 %) | 49.8 % | crossing into SQLite (unavoidable) |

`replicaRefreshLoop` does not appear in the top 10 — after the
write-generation short-circuit and the fast-path tick, it is below the
0.5 % sampling threshold during a read-only workload.

### `pprof_writes_wal.cpu.prof` — 298k WAL writes/s

| Function | flat % | cum % | Comment |
|---|---:|---:|---|
| `runtime.cgocall` | 54.3 % | 59.2 % | SQLite work (unavoidable) |
| `internal/runtime/syscall.Syscall6` | 9.9 % | 9.9 % | `write(2)` + `fsync(2)` |
| `memdb.(*stmtCache).ExecContext` | 0 % | 72.0 % | prepared-stmt cache (v1.6.0) |
| `database/sql.resultFromStatement` | 0.3 % | 64.8 % | driver result handling |
| `memdb.(*WAL).Append` | 1.3 % | 19.7 % | our encoder; dominated by `fsync` |

`gob` is completely absent from the profile, confirming the v1.4 binary
codec change carries through to v1.8.0. `(*stmtCache).ExecContext` at
72.0 % cumulative is the v1.6.0 prepared-statement cache — it sits in
front of `database/sql.(*Stmt).ExecContext` and lets repeated INSERT
statements skip per-call `Prepare`/`Close`. `WAL.Append` itself remains
a thin wrapper around the `write(2)` + `fsync(2)` pair, which cannot be
eliminated.

> Note: server pprof captures land in `server/coverage/pprof/` (the test
> resolves the `MEMDB_PPROF_DIR` env var relative to the package), so
> `coverage/pprof/` at the repo root will not contain `pprof_server_*`
> files. Use the package-relative path when running `go tool pprof`.

### `pprof_server_select_wide.cpu.prof` — 9 390 q/s, 500 rows each

| Function | flat % | cum % | Comment |
|---|---:|---:|---|
| `internal/runtime/syscall.Syscall6` | 46.7 % | 46.7 % | cgo + network read/write |
| `runtime.cgocall` | 12.8 % | 19.3 % | cgo boundary crossing |
| `(*SQLiteRows).Next` | 1.0 % | 24.1 % | driver row iteration |
| `memdb/server.(*handler).sendDataRowInto` | 1.1 % | 2.3 % | per-row wire encode |

The dominant `Syscall6` at 47 % is split between sqlite3's internal cgo
read operations and the per-connection network reads on the test
clients; the memdb-side `sendDataRowInto` sits at ~2 % cumulative —
almost all the time inside the SELECT path is spent in `db.Query` and
the driver's row iteration, not in handler code. `bufio.Writer.Flush`
does not appear in the top 30 — confirming that the 32 KB server write
buffer (v1.5.0) collapses the ~6 implicit mid-response flushes into a
single explicit flush in `sendReadyForQuery`.

### `pprof_server_select_wide.heap.prof` — 9 390 q/s, 500 rows each

Cumulative allocation: **~1.79 GB / 3 s** (~597 MB/s).

| Function | alloc_space % | Comment |
|---|---:|---|
| `(*SQLiteRows).nextSyncLocked` (flat 29.0 %, cum 58.3 %) | 29.0 % | driver row iteration |
| `mattn/go-sqlite3._Cfunc_GoStringN` | 27.7 % | cgo string copy — driver-internal |
| `memdb/server.(*handler).handleSelect` (cum 85.6 %) | 3.5 % | server handler |
| `(*SQLiteConn).prepare` | 3.1 % | per-query SQL prepare on replicas |
| `memdb/server.trimTrailingNUL` | 2.8 % | wire-format helper |
| `memdb/server.(*handler).sendRowDescriptionTyped` | 1.5 % | typed column metadata write (v1.7.2 typed Describe) |

The dominant remaining hotspots are still inside `mattn/go-sqlite3`'s
cgo boundary (the `_Cfunc_GoStringN` string copy and the row
iteration), which cannot be reduced without replacing the driver. On
the memdb side, `(*SQLiteConn).prepare` at 3.1 % is the per-query
prepare on the read replicas (which deliberately bypass the writer's
prepared-statement cache — `sqlite3_deserialize` invalidates statement
handles on each refresh tick). `sendRowDescriptionTyped` is new in
v1.7.2 (Extended Query Protocol) and adds the typed-Describe path; it
sits below 2 % of allocation space.

### `pprof_reads.block.prof`

The biggest named blockers under the read replica pool are
`runtime.selectgo` (55.1 %) and `runtime.chanrecv1` (44.7 %) — both are
the channel-based replica check-in/check-out machinery in
`replicaPool`. `database/sql.(*DB).conn` follows at ~9.5 %: readers
waiting for the per-replica single connection to free. This is by
design — it is how the replica pool maintains mutual exclusion between
open `*sql.Rows` cursors and `sqlite3_deserialize` calls during
refresh. Adding more replicas (`ReadPoolSize`) scales this linearly.

---

## Where the remaining cost lives

After the v1.6.x sweeps (v1.7.x/v1.8.0 added no hot-path code), the
profiles say memdb is spending its cycles on:

1. **cgo boundary crossing into SQLite** (~40–60 % depending on workload)
   — structural, requires replacing the driver to reduce.
2. **`database/sql` driver plumbing** (~15–20 %) — connection management,
   statement caching, argument binding.
3. **SQLite itself** — inside cgo, not visible in Go pprof.
4. **`write(2)` + `fsync(2)` syscalls** on the WAL path (~10 %) —
   inherent to durability.
5. **Streaming SHA-256** on the flush/restore path (v1.6.2+) — only
   visible during `Flush` and `Open+restore`, with a constant ~37 KiB
   per-call buffer regardless of payload size, not in steady-state
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

Full results from the v1.8.0 run are written to `coverage/bench.txt`
alongside the raw `.prof` files in `coverage/pprof/` (server captures
land in `server/coverage/pprof/`). Both paths are gitignored — this
report (`BENCHMARKS.md`) lives at the repo root and is the committed
summary of that data. Re-run on your own hardware to refresh.
