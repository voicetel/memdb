# memdb v1.5.0 — Benchmark Report

Consolidated results from the full benchmark suite and the pprof-driven
throughput scenarios. All numbers captured in a single session against the
commit tagged `v1.5.0` after the pprof-driven optimisation sprint; see the
Setup section for the exact environment.

> **Update for v1.6.0** — a second pprof sweep removed the remaining `gob`
> usage from the Raft FSM apply path (`replication/codec.go`, shared with
> the WAL hot path), added a writer-side prepared-statement cache
> (`stmt_cache.go`), let AEAD backends opt out of the SHA-256 snapshot
> header (`memdb.AuthenticatedBackend`), and pinned `CompressedBackend` to
> `zstd.SpeedFastest`. Measured deltas from `make pprof`:
>
> | Scenario | v1.5 | v1.6 | Delta |
> |---|---:|---:|---:|
> | Raft Apply (3-node cluster, writes/s) | 5,082 | 12,579 | **2.47×** |
> | Contended writes (16 goroutines, ops/s) | 163,186 | 224,568 | **1.38×** |
> | WAL cold-start replay (entries/s) | 123,687 | 164,595 | **1.33×** |
> | Compressed flush (CPU samples, 5×50k rows) | 120 ms | 90 ms | **−25 %** |
> | Encrypted flush (CPU samples) | 60 ms | 50 ms; SHA-256 fully removed | **−17 %** |
>
> Numbers from `/tmp/memdb-prof-after/` on the v1.5 setup — re-run `make pprof`
> on your hardware for fresh comparisons.

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
| Commit | v1.5.0 |
| `-benchtime` | 5 s per bench |
| `-cpu` | default (GOMAXPROCS=20) unless noted |

Background flushing is disabled in every benchmark (`FlushInterval: -1`) so
timings reflect only the operation under measurement, not periodic I/O.

---

## What changed in v1.5.0 (Sprint 1 security + pprof optimisations)

### Security hardening

v1.5.0 adds four security hardening items with observable performance impact
on the flush and restore paths. All other hot paths (write, read, WAL append)
are unaffected.

| Change | Impact |
|---|---|
| **Snapshot integrity** — SHA-256 header (`"MDBK"`) prepended to every flush | `snapshotWriter` buffers the full payload to hash before writing; flush `B/op` now scales with DB size |
| **Restore verification** — `verifyAndStrip` reads the full payload via `io.ReadAll` to verify the digest | `OpenRestore` latency is roughly proportional to snapshot size |
| **FK enforcement on by default** | No measurable hot-path impact (FK checks happen inside SQLite's cgo layer) |
| **Panic recovery in callbacks** | No measurable hot-path impact (fast-path is a single `defer recover()`) |

### pprof-driven optimisations

Two optimisations were applied after profiling the v1.5.0 security baseline.

| Change | Impact |
|---|---|
| **`WAL.Append` zero-alloc** — 4-byte length prefix now reserved in the pool buffer before `walEncodeBinary`, eliminating the separate `make+copy` | `WAL_Append`: **642 → 476 ns/op**, **128 → 0 B/op**, **1 → 0 allocs/op** (~26% faster) |
| **Server write buffer 4 KB → 32 KB** — `bufio.NewWriterSize(conn, 32×1024)` | Eliminates ~6 implicit mid-response `write(2)` syscalls on wide SELECT (~500 rows); only the final `Flush()` in `sendReadyForQuery` triggers a syscall |

The write, read, and server paths otherwise remain within noise of the
v1.4.0 reference run.

---

## Headline throughput (pprof scenarios, 3 s wall-clock each)

These numbers come from the `TestPProf_*` harnesses, which count real
operations over a fixed wall-clock window and include all goroutine
orchestration, buffered-writer flushing, and connection churn.

### Core DB

| Scenario | Throughput |
|---|---|
| Pure writes (single goroutine, `DurabilityNone`) | **260 322 ops/s** |
| Pure writes (single goroutine, `DurabilityWAL`) | **200 280 ops/s** |
| Concurrent reads (16 goroutines, replica pool) | **832 623 ops/s** |
| Mixed RW, `ReplicaRefreshInterval=2ms` | 14 489 w/s + 116 411 r/s |
| Mixed RW, `ReplicaRefreshInterval=50ms` *(default)* | 44 190 w/s + 305 223 r/s |
| Flush (50 000 rows) | ~41 ms / flush |

Notes:

- `DurabilityWAL` pays ~0.5 µs per write for `WAL.Append` + fsync; throughput
  is 77 % of the no-durability path. The WAL hot path contributes only ~9 % of
  total DurabilityWAL CPU after the zero-alloc optimisation.
- The mixed-RW default refresh case represents a 3-second wall-clock window;
  variance between runs is ±10–15 % — use `BenchmarkReplicaRefreshInterval`
  for stable parameter-sweep data.

### Postgres wire-protocol server

| Scenario | Throughput |
|---|---|
| Narrow SELECT (16 clients, ~10 rows/query) | **41 836 q/s** |
| Wide SELECT (8 clients, 500 rows/query) | **3 467 q/s (~1.7M rows/s)** |
| INSERT (8 clients, simple-query protocol) | **65 611 q/s** |
| Mixed SELECT/INSERT (20 clients, 1-in-4 writes) | **56 705 q/s** |
| Connect → query → disconnect (8 clients) | **14 690 cycles/s** |

---

## Microbenchmarks

### Single-row operations

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `Exec_Insert` | 6 203 | 448 | 14 |
| `Exec_Insert_WAL` | 7 210 | 448 | 14 |
| `Exec_Update` | 4 724 | 454 | 14 |
| `Exec_Delete` | 3 587 | 264 | 12 |
| `QueryRow` | 5 547 | 758 | 23 |
| `Query_RangeScan` (100 rows) | 45 464 | 7 080 | 420 |

The absolute allocation counts are dominated by `database/sql` and the
`mattn/go-sqlite3` driver — memdb itself contributes <2 allocs/op on the
write path. `Exec_Insert_WAL` carries 1 fewer alloc and 128 fewer B/op than
the v1.5.0 pre-optimisation baseline, exactly matching the WAL pool-buffer
allocation that was eliminated.

### Transactions

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `WithTx_SingleInsert` | 8 498 | 896 | 29 |
| `WithTx_BatchInsert` (50 rows) | 180 695 | 15 414 | 617 |
| `WithTx_ReadOnly` | 7 963 | 1 416 | 44 |

Batching 50 inserts inside a single transaction is ~1.7× faster per row
than 50 individual `Exec` calls (3 614 ns/row vs 6 203 ns/row) — the
amortised SQLite `COMMIT` cost is the reason.

### WAL primitives (binary format, v1.4+)

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `WAL_Append` | **476.4** | **0** | **0** |
| `WAL_Replay` / 100 entries | 72 230 | 39 008 | 609 |
| `WAL_Replay` / 1 000 entries | 726 822 | 366 311 | 6 012 |
| `WAL_Replay` / 10 000 entries | 8 772 540 | 5 129 774 | 60 023 |

Replay scales linearly and costs ~9 ms for a 10 000-entry WAL — a one-time
cost at startup.

For context, the v1.3 baseline (gob-encoded, unpooled buffers) was
**2 809 ns/op / 1 602 B/op / 20 allocs/op** for `WAL_Append`. The binary
format plus buffer pooling (v1.4) and the zero-alloc length-prefix trick
(v1.5.0) together yield a 5.9× speed-up, infinite memory reduction, and
zero hot-path allocations.

### Flush (SQLite backup API + SHA-256 integrity, v1.5.0)

| Rows | ns/op | B/op | allocs/op |
|---:|---:|---:|---:|
| 100 | 115 646 | 49 108 | 88 |
| 1 000 | 252 705 | 266 430 | 90 |
| 10 000 | 1 481 689 | 2 103 215 | 97 |

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
| `Open` (empty DB) | 84 650 | 3 707 | 92 |
| `OpenRestore` (1 000-row snapshot) | 278 191 | 218 258 | 173 |

`OpenRestore` is notably higher than v1.4.0 (153 369 ns/op, 6 413 B/op)
because `verifyAndStrip` reads the entire snapshot payload via `io.ReadAll`
to verify the SHA-256 digest before handing bytes to the backup API. Memory
cost at restore time scales with snapshot size.

### Concurrent mixed read/write (GOMAXPROCS=20)

| Write mix | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| 10 % writes | 5 862 | 727 | 22 |
| 50 % writes | 5 923 | 603 | 19 |
| 90 % writes | 5 968 | 479 | 15 |

Per-op cost is remarkably flat across the write ratio — read contention
on the replica pool and write contention on the single writer connection
balance out near 6 µs/op regardless of mix.

---

## Parameter sweeps

### `ReplicaRefreshInterval` (pprof-driven default guidance)

1 000-row dataset, 8 concurrent readers with an active writer.

| Interval | ns/op (write) | B/op | Note |
|---|---:|---:|---|
| 250 µs | 86 944 | 31 859 | refresh dominates CPU |
| 1 ms | 82 320 | 33 105 | 4× slower than 100 ms |
| 5 ms | 71 803 | 28 444 | marginal improvement |
| 25 ms | 57 669 | 26 702 | knee of the curve |
| 100 ms | **11 061** | **4 961** | writes near full speed |

**v1.5.0 default is 50 ms.** Values below 5 ms emit a warning at `Open`
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
| `memdb` | 3 686 | 1.00× |
| `memdb/wal` | 4 559 | 1.24× |
| `file/sync=off` | 7 436 | 2.02× |
| `file/sync=normal` | 7 558 | 2.05× |
| `file/sync=full` | 7 838 | 2.13× |

`memdb` is **2.02× faster** than the *fastest* file-backed configuration
(`sync=off`) and `memdb/wal` is still **1.63× faster** while providing
near-zero data loss durability.

### Single-row UPDATE

| Backend | ns/op | vs `memdb` |
|---|---:|---:|
| `memdb` | 2 951 | 1.00× |
| `file/sync=off` | 4 242 | 1.44× |
| `file/sync=normal` | 4 260 | 1.44× |
| `file/sync=full` | 4 205 | 1.42× |

### Single-row QueryRow

| Backend | ns/op | vs `memdb` |
|---|---:|---:|
| `memdb` | 3 665 | 1.00× |
| `file/sync=off` | 4 503 | 1.23× |
| `file/sync=normal` | 4 553 | 1.24× |
| `file/sync=full` | 4 562 | 1.25× |

### 100-row range scan

| Backend | ns/op | vs `memdb` |
|---|---:|---:|
| `memdb` | 39 750 | 1.00× |
| `file/sync=off` | 40 603 | 1.02× |
| `file/sync=normal` | 40 675 | 1.02× |
| `file/sync=full` | 41 939 | 1.06× |

Sequential scans spend almost all their time in SQLite's cursor machinery,
which is identical across backends — the gap collapses to noise.

### 50-row batch INSERT (in one transaction)

| Backend | ns/op | vs `memdb` |
|---|---:|---:|
| `memdb` | 174 048 | 1.00× |
| `file/sync=off` | 178 328 | 1.02× |
| `file/sync=normal` | 180 758 | 1.04× |
| `file/sync=full` | 182 092 | 1.05× |

### Concurrent read (parallelism=4, 500-row dataset)

Baseline (1.00×) is `memdb/pool` — the fastest backend in this table.

| Backend | ns/op | vs `memdb/pool` |
|---|---:|---:|
| `memdb/pool` (`ReadPoolSize=4`) | **2 058** | **1.00×** |
| `file/sync=off` | 2 599 | 1.26× |
| `file/sync=normal` | 2 597 | 1.26× |
| `file/sync=full` | 2 588 | 1.26× |
| `memdb` (single-conn) | 5 646 | 2.74× |

With the replica pool `memdb` is **~26 % faster than file SQLite** on
concurrent reads, and **2.74× faster than `memdb` without the pool**.

---

## Profile analysis (what the pprof files say)

### `pprof_reads.cpu.prof` — 832k reads/s

| Function | cum % | Comment |
|---|---:|---|
| `TestPProf_Reads_Replicas.func1.1` | 93.2 % | user goroutine doing the work |
| `database/sql.withLock` | 71.6 % | driver-internal lock (unavoidable) |
| `memdb.(*DB).QueryRowContext` | 54.3 % | our entry point |
| `runtime.cgocall` (flat 39.6 %) | 49.4 % | crossing into SQLite (unavoidable) |

`replicaRefreshLoop` does not appear in the top 10 — after the
write-generation short-circuit and the fast-path tick, it is below the
0.5 % sampling threshold during a read-only workload.

### `pprof_writes_wal.cpu.prof` — 200k WAL writes/s

| Function | flat % | Comment |
|---|---:|---|
| `runtime.cgocall` | 59.3 % | SQLite work (unavoidable) |
| `internal/runtime/syscall.Syscall6` | 6.9 % | `write(2)` + `fsync(2)` |
| `WAL.Append` (self + cum 9.8 %) | <1 % | our encoder |
| `runtime.casgstatus` | 3.3 % | goroutine scheduling |
| (no `encoding/gob.*` anywhere) | — | eliminated by the v1.4 binary format |
| (no allocation site in `WAL.Append`) | — | eliminated by the v1.5.0 zero-alloc change |

`gob` is completely absent from the profile, confirming the v1.4 change
carries through to v1.5. The `WAL.Append` cumulative dropped from 13.8 %
to 9.8 % after the zero-alloc optimisation — the function is now dominated
by the `write(2)` + `fsync(2)` pair which cannot be eliminated.

### `pprof_server_select_wide.cpu.prof` — 3 467 q/s, 500 rows each

| Function | flat % | Comment |
|---|---:|---|
| `internal/runtime/syscall.Syscall6` | 45.1 % | CGO sqlite3 reads |
| `runtime.cgocall` | 13.6 % | cgo boundary crossing |
| `handleSelect` (self + cum 35.8 %) | 1.5 % | our handler |
| `bufio.(*Writer).Flush` | 0.9 % | **single** flush per response (was ~6 with 4 KB buffer) |
| `internal/poll.(*FD).Write` | 1.6 % cum | network write — low after 32 KB buffer |

The dominant `Syscall6` at 45 % is now attributed to sqlite3's internal CGO
read operations, not to network `write(2)` calls. `FD.Write` contributes
only 1.6 % cumulative — confirming that the 32 KB write buffer collapses
the ~6 mid-response implicit flushes into the one explicit `sendReadyForQuery`
flush.

### `pprof_server_select_wide.heap.prof` — 3 467 q/s, 500 rows each

Allocation rate: **~340 MB / 3 s**.

| Function | alloc_space % | Comment |
|---|---:|---|
| `mattn/go-sqlite3._Cfunc_GoStringN` | 45.4 % | cgo string copy — driver-internal |
| `(*SQLiteRows).nextSyncLocked` | 41.7 % | driver row iteration |
| `memdb/server.(*handler).handleSelect` | **< 1 %** | server handler overhead |

The server-side row hotspots from v1.3 remain below the sampling threshold —
remaining allocations are in the SQLite driver's cgo boundary, which cannot
be optimised without replacing the driver.

### `pprof_reads.block.prof`

The biggest named blocker is `database/sql.(*DB).conn` (readers waiting
for the per-replica single connection to free). This is by design: it is
how the replica pool maintains mutual exclusion between open `*sql.Rows`
cursors and `sqlite3_deserialize` calls during refresh. Adding more
replicas (`ReadPoolSize`) scales this linearly.

---

## Where the remaining cost lives

After the v1.5.0 sprint, the profiles say memdb is spending its cycles on:

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

Full results from the v1.5.0 run are written to `coverage/bench.txt`
alongside the raw `.prof` files in `coverage/pprof/`. Both paths are
gitignored — this report (`BENCHMARKS.md`) lives at the repo root and is
the committed summary of that data.
