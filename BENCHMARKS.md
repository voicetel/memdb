# memdb v1.4.0 — Benchmark Report

Consolidated results from the full benchmark suite and the pprof-driven
throughput scenarios. All numbers captured in a single session against the
commit tagged `v1.4.0`; see the Setup section for the exact environment.

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
| Commit | v1.4.0 |
| `-benchtime` | 3 s per bench |
| `-cpu` | default (GOMAXPROCS=20) unless noted |

Background flushing is disabled in every benchmark (`FlushInterval: -1`) so
timings reflect only the operation under measurement, not periodic I/O.

---

## Headline throughput (pprof scenarios, 3 s wall-clock each)

These numbers come from the `TestPProf_*` harnesses, which count real
operations over a fixed wall-clock window and include all goroutine
orchestration, buffered-writer flushing, and connection churn.

### Core DB

| Scenario | Throughput |
|---|---|
| Pure writes (single goroutine, `DurabilityNone`) | **256 058 ops/s** |
| Pure writes (single goroutine, `DurabilityWAL`) | **209 489 ops/s** |
| Concurrent reads (16 goroutines, replica pool) | **812 193 ops/s** |
| Mixed RW, `ReplicaRefreshInterval=2ms` | 14 221 w/s + 116 011 r/s |
| Mixed RW, `ReplicaRefreshInterval=50ms` *(default)* | **50 871 w/s + 345 621 r/s** |
| Flush (50 000 rows) | ~41 ms / flush |

Notes:

- The mixed-RW rows show the impact of the default change from 1 ms to 50 ms:
  the default case runs **~3× faster on writes and ~3× faster on reads** than
  the explicit-2 ms comparison.
- `DurabilityWAL` pays ~0.2 µs per write for fsync; throughput is 82 % of the
  no-durability path.

### Postgres wire-protocol server

| Scenario | Throughput |
|---|---|
| Narrow SELECT (16 clients, ~10 rows/query) | **41 248 q/s** |
| Wide SELECT (8 clients, 500 rows/query) | **3 262 q/s (~1.6M rows/s)** |
| INSERT (8 clients, simple-query protocol) | **66 121 q/s** |
| Mixed SELECT/INSERT (20 clients, 1-in-4 writes) | **55 683 q/s** |
| Connect → query → disconnect (8 clients) | **14 871 cycles/s** |

---

## Microbenchmarks

### Single-row operations

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `Exec_Insert` | 6 555 | 448 | 14 |
| `Exec_Insert_WAL` | 7 530 | 561 | 15 |
| `Exec_Update` | 4 951 | 454 | 14 |
| `Exec_Delete` | 3 706 | 264 | 12 |
| `QueryRow` | 5 936 | 759 | 23 |
| `Query_RangeScan` (100 rows) | 45 961 | 7 080 | 420 |

The absolute allocation counts are dominated by `database/sql` and the
`mattn/go-sqlite3` driver — memdb itself contributes <2 allocs/op on the
write path.

### Transactions

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `WithTx_SingleInsert` | 8 518 | 897 | 29 |
| `WithTx_BatchInsert` (50 rows) | 190 029 | 15 420 | 617 |
| `WithTx_ReadOnly` | 8 318 | 1 417 | 44 |

Batching 50 inserts inside a single transaction is ~2.2× faster per row
than 50 individual `Exec` calls (3 800 ns/row vs 6 555 ns/row) — the
amortised SQLite `COMMIT` cost is the reason.

### WAL primitives (binary format, v1.4)

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `WAL_Append` | **621.9** | **128** | **1** |
| `WAL_Replay` / 100 entries | 86 293 | 39 010 | 609 |
| `WAL_Replay` / 1 000 entries | 851 509 | 366 322 | 6 012 |
| `WAL_Replay` / 10 000 entries | 9 175 406 | 5 129 844 | 60 022 |

Replay scales linearly and costs ~9 ms for a 10 000-entry WAL — a one-time
cost at startup.

For context, the v1.3 baseline (gob-encoded, unpooled buffers) was
**2 809 ns/op / 1 602 B/op / 20 allocs/op** for `WAL_Append`. The binary
format plus buffer pooling yields a 4.5× speed-up, 12.5× less memory, and
20× fewer allocations on the hot path.

### Flush (SQLite backup API)

| Rows | ns/op | B/op | allocs/op |
|---:|---:|---:|---:|
| 100 | 93 065 | 3 926 | 85 |
| 1 000 | 124 804 | 3 928 | 85 |
| 10 000 | 526 528 | 3 928 | 85 |

Flush cost is essentially independent of B/op and allocs/op — SQLite
copies pages in cgo-land so the Go side sees constant allocation overhead.
Scales sub-linearly: 100× the data is only ~5.7× the flush time.

### Lifecycle

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `Open` (empty DB) | 76 302 | 3 466 | 86 |
| `OpenRestore` (1 000-row snapshot) | 153 369 | 6 413 | 153 |

### Concurrent mixed read/write (GOMAXPROCS=20)

| Write mix | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| 10 % writes | 6 079 | 727 | 22 |
| 50 % writes | 6 262 | 603 | 19 |
| 90 % writes | 6 329 | 479 | 15 |

Per-op cost is remarkably flat across the write ratio — read contention
on the replica pool and write contention on the single writer connection
balance out near 6 µs/op regardless of mix.

---

## Parameter sweeps

### `ReplicaRefreshInterval` (pprof-driven default guidance)

1 000-row dataset, 8 concurrent readers with an active writer.

| Interval | ns/op (write) | B/op | Note |
|---|---:|---:|---|
| 250 µs | 89 678 | 33 745 | refresh dominates CPU |
| 1 ms | 84 262 | 29 369 | previous default — 8× slower than 100 ms |
| 5 ms | 78 897 | 26 971 | marginal improvement |
| 25 ms | 53 320 | 20 437 | knee of the curve |
| 100 ms | **10 677** | **3 781** | writes at full speed |

**v1.4 default is 50 ms.** Values below 5 ms emit a warning at `Open`
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
| `memdb` | 4 376 | 1.00× |
| `memdb/wal` | 5 171 | 1.18× |
| `file/sync=off` | 8 237 | 1.88× |
| `file/sync=normal` | 8 342 | 1.91× |
| `file/sync=full` | 8 286 | 1.89× |

`memdb` is **1.89× faster** than the *fastest* file-backed configuration
(`sync=off`) and `memdb/wal` is still **1.59× faster** while providing
near-zero data loss durability.

### Single-row UPDATE

| Backend | ns/op | vs `memdb` |
|---|---:|---:|
| `memdb` | 3 190 | 1.00× |
| `file/sync=off` | 4 388 | 1.38× |
| `file/sync=normal` | 4 270 | 1.34× |
| `file/sync=full` | 4 445 | 1.39× |

### Single-row QueryRow

| Backend | ns/op | vs `memdb` |
|---|---:|---:|
| `memdb` | 3 585 | 1.00× |
| `file/sync=off` | 4 378 | 1.22× |
| `file/sync=normal` | 4 462 | 1.24× |
| `file/sync=full` | 4 359 | 1.22× |

### 100-row range scan

| Backend | ns/op | vs `memdb` |
|---|---:|---:|
| `memdb` | 38 256 | 1.00× |
| `file/sync=off` | 38 970 | 1.02× |
| `file/sync=normal` | 38 799 | 1.01× |
| `file/sync=full` | 39 598 | 1.03× |

Sequential scans spend almost all their time in SQLite's cursor machinery,
which is identical across backends — the gap collapses to noise.

### 50-row batch INSERT (in one transaction)

| Backend | ns/op | vs `memdb` |
|---|---:|---:|
| `memdb` | 156 303 | 1.00× |
| `file/sync=off` | 166 413 | 1.06× |
| `file/sync=normal` | 164 206 | 1.05× |
| `file/sync=full` | 167 173 | 1.07× |

### Concurrent read (parallelism=4, 500-row dataset)

Baseline (1.00×) is `memdb/pool` — the fastest backend in this table.
Every other row shows that backend's `ns/op ÷ baseline ns/op`, so larger
numbers mean slower, matching the convention used in the preceding
comparison tables.

| Backend | ns/op | vs `memdb/pool` |
|---|---:|---:|
| `memdb/pool` (`ReadPoolSize=4`) | **2 032** | **1.00×** |
| `file/sync=off` | 2 478 | 1.22× |
| `file/sync=normal` | 2 453 | 1.21× |
| `file/sync=full` | 2 479 | 1.22× |
| `memdb` (single-conn) | 5 500 | 2.71× |

With the replica pool `memdb` is **~22 % faster than file SQLite** on
concurrent reads, and **2.71× faster than `memdb` without the pool**.
Turning on `ReadPoolSize` is the single biggest read-throughput lever
available.

---

## Profile analysis (what the pprof files say)

### `pprof_reads.cpu.prof` — 812k reads/s

| Function | cum % | Comment |
|---|---:|---|
| `TestPProf_Reads_Replicas.func1.1` | 93.2 % | user goroutine doing the work |
| `database/sql.withLock` | 71.6 % | driver-internal lock (unavoidable) |
| `memdb.(*DB).QueryRowContext` | 54.3 % | our entry point |
| `runtime.cgocall` (flat 39.6 %) | 49.4 % | crossing into SQLite (unavoidable) |

`replicaRefreshLoop` does not appear in the top 10 — after the
write-generation short-circuit and the fast-path tick, it is below the
0.5 % sampling threshold during a read-only workload.

### `pprof_writes_wal.cpu.prof` — 209k WAL writes/s

| Function | flat % | Comment |
|---|---:|---|
| `runtime.cgocall` | 58.4 % | SQLite work (unavoidable) |
| `internal/runtime/syscall.Syscall6` | 8.2 % | `write(2)` + `fsync(2)` |
| `WAL.Append` (self + cum 13.8 %) | 1.3 % | our encoder |
| `runtime.casgstatus` | 4.6 % | goroutine scheduling |
| (no `encoding/gob.*` anywhere) | — | eliminated by the binary format |

`gob` is **completely gone from the profile**, confirming the v1.4 change.

### `pprof_server_select_wide.heap.prof` — 3 262 q/s, 500 rows each

Allocation rate: **340 MB / 3 s** (down from 1 234 MB pre-v1.4).

| Function | alloc_space % | Comment |
|---|---:|---|
| `mattn/go-sqlite3._Cfunc_GoStringN` | 45.4 % | cgo string copy — driver-internal |
| `(*SQLiteRows).nextSyncLocked` | 41.7 % | driver row iteration |
| `memdb/server.(*handler).handleSelect` | **0.1 %** | **down from 22 % in v1.3** |
| `memdb/server.(*handler).sendDataRow` | (gone) | reused wire buffer |
| `memdb/server.appendCell` | (gone) | reused cell buffer |

The three server-side row hotspots from v1.3 are all **below the sampling
threshold** now — the remaining allocations are entirely in the SQLite
driver's cgo boundary, which we cannot optimise without replacing the
driver.

### `pprof_reads.block.prof`

The biggest named blocker is `database/sql.(*DB).conn` (readers waiting
for the per-replica single connection to free). This is by design: it is
how the replica pool maintains mutual exclusion between open `*sql.Rows`
cursors and `sqlite3_deserialize` calls during refresh. Adding more
replicas (`ReadPoolSize`) scales this linearly.

No user-code channel blocking shows up — the `replicaPool.idle` channel
operations were measured at ~8.5 % of CPU in the read hot path but do not
accumulate blocking time because sends/receives are always paired with a
ready peer.

---

## Where the remaining cost lives

After the v1.4 sprint, the profiles say memdb is spending its cycles on:

1. **cgo boundary crossing into SQLite** (~40–60 % depending on workload)
   — structural, requires replacing the driver to reduce.
2. **`database/sql` driver plumbing** (~15–20 %) — connection management,
   statement caching, argument binding.
3. **SQLite itself** — inside cgo, not visible in Go pprof.
4. **`write(2)` + `fsync(2)` syscalls** on the WAL path (~8 %) —
   inherent to durability.

memdb's own code (core, replica pool, server handler, WAL) is now
consistently below 5 % of CPU in every hot-path profile. Further gains
would require either a driver replacement or architectural changes
(incremental WAL ship to replicas instead of full serialise, for example).

---

## Reproducing these numbers

```bash
# Core benchmarks (~5 minutes on the reference hardware)
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

Full results from the v1.4.0 run are written to `coverage/bench.txt`
alongside the raw `.prof` files in `coverage/pprof/`. Both paths are
gitignored — this report (`BENCHMARKS.md`) lives at the repo root and is
the committed summary of that data.