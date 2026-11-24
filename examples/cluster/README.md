# Three-node Raft cluster + PostgreSQL wire

A single Go process brings up three replicated memdb nodes joined into
one Raft cluster, each exposing the PostgreSQL wire protocol on its
own port. Connect any standard Postgres client to **any node** —
writes are forwarded to the leader, replicated under consensus, and
applied on every node before the response returns. Reads on a
follower see committed writes.

This is the same shape that would deploy across three machines: just
hand each `startNode` real network addresses instead of loopback.

## Run

```bash
go run ./examples/cluster
```

A fresh self-signed TLS cert is generated in-process on every run, so
there's no setup. The data directory is a fresh temporary directory,
removed on exit, unless you pass `-data /some/dir` to persist it across
runs.

```
node-1: raft=127.0.0.1:7000 forward=127.0.0.1:7100 pg=127.0.0.1:5433
node-2: raft=127.0.0.1:7001 forward=127.0.0.1:7101 pg=127.0.0.1:5434
node-3: raft=127.0.0.1:7002 forward=127.0.0.1:7102 pg=127.0.0.1:5435
leader: node-2
cluster ready. Try:
  psql -h 127.0.0.1 -p 5433 -U memdb -d memdb -c 'CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT)'
  ...
```

## Demonstrate replication

```bash
# CREATE on node-1 (follower) — forwarded to leader, replicated.
psql -h 127.0.0.1 -p 5433 -U memdb -d memdb \
     -c 'CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT)'

# INSERT on node-2.
psql -h 127.0.0.1 -p 5434 -U memdb -d memdb \
     -c "INSERT INTO kv VALUES ('a','1'),('b','2'),('c','3')"

# Read on node-3 — sees all three rows.
psql -h 127.0.0.1 -p 5435 -U memdb -d memdb -c 'SELECT * FROM kv'

# UPDATE on whichever node is the follower right now.
psql -h 127.0.0.1 -p 5433 -U memdb -d memdb \
     -c "UPDATE kv SET v='hello' WHERE k='a'"

# Read on any node — sees the update.
psql -h 127.0.0.1 -p 5435 -U memdb -d memdb -c "SELECT v FROM kv WHERE k='a'"
```

Driver-side it works the same way — `pgx`, `lib/pq`, `sqlx`, GORM, all
talk to any node.

## What's wired up

```
psql / pgx / lib/pq
        │
        ▼  PostgreSQL wire
  ┌───────────────┐
  │  PG handler   │ — extended-query support, prepared statements,
  │  (per node)   │   binary format codes, SCRAM auth (if configured)
  └───────┬───────┘
          │  db.Exec
          ▼
  ┌───────────────┐  OnExecContext → node.ExecResultContext
  │   memdb.DB    │ ─────────────────┐
  │  (per node)   │                  │
  └───────────────┘                  │
          ▲                          │
          │ ExecDirectResult         │
          │ (FSM apply)              ▼
  ┌─────────────────────────────────────┐
  │  hashicorp/raft over mutual TLS     │
  │  Bind / Forward / consensus         │
  │  (3 nodes, quorum = 2)              │
  └─────────────────────────────────────┘
```

## Rows-affected count

The command tag is the real count, on every node: `INSERT 0 3`,
`UPDATE 1`. `OnExecContext` returns the count from the leader's FSM
apply (`ExecDirectResult`, exposed to Raft through the adapter's
`ExecLocalResult`), and a follower gets it back in the forwarding
response. The older `OnExecResult` hook does the same without the
caller's context; only the legacy error-only `OnExec` hook reports a
synthetic `0`.

## Shutdown

Ctrl-C stops the three PG servers, then the three Raft nodes, then
closes the three DBs.

- `Stop` terminates live client sessions rather than waiting for them
  to disconnect, and returns once every handler has exited — wait on
  it, not on `ListenAndServe`, before tearing the DB down.
- The nodes set `DisableShutdownHandover` because the whole cluster
  stops at once. Leave it unset in a real deployment: `Shutdown` on a
  leader then hands leadership to another voter first, so a rolling
  restart does not leave the cluster leaderless for an election
  timeout.

## Growing the cluster

Not shown here: a fourth node would start with `NodeConfig.Join` (so
it does not bootstrap a cluster of its own), the leader would add it
with `AddVoter`, and every node already running would be told its
forwarding address with `SetForwardPeer` — `ForwardPeers` is only read
at start. See the join section of the top-level README.

## Flags

```
-data string         directory for raft state and snapshots; defaults to a fresh tempdir
-pg-port int         first PG-wire port (nodes get N, N+1, N+2) (default 5433)
-raft-port int       first Raft RPC port (nodes get N, N+1, N+2)  (default 7000)
-forward-port int    first write-forwarding port (nodes get N, N+1, N+2) (default 7100)
```

To deploy for real across three machines, replace the loopback
addresses in `main.go` with the host's external IP / DNS name and
ship the same binary to each, with each instance picking its own
`nodeID` and matching `Peers` / `ForwardPeers` lists. Or use
`memdb serve -raft-*` from `cmd/memdb` — same wiring, different
front door.
