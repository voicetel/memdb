# Dual-protocol Todo example

One process. One in-memory SQLite database. Two listeners over it:

- **HTTP** (JSON REST) on `-http-addr` (default `:8080`)
- **PostgreSQL wire protocol** on `-pg-addr` (default `:5433`)

Both endpoints share the same `*memdb.DB`, so a row written via `curl`
is immediately visible to `psql`, `pgx`, `lib/pq`, `sqlx`, or any other
Postgres client — and vice versa. The point of the example is to show
that embedding the database into your service (no out-of-process DB,
no extra hop) doesn't have to mean giving up the Postgres tooling
ecosystem.

## Run

```bash
go run ./examples/restapi -file /tmp/todos.db
```

The process logs both listeners on startup:

```
2026/04/26 13:00:51 INFO memdb: WAL replayed
2026/04/26 13:00:51 Postgres wire listening on 127.0.0.1:5433 (try: psql -h 127.0.0.1 -p 5433 -U memdb -d memdb)
2026/04/26 13:00:51 HTTP listening on http://127.0.0.1:8080
```

## Use it

### REST

```bash
curl -X POST   localhost:8080/todos   -d '{"title":"buy milk"}'
curl           localhost:8080/todos
curl -X PATCH  localhost:8080/todos/1 -d '{"done":true}'
curl -X DELETE localhost:8080/todos/1
```

### psql (cleartext, simple-query)

```bash
psql -h 127.0.0.1 -p 5433 -U memdb -d memdb -c 'SELECT * FROM todos'
psql -h 127.0.0.1 -p 5433 -U memdb -d memdb \
     -c "INSERT INTO todos(title) VALUES ('book flight')"
```

### pgx (extended-query, binary format)

```go
conn, _ := pgx.Connect(ctx, "postgres://memdb@127.0.0.1:5433/memdb?sslmode=disable")
rows, _ := conn.Query(ctx, `SELECT id, title, done FROM todos WHERE done = $1`, false)
```

### lib/pq (extended-query, prepared statements)

```go
db, _ := sql.Open("postgres", "postgres://memdb@127.0.0.1:5433/memdb?sslmode=disable")
stmt, _ := db.Prepare(`SELECT title FROM todos WHERE id = $1`)
var title string
stmt.QueryRow(1).Scan(&title)
```

## Why this matters

A typical "embedded SQLite" setup gives you fast local data but a closed
ecosystem — every operator query, BI tool, and migration helper has to
go through your application API. Wiring memdb's PG-wire server alongside
your HTTP layer means:

- **Operators** can `psql` into a running service for ad-hoc reads.
- **BI / dashboarding** tools that speak Postgres (Metabase, Grafana,
  Redash) plug in directly with no extra infrastructure.
- **Database tools** (`pg_dump`-style backups via `COPY` are not yet
  supported, but `SELECT * FROM ...` over the wire works for snapshots)
  and migration tools like `goose` / `dbmate` work over the same socket.
- **Tests** can use the in-process API for setup and `pgx` for the
  integration-style assertions.

All without spawning a separate `postgres` process or changing the
on-disk format.

## Endpoints

```
POST   /todos          {"title": "..."}             → 201 + {"id": ...}
GET    /todos                                       → 200 + [{...}, ...]
GET    /todos/{id}                                  → 200 + {...}        | 404
PATCH  /todos/{id}     {"title": "...", "done": …}  → 204                | 404
DELETE /todos/{id}                                  → 204                | 404
```

## Schema

```sql
CREATE TABLE todos (
    id    INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT    NOT NULL,
    done  INTEGER NOT NULL DEFAULT 0
);
```

`done` is an `INTEGER` (0/1) for the broadest client compatibility —
the JSON API converts it to/from `bool` automatically.

## Disabling a listener

Pass an empty `-http-addr` or `-pg-addr` to disable that protocol:

```bash
# PG-only daemon:
go run ./examples/restapi -http-addr=

# HTTP-only:
go run ./examples/restapi -pg-addr=
```
