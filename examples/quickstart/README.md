# Quickstart

The library-API smoke test. Opens a memdb with a `sessions` table,
writes a row, reads it back, runs a transaction, and forces a flush —
the same code shown in the top-level README's *Quick Start* section,
made runnable.

```bash
go run ./examples/quickstart
```

Look here first if you want to see what the bare `memdb.Open` /
`db.Exec` / `db.QueryRow` / `memdb.WithTx` / `db.Flush` calls look
like in real Go without the HTTP or PG-wire wrapper that the other
examples add on top.
