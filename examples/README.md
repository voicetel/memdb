# Examples

Self-contained programs showing what memdb feels like to embed.

| Example | What it demonstrates |
|---|---|
| [`quickstart`](quickstart) | The library-API smoke test: open, exec, query, transaction, flush. Same code as the top-level README's *Quick Start* section, made runnable. Look here first. |
| [`restapi`](restapi) | One process, one in-memory SQLite database, two listeners over it: an HTTP/JSON CRUD API and the PostgreSQL wire protocol. A row written via `curl -X POST /todos` is immediately visible to `psql … SELECT *`, and vice versa. |
| [`cluster`](cluster) | Three Raft nodes joined into one cluster, each exposing the PostgreSQL wire protocol. Writes from any node forward to the leader, replicate under consensus, and become visible everywhere. Self-generates a TLS cert on startup so there's no setup. |

Each example is a single `main.go` plus its own README. Run with
`go run ./examples/<name>` from the repository root.
