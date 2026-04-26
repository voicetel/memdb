// Dual-protocol CRUD example backed by memdb.
//
// One process. One in-memory SQLite database. Two listeners over it:
//
//   - HTTP (JSON REST) on -http-addr (default :8080)
//   - PostgreSQL wire protocol on -pg-addr (default :5433)
//
// Both endpoints read and write the same memdb instance, so a row
// inserted via `curl -X POST /todos` is immediately visible to
// `psql ... SELECT * FROM todos`, and vice versa. This is the trick
// the library makes easy: embedding the DB into your app removes the
// network hop, but you still get a Postgres-compatible socket for
// operators / BI tools / ad-hoc queries when you want one.
//
// REST endpoints:
//
//	POST   /todos          {"title": "..."} → 201 + {"id": ...}
//	GET    /todos          → 200 + [{...}, ...]
//	GET    /todos/{id}     → 200 + {...} | 404
//	PATCH  /todos/{id}     {"title": "...", "done": true} → 204 | 404
//	DELETE /todos/{id}     → 204 | 404
//
// Run:
//
//	go run ./examples/restapi -file /tmp/todos.db -http-addr :8080 -pg-addr :5433
//
// REST:
//
//	curl -X POST   localhost:8080/todos   -d '{"title":"buy milk"}'
//	curl           localhost:8080/todos
//
// SQL (any Postgres client — psql, pgx, lib/pq, sqlx, ...):
//
//	psql -h 127.0.0.1 -p 5433 -U memdb -d memdb -c 'SELECT * FROM todos'
//	psql -h 127.0.0.1 -p 5433 -U memdb -d memdb \
//	     -c "UPDATE todos SET done=1 WHERE title LIKE 'buy %'"
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/voicetel/memdb"
	pgserver "github.com/voicetel/memdb/server"
)

type Todo struct {
	ID    int64  `json:"id"`
	Title string `json:"title"`
	Done  bool   `json:"done"`
}

type server struct {
	db *memdb.DB
}

func main() {
	file := flag.String("file", "todos.db", "snapshot file path")
	httpAddr := flag.String("http-addr", "127.0.0.1:8080", "HTTP listen address (empty disables)")
	pgAddr := flag.String("pg-addr", "127.0.0.1:5433", "Postgres wire-protocol listen address (empty disables)")
	flag.Parse()

	db, err := memdb.Open(memdb.Config{
		FilePath:      *file,
		FlushInterval: 10 * time.Second,
		Durability:    memdb.DurabilityWAL,
		InitSchema: func(db *memdb.DB) error {
			_, err := db.Exec(`
				CREATE TABLE IF NOT EXISTS todos (
					id    INTEGER PRIMARY KEY AUTOINCREMENT,
					title TEXT    NOT NULL,
					done  INTEGER NOT NULL DEFAULT 0
				)`)
			return err
		},
	})
	if err != nil {
		log.Fatalf("memdb.Open: %v", err)
	}
	defer db.Close()

	s := &server{db: db}
	mux := http.NewServeMux()
	mux.HandleFunc("/todos", s.handleCollection)
	mux.HandleFunc("/todos/", s.handleItem)

	httpSrv := &http.Server{
		Addr:              *httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Both listeners are children of one quit signal: ctrl-C / SIGTERM
	// shuts down each, then memdb.Close (deferred above) runs the final
	// flush so the snapshot file is current. Without this coordination,
	// killing the process while psql is connected would leak the
	// pg-wire goroutine.
	pgSrv := pgserver.New(db, pgserver.Config{ListenAddr: *pgAddr})

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup

	if *httpAddr != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Printf("HTTP listening on http://%s", *httpAddr)
			if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Fatalf("HTTP listen: %v", err)
			}
		}()
	}
	if *pgAddr != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Printf("Postgres wire listening on %s (try: psql -h 127.0.0.1 -p %s -U memdb -d memdb)",
				*pgAddr, portOnly(*pgAddr))
			// pgserver.ListenAndServe blocks until Stop is called.
			// Returns nil on graceful shutdown, error on bind failure.
			if err := pgSrv.ListenAndServe(); err != nil {
				log.Fatalf("PG listen: %v", err)
			}
		}()
	}

	go func() {
		<-quit
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if *httpAddr != "" {
			_ = httpSrv.Shutdown(ctx)
		}
		if *pgAddr != "" {
			pgSrv.Stop()
		}
	}()

	wg.Wait()
	log.Print("shutdown complete")
}

// portOnly extracts the port from "host:port" or ":port" so the log
// hint can render a copy-paste-ready psql command without having to
// teach the user about empty-host bind syntax.
func portOnly(addr string) string {
	if i := strings.LastIndexByte(addr, ':'); i >= 0 {
		return addr[i+1:]
	}
	return addr
}

// ── handlers ────────────────────────────────────────────────────────────────

func (s *server) handleCollection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listTodos(w, r)
	case http.MethodPost:
		s.createTodo(w, r)
	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *server) handleItem(w http.ResponseWriter, r *http.Request) {
	idStr := strings.TrimPrefix(r.URL.Path, "/todos/")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.getTodo(w, r, id)
	case http.MethodPatch:
		s.patchTodo(w, r, id)
	case http.MethodDelete:
		s.deleteTodo(w, r, id)
	default:
		w.Header().Set("Allow", "GET, PATCH, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *server) listTodos(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.QueryContext(r.Context(),
		`SELECT id, title, done FROM todos ORDER BY id`)
	if err != nil {
		writeErr(w, err, http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	out := make([]Todo, 0)
	for rows.Next() {
		var t Todo
		var done int
		if err := rows.Scan(&t.ID, &t.Title, &done); err != nil {
			writeErr(w, err, http.StatusInternalServerError)
			return
		}
		t.Done = done != 0
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		writeErr(w, err, http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *server) createTodo(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Title string `json:"title"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(in.Title) == "" {
		http.Error(w, "title is required", http.StatusBadRequest)
		return
	}
	res, err := s.db.ExecContext(r.Context(),
		`INSERT INTO todos (title) VALUES (?)`, in.Title)
	if err != nil {
		writeErr(w, err, http.StatusInternalServerError)
		return
	}
	id, err := res.LastInsertId()
	if err != nil {
		// Fall back to currval-style read for drivers that don't return it.
		// memdb's mattn/go-sqlite3 driver does, so this branch is defensive.
		writeErr(w, err, http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, Todo{ID: id, Title: in.Title})
}

func (s *server) getTodo(w http.ResponseWriter, r *http.Request, id int64) {
	var t Todo
	var done int
	err := s.db.QueryRowContext(r.Context(),
		`SELECT id, title, done FROM todos WHERE id = ?`, id).
		Scan(&t.ID, &t.Title, &done)
	if errors.Is(err, sql.ErrNoRows) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		writeErr(w, err, http.StatusInternalServerError)
		return
	}
	t.Done = done != 0
	writeJSON(w, http.StatusOK, t)
}

func (s *server) patchTodo(w http.ResponseWriter, r *http.Request, id int64) {
	// Pointer fields so we can distinguish "field omitted" from
	// "field set to zero value". Without this, a PATCH that only sends
	// `{"done": true}` would clear the title.
	var in struct {
		Title *string `json:"title"`
		Done  *bool   `json:"done"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if in.Title == nil && in.Done == nil {
		http.Error(w, "no fields to update", http.StatusBadRequest)
		return
	}

	// Build the SET clause dynamically from the fields actually provided.
	// The placeholders are positional `?` so SQL injection via field
	// values is impossible; only the column list is templated.
	sets := make([]string, 0, 2)
	args := make([]any, 0, 3)
	if in.Title != nil {
		sets = append(sets, "title = ?")
		args = append(args, *in.Title)
	}
	if in.Done != nil {
		sets = append(sets, "done = ?")
		done := 0
		if *in.Done {
			done = 1
		}
		args = append(args, done)
	}
	args = append(args, id)
	q := "UPDATE todos SET " + strings.Join(sets, ", ") + " WHERE id = ?"

	res, err := s.db.ExecContext(r.Context(), q, args...)
	if err != nil {
		writeErr(w, err, http.StatusInternalServerError)
		return
	}
	n, err := res.RowsAffected()
	if err != nil {
		writeErr(w, err, http.StatusInternalServerError)
		return
	}
	if n == 0 {
		http.NotFound(w, r)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) deleteTodo(w http.ResponseWriter, r *http.Request, id int64) {
	res, err := s.db.ExecContext(r.Context(),
		`DELETE FROM todos WHERE id = ?`, id)
	if err != nil {
		writeErr(w, err, http.StatusInternalServerError)
		return
	}
	n, err := res.RowsAffected()
	if err != nil {
		writeErr(w, err, http.StatusInternalServerError)
		return
	}
	if n == 0 {
		http.NotFound(w, r)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ── helpers ─────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("encode: %v", err)
	}
}

func writeErr(w http.ResponseWriter, err error, status int) {
	http.Error(w, fmt.Sprintf("%v", err), status)
}
