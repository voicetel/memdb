// memdb-cli is an inspection / query shell for memdb. It operates
// in one of two mutually exclusive modes:
//
//   - Snapshot mode (-file PATH): opens a snapshot file directly,
//     unwraps the MDBK envelope, and exposes a sqlite3-style REPL
//     in read-only mode. Works whether the daemon is running or
//     not — the daemon writes via atomic rename, so the inode the
//     CLI opens is a stable point-in-time view. Data may lag the
//     live in-memory state by up to one flush interval.
//   - Wire mode (-addr HOST:PORT): connects to a running memdb
//     server over the PostgreSQL wire protocol. Behaves like a
//     small psql — full read AND write access (subject to server
//     auth), point-in-time-correct, with the same REPL/history/
//     completion as snapshot mode.
//
// # Usage
//
//	memdb-cli -file /var/lib/myapp/data.db          # snapshot RO
//	memdb-cli -file /var/lib/myapp/data.db -c "SELECT * FROM users LIMIT 5"
//	memdb-cli -addr 127.0.0.1:5433 -user memdb      # wire RW
//	memdb-cli -addr 127.0.0.1:5433 -tls -user memdb -c "INSERT INTO ..."
//
// # Read-only enforcement (snapshot mode)
//
// In snapshot mode memdb-cli opens the unwrapped SQLite payload
// with `mode=ro&immutable=1` — any INSERT/UPDATE/DELETE/CREATE
// returns a "readonly database" error from SQLite, which the REPL
// catches and prints as a friendly "memdb-cli is for inspection
// only" message. Wire mode does not apply that restriction.
//
// # Interactive features
//
// When stdin is a terminal, the REPL is built on github.com/peterh/liner
// and supports:
//
//   - Line editing (left/right, home/end, ctrl-w word delete, etc.)
//   - History via up/down arrows; ctrl-r reverse search
//   - Persistent history file at $XDG_STATE_HOME/memdb-cli/history (or
//     ~/.local/state/memdb-cli/history if XDG_STATE_HOME is unset),
//     overridable with -history
//   - Tab completion: meta-commands (.tables/.schema/...), table names
//     after FROM/JOIN/INTO/UPDATE/.schema/.indexes, and SQL keywords
//
// When stdin is redirected (a pipe or here-doc, e.g.
// `printf '...' | memdb-cli ...`), liner falls back to a bufio reader
// automatically — line editing is skipped but everything else works.
package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/peterh/liner"
	"github.com/voicetel/memdb"
)

func main() {
	fs := flag.NewFlagSet("memdb-cli", flag.ExitOnError)
	// Backend selection — exactly one is required.
	file := fs.String("file", "", "snapshot file path (snapshot mode, read-only). Mutually exclusive with -addr.")
	addr := fs.String("addr", "", "memdb server address host:port (wire mode, read+write). Mutually exclusive with -file.")
	user := fs.String("user", "", "username for wire mode (also reads MEMDB_USER env var)")
	password := fs.String("password", "", "password for wire mode (prefer the MEMDB_PASSWORD env var to avoid leaking via ps)")
	database := fs.String("database", "memdb", "database name for wire mode (the memdb server ignores this; included for psql compatibility)")
	useTLS := fs.Bool("tls", false, "require TLS for wire connections")
	tlsSkipVerify := fs.Bool("tls-skip-verify", false, "skip TLS certificate verification (for self-signed certs only)")
	connectTimeout := fs.Duration("connect-timeout", 5*time.Second, "wire-mode connect timeout")

	cmd := fs.String("c", "", "execute a single SQL statement and exit (non-interactive)")
	headers := fs.Bool("headers", true, "print column headers in tabular output")
	mode := fs.String("mode", "column", "output mode: column|list|line|csv")
	separator := fs.String("separator", "|", "column separator for list/csv modes (column mode uses fixed-width)")
	history := fs.String("history", "", "path to history file (default: $XDG_STATE_HOME/memdb-cli/history)")
	noHistory := fs.Bool("no-history", false, "disable persistent history")
	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(1)
	}

	switch {
	case *file == "" && *addr == "":
		fs.Usage()
		fmt.Fprintln(os.Stderr, "\nmemdb-cli: one of -file (snapshot mode) or -addr (wire mode) is required")
		os.Exit(1)
	case *file != "" && *addr != "":
		fmt.Fprintln(os.Stderr, "memdb-cli: -file and -addr are mutually exclusive")
		os.Exit(1)
	}

	var (
		db      *sql.DB
		cleanup func()
		err     error
		modeTag string
	)
	if *file != "" {
		db, cleanup, err = openSnapshot(*file)
		modeTag = "snapshot"
	} else {
		db, cleanup, err = openWire(wireOptions{
			Addr:           *addr,
			User:           resolveUser(*user),
			Password:       resolvePassword(*password),
			Database:       *database,
			RequireTLS:     *useTLS,
			SkipVerify:     *tlsSkipVerify,
			ConnectTimeout: *connectTimeout,
		})
		modeTag = "wire"
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "memdb-cli: %v\n", err)
		os.Exit(1)
	}
	defer cleanup()
	defer db.Close()

	out := newPrinter(os.Stdout, *headers, *mode, *separator)

	if *cmd != "" {
		if err := runStatement(db, *cmd, out); err != nil {
			fmt.Fprintf(os.Stderr, "memdb-cli: %s\n", friendlyError(err))
			os.Exit(1)
		}
		return
	}

	histPath := *history
	if histPath == "" && !*noHistory {
		histPath = defaultHistoryPath()
	}
	if *noHistory {
		histPath = ""
	}
	repl(db, out, histPath, modeTag)
}

// resolveUser returns flag if non-empty, else the MEMDB_USER env var.
// Empty result means the wire client connects with no startup-message
// "user" parameter — useful when the server has auth disabled.
func resolveUser(flag string) string {
	if flag != "" {
		return flag
	}
	return os.Getenv("MEMDB_USER")
}

// resolvePassword returns flag if non-empty, else the MEMDB_PASSWORD
// env var. Reading from env keeps the password out of `ps` /
// /proc/<pid>/cmdline output the same way the server does.
func resolvePassword(flag string) string {
	if flag != "" {
		return flag
	}
	return os.Getenv("MEMDB_PASSWORD")
}

// wireOptions bundles every knob the wire-mode connection consumes.
// Grouped to keep openWire's signature flat and so future flags
// (e.g. application_name, sslrootcert) can be added without
// disturbing call sites.
type wireOptions struct {
	Addr           string // host:port
	User           string
	Password       string
	Database       string
	RequireTLS     bool
	SkipVerify     bool
	ConnectTimeout time.Duration
}

// openWire dials a memdb server over the PostgreSQL wire protocol
// using lib/pq and returns a *sql.DB ready to issue queries.
//
// Mode selection: when -addr is provided, this is the alternative
// to openSnapshot. Unlike snapshot mode there is NO read-only
// enforcement here — the server's auth model decides what the
// client can do, just like psql.
//
// TLS: lib/pq's `sslmode` parameter selects the policy. We map
// -tls / -tls-skip-verify to `verify-full` / `require` and default
// to `disable` so a plaintext loopback connection works without
// extra flags (matches the memdb server defaults).
func openWire(opt wireOptions) (*sql.DB, func(), error) {
	host, port, err := net.SplitHostPort(opt.Addr)
	if err != nil {
		return nil, nil, fmt.Errorf("parse -addr %q: %w", opt.Addr, err)
	}

	sslmode := "disable"
	switch {
	case opt.RequireTLS && opt.SkipVerify:
		sslmode = "require" // TLS yes, server cert not validated
	case opt.RequireTLS:
		sslmode = "verify-full"
	}

	// lib/pq DSN is a space-separated list of key=value pairs.
	// connect_timeout is in seconds.
	parts := []string{
		"host=" + host,
		"port=" + port,
		"sslmode=" + sslmode,
		fmt.Sprintf("connect_timeout=%d", int(opt.ConnectTimeout.Round(time.Second).Seconds())),
	}
	if opt.User != "" {
		parts = append(parts, "user="+opt.User)
	}
	if opt.Password != "" {
		parts = append(parts, "password="+opt.Password)
	}
	if opt.Database != "" {
		parts = append(parts, "dbname="+opt.Database)
	}
	dsn := strings.Join(parts, " ")

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("open wire connection: %w", err)
	}
	// Single connection — interactive shells don't benefit from a
	// pool, and a single conn keeps any session-state semantics
	// (PRAGMA, transactions if added later) coherent.
	db.SetMaxOpenConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), opt.ConnectTimeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, nil, fmt.Errorf("connect to %s: %w", opt.Addr, err)
	}

	cleanup := func() {} // db.Close is already deferred by main
	return db, cleanup, nil
}

// openSnapshot unwraps the MDBK envelope to a temp file and opens it
// read-only. Returns the *sql.DB and a cleanup function that removes
// the temp file. The caller must invoke cleanup even on error paths
// after a successful return.
func openSnapshot(path string) (*sql.DB, func(), error) {
	in, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("open snapshot: %w", err)
	}
	defer in.Close()

	tmp, err := os.CreateTemp("", "memdb-cli-*.db")
	if err != nil {
		return nil, nil, fmt.Errorf("create temp: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { os.Remove(tmpName) }

	isLegacy, err := memdb.UnwrapSnapshot(in, tmp)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("unwrap snapshot: %w", err)
	}
	if isLegacy {
		fmt.Fprintln(os.Stderr,
			"memdb-cli: snapshot has no MDBK integrity wrap (legacy raw SQLite); "+
				"loading without checksum verification")
	}

	// mode=ro&immutable=1 — SQLite enforces no writes; the immutable=1
	// flag also disables the WAL checkpoint attempt SQLite would
	// otherwise make on first open.
	dsn := "file:" + url.PathEscape(tmpName) + "?mode=ro&immutable=1"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("open sqlite: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		cleanup()
		return nil, nil, fmt.Errorf("ping sqlite: %w", err)
	}
	return db, cleanup, nil
}

// repl runs the interactive read-eval-print loop until EOF, ctrl-c
// at an empty prompt, or .exit/.quit.
//
// Multi-line input: lines accumulate until a `;` is seen at end (after
// trimming trailing whitespace). Meta-commands beginning with `.` are
// always single-line and execute immediately.
//
// The line reader is github.com/peterh/liner — it provides line
// editing, history (up/down arrows, ctrl-r reverse search), and tab
// completion when stdin is a terminal. When stdin is redirected
// (pipe, here-doc), liner falls back to a plain bufio reader so
// scripted use of the CLI keeps working.
//
// historyPath is loaded on entry and rewritten on exit; an empty
// string disables persistent history.
//
// modeTag distinguishes the welcome banner ("snapshot (read-only)"
// vs "wire") so operators see immediately whether writes will go
// through. The two modes share everything else.
func repl(db *sql.DB, out *printer, historyPath, modeTag string) {
	switch modeTag {
	case "wire":
		fmt.Println("memdb-cli — wire mode (read+write). Type .help for commands, .quit to exit.")
	default:
		fmt.Println("memdb-cli — snapshot mode (read-only). Type .help for commands, .quit to exit.")
	}

	line := liner.NewLiner()
	defer line.Close()
	line.SetCtrlCAborts(true) // ctrl-c on empty prompt → io.EOF, exit cleanly
	line.SetMultiLineMode(true)

	tables := loadTableNames(db) // cached for the completer; schema is static for a snapshot
	line.SetWordCompleter(makeCompleter(tables))

	if historyPath != "" {
		if f, err := os.Open(historyPath); err == nil {
			_, _ = line.ReadHistory(f)
			f.Close()
		}
		// Persist on exit. Best-effort — a write failure should not
		// mask a successful inspection session.
		defer func() {
			if err := writeHistory(historyPath, line); err != nil {
				fmt.Fprintf(os.Stderr, "memdb-cli: history save failed: %v\n", err)
			}
		}()
	}

	var buf strings.Builder
	for {
		prompt := "memdb> "
		if buf.Len() > 0 {
			prompt = "   ...> "
		}
		input, err := line.Prompt(prompt)
		if err != nil {
			// liner returns io.EOF on ctrl-d (and on ctrl-c when
			// SetCtrlCAborts is true). Either is "done".
			if errors.Is(err, io.EOF) || errors.Is(err, liner.ErrPromptAborted) {
				fmt.Println()
				return
			}
			fmt.Fprintf(os.Stderr, "memdb-cli: input error: %v\n", err)
			return
		}

		// Meta-commands only at the start of a fresh statement.
		if buf.Len() == 0 && strings.HasPrefix(strings.TrimSpace(input), ".") {
			line.AppendHistory(input)
			if done := runMeta(db, strings.TrimSpace(input), out); done {
				return
			}
			continue
		}

		// Always remember the line as typed, before merging into the
		// statement buffer — keeps history granular for editing.
		if strings.TrimSpace(input) != "" {
			line.AppendHistory(input)
		}

		buf.WriteString(input)
		buf.WriteByte('\n')

		// Statement terminator: trailing `;` after optional whitespace.
		if strings.HasSuffix(strings.TrimRight(input, " \t"), ";") {
			stmt := strings.TrimSpace(buf.String())
			buf.Reset()
			if stmt != ";" && stmt != "" {
				if err := runStatement(db, stmt, out); err != nil {
					fmt.Fprintf(os.Stderr, "Error: %s\n", friendlyError(err))
				}
			}
		}
	}
}

// writeHistory persists the in-memory history to historyPath via a
// temp-file + rename, so a crash mid-write cannot corrupt the file
// and replace good history with a half-flushed truncation.
func writeHistory(historyPath string, line *liner.State) error {
	if err := os.MkdirAll(filepath.Dir(historyPath), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(historyPath), ".memdb-cli-history-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := line.WriteHistory(tmp); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, historyPath)
}

// defaultHistoryPath returns the XDG-conformant default for the
// persistent history file. Falls back to the OS-specific user-state
// dir, then to /tmp if all else fails — better to keep history
// somewhere ephemeral than silently lose it.
func defaultHistoryPath() string {
	if x := os.Getenv("XDG_STATE_HOME"); x != "" {
		return filepath.Join(x, "memdb-cli", "history")
	}
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".local", "state", "memdb-cli", "history")
	}
	return filepath.Join(os.TempDir(), "memdb-cli-history")
}

// loadTableNames reads the user-table list once at REPL start.
// Snapshot schema does not change between queries (the CLI is
// read-only and immutable=1) so a one-shot load is correct;
// callers that mutate the underlying file should restart the CLI.
//
// Best-effort: a failed load returns nil and the completer simply
// has no table names to suggest — the rest of completion still
// works.
func loadTableNames(db *sql.DB) []string {
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	if err != nil {
		return nil
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err == nil {
			names = append(names, n)
		}
	}
	return names
}

// metaCommands is the static list of dot-commands the completer
// proposes. Kept in sync by hand with the runMeta switch — the
// trailing space disambiguates from prefix matches.
var metaCommands = []string{
	".databases", ".echo", ".exit", ".headers", ".help", ".indexes",
	".indices", ".mode", ".quit", ".read", ".schema", ".separator",
	".show", ".tables", ".timer", ".version",
}

// sqlKeywords is a small set of common SQL keywords for the
// "I don't know what context you're in" fallback case. Kept short
// on purpose — completion floods are worse than no completion.
var sqlKeywords = []string{
	"SELECT", "FROM", "WHERE", "ORDER BY", "GROUP BY", "HAVING",
	"LIMIT", "OFFSET", "JOIN", "LEFT JOIN", "INNER JOIN", "ON",
	"AS", "AND", "OR", "NOT", "IN", "LIKE", "IS NULL", "IS NOT NULL",
	"COUNT(*)", "DISTINCT", "WITH", "UNION", "EXPLAIN", "PRAGMA",
}

// tableContextWords are the (case-insensitive) tokens whose
// immediately following word is a table name — the completer
// switches to table-name suggestions after seeing one of these.
var tableContextWords = map[string]bool{
	"from": true, "join": true, "into": true, "update": true,
	"table": true, ".schema": true, ".indexes": true, ".indices": true,
}

// makeCompleter returns a liner.WordCompleter that proposes meta-
// commands, table names, or SQL keywords depending on what comes
// before the cursor. The closure captures the table list so the
// completer is independent of the *sql.DB after construction —
// makes it trivial to unit-test in isolation.
//
// Decision tree, applied in order:
//
//  1. Word being completed starts with `.`     → meta-commands
//  2. Previous word is FROM/JOIN/INTO/UPDATE/
//     TABLE/.schema/.indexes (case-insensitive) → table names
//  3. Otherwise                                → tables ∪ keywords
//
// Empty prefix returns no suggestions — dumping the whole keyword
// list on every space is noise rather than help.
func makeCompleter(tables []string) liner.WordCompleter {
	sort.Strings(tables)
	return func(line string, pos int) (head string, completions []string, tail string) {
		if pos > len(line) {
			pos = len(line)
		}
		// Find the start of the word being completed: scan back
		// from pos until whitespace or an opening delimiter.
		start := pos
		for start > 0 {
			c := line[start-1]
			if c == ' ' || c == '\t' || c == '(' || c == ',' {
				break
			}
			start--
		}
		word := line[start:pos]
		head = line[:start]
		tail = line[pos:]

		switch {
		case strings.HasPrefix(word, "."):
			completions = matchPrefix(metaCommands, word)

		case tableContextWords[strings.ToLower(previousWord(head))]:
			completions = matchPrefix(tables, word)

		default:
			completions = append(completions, matchPrefix(tables, word)...)
			completions = append(completions, matchPrefix(sqlKeywords, word)...)
		}
		return
	}
}

// previousWord returns the last whitespace-separated token in s.
// Used by makeCompleter to detect "the word right before the
// cursor" without parsing SQL.
func previousWord(s string) string {
	s = strings.TrimRight(s, " \t")
	if s == "" {
		return ""
	}
	i := strings.LastIndexAny(s, " \t\n")
	if i < 0 {
		return s
	}
	return s[i+1:]
}

// matchPrefix returns every candidate that case-insensitively
// starts with prefix. Preserves the candidate's original casing
// (so SELECT stays SELECT) — readability over consistency.
func matchPrefix(candidates []string, prefix string) []string {
	if prefix == "" {
		return nil
	}
	lp := strings.ToLower(prefix)
	var out []string
	for _, c := range candidates {
		if strings.HasPrefix(strings.ToLower(c), lp) {
			out = append(out, c)
		}
	}
	return out
}

// runMeta dispatches a `.command` line. Returns true if the REPL
// should exit.
func runMeta(db *sql.DB, line string, out *printer) (exit bool) {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return false
	}
	switch strings.ToLower(fields[0]) {
	case ".exit", ".quit":
		return true
	case ".help":
		printHelp()
	case ".tables":
		q := "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
		if err := runStatement(db, q, out); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %s\n", friendlyError(err))
		}
	case ".schema":
		var q string
		if len(fields) >= 2 {
			q = fmt.Sprintf("SELECT sql FROM sqlite_master WHERE name=%s", sqlQuote(fields[1]))
		} else {
			q = "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type, name"
		}
		if err := runStatement(db, q, out); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %s\n", friendlyError(err))
		}
	case ".indexes", ".indices":
		var q string
		if len(fields) >= 2 {
			q = fmt.Sprintf("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=%s ORDER BY name", sqlQuote(fields[1]))
		} else {
			q = "SELECT name FROM sqlite_master WHERE type='index' ORDER BY name"
		}
		if err := runStatement(db, q, out); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %s\n", friendlyError(err))
		}
	case ".databases":
		// PRAGMA database_list returns seq, name, file — sufficient
		// for inspection. memdb-cli never ATTACHes, so this normally
		// shows just `main` and the temp file path.
		if err := runStatement(db, "PRAGMA database_list", out); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %s\n", friendlyError(err))
		}
	case ".headers":
		if len(fields) >= 2 {
			out.headers = parseBool(fields[1])
		}
		fmt.Printf("headers = %v\n", out.headers)
	case ".mode":
		if len(fields) >= 2 {
			m := strings.ToLower(fields[1])
			switch m {
			case "column", "list", "line", "csv":
				out.mode = m
			default:
				fmt.Fprintf(os.Stderr, "unknown mode %q (want column|list|line|csv)\n", fields[1])
				return false
			}
		}
		fmt.Printf("mode = %s\n", out.mode)
	case ".separator":
		if len(fields) >= 2 {
			// Allow common escapes so users can set tab/newline.
			out.separator = unescape(fields[1])
		}
		fmt.Printf("separator = %q\n", out.separator)
	case ".timer":
		if len(fields) >= 2 {
			out.timer = parseBool(fields[1])
		}
		fmt.Printf("timer = %v\n", out.timer)
	case ".echo":
		if len(fields) >= 2 {
			out.echo = parseBool(fields[1])
		}
		fmt.Printf("echo = %v\n", out.echo)
	case ".show":
		fmt.Printf("headers   = %v\nmode      = %s\nseparator = %q\ntimer     = %v\necho      = %v\n",
			out.headers, out.mode, out.separator, out.timer, out.echo)
	case ".read":
		if len(fields) < 2 {
			fmt.Fprintln(os.Stderr, "usage: .read FILE")
			return false
		}
		if err := runFile(db, fields[1], out); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %s\n", friendlyError(err))
		}
	case ".version":
		fmt.Println("memdb-cli — read-only inspection shell for memdb snapshots")
	default:
		fmt.Fprintf(os.Stderr, "unknown meta-command %q — type .help\n", fields[0])
	}
	return false
}

func printHelp() {
	fmt.Println(`Meta-commands:
  .help                  Show this help
  .tables                List tables in the snapshot
  .schema [name]         Show CREATE statements (all, or for one object)
  .indexes [table]       List indexes (all, or for one table)
  .databases             List attached databases (PRAGMA database_list)
  .headers on|off        Toggle column headers in output
  .mode MODE             column | list | line | csv
  .separator STR         Field separator for list/csv modes (\t, \n recognised)
  .timer on|off          Print elapsed time after each statement
  .echo on|off           Echo each SQL statement before executing
  .show                  Print current settings
  .read FILE             Execute SQL read from FILE
  .version               Print tool description
  .quit / .exit          Leave memdb-cli

Anything else is treated as SQL. memdb-cli is READ-ONLY — INSERT,
UPDATE, DELETE, CREATE, DROP, etc. will be rejected by SQLite.

Statements may span multiple lines and must end with a semicolon.
For live (non-snapshot) data, connect via psql to the running
memdb server instead.`)
}

// runFile executes a file's worth of SQL, splitting on `;` line
// terminators (the same rule the REPL uses interactively). Stops at
// the first error so a typo in the middle of a script does not run
// trailing statements against unintended state.
func runFile(db *sql.DB, path string, out *printer) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	var buf strings.Builder
	for scanner.Scan() {
		line := scanner.Text()
		buf.WriteString(line)
		buf.WriteByte('\n')
		if strings.HasSuffix(strings.TrimRight(line, " \t"), ";") {
			stmt := strings.TrimSpace(buf.String())
			buf.Reset()
			if stmt == ";" || stmt == "" {
				continue
			}
			if err := runStatement(db, stmt, out); err != nil {
				return err
			}
		}
	}
	return scanner.Err()
}

func parseBool(s string) bool {
	switch strings.ToLower(s) {
	case "on", "1", "true", "yes":
		return true
	default:
		return false
	}
}

// unescape translates the conventional sqlite3 separator escapes so
// `.separator \t` does what users expect.
func unescape(s string) string {
	r := strings.NewReplacer(`\t`, "\t", `\n`, "\n", `\r`, "\r", `\\`, `\`)
	return r.Replace(s)
}

// runStatement executes one SQL statement against the read-only DB.
//
// Always uses Query (not Exec) — Query is a superset that handles
// both row-returning SELECTs and statements that return zero rows
// without losing diagnostics. Critically: db.Query("INSERT ...") in
// `mode=ro` returns a nil error from Query itself; the
// "readonly database" failure surfaces only on rows.Err() after
// rows.Next() steps the prepared statement. We must always drive
// the iteration and check rows.Err(), even when the statement
// returns no columns, or write attempts will silently appear to
// succeed.
func runStatement(db *sql.DB, sqlStmt string, out *printer) error {
	if out.echo {
		fmt.Fprintln(out.w, sqlStmt)
	}
	start := time.Now()
	rows, err := db.Query(sqlStmt)
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	if len(cols) == 0 {
		// Drive iteration so the underlying sqlite3_step is actually
		// called — this is what surfaces a SQLITE_READONLY error from
		// an INSERT/UPDATE/DELETE that snuck past us. Then check
		// rows.Err() before reporting success.
		for rows.Next() { //nolint:revive // intentional — drain to surface step errors
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if out.timer {
			fmt.Fprintf(out.w, "Time: %s\n", time.Since(start))
		}
		return nil
	}

	out.printRows(cols, rows)
	if err := rows.Err(); err != nil {
		return err
	}
	if out.timer {
		fmt.Fprintf(out.w, "Time: %s\n", time.Since(start))
	}
	return nil
}

// printer formats query results in one of several display modes.
//
// Buffering: column and line modes read all rows into memory so
// widths can be computed from the actual data. list and csv modes
// stream — the choice is made per-mode in printRows.
type printer struct {
	w         io.Writer
	headers   bool
	mode      string // column | list | line | csv
	separator string // used by list/csv modes only
	timer     bool
	echo      bool
}

func newPrinter(w io.Writer, headers bool, mode, separator string) *printer {
	if mode == "" {
		mode = "column"
	}
	return &printer{w: w, headers: headers, mode: mode, separator: separator}
}

func (p *printer) printRows(cols []string, rows *sql.Rows) {
	switch p.mode {
	case "list":
		p.printList(cols, rows)
	case "line":
		p.printLine(cols, rows)
	case "csv":
		p.printCSV(cols, rows)
	default: // "column"
		p.printColumn(cols, rows)
	}
}

// printColumn buffers all rows so widths can be sized to the widest
// cell (including header). Output:
//
//	id  name   email
//	--  ----   -----
//	1   alice  a@x.com
func (p *printer) printColumn(cols []string, rows *sql.Rows) {
	data, err := scanAll(cols, rows)
	if err != nil {
		fmt.Fprintf(os.Stderr, "scan error: %v\n", err)
		return
	}

	widths := make([]int, len(cols))
	if p.headers {
		for i, c := range cols {
			widths[i] = len(c)
		}
	}
	for _, row := range data {
		for i, cell := range row {
			if len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}

	const colSep = "  "
	if p.headers {
		writeFixedWidthRow(p.w, cols, widths, colSep)
		sep := make([]string, len(cols))
		for i, w := range widths {
			sep[i] = strings.Repeat("-", w)
		}
		writeFixedWidthRow(p.w, sep, widths, colSep)
	}
	for _, row := range data {
		writeFixedWidthRow(p.w, row, widths, colSep)
	}
	printRowCount(p.w, len(data))
}

// printList streams rows separated by p.separator (sqlite3 default
// "|"). Lower memory than column mode for large result sets.
func (p *printer) printList(cols []string, rows *sql.Rows) {
	if p.headers {
		fmt.Fprintln(p.w, strings.Join(cols, p.separator))
	}
	n := 0
	row := make([]string, len(cols))
	raw := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range raw {
		ptrs[i] = &raw[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			fmt.Fprintf(os.Stderr, "scan error: %v\n", err)
			return
		}
		for i, v := range raw {
			row[i] = formatCell(v)
		}
		fmt.Fprintln(p.w, strings.Join(row, p.separator))
		n++
	}
	printRowCount(p.w, n)
}

// printLine prints one column-per-line (sqlite3 ".mode line"). Useful
// when rows are wide and a single record needs vertical inspection.
func (p *printer) printLine(cols []string, rows *sql.Rows) {
	maxName := 0
	for _, c := range cols {
		if len(c) > maxName {
			maxName = len(c)
		}
	}
	n := 0
	raw := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range raw {
		ptrs[i] = &raw[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			fmt.Fprintf(os.Stderr, "scan error: %v\n", err)
			return
		}
		if n > 0 {
			fmt.Fprintln(p.w)
		}
		for i, v := range raw {
			fmt.Fprintf(p.w, "%*s = %s\n", maxName, cols[i], formatCell(v))
		}
		n++
	}
	printRowCount(p.w, n)
}

// printCSV writes RFC 4180 CSV using encoding/csv so quoting and
// escaping is correct for cells that contain commas, quotes, or
// newlines.
func (p *printer) printCSV(cols []string, rows *sql.Rows) {
	cw := csv.NewWriter(p.w)
	if p.headers {
		_ = cw.Write(cols)
	}
	n := 0
	row := make([]string, len(cols))
	raw := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range raw {
		ptrs[i] = &raw[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			fmt.Fprintf(os.Stderr, "scan error: %v\n", err)
			cw.Flush()
			return
		}
		for i, v := range raw {
			row[i] = formatCell(v)
		}
		_ = cw.Write(row)
		n++
	}
	cw.Flush()
	printRowCount(p.w, n)
}

// scanAll buffers every row from rows into a [][]string for column
// mode (which needs all data before sizing column widths).
//
// The raw and ptrs scratch slices are hoisted out of the per-row
// loop — Scan(ptrs...) overwrites raw[i] each call, and formatCell
// converts to a fresh string (string([]byte) copies; primitive
// renderings allocate) so we can safely reuse the scratch. This
// roughly halves the per-row allocation count compared with
// allocating raw/ptrs inside the loop.
func scanAll(cols []string, rows *sql.Rows) ([][]string, error) {
	raw := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range raw {
		ptrs[i] = &raw[i]
	}
	var data [][]string
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make([]string, len(cols))
		for i, v := range raw {
			row[i] = formatCell(v)
		}
		data = append(data, row)
	}
	return data, nil
}

// padSpaces is a 256-byte slab of ASCII spaces used by
// writeFixedWidthRow for left-pad of column cells. Sized large
// enough to satisfy any realistic single-column padding in one
// io.WriteString; the loop below covers wider cases.
const padSpaces = "                                                                " +
	"                                                                " +
	"                                                                " +
	"                                                                "

// writeFixedWidthRow writes cells separated by sep with each cell
// left-aligned in widths[i] columns.
//
// Avoids fmt.Fprintf("%-*s", w, cell): the dynamic-width %s verb
// allocates a per-call padding buffer (~33 M small allocs in the
// 10k-row Wide bench). This version uses a fixed string slab and
// io.WriteString — zero allocations on the steady state.
func writeFixedWidthRow(w io.Writer, cells []string, widths []int, sep string) {
	for i, cell := range cells {
		if i > 0 {
			_, _ = io.WriteString(w, sep)
		}
		_, _ = io.WriteString(w, cell)
		pad := widths[i] - len(cell)
		for pad > 0 {
			n := pad
			if n > len(padSpaces) {
				n = len(padSpaces)
			}
			_, _ = io.WriteString(w, padSpaces[:n])
			pad -= n
		}
	}
	_, _ = io.WriteString(w, "\n")
}

func printRowCount(w io.Writer, n int) {
	if n == 0 {
		fmt.Fprintln(w, "(0 rows)")
	} else {
		fmt.Fprintf(w, "(%d row%s)\n", n, pluralS(n))
	}
}

// formatCell renders a value scanned via *any into a printable string.
// SQLite via mattn/go-sqlite3 returns []byte for TEXT and BLOB; int64
// for INTEGER; float64 for REAL; nil for NULL; time.Time for the
// configured datetime columns.
func formatCell(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case []byte:
		return string(x)
	case string:
		return x
	default:
		return fmt.Sprintf("%v", x)
	}
}

func pluralS(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// friendlyError translates SQLite's terse "attempt to write a
// readonly database" into a clear "this tool is for inspection only"
// hint, while leaving other errors untouched.
func friendlyError(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	if strings.Contains(msg, "readonly database") || strings.Contains(msg, "read-only database") {
		return "memdb-cli is for inspection only — writes (INSERT/UPDATE/DELETE/CREATE/DROP) are not permitted. " +
			"To modify data, connect to a running 'memdb serve' via psql."
	}
	// errors.Is hooks for any future sentinel checks land here.
	if errors.Is(err, sql.ErrNoRows) {
		return "no rows"
	}
	return msg
}

// sqlQuote wraps a string identifier in single quotes with internal
// quotes doubled — sufficient for the literal table/index names
// supplied to .schema and .indexes meta-commands.
func sqlQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
