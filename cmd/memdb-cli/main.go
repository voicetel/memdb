// memdb-cli is a read-only inspection shell for memdb snapshot files,
// modelled on the sqlite3 CLI but limited to SELECT-style queries.
//
// # Usage
//
//	memdb-cli -file /var/lib/myapp/data.db          # interactive REPL
//	memdb-cli -file /var/lib/myapp/data.db -c "SELECT * FROM users LIMIT 5"
//
// # When the daemon is running
//
// The snapshot file may be opened safely while `memdb serve` is writing
// to it: the server replaces the file atomically via os.Rename, so the
// inode this tool opens is a stable, point-in-time view. The data may
// lag the live in-memory state by up to one flush interval (default
// 30s). For absolute freshness, connect via psql against the live
// PostgreSQL wire-protocol port instead.
//
// # When the daemon is not running
//
// Just point -file at the snapshot. The on-disk file is the source of
// truth in this case.
//
// # Read-only enforcement
//
// memdb-cli opens the unwrapped SQLite payload with `mode=ro&immutable=1`
// — any INSERT/UPDATE/DELETE/CREATE returns a "readonly database"
// error from SQLite, which the REPL catches and prints as a friendly
// "memdb-cli is for inspection only" message.
package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/voicetel/memdb"
)

func main() {
	fs := flag.NewFlagSet("memdb-cli", flag.ExitOnError)
	file := fs.String("file", "", "path to memdb snapshot file (required)")
	cmd := fs.String("c", "", "execute a single SQL statement and exit (non-interactive)")
	headers := fs.Bool("headers", true, "print column headers in tabular output")
	mode := fs.String("mode", "column", "output mode: column|list|line|csv")
	separator := fs.String("separator", "|", "column separator for list/csv modes (column mode uses fixed-width)")
	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(1)
	}
	if *file == "" {
		fs.Usage()
		fmt.Fprintln(os.Stderr, "\nmemdb-cli: -file is required")
		os.Exit(1)
	}

	db, cleanup, err := openSnapshot(*file)
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

	repl(db, out)
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

// repl runs the interactive read-eval-print loop until EOF or .exit/.quit.
//
// Multi-line input: lines accumulate until a `;` is seen at end (after
// trimming trailing whitespace). Meta-commands beginning with `.` are
// always single-line and execute immediately.
func repl(db *sql.DB, out *printer) {
	fmt.Println("memdb-cli — read-only snapshot shell. Type .help for commands, .quit to exit.")

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	var buf strings.Builder

	prompt := func() {
		if buf.Len() == 0 {
			fmt.Print("memdb> ")
		} else {
			fmt.Print("   ...> ")
		}
	}

	prompt()
	for scanner.Scan() {
		line := scanner.Text()

		// Meta-commands only at the start of a fresh statement.
		if buf.Len() == 0 && strings.HasPrefix(strings.TrimSpace(line), ".") {
			if done := runMeta(db, strings.TrimSpace(line), out); done {
				return
			}
			prompt()
			continue
		}

		buf.WriteString(line)
		buf.WriteByte('\n')

		// Statement terminator detection: trailing `;` after optional
		// whitespace.
		if strings.HasSuffix(strings.TrimRight(line, " \t"), ";") {
			stmt := strings.TrimSpace(buf.String())
			buf.Reset()
			if stmt != ";" && stmt != "" {
				if err := runStatement(db, stmt, out); err != nil {
					fmt.Fprintf(os.Stderr, "Error: %s\n", friendlyError(err))
				}
			}
		}
		prompt()
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "memdb-cli: input error: %v\n", err)
	}
	fmt.Println()
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
