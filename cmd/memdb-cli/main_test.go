package main

import (
	"bytes"
	"database/sql"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/peterh/liner"
	"github.com/voicetel/memdb"
)

// openTestSnapshot is the standard test setup — build a small
// wrapped snapshot with `rows` rows in the users table, open it
// through openSnapshot exactly the way the CLI does, and return the
// *sql.DB plus a deferred-cleanup helper.
//
// Reuses fixturePath from cli_bench_test.go (same package).
func openTestSnapshot(tb testing.TB, rows int) (*sql.DB, func()) {
	tb.Helper()
	path := fixturePath(tb, rows)
	db, cleanup, err := openSnapshot(path)
	if err != nil {
		tb.Fatalf("openSnapshot: %v", err)
	}
	return db, func() {
		db.Close()
		cleanup()
	}
}

// TestOpenSnapshot covers the file-open contract: wrapped MDBK
// snapshots must open cleanly, missing files must report a useful
// error, and a corrupt MDBK envelope must surface as
// ErrSnapshotCorrupt.
func TestOpenSnapshot(t *testing.T) {
	t.Run("Wrapped", func(t *testing.T) {
		path := fixturePath(t, 10)
		db, cleanup, err := openSnapshot(path)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() {
			db.Close()
			cleanup()
		})
		var n int
		if err := db.QueryRow("SELECT COUNT(*) FROM users").Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		if n != 10 {
			t.Errorf("got %d rows, want 10", n)
		}
	})

	t.Run("Missing", func(t *testing.T) {
		_, _, err := openSnapshot(filepath.Join(t.TempDir(), "no-such.db"))
		if err == nil {
			t.Fatal("expected error opening missing file, got nil")
		}
		if !strings.Contains(err.Error(), "open snapshot") {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("Corrupt", func(t *testing.T) {
		path := fixturePath(t, 10)
		// Flip a byte in the SHA-256 footer (last 32 bytes).
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		stat, _ := f.Stat()
		if _, err := f.WriteAt([]byte{0xFF}, stat.Size()-1); err != nil {
			t.Fatal(err)
		}
		f.Close()

		_, _, err = openSnapshot(path)
		if err == nil {
			t.Fatal("expected error opening corrupt snapshot, got nil")
		}
		if !errors.Is(err, memdb.ErrSnapshotCorrupt) {
			t.Errorf("want ErrSnapshotCorrupt, got %v", err)
		}
	})
}

// TestRunStatement_RejectsWrites is the load-bearing test for the
// CLI's read-only promise. Every DML/DDL form must round-trip
// through SQLITE_READONLY and surface as the "inspection only"
// friendly error. If any of these silently succeed the tool has
// failed its core invariant.
func TestRunStatement_RejectsWrites(t *testing.T) {
	db, cleanup := openTestSnapshot(t, 5)
	t.Cleanup(cleanup)

	out := newPrinter(io.Discard, true, "column", "|")

	cases := []struct {
		name string
		sql  string
	}{
		{"Insert", "INSERT INTO users(name) VALUES('mallory')"},
		{"Update", "UPDATE users SET name='nope' WHERE id=1"},
		{"Delete", "DELETE FROM users WHERE id=1"},
		{"Create", "CREATE TABLE evil(x INT)"},
		{"Drop", "DROP TABLE users"},
		{"AlterTable", "ALTER TABLE users ADD COLUMN evil TEXT"},
		{"CreateIndex", "CREATE INDEX idx_users_name ON users(name)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := runStatement(db, tc.sql, out)
			if err == nil {
				t.Fatal("write was not rejected — read-only invariant broken")
			}
			msg := friendlyError(err)
			if !strings.Contains(msg, "inspection only") {
				t.Errorf("expected friendly 'inspection only' message, got: %s", msg)
			}
		})
	}
}

// TestRunStatement_AllowsReads verifies that the read paths SQLite
// accepts in mode=ro work — SELECT, PRAGMA (the read-only forms),
// EXPLAIN, and CTEs (WITH … SELECT). These are the queries users
// will actually issue.
func TestRunStatement_AllowsReads(t *testing.T) {
	db, cleanup := openTestSnapshot(t, 5)
	t.Cleanup(cleanup)
	out := newPrinter(io.Discard, true, "column", "|")

	cases := []struct {
		name, sql string
	}{
		{"Select", "SELECT * FROM users"},
		{"SelectCount", "SELECT COUNT(*) FROM users"},
		{"PragmaTableInfo", "PRAGMA table_info(users)"},
		{"Explain", "EXPLAIN QUERY PLAN SELECT * FROM users"},
		{"WithCTE", "WITH x AS (SELECT id FROM users) SELECT COUNT(*) FROM x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := runStatement(db, tc.sql, out); err != nil {
				t.Errorf("read should succeed, got: %v", err)
			}
		})
	}
}

// TestPrinterModes verifies each output mode produces parseable
// output containing the expected row data. Detailed format choices
// (exact column widths, padding rules) are intentionally NOT
// asserted — only that headers and data appear and that mode-
// specific separators / structure are right.
func TestPrinterModes(t *testing.T) {
	db, cleanup := openTestSnapshot(t, 3)
	t.Cleanup(cleanup)

	cases := []struct {
		name string
		mode string
		// substrings that MUST appear in output
		wantContains []string
	}{
		{
			name:         "Column",
			mode:         "column",
			wantContains: []string{"id", "name", "user-0000000", "(3 rows)"},
		},
		{
			name:         "List",
			mode:         "list",
			wantContains: []string{"id|name", "1|user-0000000", "(3 rows)"},
		},
		{
			name:         "CSV",
			mode:         "csv",
			wantContains: []string{"id,name", "1,user-0000000", "(3 rows)"},
		},
		{
			name:         "Line",
			mode:         "line",
			wantContains: []string{"id = 1", "name = user-0000000", "(3 rows)"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			out := newPrinter(&buf, true, tc.mode, "|")
			if err := runStatement(db, "SELECT id, name FROM users ORDER BY id", out); err != nil {
				t.Fatal(err)
			}
			got := buf.String()
			for _, want := range tc.wantContains {
				if !strings.Contains(got, want) {
					t.Errorf("mode=%s: output missing %q\nfull output:\n%s", tc.mode, want, got)
				}
			}
		})
	}
}

// TestPrinterHeadersOff verifies that .headers off (or the -headers=false
// flag) suppresses the header row across all modes.
func TestPrinterHeadersOff(t *testing.T) {
	db, cleanup := openTestSnapshot(t, 2)
	t.Cleanup(cleanup)

	for _, mode := range []string{"column", "list", "csv"} {
		t.Run(mode, func(t *testing.T) {
			var buf bytes.Buffer
			out := newPrinter(&buf, false, mode, "|")
			if err := runStatement(db, "SELECT id, name FROM users", out); err != nil {
				t.Fatal(err)
			}
			got := buf.String()
			// The literal column NAME "name" appears in row data
			// (user-0000000 contains no "name"); a true header line
			// would start with "id" alone. Check for the absence of
			// the dashed separator that column mode emits and the
			// absence of "id|name" / "id,name" header lines.
			if strings.Contains(got, "id|name") || strings.Contains(got, "id,name") {
				t.Errorf("headers=false but header line is present:\n%s", got)
			}
			if strings.Contains(got, "----") {
				t.Errorf("headers=false but dashed separator is present:\n%s", got)
			}
		})
	}
}

// TestRunMeta covers the interactive meta-commands. The key cases
// are .quit (must signal exit), .tables (must surface schema), and
// .schema (must emit CREATE statements). Settings toggles like
// .headers / .mode / .echo / .timer just print state and don't
// need behavioural tests beyond their documented effect captured
// elsewhere.
func TestRunMeta(t *testing.T) {
	db, cleanup := openTestSnapshot(t, 1)
	t.Cleanup(cleanup)

	t.Run("Quit", func(t *testing.T) {
		out := newPrinter(io.Discard, true, "column", "|")
		if !runMeta(db, ".quit", out) {
			t.Error(".quit must return true to exit the REPL")
		}
		if !runMeta(db, ".exit", out) {
			t.Error(".exit must return true to exit the REPL")
		}
	})

	t.Run("Tables", func(t *testing.T) {
		var buf bytes.Buffer
		out := newPrinter(&buf, true, "list", "|")
		runMeta(db, ".tables", out)
		if !strings.Contains(buf.String(), "users") {
			t.Errorf(".tables did not list 'users':\n%s", buf.String())
		}
	})

	t.Run("Schema", func(t *testing.T) {
		var buf bytes.Buffer
		out := newPrinter(&buf, true, "list", "|")
		runMeta(db, ".schema users", out)
		got := buf.String()
		if !strings.Contains(got, "CREATE TABLE") || !strings.Contains(got, "users") {
			t.Errorf(".schema users did not show CREATE TABLE:\n%s", got)
		}
	})

	t.Run("Settings", func(t *testing.T) {
		// Toggling settings should mutate printer state, not error.
		out := newPrinter(io.Discard, true, "column", "|")
		runMeta(db, ".headers off", out)
		if out.headers {
			t.Error(".headers off did not disable headers")
		}
		runMeta(db, ".mode csv", out)
		if out.mode != "csv" {
			t.Errorf(".mode csv: got %q", out.mode)
		}
		runMeta(db, ".timer on", out)
		if !out.timer {
			t.Error(".timer on did not enable timer")
		}
	})
}

// TestFriendlyError pins the error-translation contract: SQLite's
// "readonly database" must become the inspection-only message;
// other errors pass through unchanged so users see real diagnostics
// for typos and similar.
func TestFriendlyError(t *testing.T) {
	cases := []struct {
		name        string
		err         error
		wantMatch   string
		wantPassthr bool
	}{
		{"Nil", nil, "", false},
		{"ReadonlyClassic", errors.New("attempt to write a readonly database"),
			"inspection only", false},
		{"ReadonlyDashed", errors.New("attempt to write a read-only database"),
			"inspection only", false},
		{"NoRows", sql.ErrNoRows, "no rows", false},
		{"Other", errors.New("syntax error near foo"), "syntax error near foo", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := friendlyError(tc.err)
			if tc.err == nil && got != "" {
				t.Errorf("friendlyError(nil) = %q, want empty", got)
				return
			}
			if tc.wantMatch != "" && !strings.Contains(got, tc.wantMatch) {
				t.Errorf("friendlyError(%v) = %q, want it to contain %q", tc.err, got, tc.wantMatch)
			}
			if tc.wantPassthr && got != tc.err.Error() {
				t.Errorf("friendlyError(%v) = %q, expected unchanged passthrough", tc.err, got)
			}
		})
	}
}

// TestUnescape verifies that the conventional sqlite3 separator
// escapes are translated for the .separator meta-command.
func TestUnescape(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{`\t`, "\t"},
		{`\n`, "\n"},
		{`\r`, "\r"},
		{`\\`, `\`},
		{`,`, ","},
		{`|`, "|"},
		{`a\tb`, "a\tb"},
	}
	for _, tc := range cases {
		if got := unescape(tc.in); got != tc.want {
			t.Errorf("unescape(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestParseBool covers the on/off/1/true accept set used by every
// boolean meta-command.
func TestParseBool(t *testing.T) {
	for _, s := range []string{"on", "ON", "1", "true", "TRUE", "yes"} {
		if !parseBool(s) {
			t.Errorf("parseBool(%q) = false, want true", s)
		}
	}
	for _, s := range []string{"off", "0", "false", "no", "", "garbage"} {
		if parseBool(s) {
			t.Errorf("parseBool(%q) = true, want false", s)
		}
	}
}

// TestCompleter pins the decision tree behind tab completion: meta
// commands when the word starts with `.`, table names after
// FROM/JOIN/etc., the union of tables and keywords otherwise. The
// closure form of makeCompleter accepts a fixed table list so this
// test is independent of the *sql.DB.
func TestCompleter(t *testing.T) {
	tables := []string{"users", "orders", "products"}
	complete := makeCompleter(tables)

	cases := []struct {
		name        string
		line        string
		pos         int // -1 means "end of line"
		wantHead    string
		wantInclude []string // every entry must appear in completions
		wantExclude []string // none of these may appear
	}{
		{
			name:        "MetaCommandPrefix",
			line:        ".ta",
			wantHead:    "",
			wantInclude: []string{".tables"},
			wantExclude: []string{".help", "users"},
		},
		{
			name:        "MetaCommandFullList",
			line:        ".s",
			wantHead:    "",
			wantInclude: []string{".schema", ".separator", ".show"},
		},
		{
			name:        "TableNameAfterFROM",
			line:        "SELECT * FROM us",
			wantHead:    "SELECT * FROM ",
			wantInclude: []string{"users"},
			wantExclude: []string{"orders", "products"}, // wrong prefix
		},
		{
			name:        "TableNameAfterJOIN",
			line:        "SELECT * FROM users JOIN ord",
			wantHead:    "SELECT * FROM users JOIN ",
			wantInclude: []string{"orders"},
		},
		{
			name:        "TableNameAfterDotSchema",
			line:        ".schema us",
			wantHead:    ".schema ",
			wantInclude: []string{"users"},
		},
		{
			name:        "FallbackKeywordsAndTables",
			line:        "sel",
			wantHead:    "",
			wantInclude: []string{"SELECT"},
		},
		{
			name:        "EmptyPrefixSuggestsNothing",
			line:        "SELECT * FROM ",
			wantHead:    "SELECT * FROM ",
			wantExclude: []string{"users", "orders", "products"},
		},
		{
			name:        "CaseInsensitivePrefix",
			line:        "select * from US",
			wantHead:    "select * from ",
			wantInclude: []string{"users"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pos := tc.pos
			if pos < 0 {
				pos = len(tc.line)
			} else if pos == 0 && tc.pos == 0 {
				pos = len(tc.line)
			}
			head, completions, _ := complete(tc.line, pos)
			if head != tc.wantHead {
				t.Errorf("head = %q, want %q", head, tc.wantHead)
			}
			for _, want := range tc.wantInclude {
				if !contains(completions, want) {
					t.Errorf("completions missing %q; got %v", want, completions)
				}
			}
			for _, dontWant := range tc.wantExclude {
				if contains(completions, dontWant) {
					t.Errorf("completions should not contain %q; got %v", dontWant, completions)
				}
			}
		})
	}
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// TestMatchPrefix is a focused unit test for the case-insensitive
// prefix matcher used by the completer. Empty prefix returns nil so
// tab on whitespace doesn't dump the entire keyword list.
func TestMatchPrefix(t *testing.T) {
	cands := []string{"SELECT", "FROM", "users", "user_sessions"}
	cases := []struct {
		prefix string
		want   []string
	}{
		{"", nil},
		{"sel", []string{"SELECT"}},
		{"SEL", []string{"SELECT"}},
		{"user", []string{"users", "user_sessions"}},
		{"zzz", nil},
	}
	for _, tc := range cases {
		got := matchPrefix(cands, tc.prefix)
		if !equalStrings(got, tc.want) {
			t.Errorf("matchPrefix(%q) = %v, want %v", tc.prefix, got, tc.want)
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestPreviousWord — the helper that picks "the word just before
// the cursor" so the completer can decide whether we're after FROM,
// JOIN, etc.
func TestPreviousWord(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"", ""},
		{"   ", ""},
		{"SELECT * FROM ", "FROM"},
		{"SELECT * FROM users JOIN ", "JOIN"},
		{".schema ", ".schema"},
		{"single", "single"},
	}
	for _, tc := range cases {
		if got := previousWord(tc.in); got != tc.want {
			t.Errorf("previousWord(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestDefaultHistoryPath: XDG_STATE_HOME wins when set; otherwise
// the path is rooted under the user's home dir. We only check the
// suffix structure to avoid coupling to whichever HOME the test
// machine has.
func TestDefaultHistoryPath(t *testing.T) {
	t.Run("XDGStateHome", func(t *testing.T) {
		t.Setenv("XDG_STATE_HOME", "/tmp/xdg-state")
		got := defaultHistoryPath()
		want := "/tmp/xdg-state/memdb-cli/history"
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})
	t.Run("HomeFallback", func(t *testing.T) {
		t.Setenv("XDG_STATE_HOME", "")
		got := defaultHistoryPath()
		if !strings.Contains(got, filepath.Join(".local", "state", "memdb-cli", "history")) {
			t.Errorf("got %q, expected to contain .local/state/memdb-cli/history", got)
		}
	})
}

// TestLoadTableNames verifies the completer's data source: the
// snapshot's user tables (excluding sqlite_* internal tables).
func TestLoadTableNames(t *testing.T) {
	db, cleanup := openTestSnapshot(t, 1)
	t.Cleanup(cleanup)

	got := loadTableNames(db)
	if !contains(got, "users") {
		t.Errorf("expected 'users' in table list; got %v", got)
	}
	for _, name := range got {
		if strings.HasPrefix(name, "sqlite_") {
			t.Errorf("internal table %q leaked into completion list", name)
		}
	}
}

// TestWriteHistory_AtomicRename writes a small history file, then
// re-reads it raw to confirm the temp-file + rename pattern landed
// the contents at the requested path. This guards against the
// "history vanishes after a clean exit" footgun.
func TestWriteHistory_AtomicRename(t *testing.T) {
	dir := t.TempDir()
	histPath := filepath.Join(dir, "subdir", "history")

	state := liner.NewLiner()
	defer state.Close()
	state.AppendHistory("SELECT * FROM users;")
	state.AppendHistory(".tables")

	if err := writeHistory(histPath, state); err != nil {
		t.Fatalf("writeHistory: %v", err)
	}
	got, err := os.ReadFile(histPath)
	if err != nil {
		t.Fatalf("read history: %v", err)
	}
	body := string(got)
	if !strings.Contains(body, "SELECT * FROM users;") || !strings.Contains(body, ".tables") {
		t.Errorf("history file missing expected entries:\n%s", body)
	}

	// Confirm no stale temp file is left behind on success.
	entries, _ := os.ReadDir(filepath.Dir(histPath))
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".memdb-cli-history-") {
			t.Errorf("leftover temp file: %s", e.Name())
		}
	}
}
