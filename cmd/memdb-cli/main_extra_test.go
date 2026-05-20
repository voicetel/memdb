package main

// Tests for the entry-point helpers (resolveUser, resolvePassword, openWire,
// runFile, printHelp, repl) that the existing main_test.go does not cover.

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestResolveUser(t *testing.T) {
	t.Setenv("MEMDB_USER", "envuser")
	if got := resolveUser("flagval"); got != "flagval" {
		t.Errorf("flag should win: got %q", got)
	}
	if got := resolveUser(""); got != "envuser" {
		t.Errorf("env fallback: got %q, want envuser", got)
	}
	t.Setenv("MEMDB_USER", "")
	if got := resolveUser(""); got != "" {
		t.Errorf("both empty: got %q", got)
	}
}

func TestResolvePassword(t *testing.T) {
	t.Setenv("MEMDB_PASSWORD", "envpw")
	if got := resolvePassword("flagpw"); got != "flagpw" {
		t.Errorf("flag should win: got %q", got)
	}
	if got := resolvePassword(""); got != "envpw" {
		t.Errorf("env fallback: got %q", got)
	}
	t.Setenv("MEMDB_PASSWORD", "")
	if got := resolvePassword(""); got != "" {
		t.Errorf("both empty: got %q", got)
	}
}

func TestPrintHelp(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	orig := os.Stdout
	os.Stdout = w
	t.Cleanup(func() { os.Stdout = orig })

	printHelp()
	_ = w.Close()

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(r); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{
		".tables", ".schema", ".mode", ".quit", "READ-ONLY",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("printHelp missing %q", want)
		}
	}
}

func TestRunFile_HappyPath(t *testing.T) {
	db, cleanup := openTestSnapshot(t, 3)
	t.Cleanup(cleanup)

	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "script.sql")
	if err := os.WriteFile(scriptPath, []byte(
		"SELECT COUNT(*) FROM users;\nSELECT MIN(id) FROM users;\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	out := newPrinter(&buf, true, "list", "|")
	if err := runFile(db, scriptPath, out); err != nil {
		t.Fatalf("runFile: %v", err)
	}
	if buf.Len() == 0 {
		t.Error("runFile produced no output")
	}
}

func TestRunFile_StopsAtFirstError(t *testing.T) {
	db, cleanup := openTestSnapshot(t, 3)
	t.Cleanup(cleanup)

	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "bad.sql")
	if err := os.WriteFile(scriptPath, []byte(
		"INSERT INTO users(name) VALUES('x');\nSELECT 1;\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	out := newPrinter(io.Discard, true, "list", "|")
	err := runFile(db, scriptPath, out)
	if err == nil {
		t.Fatal("expected error from INSERT against read-only DB")
	}
}

func TestRunFile_MissingFile(t *testing.T) {
	db, cleanup := openTestSnapshot(t, 1)
	t.Cleanup(cleanup)
	out := newPrinter(io.Discard, true, "list", "|")
	err := runFile(db, filepath.Join(t.TempDir(), "no-such.sql"), out)
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestRunFile_SkipsBlankAndBareSemicolon(t *testing.T) {
	db, cleanup := openTestSnapshot(t, 1)
	t.Cleanup(cleanup)

	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "blanks.sql")
	if err := os.WriteFile(scriptPath, []byte(";\n\n   ;\nSELECT 1;\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out := newPrinter(io.Discard, true, "list", "|")
	if err := runFile(db, scriptPath, out); err != nil {
		t.Fatalf("runFile: %v", err)
	}
}

func TestOpenWire_BadAddr(t *testing.T) {
	_, _, err := openWire(wireOptions{
		Addr:           "not-a-valid-host-port",
		ConnectTimeout: 100 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("expected error for invalid addr, got nil")
	}
	if !strings.Contains(err.Error(), "parse -addr") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestOpenWire_ConnectFailure(t *testing.T) {
	// Port 1 is reserved and not listened on — connect should fail fast.
	_, _, err := openWire(wireOptions{
		Addr:           "127.0.0.1:1",
		ConnectTimeout: 200 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("expected connect error, got nil")
	}
}

func TestOpenWire_TLSModeSelection(t *testing.T) {
	// We can't actually complete a connection without a server, but we
	// can verify the DSN / parsing path doesn't panic on the various
	// TLS-mode flag combinations.
	cases := []struct {
		name       string
		requireTLS bool
		skipVerify bool
	}{
		{"plain", false, false},
		{"verify-full", true, false},
		{"require-skip-verify", true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := openWire(wireOptions{
				Addr:           "127.0.0.1:1",
				RequireTLS:     tc.requireTLS,
				SkipVerify:     tc.skipVerify,
				ConnectTimeout: 100 * time.Millisecond,
			})
			if err == nil {
				t.Error("expected connect error, got nil")
			}
		})
	}
}

func TestRepl_ExitOnQuit(t *testing.T) {
	// Pipe `.quit\n` to stdin and verify repl returns cleanly. liner
	// detects that stdin is not a terminal and falls back to bufio.
	pipeR, pipeW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	origStdin := os.Stdin
	os.Stdin = pipeR
	t.Cleanup(func() { os.Stdin = origStdin })

	if _, err := pipeW.WriteString(".quit\n"); err != nil {
		t.Fatal(err)
	}
	_ = pipeW.Close()

	db, cleanup := openTestSnapshot(t, 1)
	t.Cleanup(cleanup)

	out := newPrinter(io.Discard, true, "list", "|")

	done := make(chan struct{})
	go func() {
		repl(db, out, "", "snapshot")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("repl did not return after .quit")
	}
}

func TestRepl_ExecutesStatement(t *testing.T) {
	pipeR, pipeW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	origStdin := os.Stdin
	os.Stdin = pipeR
	t.Cleanup(func() { os.Stdin = origStdin })

	if _, err := pipeW.WriteString("SELECT COUNT(*) FROM users;\n.quit\n"); err != nil {
		t.Fatal(err)
	}
	_ = pipeW.Close()

	db, cleanup := openTestSnapshot(t, 4)
	t.Cleanup(cleanup)

	var buf bytes.Buffer
	out := newPrinter(&buf, true, "list", "|")

	done := make(chan struct{})
	go func() {
		repl(db, out, "", "snapshot")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("repl did not return")
	}
	// Output should contain the count value 4.
	if !strings.Contains(buf.String(), "4") {
		t.Errorf("repl output missing count 4: %q", buf.String())
	}
}

func TestRepl_WireBannerSelected(t *testing.T) {
	// Wire-mode banner has a different first line.
	pipeR, pipeW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	origStdin := os.Stdin
	os.Stdin = pipeR
	t.Cleanup(func() { os.Stdin = origStdin })

	origStdout := os.Stdout
	rOut, wOut, _ := os.Pipe()
	os.Stdout = wOut
	t.Cleanup(func() { os.Stdout = origStdout })

	if _, err := pipeW.WriteString(".quit\n"); err != nil {
		t.Fatal(err)
	}
	_ = pipeW.Close()

	db, cleanup := openTestSnapshot(t, 1)
	t.Cleanup(cleanup)

	done := make(chan struct{})
	go func() {
		repl(db, newPrinter(io.Discard, true, "list", "|"), "", "wire")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("repl wire-mode did not return")
	}
	_ = wOut.Close()

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rOut); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "wire mode") {
		t.Errorf("expected 'wire mode' banner, got: %q", buf.String())
	}
}

func TestRepl_HistoryRoundTrip(t *testing.T) {
	dir := t.TempDir()
	histPath := filepath.Join(dir, "hist")

	pipeR, pipeW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	origStdin := os.Stdin
	os.Stdin = pipeR
	t.Cleanup(func() { os.Stdin = origStdin })

	if _, err := pipeW.WriteString(".tables\n.quit\n"); err != nil {
		t.Fatal(err)
	}
	_ = pipeW.Close()

	db, cleanup := openTestSnapshot(t, 1)
	t.Cleanup(cleanup)

	done := make(chan struct{})
	go func() {
		repl(db, newPrinter(io.Discard, true, "list", "|"), histPath, "snapshot")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("repl did not return")
	}

	if _, err := os.Stat(histPath); err != nil {
		t.Errorf("history file not created: %v", err)
	}
}
