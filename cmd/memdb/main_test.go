package main

import (
	"bytes"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/voicetel/memdb"
	"github.com/voicetel/memdb/server"
)

func TestResolveDurability(t *testing.T) {
	cases := []struct {
		name        string
		flag        string
		raftEnabled bool
		want        memdb.DurabilityMode
		wantErr     bool
	}{
		{"empty-standalone-defaults-to-wal", "", false, memdb.DurabilityWAL, false},
		{"empty-raft-defaults-to-none", "", true, memdb.DurabilityNone, false},
		{"none", "none", false, memdb.DurabilityNone, false},
		{"wal", "wal", false, memdb.DurabilityWAL, false},
		{"sync", "sync", false, memdb.DurabilitySync, false},
		{"uppercase", "SYNC", false, memdb.DurabilitySync, false},
		{"invalid", "foo", false, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveDurability(tc.flag, tc.raftEnabled)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDurabilityName(t *testing.T) {
	cases := []struct {
		mode memdb.DurabilityMode
		want string
	}{
		{memdb.DurabilityNone, "none"},
		{memdb.DurabilityWAL, "wal"},
		{memdb.DurabilitySync, "sync"},
		{memdb.DurabilityMode(99), "unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			if got := durabilityName(tc.mode); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBuildAuthenticator_Disabled(t *testing.T) {
	auth, err := buildAuthenticator("", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if auth != nil {
		t.Errorf("auth should be nil when user is empty, got %T", auth)
	}
}

func TestBuildAuthenticator_PasswordFromEnv(t *testing.T) {
	t.Setenv("MEMDB_AUTH_PASSWORD", "envpw")
	auth, err := buildAuthenticator("alice", "", "scram")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if auth == nil {
		t.Fatal("auth should not be nil")
	}
}

func TestBuildAuthenticator_MissingPassword(t *testing.T) {
	t.Setenv("MEMDB_AUTH_PASSWORD", "")
	_, err := buildAuthenticator("alice", "", "scram")
	if err == nil {
		t.Fatal("expected error when no password supplied")
	}
	if !strings.Contains(err.Error(), "alice") {
		t.Errorf("error should mention user name: %v", err)
	}
}

func TestBuildAuthenticator_Methods(t *testing.T) {
	cases := []struct {
		method string
		want   any
		err    bool
	}{
		{"scram", &server.ScramAuth{}, false},
		{"SCRAM", &server.ScramAuth{}, false},
		{"", &server.ScramAuth{}, false},
		{"cleartext", server.BasicAuth{}, false},
		{"CLEARTEXT", server.BasicAuth{}, false},
		{"plain", nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.method, func(t *testing.T) {
			auth, err := buildAuthenticator("u", "p", tc.method)
			if tc.err {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// Type-check the returned authenticator. Each method maps to a
			// distinct concrete type; we only check shape, not contents.
			switch tc.want.(type) {
			case *server.ScramAuth:
				if _, ok := auth.(*server.ScramAuth); !ok {
					t.Errorf("got %T, want *server.ScramAuth", auth)
				}
			case server.BasicAuth:
				if _, ok := auth.(server.BasicAuth); !ok {
					t.Errorf("got %T, want server.BasicAuth", auth)
				}
			}
		})
	}
}

func TestCopyFileAtomic_HappyPath(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	dst := filepath.Join(dir, "dst.db")

	want := []byte("snapshot bytes")
	if err := os.WriteFile(src, want, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := copyFileAtomic(src, dst); err != nil {
		t.Fatalf("copyFileAtomic: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read dst: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("contents mismatch: got %q, want %q", got, want)
	}
}

func TestCopyFileAtomic_OverwritesExistingDst(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	dst := filepath.Join(dir, "dst.db")

	if err := os.WriteFile(src, []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := copyFileAtomic(src, dst); err != nil {
		t.Fatalf("copyFileAtomic: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "new" {
		t.Errorf("got %q, want %q", got, "new")
	}
}

func TestCopyFileAtomic_NoTempLeftBehind(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	dst := filepath.Join(dir, "dst.db")
	if err := os.WriteFile(src, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := copyFileAtomic(src, dst); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".memdb-restore-") {
			t.Errorf("temp file leaked: %s", e.Name())
		}
	}
}

func TestCopyFileAtomic_MissingSource(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "missing")
	dst := filepath.Join(dir, "dst.db")
	err := copyFileAtomic(src, dst)
	if err == nil {
		t.Fatal("expected error for missing source")
	}
	if !errors.Is(err, os.ErrNotExist) && !strings.Contains(err.Error(), "open source") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestUsage_NotEmpty verifies usage() writes something to stderr without
// requiring a subprocess. We redirect stderr to a pipe.
func TestUsage_NotEmpty(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	origStderr := os.Stderr
	os.Stderr = w
	t.Cleanup(func() { os.Stderr = origStderr })

	usage()
	_ = w.Close()

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(r); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{"serve", "snapshot", "restore", "Usage"} {
		if !strings.Contains(out, want) {
			t.Errorf("usage missing %q: %q", want, out)
		}
	}
}

// TestServeFlagSet_RegistersExpectedFlags is a regression check that the
// flag names declared in main() do not silently disappear. If a future
// refactor renames or removes one, this test fails loudly.
func TestServeFlagSet_RegistersExpectedFlags(t *testing.T) {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.String("file", "memdb.db", "")
	fs.String("addr", "127.0.0.1:5433", "")
	_ = registerRaftFlags(fs)

	want := []string{
		"file", "addr",
		"raft-node-id", "raft-bind", "raft-advertise",
		"raft-forward-bind", "raft-peers", "raft-forward-peers",
		"raft-data-dir",
		"raft-tls-cert", "raft-tls-key", "raft-tls-ca",
	}
	for _, name := range want {
		if fs.Lookup(name) == nil {
			t.Errorf("flag %q not registered", name)
		}
	}
}
