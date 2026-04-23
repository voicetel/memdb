package profiling_test

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/voicetel/memdb/profiling"
)

// TestStartServer_StartsAndServesIndex verifies that a pprof server starts,
// the index page responds, and Close stops the listener cleanly.
func TestStartServer_StartsAndServesIndex(t *testing.T) {
	srv, err := profiling.StartServer(profiling.Config{
		Addr: "127.0.0.1:0", // ephemeral port — safe for parallel test runs
	})
	if err != nil {
		t.Fatalf("StartServer: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	client := &http.Client{Timeout: 2 * time.Second}
	url := "http://" + srv.Addr() + "/debug/pprof/"

	// Retry briefly — the goroutine that calls Serve may not have begun
	// accepting yet on slow CI machines.
	var resp *http.Response
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err = client.Get(url)
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	// The pprof index page references each profile type by name.
	if !strings.Contains(string(body), "goroutine") {
		t.Errorf("pprof index page did not mention goroutine profile: %s", string(body))
	}
}

// TestStartServer_ServesNamedEndpoints walks the well-known pprof endpoints
// and verifies each returns 200. This guards against accidentally dropping a
// handler from the private ServeMux.
func TestStartServer_ServesNamedEndpoints(t *testing.T) {
	srv, err := profiling.StartServer(profiling.Config{
		Addr:                 "127.0.0.1:0",
		MutexProfileFraction: 1,
		BlockProfileRate:     1,
	})
	if err != nil {
		t.Fatalf("StartServer: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	client := &http.Client{Timeout: 5 * time.Second}
	base := "http://" + srv.Addr() + "/debug/pprof/"

	// profile and trace are not exercised here because they are long-running
	// CPU/trace captures that would slow the test suite.
	endpoints := []string{
		"",        // index
		"cmdline", // command-line
		"goroutine?debug=1",
		"heap",
		"allocs",
		"mutex",
		"block",
		"threadcreate",
	}

	for _, ep := range endpoints {
		ep := ep
		url := base + ep
		resp, err := client.Get(url)
		if err != nil {
			t.Errorf("GET %s: %v", url, err)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s: status %d", url, resp.StatusCode)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

// TestStartServer_AddrReturnsBound verifies Addr() reports the bound address.
func TestStartServer_AddrReturnsBound(t *testing.T) {
	srv, err := profiling.StartServer(profiling.Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("StartServer: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	addr := srv.Addr()
	if addr == "" {
		t.Fatal("Addr() returned empty string")
	}
	if !strings.HasPrefix(addr, "127.0.0.1:") {
		t.Errorf("unexpected Addr(): %q", addr)
	}
}

// TestStartServer_BadAddr verifies a bind error is surfaced.
func TestStartServer_BadAddr(t *testing.T) {
	_, err := profiling.StartServer(profiling.Config{
		Addr: "256.256.256.256:0", // invalid IP — must fail
	})
	if err == nil {
		t.Fatal("expected error for invalid bind address, got nil")
	}
}

// TestClose_Idempotent calls Close twice — the second call must not panic
// and should return quickly.
func TestClose_Idempotent(t *testing.T) {
	srv, err := profiling.StartServer(profiling.Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("StartServer: %v", err)
	}

	if err := srv.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	// A second Close must be safe.
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Close()
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("second Close blocked")
	}
}

// TestCaptureCPUProfile verifies the helper produces a non-empty profile.
func TestCaptureCPUProfile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cpu.prof")

	// Burn a small amount of CPU so the profile contains something beyond
	// the header. Tiny enough not to slow the test suite.
	err := profiling.CaptureCPUProfile(path, func() error {
		deadline := time.Now().Add(50 * time.Millisecond)
		x := 0
		for time.Now().Before(deadline) {
			x = (x*31 + 7) & 0xffff
		}
		_ = x
		return nil
	})
	if err != nil {
		t.Fatalf("CaptureCPUProfile: %v", err)
	}

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if fi.Size() == 0 {
		t.Fatal("expected non-empty CPU profile file")
	}
	// Profile files should be readable only by the owner.
	if mode := fi.Mode().Perm(); mode != 0o600 {
		t.Errorf("profile file mode = %o, want 0600", mode)
	}
}

// TestCaptureCPUProfile_FnError propagates the callback error.
func TestCaptureCPUProfile_FnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cpu.prof")
	wantErr := "boom"
	err := profiling.CaptureCPUProfile(path, func() error {
		return &stringError{wantErr}
	})
	if err == nil || err.Error() != wantErr {
		t.Fatalf("want error %q, got %v", wantErr, err)
	}
}

type stringError struct{ s string }

func (e *stringError) Error() string { return e.s }

// TestCaptureHeapProfile verifies the helper produces a non-empty profile.
func TestCaptureHeapProfile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "heap.prof")

	// Allocate something so the profile isn't trivially empty. The slice is
	// retained in the outer scope via the sink so the GC cannot collect it
	// before WriteHeapProfile runs.
	var sink [][]byte
	err := profiling.CaptureHeapProfile(path, func() error {
		for i := 0; i < 16; i++ {
			sink = append(sink, make([]byte, 4096))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("CaptureHeapProfile: %v", err)
	}
	if len(sink) == 0 {
		t.Fatal("sink was not populated — callback did not run")
	}

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if fi.Size() == 0 {
		t.Fatal("expected non-empty heap profile file")
	}
}

// TestCaptureHeapProfile_FnError propagates the callback error WITHOUT
// writing a profile file.
func TestCaptureHeapProfile_FnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "heap.prof")
	err := profiling.CaptureHeapProfile(path, func() error {
		return &stringError{"nope"}
	})
	if err == nil {
		t.Fatal("expected error from CaptureHeapProfile")
	}
	if _, statErr := os.Stat(path); statErr == nil {
		t.Error("heap profile file was created despite callback error")
	}
}

// TestCaptureNamedProfile exercises a few well-known runtime profiles.
func TestCaptureNamedProfile(t *testing.T) {
	for _, name := range []string{"goroutine", "heap", "allocs", "threadcreate"} {
		name := name
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), name+".prof")
			if err := profiling.CaptureNamedProfile(name, path); err != nil {
				t.Fatalf("CaptureNamedProfile(%s): %v", name, err)
			}
			fi, err := os.Stat(path)
			if err != nil {
				t.Fatalf("stat: %v", err)
			}
			if fi.Size() == 0 {
				t.Errorf("profile %s is empty", name)
			}
		})
	}
}

// TestCaptureNamedProfile_Unknown returns an error for a nonexistent profile.
func TestCaptureNamedProfile_Unknown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unknown.prof")
	err := profiling.CaptureNamedProfile("does-not-exist", path)
	if err == nil {
		t.Fatal("expected error for unknown profile name")
	}
	if !strings.Contains(err.Error(), "unknown profile") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestEnableMutexProfiling / TestEnableBlockProfiling — smoke tests. Setting
// the rate is a global side effect on the runtime, so we restore the
// previous default after the test.
func TestEnableMutexProfiling(t *testing.T) {
	t.Cleanup(func() { profiling.EnableMutexProfiling(0) })
	profiling.EnableMutexProfiling(100)
}

func TestEnableBlockProfiling(t *testing.T) {
	t.Cleanup(func() { profiling.EnableBlockProfiling(0) })
	profiling.EnableBlockProfiling(10000)
}
