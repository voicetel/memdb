// Package profiling exposes a small wrapper around net/http/pprof that makes
// it straightforward to enable CPU, heap, goroutine, mutex, and block profiles
// from both the memdb binary (via the serve -pprof flag) and from tests /
// benchmarks (via the test-helper functions in this file).
//
// # HTTP endpoint
//
// StartServer launches net/http/pprof on a loopback address and returns a
// Server handle whose Close method stops the listener. All standard pprof
// endpoints are available:
//
//	/debug/pprof/              index page
//	/debug/pprof/profile       CPU profile (default 30 s)
//	/debug/pprof/heap          heap profile
//	/debug/pprof/goroutine     goroutine stacks
//	/debug/pprof/mutex         mutex contention (requires SetMutexProfileFraction)
//	/debug/pprof/block         block profile (requires SetBlockProfileRate)
//	/debug/pprof/allocs        all-time allocation samples
//	/debug/pprof/trace         runtime/trace execution trace
//
// The listener is bound to 127.0.0.1 by default — callers that need remote
// access must set the Addr field explicitly and understand the security
// implications (pprof exposes full heap contents).
//
// # Test / benchmark helpers
//
// CaptureCPUProfile and CaptureHeapProfile write a profile to disk for the
// duration of the caller-supplied function. They are intended for use inside
// long-running integration tests (for example, the replication soak tests in
// replication/raft) where sampling the steady-state workload is more useful
// than wiring an HTTP server.
//
// EnableMutexProfiling and EnableBlockProfiling install the runtime hooks
// required for the /debug/pprof/mutex and /debug/pprof/block endpoints to
// produce non-empty output. They are no-ops in production code paths and
// should only be called from main() or from test setup.
package profiling

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	runtimepprof "runtime/pprof"
	"sync"
	"time"
)

// DefaultAddr is the loopback-only address the HTTP pprof server binds to
// when the caller does not specify one. Loopback-only is a deliberate default
// because pprof exposes full process memory through /debug/pprof/heap.
const DefaultAddr = "127.0.0.1:6060"

// Config holds options for StartServer.
type Config struct {
	// Addr is the TCP address to bind. Defaults to DefaultAddr.
	Addr string

	// MutexProfileFraction, when > 0, enables mutex contention sampling with
	// the given fraction (see runtime.SetMutexProfileFraction). A value of
	// 100 samples 1% of contention events; a value of 1 samples every event.
	MutexProfileFraction int

	// BlockProfileRate, when > 0, enables goroutine blocking profile sampling
	// at the given rate in nanoseconds (see runtime.SetBlockProfileRate). A
	// rate of 10000 (10 µs) is a reasonable production setting; 1 samples
	// every block.
	BlockProfileRate int

	// ShutdownTimeout bounds how long Close waits for in-flight pprof requests
	// to finish before forcibly closing the listener. Defaults to 5 s.
	ShutdownTimeout time.Duration
}

// Server is a running pprof HTTP server. Close stops the listener and waits
// for in-flight requests to finish (or the shutdown timeout to elapse).
type Server struct {
	srv      *http.Server
	addr     string
	stopOnce sync.Once
	stopped  chan struct{}
	serveErr error
	shutdown time.Duration
}

// Addr returns the address the server is listening on. Useful when Addr was
// set to ":0" to pick a free port.
func (s *Server) Addr() string { return s.addr }

// StartServer starts an HTTP pprof server on cfg.Addr. The returned Server's
// Close method stops the listener. Errors during Accept (other than the
// intentional close signalled by Close) are surfaced by Close's return value.
//
// This is safe to call once per process. Calling it a second time from a
// different goroutine with the same Addr returns an error because the port
// is already bound.
func StartServer(cfg Config) (*Server, error) {
	if cfg.Addr == "" {
		cfg.Addr = DefaultAddr
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 5 * time.Second
	}
	if cfg.MutexProfileFraction > 0 {
		runtime.SetMutexProfileFraction(cfg.MutexProfileFraction)
	}
	if cfg.BlockProfileRate > 0 {
		runtime.SetBlockProfileRate(cfg.BlockProfileRate)
	}

	// Register the handlers on a private ServeMux so we do not pollute
	// http.DefaultServeMux. This also lets multiple processes in the same
	// binary (unlikely but possible) coexist without duplicate-registration
	// panics from the default mux.
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("profiling: listen %s: %w", cfg.Addr, err)
	}

	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	s := &Server{
		srv:      srv,
		addr:     ln.Addr().String(),
		stopped:  make(chan struct{}),
		shutdown: cfg.ShutdownTimeout,
	}
	go func() {
		defer close(s.stopped)
		err := srv.Serve(ln)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.serveErr = err
		}
	}()
	return s, nil
}

// Close stops the HTTP server and waits for in-flight requests to finish or
// the shutdown timeout to elapse, whichever comes first. The returned error
// is the first of (Serve error, Shutdown error).
func (s *Server) Close() error {
	var shutdownErr error
	s.stopOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), s.shutdown)
		defer cancel()
		shutdownErr = s.srv.Shutdown(ctx)
		<-s.stopped
	})
	if s.serveErr != nil {
		return s.serveErr
	}
	return shutdownErr
}

// EnableMutexProfiling enables mutex contention sampling at the given
// fraction. A value <= 0 disables sampling. See runtime.SetMutexProfileFraction
// for the full semantics.
func EnableMutexProfiling(fraction int) {
	runtime.SetMutexProfileFraction(fraction)
}

// EnableBlockProfiling enables goroutine blocking profile sampling at the
// given rate in nanoseconds. A rate <= 0 disables sampling. See
// runtime.SetBlockProfileRate for the full semantics.
func EnableBlockProfiling(rate int) {
	runtime.SetBlockProfileRate(rate)
}

// CaptureCPUProfile runs fn while a CPU profile is being written to path.
// It is the test-helper equivalent of `go test -cpuprofile=path`, but scoped
// to a single function call so that test setup/teardown is excluded.
//
// The file is created with mode 0o600 — profiles may contain fragments of
// in-memory data and are treated as sensitive.
func CaptureCPUProfile(path string, fn func() error) (err error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("profiling: create cpu profile %s: %w", path, err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	if err := runtimepprof.StartCPUProfile(f); err != nil {
		return fmt.Errorf("profiling: start cpu profile: %w", err)
	}
	defer runtimepprof.StopCPUProfile()

	return fn()
}

// CaptureHeapProfile writes a heap profile to path after fn returns. A
// runtime.GC is called before the profile is written so that only live
// objects are included — matching the behaviour of `go test -memprofile`.
func CaptureHeapProfile(path string, fn func() error) (err error) {
	if err := fn(); err != nil {
		return err
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("profiling: create heap profile %s: %w", path, err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	// GC first so the profile reflects live-heap usage, not allocation rate.
	runtime.GC()
	if err := runtimepprof.WriteHeapProfile(f); err != nil {
		return fmt.Errorf("profiling: write heap profile: %w", err)
	}
	return nil
}

// CaptureNamedProfile writes the named runtime profile (e.g. "goroutine",
// "allocs", "mutex", "block") to path. The profile is captured at the moment
// of the call — fn is not involved. This helper exists for the cases where
// CaptureHeapProfile / CaptureCPUProfile are not applicable.
func CaptureNamedProfile(name, path string) (err error) {
	p := runtimepprof.Lookup(name)
	if p == nil {
		return fmt.Errorf("profiling: unknown profile %q", name)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("profiling: create %s profile %s: %w", name, path, err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	if err := p.WriteTo(f, 0); err != nil {
		return fmt.Errorf("profiling: write %s profile: %w", name, err)
	}
	return nil
}
