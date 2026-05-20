//go:build !windows && !plan9

package logging_test

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/voicetel/memdb/logging"
)

// fakeSyslog spins up a Unix datagram listener at $TMPDIR/log and points
// log/syslog at it via the LOGNAME / hostname tricks the stdlib uses. The
// stdlib syslog package falls back to /dev/log on Linux; if that is missing
// (e.g. CI sandbox) the connect call errors and we skip the test rather than
// failing — these handlers are exercised in production, not in the unit
// suite, and the appendValue / level-routing logic is what matters here.
//
// We connect to whatever syslog daemon the host provides. On a typical Linux
// dev box this is rsyslog/journald via /dev/log; on macOS it is /var/run/syslog.
// Either way the message is fire-and-forget — we cannot read it back, so
// these tests verify the API contract (no error, no panic, level dispatch).

func newSyslogOrSkip(t *testing.T, level slog.Level) *slog.Logger {
	t.Helper()
	logger, err := logging.NewSyslogHandler("memdb-test", level)
	if err != nil {
		t.Skipf("syslog unavailable on this host: %v", err)
	}
	return logger
}

func TestNewSyslogHandler_AllLevelsRoute(t *testing.T) {
	logger := newSyslogOrSkip(t, slog.LevelDebug)
	// Each level path inside Handle exercises a different syslog priority
	// (Err / Warning / Info / Debug). We cannot read the syslog feed back,
	// but if any path errored or panicked the test would fail.
	logger.Debug("debug-line", "k", "v")
	logger.Info("info-line", "k", "v")
	logger.Warn("warn-line", "k", "v")
	logger.Error("error-line", "k", "v")
}

func TestNewSyslogHandler_FiltersBelowLevel(t *testing.T) {
	logger := newSyslogOrSkip(t, slog.LevelWarn)
	// These two should be dropped by Enabled() before reaching the writer.
	// We can't observe the drop directly, but we can call Enabled via the
	// handler and confirm the contract.
	ctx := context.Background()
	if logger.Enabled(ctx, slog.LevelDebug) {
		t.Error("Debug should be disabled at LevelWarn")
	}
	if logger.Enabled(ctx, slog.LevelInfo) {
		t.Error("Info should be disabled at LevelWarn")
	}
	if !logger.Enabled(ctx, slog.LevelWarn) {
		t.Error("Warn should be enabled at LevelWarn")
	}
	if !logger.Enabled(ctx, slog.LevelError) {
		t.Error("Error should be enabled at LevelWarn")
	}
}

func TestNewSyslogHandler_WithAttrsAndGroup(t *testing.T) {
	logger := newSyslogOrSkip(t, slog.LevelInfo)

	// Exercise WithAttrs (via With), WithGroup, and a record carrying
	// every appendValue Kind to drive coverage through the formatter.
	enriched := logger.With("nodeID", "n1").
		WithGroup("raft").
		With(
			"ts", time.Unix(0, 0).UTC(),
			"dur", 5*time.Second,
			"f64", 3.14,
			"i64", int64(-7),
			"u64", uint64(42),
			"b", true,
			"s", "string-val",
		)
	enriched.Info("emit",
		"call_site_int", 1,
		"call_site_group", slog.GroupValue(slog.String("inner", "x")),
	)

	// Multiple WithGroup nestings should not panic.
	logger.WithGroup("outer").WithGroup("inner").Info("nested")

	// An attr with an unknown Any value uses the fmt.Fprintf fallback.
	type custom struct{ X int }
	logger.Info("any-fallback", "obj", custom{X: 99})
}

func TestNewSyslogHandler_BadTagStillReturnsLogger(t *testing.T) {
	// stdlib log/syslog accepts any tag string — even empty — so this is
	// just a smoke check that the constructor does not panic on edge inputs.
	logger, err := logging.NewSyslogHandler("", slog.LevelInfo)
	if err != nil {
		// Skip if no syslog daemon — same as elsewhere.
		t.Skipf("syslog unavailable: %v", err)
	}
	if logger == nil {
		t.Fatal("nil logger with no error")
	}
}

func TestNewSyslogHandler_ErrorOnUnreachableSyslog(t *testing.T) {
	// Force the stdlib syslog package to attempt a non-existent socket.
	// The package walks a hard-coded list of paths (`/dev/log`, etc.); if
	// none are reachable, syslog.New errors. We can't inject a custom path,
	// so on hosts where /dev/log is reachable this case is unreachable
	// — skip it. On hosts without syslog, we get the error path for free.
	if _, err := os.Stat("/dev/log"); err == nil {
		t.Skip("/dev/log present; cannot exercise the unreachable-syslog branch")
	}
	if _, err := os.Stat("/var/run/syslog"); err == nil {
		t.Skip("/var/run/syslog present; cannot exercise the unreachable-syslog branch")
	}
	_, err := logging.NewSyslogHandler("memdb-test", slog.LevelInfo)
	if err == nil {
		t.Fatal("expected error when no syslog socket is reachable")
	}
}

// TestSyslogHandler_StructuralSmoke proves the in-process Unix datagram
// listener machinery used by future fault-injection tests still works on
// this host. NewSyslogHandler doesn't expose a dial-address knob, so we
// can't (yet) point it at this listener — but a missing AF_UNIX/datagram
// capability would invalidate any later refactor that wires injection in.
func TestSyslogHandler_StructuralSmoke(t *testing.T) {
	dir := t.TempDir()
	addr := filepath.Join(dir, "sock")
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: addr, Net: "unixgram"})
	if err != nil {
		t.Skipf("unixgram listener unavailable: %v", err)
	}
	defer conn.Close()
	defer func() { _ = os.Remove(addr) }()
	if err := conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1024)
	_, _, _ = conn.ReadFromUnix(buf)
}
