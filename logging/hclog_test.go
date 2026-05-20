package logging_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/voicetel/memdb/logging"
)

// newCapturingLogger returns a JSON-formatted slog.Logger whose output is
// written into buf. JSON makes it easy to assert on individual fields.
func newCapturingLogger(buf *bytes.Buffer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: level}))
}

func TestHCLogAdapter_LevelMapping(t *testing.T) {
	cases := []struct {
		name     string
		emit     func(hclog.Logger)
		wantMsg  string
		wantLvl  string
		wantArgs map[string]any
	}{
		{
			name:     "trace-maps-to-debug",
			emit:     func(l hclog.Logger) { l.Trace("trace-msg", "k", "v") },
			wantMsg:  "trace-msg",
			wantLvl:  "DEBUG",
			wantArgs: map[string]any{"k": "v"},
		},
		{
			name:     "debug",
			emit:     func(l hclog.Logger) { l.Debug("debug-msg", "k", "v") },
			wantMsg:  "debug-msg",
			wantLvl:  "DEBUG",
			wantArgs: map[string]any{"k": "v"},
		},
		{
			name:     "info",
			emit:     func(l hclog.Logger) { l.Info("info-msg", "k", "v") },
			wantMsg:  "info-msg",
			wantLvl:  "INFO",
			wantArgs: map[string]any{"k": "v"},
		},
		{
			name:    "warn",
			emit:    func(l hclog.Logger) { l.Warn("warn-msg") },
			wantMsg: "warn-msg",
			wantLvl: "WARN",
		},
		{
			name:    "error",
			emit:    func(l hclog.Logger) { l.Error("error-msg") },
			wantMsg: "error-msg",
			wantLvl: "ERROR",
		},
		{
			name:    "log-info-via-Log",
			emit:    func(l hclog.Logger) { l.Log(hclog.Info, "log-info-msg") },
			wantMsg: "log-info-msg",
			wantLvl: "INFO",
		},
		{
			name:    "log-warn-via-Log",
			emit:    func(l hclog.Logger) { l.Log(hclog.Warn, "log-warn-msg") },
			wantMsg: "log-warn-msg",
			wantLvl: "WARN",
		},
		{
			name:    "log-error-via-Log",
			emit:    func(l hclog.Logger) { l.Log(hclog.Error, "log-error-msg") },
			wantMsg: "log-error-msg",
			wantLvl: "ERROR",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			adapter := logging.NewHCLogAdapter(newCapturingLogger(&buf, slog.LevelDebug), "raft")
			tc.emit(adapter)

			rec := decodeJSONLog(t, &buf)
			if rec["msg"] != tc.wantMsg {
				t.Errorf("msg=%v, want %v", rec["msg"], tc.wantMsg)
			}
			if rec["level"] != tc.wantLvl {
				t.Errorf("level=%v, want %v", rec["level"], tc.wantLvl)
			}
			if rec["component"] != "raft" {
				t.Errorf("component=%v, want raft", rec["component"])
			}
			for k, want := range tc.wantArgs {
				if rec[k] != want {
					t.Errorf("rec[%q]=%v, want %v", k, rec[k], want)
				}
			}
		})
	}
}

func TestHCLogAdapter_NilLoggerUsesDefault(t *testing.T) {
	// Should not panic when given nil — must fall back to slog.Default().
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic with nil logger: %v", r)
		}
	}()
	adapter := logging.NewHCLogAdapter(nil, "x")
	adapter.Info("smoke")
}

func TestHCLogAdapter_With_AppendsArgs(t *testing.T) {
	var buf bytes.Buffer
	base := logging.NewHCLogAdapter(newCapturingLogger(&buf, slog.LevelDebug), "raft")
	enriched := base.With("nodeID", "n1", "term", 7)
	enriched.Info("step")

	rec := decodeJSONLog(t, &buf)
	if rec["nodeID"] != "n1" {
		t.Errorf("nodeID=%v", rec["nodeID"])
	}
	if rec["term"] != float64(7) {
		t.Errorf("term=%v", rec["term"])
	}
	// ImpliedArgs should reflect the With() args.
	implied := enriched.ImpliedArgs()
	if len(implied) != 4 {
		t.Errorf("ImpliedArgs len=%d, want 4", len(implied))
	}
}

func TestHCLogAdapter_Named_PrefixesName(t *testing.T) {
	base := logging.NewHCLogAdapter(slog.Default(), "raft")
	if got := base.Name(); got != "raft" {
		t.Errorf("base Name=%q", got)
	}
	child := base.Named("transport")
	if got := child.Name(); got != "raft.transport" {
		t.Errorf("child Name=%q, want raft.transport", got)
	}
	grand := child.Named("conn")
	if got := grand.Name(); got != "raft.transport.conn" {
		t.Errorf("grandchild Name=%q", got)
	}
}

func TestHCLogAdapter_ResetNamed_ReplacesName(t *testing.T) {
	var buf bytes.Buffer
	base := logging.NewHCLogAdapter(newCapturingLogger(&buf, slog.LevelDebug), "raft")
	reset := base.ResetNamed("storage")
	if got := reset.Name(); got != "storage" {
		t.Errorf("Name=%q, want storage", got)
	}
	reset.Info("x")
	rec := decodeJSONLog(t, &buf)
	if rec["component"] != "storage" {
		t.Errorf("component=%v, want storage", rec["component"])
	}
}

func TestHCLogAdapter_LevelChecks(t *testing.T) {
	// Trace is always false (slog has no Trace level).
	var buf bytes.Buffer
	debugAdapter := logging.NewHCLogAdapter(newCapturingLogger(&buf, slog.LevelDebug), "x")
	if debugAdapter.IsTrace() {
		t.Error("IsTrace should always be false")
	}
	if !debugAdapter.IsDebug() {
		t.Error("IsDebug should be true at LevelDebug")
	}
	if !debugAdapter.IsInfo() {
		t.Error("IsInfo should be true at LevelDebug")
	}
	if !debugAdapter.IsWarn() {
		t.Error("IsWarn should be true at LevelDebug")
	}
	if !debugAdapter.IsError() {
		t.Error("IsError should be true at LevelDebug")
	}

	errorAdapter := logging.NewHCLogAdapter(newCapturingLogger(&buf, slog.LevelError), "x")
	if errorAdapter.IsDebug() {
		t.Error("IsDebug should be false at LevelError")
	}
	if errorAdapter.IsInfo() {
		t.Error("IsInfo should be false at LevelError")
	}
	if errorAdapter.IsWarn() {
		t.Error("IsWarn should be false at LevelError")
	}
	if !errorAdapter.IsError() {
		t.Error("IsError should be true at LevelError")
	}
}

func TestHCLogAdapter_GetLevel(t *testing.T) {
	cases := []struct {
		slogLevel slog.Level
		want      hclog.Level
	}{
		{slog.LevelDebug, hclog.Debug},
		{slog.LevelInfo, hclog.Info},
		{slog.LevelWarn, hclog.Warn},
		{slog.LevelError, hclog.Error},
	}
	for _, tc := range cases {
		t.Run(tc.slogLevel.String(), func(t *testing.T) {
			var buf bytes.Buffer
			adapter := logging.NewHCLogAdapter(newCapturingLogger(&buf, tc.slogLevel), "x")
			if got := adapter.GetLevel(); got != tc.want {
				t.Errorf("GetLevel=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestHCLogAdapter_SetLevel_NoOp(t *testing.T) {
	// SetLevel is a documented no-op; just verify it doesn't panic.
	var buf bytes.Buffer
	adapter := logging.NewHCLogAdapter(newCapturingLogger(&buf, slog.LevelInfo), "x")
	before := adapter.GetLevel()
	adapter.SetLevel(hclog.Trace)
	after := adapter.GetLevel()
	if before != after {
		t.Errorf("SetLevel mutated level: before=%v after=%v", before, after)
	}
}

func TestHCLogAdapter_StandardLogger(t *testing.T) {
	var buf bytes.Buffer
	adapter := logging.NewHCLogAdapter(newCapturingLogger(&buf, slog.LevelDebug), "x")
	std := adapter.StandardLogger(nil)
	if std == nil {
		t.Fatal("StandardLogger returned nil")
	}
	std.Print("standard-msg")
	if !strings.Contains(buf.String(), "standard-msg") {
		t.Errorf("standard logger output missing: %q", buf.String())
	}
}

func TestHCLogAdapter_StandardWriter(t *testing.T) {
	var buf bytes.Buffer
	adapter := logging.NewHCLogAdapter(newCapturingLogger(&buf, slog.LevelDebug), "x")
	w := adapter.StandardWriter(nil)
	if w == nil {
		t.Fatal("StandardWriter returned nil")
	}
	n, err := io.WriteString(w, "writer-msg\n")
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len("writer-msg\n") {
		t.Errorf("Write n=%d, want %d", n, len("writer-msg\n"))
	}
	rec := decodeJSONLog(t, &buf)
	if rec["msg"] != "writer-msg" {
		t.Errorf("msg=%v (trailing newline should be trimmed)", rec["msg"])
	}
	if rec["level"] != "INFO" {
		t.Errorf("level=%v, want INFO", rec["level"])
	}

	// Empty / whitespace-only writes are dropped.
	buf.Reset()
	if _, err := io.WriteString(w, "\n"); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Errorf("whitespace-only write produced output: %q", buf.String())
	}
}

// decodeJSONLog reads exactly one JSON object from buf.
func decodeJSONLog(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	dec := json.NewDecoder(buf)
	var rec map[string]any
	if err := dec.Decode(&rec); err != nil {
		t.Fatalf("decode JSON log: %v (raw: %q)", err, buf.String())
	}
	return rec
}
