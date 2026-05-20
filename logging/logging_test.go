package logging_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/voicetel/memdb/logging"
)

func TestNewTextHandler_WritesKeyValuePairs(t *testing.T) {
	var buf bytes.Buffer
	logger := logging.NewTextHandler(&buf, slog.LevelInfo)
	logger.Info("flush complete", "duration", "112ms", "rows", 17)

	out := buf.String()
	if !strings.Contains(out, "msg=") {
		t.Errorf("missing msg= key: %q", out)
	}
	if !strings.Contains(out, "duration=112ms") {
		t.Errorf("missing duration: %q", out)
	}
	if !strings.Contains(out, "rows=17") {
		t.Errorf("missing rows: %q", out)
	}
	if !strings.Contains(out, "level=INFO") {
		t.Errorf("missing INFO level: %q", out)
	}
}

func TestNewTextHandler_FiltersBelowLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := logging.NewTextHandler(&buf, slog.LevelWarn)
	logger.Info("info message")
	logger.Debug("debug message")

	if buf.Len() != 0 {
		t.Errorf("expected no output below Warn, got: %q", buf.String())
	}

	logger.Warn("warn message")
	if !strings.Contains(buf.String(), "warn message") {
		t.Errorf("warn message dropped: %q", buf.String())
	}
}

func TestNewJSONHandler_WritesValidJSON(t *testing.T) {
	var buf bytes.Buffer
	logger := logging.NewJSONHandler(&buf, slog.LevelInfo)
	logger.Info("flush complete", "duration", "112ms", "rows", 17)

	// Each log line is one JSON object.
	dec := json.NewDecoder(&buf)
	var rec map[string]any
	if err := dec.Decode(&rec); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	if rec["msg"] != "flush complete" {
		t.Errorf("msg=%v", rec["msg"])
	}
	if rec["level"] != "INFO" {
		t.Errorf("level=%v", rec["level"])
	}
	if rec["duration"] != "112ms" {
		t.Errorf("duration=%v", rec["duration"])
	}
	// JSON numbers decode to float64 by default.
	if rec["rows"] != float64(17) {
		t.Errorf("rows=%v", rec["rows"])
	}
}

func TestNewJSONHandler_FiltersBelowLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := logging.NewJSONHandler(&buf, slog.LevelError)
	logger.Info("ignored")
	logger.Warn("ignored")
	if buf.Len() != 0 {
		t.Errorf("expected no output below Error, got: %q", buf.String())
	}
	logger.Error("kept")
	if !strings.Contains(buf.String(), "kept") {
		t.Errorf("error message dropped: %q", buf.String())
	}
}
