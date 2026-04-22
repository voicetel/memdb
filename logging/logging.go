// Package logging provides structured, levelled logging for memdb using
// log/slog as the single interface. Three handler constructors are provided:
//
//   - NewTextHandler  — human-readable key=value output (development)
//   - NewJSONHandler  — structured JSON output (log aggregators)
//   - NewSyslogHandler — syslog via /dev/log (production Linux; see syslog.go)
//
// All memdb components accept a *slog.Logger. Pass nil to use slog.Default().
package logging

import (
	"io"
	"log/slog"
)

// NewTextHandler returns a *slog.Logger that writes human-readable log lines
// to w at the given minimum level.
//
// Example output:
//
//	time=2024-01-15T10:00:00.000Z level=INFO msg="flush complete" duration=112ms
func NewTextHandler(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: level}))
}

// NewJSONHandler returns a *slog.Logger that writes JSON log lines to w at
// the given minimum level. Suitable for log aggregators (Datadog, Splunk,
// CloudWatch Logs, Loki).
//
// Example output:
//
//	{"time":"2024-01-15T10:00:00Z","level":"INFO","msg":"flush complete","duration":"112ms"}
func NewJSONHandler(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level}))
}
