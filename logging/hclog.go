//go:build !purego

package logging

import (
	"context"
	"io"
	"log"
	"log/slog"

	hclog "github.com/hashicorp/go-hclog"
)

// NewHCLogAdapter returns an hclog.Logger that forwards all log calls to
// logger using the corresponding slog level. Use this to route hashicorp/raft
// internal logs through the same *slog.Logger as the rest of the application.
//
//	node, err := raft.NewNode(db, raft.NodeConfig{
//	    Logger: logging.NewHCLogAdapter(myLogger, "raft"),
//	    ...
//	})
func NewHCLogAdapter(logger *slog.Logger, name string) hclog.Logger {
	if logger == nil {
		logger = slog.Default()
	}
	return &hclogAdapter{log: logger.With("component", name), name: name}
}

type hclogAdapter struct {
	log  *slog.Logger
	name string
	args []any
}

// ── hclog.Logger implementation ───────────────────────────────────────────────

func (a *hclogAdapter) Log(level hclog.Level, msg string, args ...any) {
	a.log.Log(context.Background(), hclogToSlog(level), msg, a.withArgs(args)...)
}

func (a *hclogAdapter) Trace(msg string, args ...any) {
	// slog has no Trace level; map to Debug.
	a.log.Debug(msg, a.withArgs(args)...)
}

func (a *hclogAdapter) Debug(msg string, args ...any) {
	a.log.Debug(msg, a.withArgs(args)...)
}

func (a *hclogAdapter) Info(msg string, args ...any) {
	a.log.Info(msg, a.withArgs(args)...)
}

func (a *hclogAdapter) Warn(msg string, args ...any) {
	a.log.Warn(msg, a.withArgs(args)...)
}

func (a *hclogAdapter) Error(msg string, args ...any) {
	a.log.Error(msg, a.withArgs(args)...)
}

func (a *hclogAdapter) IsTrace() bool { return a.log.Enabled(context.Background(), slog.LevelDebug) }
func (a *hclogAdapter) IsDebug() bool { return a.log.Enabled(context.Background(), slog.LevelDebug) }
func (a *hclogAdapter) IsInfo() bool  { return a.log.Enabled(context.Background(), slog.LevelInfo) }
func (a *hclogAdapter) IsWarn() bool  { return a.log.Enabled(context.Background(), slog.LevelWarn) }
func (a *hclogAdapter) IsError() bool { return a.log.Enabled(context.Background(), slog.LevelError) }

func (a *hclogAdapter) ImpliedArgs() []any { return a.args }
func (a *hclogAdapter) Name() string       { return a.name }

func (a *hclogAdapter) With(args ...any) hclog.Logger {
	return &hclogAdapter{
		log:  a.log.With(args...),
		name: a.name,
		args: append(a.args, args...),
	}
}

func (a *hclogAdapter) Named(name string) hclog.Logger {
	n := name
	if a.name != "" {
		n = a.name + "." + name
	}
	return &hclogAdapter{log: a.log.With("component", n), name: n, args: a.args}
}

func (a *hclogAdapter) ResetNamed(name string) hclog.Logger {
	return &hclogAdapter{log: a.log.With("component", name), name: name, args: a.args}
}

// SetLevel is a no-op: slog levels are controlled on the handler, not the logger.
func (a *hclogAdapter) SetLevel(level hclog.Level) {}

func (a *hclogAdapter) GetLevel() hclog.Level {
	switch {
	case a.log.Enabled(context.Background(), slog.LevelDebug):
		return hclog.Debug
	case a.log.Enabled(context.Background(), slog.LevelInfo):
		return hclog.Info
	case a.log.Enabled(context.Background(), slog.LevelWarn):
		return hclog.Warn
	default:
		return hclog.Error
	}
}

func (a *hclogAdapter) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	return slog.NewLogLogger(a.log.Handler(), slog.LevelInfo)
}

func (a *hclogAdapter) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	return &logWriter{log: a.log}
}

// withArgs merges the adapter's stored implied args with call-site args.
func (a *hclogAdapter) withArgs(args []any) []any {
	if len(a.args) == 0 {
		return args
	}
	merged := make([]any, 0, len(a.args)+len(args))
	merged = append(merged, a.args...)
	merged = append(merged, args...)
	return merged
}

// hclogToSlog converts an hclog.Level to the nearest slog.Level.
func hclogToSlog(level hclog.Level) slog.Level {
	switch level {
	case hclog.Trace, hclog.Debug:
		return slog.LevelDebug
	case hclog.Info:
		return slog.LevelInfo
	case hclog.Warn:
		return slog.LevelWarn
	default:
		return slog.LevelError
	}
}

// logWriter implements io.Writer by forwarding each write as an Info log line.
type logWriter struct{ log *slog.Logger }

func (w *logWriter) Write(p []byte) (int, error) {
	w.log.Info(string(p))
	return len(p), nil
}
