//go:build !windows && !plan9

package logging

import (
	"context"
	"fmt"
	"log/slog"
	"log/syslog"
	"strconv"
	"strings"
	"time"
)

// NewSyslogHandler returns a *slog.Logger that writes to the local syslog
// daemon via /dev/log (Unix domain socket). Each slog level maps to the
// corresponding syslog priority:
//
//	slog.LevelDebug → syslog.LOG_DEBUG
//	slog.LevelInfo  → syslog.LOG_INFO
//	slog.LevelWarn  → syslog.LOG_WARNING
//	slog.LevelError → syslog.LOG_ERR
//
// tag is the program name that appears in the syslog entry (e.g. "memdb").
// level is the minimum level to emit; messages below this level are dropped.
//
// Returns an error if the syslog socket is unavailable (e.g. non-Linux
// environments). Fall back to NewTextHandler or NewJSONHandler in that case.
func NewSyslogHandler(tag string, level slog.Level) (*slog.Logger, error) {
	w, err := syslog.New(syslog.LOG_DAEMON|syslog.LOG_INFO, tag)
	if err != nil {
		return nil, fmt.Errorf("logging: syslog connect: %w", err)
	}
	return slog.New(&syslogHandler{w: w, level: level}), nil
}

// syslogHandler implements slog.Handler, routing each record to the
// appropriate syslog priority based on its level.
type syslogHandler struct {
	w     *syslog.Writer
	level slog.Level
	attrs []slog.Attr
	group string
}

func (h *syslogHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

func appendValue(sb *strings.Builder, v slog.Value) {
	switch v.Kind() {
	case slog.KindString:
		sb.WriteString(v.String())
	case slog.KindInt64:
		sb.WriteString(strconv.FormatInt(v.Int64(), 10))
	case slog.KindUint64:
		sb.WriteString(strconv.FormatUint(v.Uint64(), 10))
	case slog.KindFloat64:
		sb.WriteString(strconv.FormatFloat(v.Float64(), 'g', -1, 64))
	case slog.KindBool:
		sb.WriteString(strconv.FormatBool(v.Bool()))
	case slog.KindDuration:
		sb.WriteString(v.Duration().String())
	case slog.KindTime:
		sb.WriteString(v.Time().Format(time.RFC3339Nano))
	case slog.KindGroup:
		// Groups emit their attrs inline with the group name as a prefix.
		for _, a := range v.Group() {
			sb.WriteString(" ")
			sb.WriteString(a.Key)
			sb.WriteString("=")
			appendValue(sb, a.Value)
		}
	case slog.KindAny:
		// Fallback for unknown types.
		fmt.Fprintf(sb, "%v", v.Any())
	default:
		fmt.Fprintf(sb, "%v", v.Any())
	}
}

func (h *syslogHandler) Handle(_ context.Context, r slog.Record) error {
	var sb strings.Builder
	sb.Grow(128) // pre-size for typical log line
	sb.WriteString(r.Message)

	prefix := ""
	if h.group != "" {
		prefix = h.group + "."
	}

	// Pre-attached attrs (from WithAttrs) come first.
	for _, a := range h.attrs {
		sb.WriteString(" ")
		sb.WriteString(prefix)
		sb.WriteString(a.Key)
		sb.WriteString("=")
		appendValue(&sb, a.Value)
	}

	// Call-site attrs come after.
	r.Attrs(func(a slog.Attr) bool {
		sb.WriteString(" ")
		sb.WriteString(prefix)
		sb.WriteString(a.Key)
		sb.WriteString("=")
		appendValue(&sb, a.Value)
		return true
	})

	msg := sb.String()

	switch {
	case r.Level >= slog.LevelError:
		return h.w.Err(msg)
	case r.Level >= slog.LevelWarn:
		return h.w.Warning(msg)
	case r.Level >= slog.LevelInfo:
		return h.w.Info(msg)
	default:
		return h.w.Debug(msg)
	}
}

func (h *syslogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	newAttrs := make([]slog.Attr, len(h.attrs)+len(attrs))
	copy(newAttrs, h.attrs)
	copy(newAttrs[len(h.attrs):], attrs)
	return &syslogHandler{w: h.w, level: h.level, attrs: newAttrs, group: h.group}
}

func (h *syslogHandler) WithGroup(name string) slog.Handler {
	g := name
	if h.group != "" {
		g = h.group + "." + name
	}
	return &syslogHandler{w: h.w, level: h.level, attrs: h.attrs, group: g}
}
