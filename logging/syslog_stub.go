//go:build windows || plan9

package logging

import (
	"fmt"
	"log/slog"
)

// NewSyslogHandler is not supported on Windows or Plan 9.
// Use NewTextHandler or NewJSONHandler instead.
func NewSyslogHandler(tag string, level slog.Level) (*slog.Logger, error) {
	return nil, fmt.Errorf("logging: syslog not supported on this platform; use NewTextHandler or NewJSONHandler")
}
