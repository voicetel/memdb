//go:build darwin

package memdb

import (
	"encoding/binary"
	"syscall"
)

// hostPhysicalMemoryBytes returns total physical RAM in bytes via
// sysctl("hw.memsize"). The sysctl returns 8 raw bytes (little-endian
// uint64 on all darwin architectures). Returns 0 on failure.
func hostPhysicalMemoryBytes() int64 {
	s, err := syscall.Sysctl("hw.memsize")
	if err != nil || len(s) < 8 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64([]byte(s)[:8]))
}
