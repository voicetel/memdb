//go:build linux

package memdb

import "syscall"

// hostPhysicalMemoryBytes returns total physical RAM in bytes via
// sysinfo(2). Returns 0 on failure. The syscall is in the stdlib;
// no extra dependency required.
func hostPhysicalMemoryBytes() int64 {
	var info syscall.Sysinfo_t
	if err := syscall.Sysinfo(&info); err != nil {
		return 0
	}
	// Totalram is in units of info.Unit bytes (always 1 on current
	// kernels but documented as variable, so honour it).
	return int64(info.Totalram) * int64(info.Unit)
}
