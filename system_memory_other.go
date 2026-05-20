//go:build !linux && !darwin

package memdb

// hostPhysicalMemoryBytes returns 0 on platforms without a stdlib
// path to read total physical RAM. The caller (defaultRestoreMaxBytes)
// falls back to GOMEMLIMIT or the hardcoded fallback in that case,
// so a missing probe is not a hard error.
func hostPhysicalMemoryBytes() int64 { return 0 }
