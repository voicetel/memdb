package memdb

import (
	"runtime/debug"
)

// defaultRestoreMaxBytes computes the host-aware default for
// Config.RestoreMaxBytes. The intent is: scale automatically so a
// Raspberry Pi doesn't default to a 4 GiB cap and a 256 GiB server is
// not artificially capped at 4 GiB either, while still keeping a sane
// lower bound for tiny VMs and a sane upper bound to avoid letting an
// unbounded snapshot exhaust host memory by accident.
//
// Signal priority:
//
//  1. GOMEMLIMIT (via runtime/debug.SetMemoryLimit(-1)). When the
//     operator or container orchestrator has set this, it is the most
//     reliable signal — it already reflects cgroup awareness if the
//     operator opted into "auto" or set the value explicitly from a
//     cgroup probe.
//  2. Host physical RAM (via the platform-specific hostPhysicalMemoryBytes
//     in system_memory_$GOOS.go). Always less reliable than GOMEMLIMIT
//     in a container, but reasonable on bare metal.
//  3. Fallback to DefaultRestoreMaxBytesFallback (4 GiB) when neither is
//     available — covers the platforms without a host-memory probe.
//
// Whichever signal is used, the result is clamped to
// [restoreMaxBytesFloor, restoreMaxBytesCeil] = [256 MiB, 16 GiB]. The
// ratio used (half of the source value) gives the cap enough headroom
// to grow into without committing the entire host/cgroup budget to a
// single SQLite database.
func defaultRestoreMaxBytes() int64 {
	if lim := debug.SetMemoryLimit(-1); lim > 0 && lim < goRuntimeMemLimitSentinel {
		return clampRestoreMaxBytes(lim / 2)
	}
	if host := hostPhysicalMemoryBytes(); host > 0 {
		return clampRestoreMaxBytes(host / 2)
	}
	return DefaultRestoreMaxBytesFallback
}

// clampRestoreMaxBytes enforces the [256 MiB, 16 GiB] band on a
// candidate value computed from GOMEMLIMIT or host memory.
func clampRestoreMaxBytes(n int64) int64 {
	if n < restoreMaxBytesFloor {
		return restoreMaxBytesFloor
	}
	if n > restoreMaxBytesCeil {
		return restoreMaxBytesCeil
	}
	return n
}

const (
	restoreMaxBytesFloor int64 = 256 << 20 // 256 MiB
	restoreMaxBytesCeil  int64 = 16 << 30  // 16 GiB

	// goRuntimeMemLimitSentinel matches the sentinel returned by
	// runtime/debug.SetMemoryLimit(-1) when GOMEMLIMIT is unset
	// (math.MaxInt64). Declared here as a const to avoid importing
	// math for one constant.
	goRuntimeMemLimitSentinel int64 = 1<<63 - 1
)
