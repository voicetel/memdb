//go:build !libsqlite3 || sqlite_serialize
// +build !libsqlite3 sqlite_serialize

package memdb

/*
#include <stdint.h>
#include <stdlib.h>

// Opaque forward-declaration. The sqlite3 type's full definition lives
// inside the SQLite amalgamation that mattn/go-sqlite3 statically links
// into the same binary; we only need to pass pointers to it, so an
// incomplete type is sufficient.
typedef struct sqlite3 sqlite3;

// SQLite symbols. These are exported by mattn's bundled SQLite and
// resolved by the linker at build time, so no extra LDFLAGS are
// required. Keeping these as plain externs (rather than including
// sqlite3-binding.h from mattn's module directory) avoids relying on
// the module cache layout in our cgo include path.
extern int sqlite3_deserialize(sqlite3* db, const char* zSchema,
    unsigned char* pData, long long szDb, long long szBuf, unsigned mFlags);
extern void* sqlite3_malloc64(unsigned long long n);
extern void sqlite3_free(void* p);
extern const char* sqlite3_db_filename(sqlite3* db, const char* zDbName);
extern const char* sqlite3_libversion(void);

// Matches the sqlite3.h constants for SQLITE_DESERIALIZE_*.
#define MEMDB_SQLITE_OK                       0
#define MEMDB_SQLITE_DESERIALIZE_FREEONCLOSE  1
#define MEMDB_SQLITE_DESERIALIZE_RESIZEABLE   2

static int memdb_deserialize_resizable(
    sqlite3* db, const char* schema,
    void* buf, long long sz
) {
    return sqlite3_deserialize(
        db, schema, (unsigned char*)buf, sz, sz,
        (unsigned)(MEMDB_SQLITE_DESERIALIZE_FREEONCLOSE |
                   MEMDB_SQLITE_DESERIALIZE_RESIZEABLE)
    );
}

static void* memdb_sqlite3_malloc64(long long n) {
    return sqlite3_malloc64((unsigned long long)n);
}

static void memdb_sqlite3_free(void* p) {
    sqlite3_free(p);
}

// Probe used by the runtime layout guard. Calls sqlite3_db_filename on
// the handle the mirror extracted; a correctly-mirrored handle returns
// a non-NULL pointer (the empty string for in-memory DBs). A wrong
// handle would (in the best case) return NULL or (more likely) crash.
static const char* memdb_db_filename_main(sqlite3* db) {
    return sqlite3_db_filename(db, "main");
}
*/
import "C"

import (
	"fmt"
	"sync"
	"unsafe"

	sqlite3 "github.com/mattn/go-sqlite3"
)

// sqliteConnMirror mirrors the prefix of mattn/go-sqlite3's
// SQLiteConn so we can read the unexported *C.sqlite3 handle.
//
// Pinned to mattn/go-sqlite3 v1.14.42. If go.mod's mattn pin changes,
// (a) re-verify the struct layout in sqlite3.go matches the prefix
// below and (b) ensure TestSQLiteConnMirrorLayout still passes — it
// uses the mirrored handle to invoke a SQLite C function and would
// fail or crash if the offset drifted.
//
// Why this is necessary: sqlite3_deserialize without
// SQLITE_DESERIALIZE_RESIZEABLE freezes the database page cap at
// len(buf). mattn's exported Deserialize passes only FREEONCLOSE
// (sqlite3_opt_serialize.go), so any write past the snapshot size
// after Restore returns SQLITE_FULL silently. mattn does not expose
// the *C.sqlite3 handle, so we read it through the mirror to call
// sqlite3_deserialize ourselves with the right flags.
type sqliteConnMirror struct {
	_  sync.Mutex     // mattn: mu sync.Mutex
	db unsafe.Pointer // mattn: db *C.sqlite3
}

// rawConnHandle extracts the underlying *C.sqlite3 handle from a
// mattn SQLiteConn via the unsafe mirror.
func rawConnHandle(c *sqlite3.SQLiteConn) unsafe.Pointer {
	return (*sqliteConnMirror)(unsafe.Pointer(c)).db
}

// layoutOnce protects the one-shot self-test of the SQLiteConn mirror.
// We can only verify the mirror's correctness once we have a real
// SQLiteConn to probe, so the check runs lazily on the first
// deserializeResizable call.
var (
	layoutOnce sync.Once
	layoutErr  error
)

// verifyLayout calls a benign SQLite C function (sqlite3_db_filename)
// through the mirrored handle and records an error if the result is
// nil. A wrong offset would dereference an arbitrary field of
// SQLiteConn as a pointer, which would either return nil or segfault;
// the segfault case isn't recoverable, but the nil case at least
// returns a clean error rather than letting the program corrupt
// state via a bogus deserialize.
func verifyLayout(c *sqlite3.SQLiteConn) error {
	layoutOnce.Do(func() {
		handle := rawConnHandle(c)
		if handle == nil {
			layoutErr = fmt.Errorf(
				"memdb: SQLiteConn mirror returned a nil handle " +
					"(mattn/go-sqlite3 layout may have drifted from v1.14.42)")
			return
		}
		fname := C.memdb_db_filename_main((*C.sqlite3)(handle))
		if fname == nil {
			layoutErr = fmt.Errorf(
				"memdb: sqlite3_db_filename returned NULL through the mirrored " +
					"SQLiteConn handle (mattn/go-sqlite3 layout may have drifted)")
		}
	})
	return layoutErr
}

// deserializeResizable installs b as the current "main" database on
// c with SQLITE_DESERIALIZE_RESIZEABLE | SQLITE_DESERIALIZE_FREEONCLOSE.
// Subsequent writes are allowed to grow the database beyond len(b) via
// sqlite3_realloc64 (the missing flag is the root cause of the
// SQLITE_FULL bug fixed in v1.9.0).
//
// The buffer is allocated via sqlite3_malloc64 and ownership transfers
// to SQLite on success (FREEONCLOSE).
//
// On any failure the SQLite-allocated buffer is freed before returning.
func deserializeResizable(c *sqlite3.SQLiteConn, b []byte) error {
	if err := verifyLayout(c); err != nil {
		return err
	}
	handle := rawConnHandle(c)
	if handle == nil {
		// Defensive — verifyLayout already covers this, but if the
		// first-call-success path was followed by an unusual second
		// call with a nil handle (e.g. closed connection), reject
		// it rather than dereferencing.
		return fmt.Errorf("memdb: deserialize: nil sqlite3 handle")
	}

	sz := C.longlong(len(b))
	cbuf := C.memdb_sqlite3_malloc64(sz)
	if cbuf == nil {
		return fmt.Errorf("memdb: sqlite3_malloc64(%d) returned NULL", len(b))
	}
	if len(b) > 0 {
		copy(unsafe.Slice((*byte)(cbuf), len(b)), b)
	}

	zSchema := C.CString("main")
	defer C.free(unsafe.Pointer(zSchema))

	rc := C.memdb_deserialize_resizable((*C.sqlite3)(handle), zSchema, cbuf, sz)
	if rc != C.MEMDB_SQLITE_OK {
		// On failure sqlite3_deserialize does not take ownership of
		// pData — free it ourselves to avoid leaking the snapshot.
		C.memdb_sqlite3_free(cbuf)
		return fmt.Errorf("memdb: sqlite3_deserialize: rc=%d", int(rc))
	}
	return nil
}
