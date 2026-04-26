package server

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Extended Query Protocol implementation.
//
// Wire flow (one cycle):
//
//	Client                          Server
//	──────                          ──────
//	Parse('P')                  →
//	                            ←   ParseComplete('1')
//	Bind('B')                   →
//	                            ←   BindComplete('2')
//	Describe('D')               →
//	                            ←   ParameterDescription('t') + RowDescription('T') | NoData('n')
//	Execute('E')                →
//	                            ←   DataRow('D')* + CommandComplete('C') | EmptyQueryResponse('I')
//	Sync('S')                   →
//	                            ←   ReadyForQuery('Z')
//
// Format support: text format (code 0) only, both for parameters and for
// result columns. We reject binary-format requests outright rather than
// silently sending text-format bytes that a binary-decoding client would
// misinterpret. Most drivers fall back to text on this rejection; pgx is
// the notable exception and may need QueryExecModeSimpleProtocol when
// binary types it expects (e.g. timestamptz) are forced through this path.
//
// SQLite placeholder compatibility: the mattn/go-sqlite3 driver natively
// understands "$1", "$2", ... in addition to "?". Since PostgreSQL uses
// "$N" exclusively, no rewriting is needed — the SQL passes through to
// SQLite verbatim and parameters bind by position.

// preparedStmt is a server-side prepared statement created by Parse.
// We deliberately do NOT call sql.DB.Prepare here: every Exec/Query call
// already routes through memdb's per-SQL prepared-statement cache (see
// memdb/stmt_cache.go), so re-preparing would only add bookkeeping for
// no gain. We retain just enough to construct a portal binding later.
type preparedStmt struct {
	sql       string
	paramOIDs []uint32
	verb      string // first uppercase keyword — drives the query/exec dispatch
}

// portal holds a Bind result: a prepared statement plus its bound
// parameter values, ready for Execute. Param values arrive on the wire
// in either text or binary format; we decode text immediately into Go
// types and reject binary up front so the executor never has to guess.
//
// Rows are materialised eagerly at Describe('P') (or Execute, if
// Describe was skipped) — the alternative of holding *sql.Rows across
// messages races with the replica pool's refresh loop, which calls
// sqlite3_deserialize on idle replicas and silently invalidates any
// open cursor that releases its connection back to the pool. By
// snapshotting rows + columns into Go memory we trade O(rows) memory
// per portal for race-freedom; for typical extended-query result sets
// (parameterised SELECT, often a handful of rows) the trade is
// invisible. Workloads with very large parameterised result sets that
// can't afford the materialisation should prefer the simple-query
// path, which streams directly from the writer.
type portal struct {
	stmt          *preparedStmt
	args          []any
	resultFormats []int16
	cols          []string
	colTypes      []*sql.ColumnType
	rows          [][]any // materialised result; nil until openRows ran
	executed      bool    // true after openRows has been called once
}

// ── parsers ──────────────────────────────────────────────────────────────────

// readCString scans body starting at pos for a NUL-terminated string.
// Returns the string (excluding the NUL), the new position, and an error
// if no NUL is found. Used for every PG protocol field declared as
// "String" — message dispatch isn't fast enough to justify hand-unrolling
// past the loop overhead, but allocating a fresh []byte per field is what
// readCString avoids.
func readCString(body []byte, pos int) (string, int, error) {
	for i := pos; i < len(body); i++ {
		if body[i] == 0 {
			return string(body[pos:i]), i + 1, nil
		}
	}
	return "", 0, errors.New("missing NUL terminator")
}

// ── handlers: client→server messages ────────────────────────────────────────

func (h *handler) handleParse(body []byte) error {
	name, pos, err := readCString(body, 0)
	if err != nil {
		return h.extendedErrf("Parse: %v", err)
	}
	query, pos, err := readCString(body, pos)
	if err != nil {
		return h.extendedErrf("Parse: %v", err)
	}
	if pos+2 > len(body) {
		return h.extendedErrf("Parse: truncated param-count")
	}
	nParams := int(binary.BigEndian.Uint16(body[pos : pos+2]))
	pos += 2
	if pos+4*nParams > len(body) {
		return h.extendedErrf("Parse: truncated param-OIDs")
	}
	oids := make([]uint32, nParams)
	for i := 0; i < nParams; i++ {
		oids[i] = binary.BigEndian.Uint32(body[pos : pos+4])
		pos += 4
	}

	// Per spec: re-parsing the unnamed statement silently replaces it.
	// Re-parsing a named statement that already exists is a protocol
	// error — drivers manage their own cache and only Parse a name once.
	if name != "" {
		if _, exists := h.prepared[name]; exists {
			return h.extendedErrf("prepared statement %q already exists", name)
		}
	}

	// PostgreSQL infers the parameter count from the highest $N
	// referenced in the SQL when Parse declares fewer OIDs than the
	// query actually uses (the standard idiom — most drivers send 0
	// OIDs and let the server figure it out). lib/pq cross-checks the
	// Bind argv length against ParameterDescription, so a count
	// mismatch here surfaces as a confusing client-side error.
	inferred := countDollarPlaceholders(query)
	if inferred > len(oids) {
		padded := make([]uint32, inferred)
		copy(padded, oids)
		oids = padded
	}

	h.prepared[name] = &preparedStmt{
		sql:       query,
		paramOIDs: oids,
		verb:      firstWordUpper(query),
	}
	return h.writeRaw([]byte{'1', 0, 0, 0, 4})
}

func (h *handler) handleBind(body []byte) error {
	portalName, pos, err := readCString(body, 0)
	if err != nil {
		return h.extendedErrf("Bind: %v", err)
	}
	stmtName, pos, err := readCString(body, pos)
	if err != nil {
		return h.extendedErrf("Bind: %v", err)
	}
	stmt, ok := h.prepared[stmtName]
	if !ok {
		return h.extendedErrf("Bind: unknown prepared statement %q", stmtName)
	}

	// Parameter format codes.
	if pos+2 > len(body) {
		return h.extendedErrf("Bind: truncated param-format-count")
	}
	nFmt := int(binary.BigEndian.Uint16(body[pos : pos+2]))
	pos += 2
	paramFormats := make([]int16, nFmt)
	for i := 0; i < nFmt; i++ {
		if pos+2 > len(body) {
			return h.extendedErrf("Bind: truncated param-format")
		}
		paramFormats[i] = int16(binary.BigEndian.Uint16(body[pos : pos+2]))
		pos += 2
	}

	// Parameter values.
	if pos+2 > len(body) {
		return h.extendedErrf("Bind: truncated param-count")
	}
	nParams := int(binary.BigEndian.Uint16(body[pos : pos+2]))
	pos += 2
	args := make([]any, nParams)
	for i := 0; i < nParams; i++ {
		if pos+4 > len(body) {
			return h.extendedErrf("Bind: truncated param length")
		}
		paramLen := int32(binary.BigEndian.Uint32(body[pos : pos+4]))
		pos += 4
		if paramLen == -1 {
			args[i] = nil
			continue
		}
		if paramLen < 0 || pos+int(paramLen) > len(body) {
			return h.extendedErrf("Bind: invalid param length %d", paramLen)
		}
		// Determine format: if nFmt == 0 → all text. If nFmt == 1 →
		// that format applies to all params. Otherwise nFmt must equal
		// nParams and provide a per-param format.
		var format int16
		switch {
		case nFmt == 0:
			format = 0
		case nFmt == 1:
			format = paramFormats[0]
		case i < nFmt:
			format = paramFormats[i]
		default:
			return h.extendedErrf("Bind: format-count %d does not match param-count %d", nFmt, nParams)
		}
		if format != 0 {
			return h.extendedErrf("Bind: binary parameter format not supported (param %d)", i+1)
		}
		// Text format: hand the raw bytes as a string. SQLite does its
		// own coercion based on column type, so passing strings into
		// integer/real/blob columns works the same way "?" parameters
		// from the simple-query path do.
		args[i] = string(body[pos : pos+int(paramLen)])
		pos += int(paramLen)
	}

	// Result format codes. Stored on the portal and consulted at
	// streaming time so each cell can be encoded per the client's
	// per-column preference. Both 0 (text) and 1 (binary) are
	// accepted; binary serialisation is implemented for int4/int8,
	// float4/float8, bool, bytea, and text in encodeCellBinary, with
	// other OIDs falling through to text bytes — same wire shape as
	// PG sends for client-requested-binary on a type the server has
	// no binary codec for.
	if pos+2 > len(body) {
		return h.extendedErrf("Bind: truncated result-format-count")
	}
	nResFmt := int(binary.BigEndian.Uint16(body[pos : pos+2]))
	pos += 2
	resFormats := make([]int16, nResFmt)
	for i := 0; i < nResFmt; i++ {
		if pos+2 > len(body) {
			return h.extendedErrf("Bind: truncated result-format")
		}
		resFormats[i] = int16(binary.BigEndian.Uint16(body[pos : pos+2]))
		pos += 2
	}

	if portalName != "" {
		if _, exists := h.portals[portalName]; exists {
			return h.extendedErrf("portal %q already exists", portalName)
		}
	}
	h.portals[portalName] = &portal{
		stmt:          stmt,
		args:          args,
		resultFormats: resFormats,
	}
	return h.writeRaw([]byte{'2', 0, 0, 0, 4})
}

func (h *handler) handleDescribe(body []byte) error {
	if len(body) < 1 {
		return h.extendedErrf("Describe: empty body")
	}
	kind := body[0]
	name, _, err := readCString(body, 1)
	if err != nil {
		return h.extendedErrf("Describe: %v", err)
	}

	switch kind {
	case 'S':
		stmt, ok := h.prepared[name]
		if !ok {
			return h.extendedErrf("Describe: unknown statement %q", name)
		}
		if err := h.sendParameterDescription(stmt.paramOIDs); err != nil {
			return err
		}
		// lib/pq populates its column-count cache from this exact
		// response and uses it on later Scan(...) calls — sending
		// NoData here means a parameterised db.Query(...) reports
		// "expected 0 destination arguments". To produce the
		// RowDescription we need column names, but Bind hasn't
		// happened yet so we have no real arg values.
		//
		// Pragmatic approach: dry-run the query with NULL bound to
		// every placeholder. SQLite's column resolution depends on
		// the SELECT projection, not on row matches, so a SELECT
		// returns its proper column list even when the predicate
		// excludes everything. INSERT/UPDATE/DELETE without RETURNING
		// hit the !isQueryVerb branch above and skip this entirely.
		// RETURNING-shaped DML still hits this path; if NULL would
		// violate a constraint we just send NoData and rely on the
		// real Execute to surface the real shape.
		if !isQueryVerb(stmt.verb) {
			return h.writeRaw([]byte{'n', 0, 0, 0, 4})
		}
		cols, colTypes, err := peekColumns(h.db, stmt.sql, len(stmt.paramOIDs))
		if err != nil || len(cols) == 0 {
			// Fall back to NoData rather than ErrorResponse — a
			// well-behaved client that sees NoData here will simply
			// learn the shape on Execute, which is still
			// protocol-legal.
			return h.writeRaw([]byte{'n', 0, 0, 0, 4})
		}
		return h.sendRowDescriptionTyped(cols, colTypes)

	case 'P':
		p, ok := h.portals[name]
		if !ok {
			return h.extendedErrf("Describe: unknown portal %q", name)
		}
		// For a query-shaped portal, run the SQL to capture the column
		// list, then stash the *sql.Rows on the portal so Execute can
		// drain it without re-running. For DML, send NoData and let
		// Execute do the actual exec.
		if isQueryVerb(p.stmt.verb) {
			if err := p.openRows(h); err != nil {
				return h.extendedErrf("Describe: %v", err)
			}
			var firstRow []any
			if len(p.rows) > 0 {
				firstRow = p.rows[0]
			}
			return h.sendRowDescriptionWithFirstRow(p.cols, p.colTypes, firstRow, p.resultFormats)
		}
		return h.writeRaw([]byte{'n', 0, 0, 0, 4})

	default:
		return h.extendedErrf("Describe: unknown kind %c", kind)
	}
}

func (h *handler) handleExecute(body []byte) error {
	name, pos, err := readCString(body, 0)
	if err != nil {
		return h.extendedErrf("Execute: %v", err)
	}
	if pos+4 > len(body) {
		return h.extendedErrf("Execute: truncated max-rows")
	}
	maxRows := int(binary.BigEndian.Uint32(body[pos : pos+4]))
	p, ok := h.portals[name]
	if !ok {
		return h.extendedErrf("Execute: unknown portal %q", name)
	}

	if isQueryVerb(p.stmt.verb) {
		// Describe('P') may have already materialised rows. If not
		// (client jumped straight from Bind to Execute), do it now.
		// Either way, do not send a RowDescription here — per
		// protocol, an Execute that follows a Describe must NOT
		// repeat the description.
		if !p.executed {
			if err := p.openRows(h); err != nil {
				return h.extendedErrf("Execute: %v", err)
			}
		}
		count, err := h.streamPortalRows(p, maxRows)
		if err != nil {
			return h.extendedErrf("Execute: %v", err)
		}
		// CommandComplete tag for SELECT is "SELECT N".
		return h.sendCommandCompleteCount("SELECT ", count)
	}

	// Exec path (INSERT / UPDATE / DELETE / CREATE / DROP / ...).
	res, err := h.db.ExecContext(context.Background(), p.stmt.sql, p.args...)
	if err != nil {
		return h.extendedErrf("Execute: %v", err)
	}
	tag, err := commandTagFor(p.stmt.verb, res)
	if err != nil {
		return h.extendedErrf("Execute: %v", err)
	}
	return h.sendCommandComplete(tag)
}

func (h *handler) handleSync() error {
	// Sync ends the implicit extended-query transaction. Drop any
	// materialised rows so a subsequent Bind on the same portal name
	// re-runs the SQL with fresh args. Memory: the previous query's
	// row snapshot is GC'd here.
	for _, p := range h.portals {
		p.rows = nil
		p.executed = false
	}
	h.inErrorState = false
	return h.sendReadyForQuery()
}

func (h *handler) handleClose(body []byte) error {
	if len(body) < 1 {
		return h.extendedErrf("Close: empty body")
	}
	kind := body[0]
	name, _, err := readCString(body, 1)
	if err != nil {
		return h.extendedErrf("Close: %v", err)
	}
	switch kind {
	case 'S':
		// Closing a statement also invalidates portals derived from
		// it — drivers normally clean those up explicitly, but the
		// spec is implicit and a leaked portal hanging onto a now-gone
		// stmt pointer would crash on Execute. Defensive sweep.
		delete(h.prepared, name)
		for pName, p := range h.portals {
			if p.stmt != nil && p.stmt == h.prepared[name] {
				delete(h.portals, pName)
			}
		}
	case 'P':
		delete(h.portals, name)
	default:
		return h.extendedErrf("Close: unknown kind %c", kind)
	}
	return h.writeRaw([]byte{'3', 0, 0, 0, 4})
}

// ── helpers ──────────────────────────────────────────────────────────────────

// peekColumns runs the query once with NULL bound to every placeholder
// to extract the result-column names + types without needing the real
// argument values. The query is opened, metadata extracted, then
// immediately closed — no rows are streamed and SQLite's
// prepared-statement cache keeps the cost negligible (~microseconds).
//
// Used by Describe('S') when the caller expects a RowDescription
// before Bind. Errors fall back to NoData at the call site rather than
// surfacing here, so a peek that violates a NOT NULL constraint on
// INSERT...RETURNING does not break the connection.
func peekColumns(db queryRunner, sql string, nParams int) ([]string, []*sql.ColumnType, error) {
	args := make([]any, nParams)
	for i := range args {
		args[i] = nil
	}
	rows, err := db.Query(sql, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	colTypes, _ := rows.ColumnTypes()
	return cols, colTypes, nil
}

// queryRunner is the *memdb.DB subset peekColumns needs. Defining the
// interface locally avoids an import cycle in tests that want to plug
// in a mock without depending on the real driver.
type queryRunner interface {
	Query(sql string, args ...any) (*sql.Rows, error)
}

// countDollarPlaceholders returns the highest N referenced as $N in
// sql, ignoring matches inside single-quoted strings, double-quoted
// identifiers, and SQL comments. PostgreSQL itself does the same scan
// when Parse leaves the parameter count unspecified — drivers like
// lib/pq cross-check this against their argv length, so getting it
// right is what makes parameterised Exec/Query work.
//
// SQLite-specific dollar-quoted strings (`$tag$...$tag$`) don't exist
// — that syntax is PostgreSQL-only and we route everything to SQLite,
// so we don't have to handle it.
func countDollarPlaceholders(sql string) int {
	maxN := 0
	inSingle := false
	inDouble := false
	inLine := false
	inBlock := false
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		switch {
		case inLine:
			if c == '\n' {
				inLine = false
			}
		case inBlock:
			if c == '*' && i+1 < len(sql) && sql[i+1] == '/' {
				inBlock = false
				i++
			}
		case inSingle:
			if c == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					// '' is an escaped single quote; consume both.
					i++
				} else {
					inSingle = false
				}
			}
		case inDouble:
			if c == '"' {
				inDouble = false
			}
		case c == '\'':
			inSingle = true
		case c == '"':
			inDouble = true
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			inLine = true
			i++
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			inBlock = true
			i++
		case c == '$':
			j := i + 1
			for j < len(sql) && sql[j] >= '0' && sql[j] <= '9' {
				j++
			}
			if j > i+1 {
				n, err := strconv.Atoi(sql[i+1 : j])
				if err == nil && n > maxN {
					maxN = n
				}
				i = j - 1
			}
		}
	}
	return maxN
}

// isQueryVerb reports whether the SQL verb returns rows. The same
// keyword set the simple-query dispatch uses (handler.go) — keeping
// them in sync ensures Describe and Execute take consistent paths.
func isQueryVerb(verb string) bool {
	switch verb {
	case "SELECT", "WITH", "EXPLAIN", "PRAGMA", "SHOW", "VALUES":
		return true
	default:
		return false
	}
}

// commandTagFor builds the CommandComplete tag for a non-query SQL
// statement. PostgreSQL has slightly different formats per verb:
// "INSERT 0 N" (the leading 0 is a long-deprecated OID field that
// drivers still parse), "UPDATE N", "DELETE N", "CREATE TABLE", etc.
// We match those for the verbs we expect; everything else falls back
// to the bare verb so an unknown DDL doesn't error.
func commandTagFor(verb string, res sql.Result) (string, error) {
	switch verb {
	case "INSERT":
		n, err := res.RowsAffected()
		if err != nil {
			return "", err
		}
		// "INSERT 0 N" — the 0 is the legacy oid field.
		var b strings.Builder
		b.WriteString("INSERT 0 ")
		b.WriteString(strconv.FormatInt(n, 10))
		return b.String(), nil
	case "UPDATE", "DELETE":
		n, err := res.RowsAffected()
		if err != nil {
			return "", err
		}
		return verb + " " + strconv.FormatInt(n, 10), nil
	case "CREATE":
		// Best-effort: PG sends "CREATE TABLE" / "CREATE INDEX" / etc.
		// We don't parse the SQL here — the bare "CREATE" tag is
		// sufficient for psql, pgx, lib/pq.
		return "CREATE", nil
	case "DROP":
		return "DROP", nil
	default:
		return verb, nil
	}
}

// openRows runs the portal's prepared SQL with its bound args, drains
// every row into p.rows, and immediately closes the underlying
// *sql.Rows. Subsequent Describe('P') and Execute calls read from the
// in-memory snapshot — they never hold a connection across messages,
// so concurrent replica-pool refreshes can't invalidate them.
//
// Idempotent: a second call (e.g. Execute when Describe('P') already
// ran) returns immediately.
func (p *portal) openRows(h *handler) error {
	if p.executed {
		return nil
	}
	rows, err := h.db.QueryContext(context.Background(), p.stmt.sql, p.args...)
	if err != nil {
		p.executed = true
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		p.executed = true
		return err
	}
	colTypes, _ := rows.ColumnTypes()
	p.cols = cols
	p.colTypes = colTypes

	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			p.executed = true
			return err
		}
		p.rows = append(p.rows, vals)
	}
	if err := rows.Err(); err != nil {
		p.executed = true
		return err
	}
	p.executed = true
	return nil
}

// sendRowDescriptionWithFirstRow emits a 'T' RowDescription, falling
// back to value-based type inference for columns SQLite can't
// statically describe. firstRow may be nil for empty result sets, in
// which case the columns inherit the default OID 25 (text) from
// sendRowDescriptionTyped. formats are the per-column wire formats
// matching what streamPortalRows will emit; correctness depends on
// these matching the formats in the portal's Bind.
func (h *handler) sendRowDescriptionWithFirstRow(cols []string, colTypes []*sql.ColumnType, firstRow []any, formats []int16) error {
	overrides := make([]uint32, len(cols))
	for i := range cols {
		overrides[i] = 25
		if i < len(colTypes) && colTypes[i] != nil {
			if name := colTypes[i].DatabaseTypeName(); name != "" {
				overrides[i] = pgOIDFromSQLite(name)
				continue
			}
		}
		if i < len(firstRow) {
			overrides[i] = pgOIDFromGoValue(firstRow[i])
		}
	}
	return h.sendRowDescriptionWithOIDs(cols, overrides, formats)
}

// pgOIDFromGoValue maps a Go value (as returned from sql.Rows.Scan
// into *interface{}) to the PG OID that best represents its concrete
// type. Used as a fallback when SQLite can't statically describe a
// column — e.g. SELECT 7 has dbtype="" but the value is an int64.
func pgOIDFromGoValue(v any) uint32 {
	switch v.(type) {
	case int64, int, int32:
		return 20 // int8
	case float64, float32:
		return 701 // float8
	case bool:
		return 16 // bool
	case []byte:
		return 17 // bytea — drivers treat raw byte slices as bytea
	case string:
		return 25 // text
	case nil:
		return 25 // text default for NULL columns
	default:
		return 25
	}
}

// streamPortalRows writes DataRow messages from the materialised
// p.rows snapshot, stopping when the slice is drained or maxRows
// non-zero is reached. The buffer-reuse pattern mirrors the
// simple-query handleSelect path so the per-row alloc savings extend
// to the extended-query flow.
//
// On non-zero maxRows the protocol allows Execute to be called again
// later to fetch more rows; this implementation supports that by
// trimming the consumed prefix off p.rows on each call.
func (h *handler) streamPortalRows(p *portal, maxRows int) (int64, error) {
	cols := p.cols
	colTypes := p.colTypes
	rb := rowBuffers{row: make([][]byte, len(cols))}
	if cap(rb.cellOffsets) < 2*len(cols) {
		rb.cellOffsets = make([]int, 2*len(cols))
	}

	// Per-column format selector. resultFormats has three legal
	// shapes: empty (all text), one entry (applies to every column),
	// or one entry per column.
	formatFor := func(i int) int16 {
		switch len(p.resultFormats) {
		case 0:
			return 0
		case 1:
			return p.resultFormats[0]
		default:
			if i < len(p.resultFormats) {
				return p.resultFormats[i]
			}
			return 0
		}
	}

	encodeRow := func(values []any) error {
		rb.reset()
		rb.cellOffsets = rb.cellOffsets[:2*len(values)]
		for i, v := range values {
			if v == nil {
				rb.cellOffsets[2*i] = -1
				rb.cellOffsets[2*i+1] = 0
				continue
			}
			start := len(rb.cellBuf)
			if formatFor(i) == 1 {
				var oid uint32 = 25
				if i < len(colTypes) && colTypes[i] != nil {
					oid = pgOIDFromSQLite(colTypes[i].DatabaseTypeName())
				}
				if oid == 25 {
					oid = pgOIDFromGoValue(v)
				}
				rb.cellBuf = encodeCellBinary(rb.cellBuf, oid, v)
			} else {
				rb.cellBuf = appendCell(rb.cellBuf, v)
			}
			rb.cellOffsets[2*i] = start
			rb.cellOffsets[2*i+1] = len(rb.cellBuf) - start
		}
		for i := range values {
			start := rb.cellOffsets[2*i]
			if start < 0 {
				rb.row[i] = nil
				continue
			}
			n := rb.cellOffsets[2*i+1]
			rb.row[i] = rb.cellBuf[start : start+n]
		}
		return h.sendDataRowInto(&rb, rb.row)
	}

	var count int64
	consumed := 0
	for _, row := range p.rows {
		if err := encodeRow(row); err != nil {
			return count, err
		}
		count++
		consumed++
		if maxRows > 0 && count >= int64(maxRows) {
			break
		}
	}
	// Drop the rows we just sent so a subsequent Execute on the same
	// portal continues from where we left off (per protocol — partial
	// Execute with maxRows is rare in practice but still correct).
	p.rows = p.rows[consumed:]
	return count, nil
}

// encodeCellBinary appends the PG binary-format representation of v
// (interpreted under the given type OID) to dst. PG's binary format
// is documented per type in src/backend/utils/adt/*.c. We implement
// the common scalar types — anything else falls through to a text
// representation, which matches what PG itself does for types that
// lack a binary codec on the server.
//
// Numeric types are big-endian; floats use IEEE 754; bool is a single
// 0/1 byte; bytea/text are written verbatim. The driver-supplied
// values from sql.Rows.Scan come back as int64 / float64 / bool /
// []byte / string (mattn/go-sqlite3's standard mapping), so most
// branches here are simple type-switches.
func encodeCellBinary(dst []byte, oid uint32, v any) []byte {
	switch oid {
	case 20: // int8
		var n int64
		switch x := v.(type) {
		case int64:
			n = x
		case int:
			n = int64(x)
		case []byte:
			parsed, err := strconv.ParseInt(string(x), 10, 64)
			if err != nil {
				return appendCell(dst, v)
			}
			n = parsed
		case string:
			parsed, err := strconv.ParseInt(x, 10, 64)
			if err != nil {
				return appendCell(dst, v)
			}
			n = parsed
		default:
			return appendCell(dst, v)
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(n))
		return append(dst, buf[:]...)

	case 23: // int4
		var n int32
		switch x := v.(type) {
		case int64:
			n = int32(x)
		case int:
			n = int32(x)
		default:
			return appendCell(dst, v)
		}
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], uint32(n))
		return append(dst, buf[:]...)

	case 701: // float8
		var f float64
		switch x := v.(type) {
		case float64:
			f = x
		case int64:
			f = float64(x)
		default:
			return appendCell(dst, v)
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], math.Float64bits(f))
		return append(dst, buf[:]...)

	case 16: // bool
		switch x := v.(type) {
		case bool:
			if x {
				return append(dst, 1)
			}
			return append(dst, 0)
		case int64:
			if x != 0 {
				return append(dst, 1)
			}
			return append(dst, 0)
		default:
			return appendCell(dst, v)
		}

	case 17: // bytea
		switch x := v.(type) {
		case []byte:
			return append(dst, x...)
		case string:
			return append(dst, x...)
		default:
			return appendCell(dst, v)
		}

	case 25: // text — wire-identical to text format
		return appendCell(dst, v)

	default:
		// Unknown OID: emit text bytes. The client either accepts
		// this (some do, treating binary same as text for unknown
		// types) or surfaces a decode error of its own — there is
		// no better fallback short of reporting a binary codec we
		// don't have.
		return appendCell(dst, v)
	}
}

// sendParameterDescription emits the 't' message describing the
// parameter type OIDs declared at Parse time. This is what pgx and
// lib/pq use to decide whether they can send a parameter as binary on
// the next Bind. Since we reject binary params anyway, the values we
// send here are advisory only.
func (h *handler) sendParameterDescription(oids []uint32) error {
	size := 1 + 4 + 2 + 4*len(oids)
	buf := make([]byte, size)
	buf[0] = 't'
	binary.BigEndian.PutUint32(buf[1:5], uint32(size-1))
	binary.BigEndian.PutUint16(buf[5:7], uint16(len(oids)))
	for i, oid := range oids {
		binary.BigEndian.PutUint32(buf[7+4*i:11+4*i], oid)
	}
	return h.writeRaw(buf)
}

// extendedErrf sends an ErrorResponse and puts the connection into the
// extended-query error state, where every subsequent message until the
// next Sync is silently dropped. Returns nil on a successful send so
// the caller can simply `return h.extendedErrf(...)` without losing the
// distinction between "wire is broken" (real error) and "this message
// failed but the connection lives".
func (h *handler) extendedErrf(format string, args ...any) error {
	msg := fmt.Sprintf(format, args...)
	h.inErrorState = true
	if err := h.sendError(msg); err != nil {
		return err
	}
	return h.bufW.Flush()
}
