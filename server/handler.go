package server

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/voicetel/memdb"
)

// handler manages a single client connection using a minimal subset of the
// PostgreSQL wire protocol (v3). Enough for psql, pgx, psycopg2, and node-postgres.
//
// Full protocol spec: https://www.postgresql.org/docs/current/protocol.html
type handler struct {
	db          *memdb.DB
	cfg         Config
	conn        net.Conn
	bufR        *bufio.Reader
	bufW        *bufio.Writer
	startupUser string // username from startup packet

	// readBuf is the per-connection scratch buffer used by readMessage to
	// hold the body of the most recently read wire message. It grows once
	// to the largest message ever seen and is then reused for the lifetime
	// of the handler. A previous version of readMessage allocated a fresh
	// []byte per call — the alloc-only pprof diff showed this at ~15 MB
	// of churn over a 3-second SELECT workload. Callers that retain any
	// part of the returned body past the next readMessage call MUST copy
	// (e.g. via string conversion in trimTrailingNUL).
	readBuf []byte

	// cmdBuf is the per-connection scratch buffer used to assemble
	// CommandComplete frames. It grows once to the widest tag ever seen
	// and is then reused for the lifetime of the handler. The previous
	// response path made two per-response allocations — one in
	// sendCommandComplete (the wire frame) and one in a now-removed
	// buildTag helper that materialised the tag string — together
	// accounting for ~20 MB of churn per 3 s INSERT pprof run.
	cmdBuf []byte
}

// writeBufSize is the bufio.Writer buffer size for each client connection.
// 32 KB covers a ~500-row result set (~25 KB) in a single flush, eliminating
// the ~6 implicit mid-response Flush calls (each a write(2) syscall) that the
// previous 4 KB buffer triggered on wide SELECT workloads. Narrow queries and
// DML responses are well under 4 KB and see no change.
const writeBufSize = 32 * 1024

func newHandler(db *memdb.DB, cfg Config, conn net.Conn) *handler {
	return &handler{
		db:   db,
		cfg:  cfg,
		conn: conn,
		bufR: bufio.NewReaderSize(conn, 4096),
		bufW: bufio.NewWriterSize(conn, writeBufSize),
	}
}

func (h *handler) serve() {
	defer h.conn.Close()
	defer h.bufW.Flush() // best-effort final flush

	// Set a generous idle timeout — refreshed per message.
	const idleTimeout = 5 * time.Minute

	if err := h.handleStartup(); err != nil {
		return
	}

	for {
		_ = h.conn.SetReadDeadline(time.Now().Add(idleTimeout))
		msgType, body, err := h.readMessage()
		if err != nil {
			return
		}
		// Reset deadline for the write/response.
		_ = h.conn.SetWriteDeadline(time.Now().Add(idleTimeout))

		switch msgType {
		case 'Q': // Simple Query
			query := trimTrailingNUL(body)
			if err := h.handleSimpleQuery(query); err != nil {
				return
			}

		case 'X': // Terminate
			return

		default:
			_ = h.sendError(fmt.Sprintf("unsupported message type: %c", msgType))
		}
	}
}

func trimTrailingNUL(b []byte) string {
	n := len(b)
	for n > 0 && b[n-1] == 0 {
		n--
	}
	return string(b[:n])
}

func (h *handler) handleStartup() error {
	buf := make([]byte, 8)
	if _, err := io.ReadFull(h.bufR, buf); err != nil {
		return err
	}

	// Check for SSLRequest magic number (80877103) before reading startup.
	// psql and most drivers send this before the real startup message.
	// We decline TLS upgrade at this layer (TLS should be configured on
	// the listener itself) by responding with 'N'.
	magic := int(binary.BigEndian.Uint32(buf[4:8]))
	if magic == 80877103 {
		if err := h.writeRaw([]byte{'N'}); err != nil {
			return err
		}
		if err := h.bufW.Flush(); err != nil {
			return err
		}
		// Re-read the actual startup message.
		if _, err := io.ReadFull(h.bufR, buf); err != nil {
			return err
		}
	}

	length := int(binary.BigEndian.Uint32(buf[0:4]))
	// Sanity-check: startup messages are at most a few hundred bytes.
	// Reject anything over 64 KB to prevent memory exhaustion.
	const maxStartupLen = 65536
	if length < 8 || length > maxStartupLen {
		return fmt.Errorf("server: invalid startup packet length %d", length)
	}
	if length > 8 {
		rest := make([]byte, length-8)
		if _, err := io.ReadFull(h.bufR, rest); err != nil {
			return err
		}
		// Parse username from startup params if Auth is configured.
		// Startup params are NUL-terminated key=value pairs after the protocol version.
		// We skip the 4-byte protocol version already consumed, so rest contains
		// the key-value pairs.
		if h.cfg.Auth != nil {
			h.startupUser = parseStartupUser(rest)
		}
	}

	if h.cfg.Auth != nil {
		if err := h.writeRaw([]byte{'R', 0, 0, 0, 8, 0, 0, 0, 3}); err != nil {
			return err
		}
		if err := h.bufW.Flush(); err != nil {
			return err
		}

		msgType, body, err := h.readMessage()
		if err != nil || msgType != 'p' {
			_ = h.sendError("authentication failed")
			_ = h.bufW.Flush()
			return fmt.Errorf("auth: expected password message")
		}
		password := trimTrailingNUL(body)
		user := h.startupUser
		if user == "" {
			user = "memdb" // default if client didn't send username
		}
		if !h.cfg.Auth.Authenticate(user, password) {
			_ = h.sendError("authentication failed")
			_ = h.bufW.Flush()
			return fmt.Errorf("auth: invalid credentials")
		}
	}

	// AuthenticationOk
	if err := h.writeRaw([]byte{'R', 0, 0, 0, 8, 0, 0, 0, 0}); err != nil {
		return err
	}
	// ReadyForQuery (idle)
	return h.sendReadyForQuery()
}

// parseStartupUser extracts the "user" parameter from PostgreSQL startup
// key-value pairs (NUL-terminated key\0value\0 sequences).
func parseStartupUser(params []byte) string {
	// params is the rest after the 8-byte header, so it's already key-value pairs.
	i := 0
	for i < len(params) {
		// Read key
		j := i
		for j < len(params) && params[j] != 0 {
			j++
		}
		key := string(params[i:j])
		if j >= len(params) {
			break
		}
		j++ // skip NUL
		// Read value
		k := j
		for k < len(params) && params[k] != 0 {
			k++
		}
		value := string(params[j:k])
		if key == "user" {
			return value
		}
		if k >= len(params) {
			break
		}
		i = k + 1
	}
	return ""
}

// firstWordUpper returns the first whitespace-delimited word of s, uppercased
// (ASCII only). If the word is already uppercase, the original string segment
// is returned without allocation.
func firstWordUpper(s string) string {
	// Skip leading whitespace.
	i := 0
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}
	if i == len(s) {
		return ""
	}
	start := i
	for i < len(s) && s[i] != ' ' && s[i] != '\t' && s[i] != '\n' && s[i] != '\r' && s[i] != ';' {
		i++
	}
	word := s[start:i]
	// ASCII-upper without allocation when already upper, else alloc.
	for _, c := range []byte(word) {
		if c >= 'a' && c <= 'z' {
			// Mixed or lower case — allocate and upper.
			b := make([]byte, len(word))
			for j, c := range []byte(word) {
				if c >= 'a' && c <= 'z' {
					b[j] = c - 32
				} else {
					b[j] = c
				}
			}
			return string(b)
		}
	}
	return word // already upper, no alloc
}

// handleSimpleQuery executes query and streams results back to the client.
// Returns an error only when the connection is broken and should be closed.
func (h *handler) handleSimpleQuery(query string) error {
	verb := firstWordUpper(query)
	if verb == "" {
		// Empty query — respond with empty CommandComplete and ReadyForQuery.
		if err := h.sendCommandComplete(""); err != nil {
			return err
		}
		return h.sendReadyForQuery()
	}
	switch verb {
	case "SELECT", "WITH", "EXPLAIN", "PRAGMA", "SHOW":
		return h.handleSelect(query)
	default:
		return h.handleExec(query, verb)
	}
}

// appendCell appends the text representation of v to dst, avoiding the
// allocation overhead of fmt.Sprintf("%v", v) for common types. The dst
// slice is grown in place so callers that hand in a reused buffer pay
// only for the incremental bytes, not a fresh allocation per cell.
func appendCell(dst []byte, v any) []byte {
	switch t := v.(type) {
	case nil:
		return nil
	case string:
		return append(dst, t...)
	case []byte:
		return append(dst, t...)
	case int64:
		return strconv.AppendInt(dst, t, 10)
	case int:
		return strconv.AppendInt(dst, int64(t), 10)
	case float64:
		return strconv.AppendFloat(dst, t, 'g', -1, 64)
	case bool:
		return strconv.AppendBool(dst, t)
	case time.Time:
		return t.AppendFormat(dst, time.RFC3339Nano)
	default:
		return fmt.Append(dst, v) // fallback for unknown types
	}
}

// rowBuffers bundles the three per-row allocations handleSelect used to
// make on every iteration so they can be reused across rows in a single
// query. Measured in pprof of the wide-SELECT scenario (500 rows/query,
// 8 concurrent clients):
//
//	row := make([][]byte, len(cols))  — 261 MB (21% of all allocations)
//	appendCell(nil, v) per cell       — 171 MB (14%)
//	buf := make([]byte, size) in
//	  sendDataRow                     — 260 MB (21%)
//
// Reusing each of these across the rows of a single query eliminates
// roughly 57% of server-side allocation churn while preserving the
// wire-protocol semantics — every row is fully written to bufW before
// we reuse the backing storage for the next row.
type rowBuffers struct {
	// row holds one sub-slice per column pointing into cellBuf. Sized
	// once from len(cols) and reused across rows.
	row [][]byte
	// cellBuf is a contiguous scratch buffer. Each row's per-cell
	// appendCell output is written into cellBuf sequentially and the
	// row sub-slices are repointed to the new offsets. Sized to
	// accommodate the widest row seen so far.
	cellBuf []byte
	// cellOffsets holds (offset, length) pairs — two ints per column —
	// recording where each non-nil cell's bytes live in cellBuf. A
	// negative offset sentinel marks a NULL cell. Used to recover
	// stable sub-slices AFTER all per-row appendCell calls have
	// completed, since append() may have grown cellBuf and moved its
	// backing array mid-loop.
	cellOffsets []int
	// wire is the outbound serialised DataRow message, reused across
	// rows by sendDataRowInto. Sized to accommodate the largest row
	// wire-payload seen so far.
	wire []byte
}

// reset prepares rb for a new row: clears the sub-slice pointers in
// row[:] so old aliases cannot be observed, and truncates cellBuf to
// zero length (retaining capacity). cellOffsets is re-sized on each
// query by the caller rather than here so the grow path is in one place.
func (rb *rowBuffers) reset() {
	for i := range rb.row {
		rb.row[i] = nil
	}
	rb.cellBuf = rb.cellBuf[:0]
}

func (h *handler) handleSelect(query string) error {
	rows, err := h.db.Query(query)
	if err != nil {
		if werr := h.sendError(err.Error()); werr != nil {
			return werr
		}
		return h.sendReadyForQuery()
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		if werr := h.sendError(err.Error()); werr != nil {
			return werr
		}
		return h.sendReadyForQuery()
	}

	if len(cols) > 0 {
		if err := h.sendRowDescription(cols); err != nil {
			return err
		}
	}

	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}

	// Per-query reusable buffers. Allocated once; their backing storage
	// grows over the first few rows and then stays stable for the
	// remainder of the query — per the pprof finding this turns three
	// per-row allocations into three per-query allocations.
	rb := rowBuffers{
		row: make([][]byte, len(cols)),
	}

	count := 0
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			if werr := h.sendError(err.Error()); werr != nil {
				return werr
			}
			return h.sendReadyForQuery()
		}

		// Serialise every non-nil cell into rb.cellBuf contiguously,
		// tracking each cell's [start, end) offset into a small
		// scratch slice (cellOffsets below). A second, post-append
		// pass then repoints rb.row[i] into the final backing array
		// — this is required because append() may grow cellBuf and
		// invalidate any slice headers captured mid-loop.
		//
		// Using offsets rather than provisional sub-slices makes the
		// two passes mechanically obvious: the first pass records
		// only (offset, length) pairs; the second pass materialises
		// stable sub-slices from them. No fragile "fixup" step that
		// reconstructs lengths from earlier slice headers.
		rb.reset()
		// Grow the per-column offset scratch once per query. Two
		// ints per column so a single backing array covers both
		// (offset, length) without another allocation.
		if cap(rb.cellOffsets) < 2*len(vals) {
			rb.cellOffsets = make([]int, 2*len(vals))
		} else {
			rb.cellOffsets = rb.cellOffsets[:2*len(vals)]
		}
		for i, v := range vals {
			if v == nil {
				rb.cellOffsets[2*i] = -1 // NULL sentinel
				rb.cellOffsets[2*i+1] = 0
				continue
			}
			start := len(rb.cellBuf)
			rb.cellBuf = appendCell(rb.cellBuf, v)
			rb.cellOffsets[2*i] = start
			rb.cellOffsets[2*i+1] = len(rb.cellBuf) - start
		}
		// Second pass: materialise stable sub-slices from the final
		// rb.cellBuf. Safe now because no further append() will
		// touch cellBuf before sendDataRowInto consumes rb.row.
		for i := range vals {
			start := rb.cellOffsets[2*i]
			if start < 0 {
				rb.row[i] = nil
				continue
			}
			n := rb.cellOffsets[2*i+1]
			rb.row[i] = rb.cellBuf[start : start+n]
		}

		if err := h.sendDataRowInto(&rb, rb.row); err != nil {
			return err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		if werr := h.sendError(err.Error()); werr != nil {
			return werr
		}
		return h.sendReadyForQuery()
	}

	if err := h.sendCommandCompleteCount("SELECT ", int64(count)); err != nil {
		return err
	}
	return h.sendReadyForQuery()
}

func (h *handler) handleExec(query, verb string) error {
	result, err := h.db.Exec(query)
	if err != nil {
		if werr := h.sendError(err.Error()); werr != nil {
			return werr
		}
		return h.sendReadyForQuery()
	}

	// result.RowsAffected() can return an error for DDL or drivers that don't
	// support it; treat such cases as "0 rows affected" rather than failing
	// the statement.
	rowsAffected, _ := result.RowsAffected()
	switch verb {
	case "INSERT":
		if err := h.sendCommandCompleteCount("INSERT 0 ", rowsAffected); err != nil {
			return err
		}
	case "UPDATE":
		if err := h.sendCommandCompleteCount("UPDATE ", rowsAffected); err != nil {
			return err
		}
	case "DELETE":
		if err := h.sendCommandCompleteCount("DELETE ", rowsAffected); err != nil {
			return err
		}
	default:
		if err := h.sendCommandComplete(verb); err != nil {
			return err
		}
	}
	return h.sendReadyForQuery()
}

// ── wire helpers ─────────────────────────────────────────────────────────────

// maxMessageLen caps attacker-controlled message lengths at 16 MB to prevent
// a malicious 5-byte header from triggering a ~4 GB allocation.
const maxMessageLen = 16 * 1024 * 1024

func (h *handler) readMessage() (byte, []byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(h.bufR, header[:]); err != nil {
		return 0, nil, err
	}
	msgType := header[0]
	length := int(binary.BigEndian.Uint32(header[1:5]))
	if length < 4 || length > maxMessageLen {
		return 0, nil, fmt.Errorf("server: invalid message length %d", length)
	}
	bodyLen := length - 4
	if cap(h.readBuf) < bodyLen {
		h.readBuf = make([]byte, bodyLen)
	} else {
		h.readBuf = h.readBuf[:bodyLen]
	}
	if _, err := io.ReadFull(h.bufR, h.readBuf); err != nil {
		return 0, nil, err
	}
	return msgType, h.readBuf, nil
}

func (h *handler) sendRowDescription(cols []string) error {
	fieldCount := len(cols)
	// Per-column fixed overhead: name + NUL (1) + 4 + 2 + 4 + 2 + 4 + 2 = 19 bytes.
	size := 1 + 4 + 2
	for _, col := range cols {
		size += len(col) + 1 + 4 + 2 + 4 + 2 + 4 + 2
	}
	buf := make([]byte, size)
	buf[0] = 'T'
	pos := 5
	buf[pos] = byte(fieldCount >> 8)
	buf[pos+1] = byte(fieldCount)
	pos += 2
	for _, col := range cols {
		copy(buf[pos:], col)
		pos += len(col)
		buf[pos] = 0 // null terminator
		pos++
		// table OID = 0
		buf[pos], buf[pos+1], buf[pos+2], buf[pos+3] = 0, 0, 0, 0
		pos += 4
		// attr number = 0
		buf[pos], buf[pos+1] = 0, 0
		pos += 2
		// data type OID (25 = text)
		buf[pos], buf[pos+1], buf[pos+2], buf[pos+3] = 0, 0, 0, 25
		pos += 4
		// type size = -1 (variable)
		buf[pos], buf[pos+1] = 0xff, 0xff
		pos += 2
		// type modifier = -1
		buf[pos], buf[pos+1], buf[pos+2], buf[pos+3] = 0xff, 0xff, 0xff, 0xff
		pos += 4
		// format = 0 (text)
		buf[pos], buf[pos+1] = 0, 0
		pos += 2
	}
	binary.BigEndian.PutUint32(buf[1:5], uint32(size-1))
	return h.writeRaw(buf)
}

// sendDataRowInto serialises row into rb.wire (growing it as needed) and
// writes the resulting DataRow message to the buffered writer. Reusing
// rb.wire across rows removes the per-row make([]byte, size) allocation
// that pprof identified as ~21% of all server-side allocations on the
// wide-SELECT profile.
//
// The wire buffer is grown via slice-expansion semantics, so its backing
// storage stabilises after the first few rows of a query and stays
// constant for the remainder — subsequent rows incur no further
// allocations for the wire framing.
func (h *handler) sendDataRowInto(rb *rowBuffers, row [][]byte) error {
	// Calculate total size: 1 (type) + 4 (length) + 2 (field count) +
	// per-cell (4 bytes for length or NULL marker, + data bytes for non-null cells).
	size := 1 + 4 + 2
	for _, val := range row {
		size += 4
		if val != nil {
			size += len(val)
		}
	}

	// Grow rb.wire in place. If cap is sufficient we simply reslice;
	// otherwise append allocates a new backing array (~1 alloc per
	// doubling, not per row).
	if cap(rb.wire) < size {
		rb.wire = make([]byte, size)
	} else {
		rb.wire = rb.wire[:size]
	}
	buf := rb.wire

	buf[0] = 'D'
	// buf[1..5] = length (written at end)
	pos := 5
	buf[pos] = byte(len(row) >> 8)
	buf[pos+1] = byte(len(row))
	pos += 2
	for _, val := range row {
		if val == nil {
			buf[pos] = 0xff
			buf[pos+1] = 0xff
			buf[pos+2] = 0xff
			buf[pos+3] = 0xff
			pos += 4
		} else {
			binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(len(val)))
			pos += 4
			copy(buf[pos:], val)
			pos += len(val)
		}
	}
	binary.BigEndian.PutUint32(buf[1:5], uint32(size-1))
	return h.writeRaw(buf)
}

func (h *handler) sendCommandComplete(tag string) error {
	h.cmdBuf = appendCommandCompleteHeader(h.cmdBuf[:0])
	h.cmdBuf = append(h.cmdBuf, tag...)
	h.cmdBuf = append(h.cmdBuf, 0)
	binary.BigEndian.PutUint32(h.cmdBuf[1:5], uint32(len(h.cmdBuf)-1))
	return h.writeRaw(h.cmdBuf)
}

// sendCommandCompleteCount writes a CommandComplete with a "PREFIX N"
// tag (e.g. "INSERT 0 42", "SELECT 17") directly into h.cmdBuf without
// first materialising the tag as a Go string. This collapses what used
// to be three allocations per response (buildTag's []byte, buildTag's
// string conversion, and sendCommandComplete's wire-frame make) down
// to zero allocations once cmdBuf has stabilised.
func (h *handler) sendCommandCompleteCount(prefix string, n int64) error {
	h.cmdBuf = appendCommandCompleteHeader(h.cmdBuf[:0])
	h.cmdBuf = append(h.cmdBuf, prefix...)
	h.cmdBuf = strconv.AppendInt(h.cmdBuf, n, 10)
	h.cmdBuf = append(h.cmdBuf, 0)
	binary.BigEndian.PutUint32(h.cmdBuf[1:5], uint32(len(h.cmdBuf)-1))
	return h.writeRaw(h.cmdBuf)
}

// appendCommandCompleteHeader writes the 5-byte CommandComplete prefix
// ('C' + 4-byte length placeholder) into dst. The caller fills in the
// length once the full frame is assembled.
func appendCommandCompleteHeader(dst []byte) []byte {
	return append(dst, 'C', 0, 0, 0, 0)
}

func (h *handler) sendReadyForQuery() error {
	if err := h.writeRaw([]byte{'Z', 0, 0, 0, 5, 'I'}); err != nil {
		return err
	}
	return h.bufW.Flush()
}

func (h *handler) sendError(msg string) error {
	// 1 (type) + 4 (length) + 1 ('S') + len("ERROR") + 1 (NUL) +
	// 1 ('M') + len(msg) + 1 (NUL) + 1 (final NUL terminator).
	size := 1 + 4 + 1 + 5 + 1 + 1 + len(msg) + 1 + 1
	buf := make([]byte, size)
	buf[0] = 'E'
	pos := 5
	buf[pos] = 'S'
	pos++
	copy(buf[pos:], "ERROR")
	pos += 5
	buf[pos] = 0
	pos++
	buf[pos] = 'M'
	pos++
	copy(buf[pos:], msg)
	pos += len(msg)
	buf[pos] = 0
	pos++
	buf[pos] = 0 // final terminator
	binary.BigEndian.PutUint32(buf[1:5], uint32(size-1))
	return h.writeRaw(buf)
}

// writeRaw buffers the entire payload. Callers rely on sendReadyForQuery (or
// an explicit Flush during startup) to drain the buffer to the underlying
// connection.
func (h *handler) writeRaw(buf []byte) error {
	_, err := h.bufW.Write(buf)
	return err
}
