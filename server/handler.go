package server

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"

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
	startupUser string // username from startup packet
}

func newHandler(db *memdb.DB, cfg Config, conn net.Conn) *handler {
	return &handler{db: db, cfg: cfg, conn: conn}
}

func (h *handler) serve() {
	defer h.conn.Close()

	if err := h.handleStartup(); err != nil {
		return
	}

	for {
		msgType, body, err := h.readMessage()
		if err != nil {
			return
		}

		switch msgType {
		case 'Q': // Simple Query
			query := strings.TrimRight(string(body), "\x00")
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

func (h *handler) handleStartup() error {
	buf := make([]byte, 8)
	if _, err := io.ReadFull(h.conn, buf); err != nil {
		return err
	}

	// Check for SSLRequest magic number (80877103) before reading startup.
	// psql and most drivers send this before the real startup message.
	// We decline TLS upgrade at this layer (TLS should be configured on
	// the listener itself) by responding with 'N'.
	magic := int(binary.BigEndian.Uint32(buf[0:4]))
	if magic == 80877103 {
		if err := h.writeRaw([]byte{'N'}); err != nil {
			return err
		}
		// Re-read the actual startup message.
		if _, err := io.ReadFull(h.conn, buf); err != nil {
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
		if _, err := io.ReadFull(h.conn, rest); err != nil {
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

		msgType, body, err := h.readMessage()
		if err != nil || msgType != 'p' {
			_ = h.sendError("authentication failed")
			return fmt.Errorf("auth: expected password message")
		}
		password := strings.TrimRight(string(body), "\x00")
		user := h.startupUser
		if user == "" {
			user = "memdb" // default if client didn't send username
		}
		if !h.cfg.Auth.Authenticate(user, password) {
			_ = h.sendError("authentication failed")
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

// handleSimpleQuery executes query and streams results back to the client.
// Returns an error only when the connection is broken and should be closed.
func (h *handler) handleSimpleQuery(query string) error {
	fields := strings.Fields(query)
	if len(fields) == 0 {
		// Empty query — respond with empty CommandComplete and ReadyForQuery.
		if err := h.sendCommandComplete(""); err != nil {
			return err
		}
		return h.sendReadyForQuery()
	}
	verb := strings.ToUpper(fields[0])
	switch verb {
	case "SELECT", "WITH", "EXPLAIN", "PRAGMA", "SHOW":
		return h.handleSelect(query)
	default:
		return h.handleExec(query, verb)
	}
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

	count := 0
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			if werr := h.sendError(err.Error()); werr != nil {
				return werr
			}
			return h.sendReadyForQuery()
		}
		row := make([][]byte, len(cols))
		for i, v := range vals {
			if v != nil {
				row[i] = []byte(fmt.Sprintf("%v", v))
			}
		}
		if err := h.sendDataRow(row); err != nil {
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

	if err := h.sendCommandComplete(fmt.Sprintf("SELECT %d", count)); err != nil {
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

	rowsAffected, _ := result.RowsAffected()
	var tag string
	switch verb {
	case "INSERT":
		tag = fmt.Sprintf("INSERT 0 %d", rowsAffected)
	case "UPDATE":
		tag = fmt.Sprintf("UPDATE %d", rowsAffected)
	case "DELETE":
		tag = fmt.Sprintf("DELETE %d", rowsAffected)
	default:
		tag = verb
	}

	if err := h.sendCommandComplete(tag); err != nil {
		return err
	}
	return h.sendReadyForQuery()
}

// ── wire helpers ─────────────────────────────────────────────────────────────

func (h *handler) readMessage() (byte, []byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(h.conn, header); err != nil {
		return 0, nil, err
	}
	msgType := header[0]
	length := int(binary.BigEndian.Uint32(header[1:5]))
	if length < 4 {
		return 0, nil, fmt.Errorf("server: invalid message length %d", length)
	}
	body := make([]byte, length-4)
	if _, err := io.ReadFull(h.conn, body); err != nil {
		return 0, nil, err
	}
	return msgType, body, nil
}

func (h *handler) sendRowDescription(cols []string) error {
	fieldCount := len(cols)
	buf := []byte{'T', 0, 0, 0, 0}
	buf = append(buf, byte(fieldCount>>8), byte(fieldCount))
	for _, col := range cols {
		buf = append(buf, []byte(col)...)
		buf = append(buf, 0)                      // null terminator
		buf = append(buf, 0, 0, 0, 0)             // table OID
		buf = append(buf, 0, 0)                   // attr number
		buf = append(buf, 0, 0, 0, 25)            // data type OID (text)
		buf = append(buf, 0xff, 0xff)             // type size (-1 = variable)
		buf = append(buf, 0xff, 0xff, 0xff, 0xff) // type modifier
		buf = append(buf, 0, 0)                   // format (text)
	}
	binary.BigEndian.PutUint32(buf[1:], uint32(len(buf)-1))
	return h.writeRaw(buf)
}

func (h *handler) sendDataRow(row [][]byte) error {
	buf := []byte{'D', 0, 0, 0, 0}
	buf = append(buf, byte(len(row)>>8), byte(len(row)))
	for _, val := range row {
		if val == nil {
			buf = append(buf, 0xff, 0xff, 0xff, 0xff) // NULL
		} else {
			n := len(val)
			buf = append(buf, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
			buf = append(buf, val...)
		}
	}
	binary.BigEndian.PutUint32(buf[1:], uint32(len(buf)-1))
	return h.writeRaw(buf)
}

func (h *handler) sendCommandComplete(tag string) error {
	buf := []byte{'C', 0, 0, 0, 0}
	buf = append(buf, []byte(tag)...)
	buf = append(buf, 0)
	binary.BigEndian.PutUint32(buf[1:], uint32(len(buf)-1))
	return h.writeRaw(buf)
}

func (h *handler) sendReadyForQuery() error {
	return h.writeRaw([]byte{'Z', 0, 0, 0, 5, 'I'})
}

func (h *handler) sendError(msg string) error {
	buf := []byte{'E', 0, 0, 0, 0}
	buf = append(buf, 'S')
	buf = append(buf, []byte("ERROR")...)
	buf = append(buf, 0)
	buf = append(buf, 'M')
	buf = append(buf, []byte(msg)...)
	buf = append(buf, 0, 0)
	binary.BigEndian.PutUint32(buf[1:], uint32(len(buf)-1))
	return h.writeRaw(buf)
}

// writeRaw writes the entire buffer to the connection.
func (h *handler) writeRaw(buf []byte) error {
	_, err := h.conn.Write(buf)
	return err
}
