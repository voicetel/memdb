package server

import (
	"fmt"
	"net"
	"strings"

	"github.com/voicetel/memdb"
)

// handler manages a single client connection using a minimal subset of the
// PostgreSQL wire protocol (v3). Enough for psql, pgx, psycopg2, and node-postgres.
//
// Full protocol spec: https://www.postgresql.org/docs/current/protocol.html
type handler struct {
	db   *memdb.DB
	cfg  Config
	conn net.Conn
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
	if _, err := readFull(h.conn, buf); err != nil {
		return err
	}
	length := int(buf[0])<<24 | int(buf[1])<<16 | int(buf[2])<<8 | int(buf[3])
	if length > 8 {
		rest := make([]byte, length-8)
		if _, err := readFull(h.conn, rest); err != nil {
			return err
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
		if !h.cfg.Auth.Authenticate("memdb", password) {
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

// handleSimpleQuery executes query and streams results back to the client.
// Returns an error only when the connection is broken and should be closed.
func (h *handler) handleSimpleQuery(query string) error {
	verb := strings.ToUpper(strings.Fields(query)[0])
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
	if _, err := readFull(h.conn, header); err != nil {
		return 0, nil, err
	}
	msgType := header[0]
	length := int(header[1])<<24 | int(header[2])<<16 | int(header[3])<<8 | int(header[4])
	if length < 4 {
		return 0, nil, fmt.Errorf("server: invalid message length %d", length)
	}
	body := make([]byte, length-4)
	if _, err := readFull(h.conn, body); err != nil {
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
	setInt32(buf, 1, len(buf)-1)
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
	setInt32(buf, 1, len(buf)-1)
	return h.writeRaw(buf)
}

func (h *handler) sendCommandComplete(tag string) error {
	buf := []byte{'C', 0, 0, 0, 0}
	buf = append(buf, []byte(tag)...)
	buf = append(buf, 0)
	setInt32(buf, 1, len(buf)-1)
	return h.writeRaw(buf)
}

func (h *handler) sendReadyForQuery() error {
	return h.writeRaw([]byte{'Z', 0, 0, 0, 5, 'I'})
}

func (h *handler) sendError(msg string) error {
	buf := []byte{'E', 0, 0, 0, 0}
	buf = append(buf, 'M')
	buf = append(buf, []byte(msg)...)
	buf = append(buf, 0, 0)
	setInt32(buf, 1, len(buf)-1)
	return h.writeRaw(buf)
}

// writeRaw writes the entire buffer to the connection.
func (h *handler) writeRaw(buf []byte) error {
	_, err := h.conn.Write(buf)
	return err
}

func setInt32(buf []byte, offset, val int) {
	buf[offset] = byte(val >> 24)
	buf[offset+1] = byte(val >> 16)
	buf[offset+2] = byte(val >> 8)
	buf[offset+3] = byte(val)
}

func readFull(conn net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
