package server_test

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/voicetel/memdb/server"
)

// TestServer_ScramAuth_Success drives a full SCRAM-SHA-256 handshake
// from outside the server package using a hand-rolled client. The point
// is to exercise the wire bytes the server actually emits and consumes —
// a unit test that mocks the parsers cannot prove the wire format
// matches what real PG clients (psql, pgx) produce.
func TestServer_ScramAuth_Success(t *testing.T) {
	addr, _ := startServerWithConfig(t, server.Config{
		Auth: server.NewScramAuth("alice", "hunter2"),
	})

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := scramClientLogin(t, conn, "alice", "hunter2"); err != nil {
		t.Fatalf("SCRAM login: %v", err)
	}
}

func TestServer_ScramAuth_BadPassword(t *testing.T) {
	addr, _ := startServerWithConfig(t, server.Config{
		Auth: server.NewScramAuth("alice", "hunter2"),
	})

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	err = scramClientLogin(t, conn, "alice", "wrong-password")
	if err == nil {
		t.Fatal("expected SCRAM login to fail with wrong password")
	}
	if !strings.Contains(err.Error(), "authentication failed") &&
		!strings.Contains(err.Error(), "ErrorResponse") {
		t.Errorf("expected auth-failed error, got: %v", err)
	}
}

func TestServer_ScramAuth_UnknownUser(t *testing.T) {
	addr, _ := startServerWithConfig(t, server.Config{
		Auth: server.NewScramAuth("alice", "hunter2"),
	})
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	err = scramClientLogin(t, conn, "bob", "hunter2")
	if err == nil {
		t.Fatal("expected SCRAM login to fail for unknown user")
	}
}

// ── hand-rolled SCRAM-SHA-256 client ──────────────────────────────────────────

// scramClientLogin completes a startup handshake using SCRAM-SHA-256 over
// the PostgreSQL SASL message flow. Returns nil on AuthenticationOk +
// ReadyForQuery; returns a descriptive error if any step fails.
func scramClientLogin(t *testing.T, conn net.Conn, username, password string) error {
	t.Helper()

	if err := sendStartupWithUser(conn, username); err != nil {
		return err
	}

	// Server emits AuthenticationSASL (subtype 10).
	mt, body := readMessage(t, conn)
	if mt != 'R' {
		return parseErr(mt, body, "AuthenticationSASL")
	}
	if subtype := binary.BigEndian.Uint32(body[:4]); subtype != 10 {
		return parseErr(mt, body, "AuthenticationSASL subtype 10")
	}
	// We don't bother verifying the advertised mechanism list — if the
	// server agrees on SCRAM-SHA-256 it will continue, otherwise the
	// next read will fail.

	// Send client-first-message.
	clientNonce := newClientNonce()
	clientFirstBare := "n=" + username + ",r=" + clientNonce
	clientFirst := "n,," + clientFirstBare

	saslInitial := []byte("SCRAM-SHA-256")
	saslInitial = append(saslInitial, 0)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(clientFirst)))
	saslInitial = append(saslInitial, lenBuf[:]...)
	saslInitial = append(saslInitial, clientFirst...)
	if err := sendMessage(conn, 'p', saslInitial); err != nil {
		return err
	}

	// Server replies with SASLContinue (subtype 11) carrying server-first-message.
	mt, body = readMessage(t, conn)
	if mt != 'R' {
		return parseErr(mt, body, "AuthenticationSASLContinue")
	}
	if subtype := binary.BigEndian.Uint32(body[:4]); subtype != 11 {
		return parseErr(mt, body, "subtype 11")
	}
	serverFirst := string(body[4:])
	combinedNonce, saltB64, iter, err := parseServerFirst(serverFirst, clientNonce)
	if err != nil {
		return err
	}
	salt, err := base64.StdEncoding.DecodeString(saltB64)
	if err != nil {
		return err
	}

	// Compute client proof.
	saltedPassword := pbkdf2For(password, salt, iter)
	clientKey := hmacFor(saltedPassword, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)
	clientFinalNoProof := "c=biws,r=" + combinedNonce
	authMessage := clientFirstBare + "," + serverFirst + "," + clientFinalNoProof
	clientSignature := hmacFor(storedKey[:], []byte(authMessage))
	proof := make([]byte, len(clientKey))
	for i := range clientKey {
		proof[i] = clientKey[i] ^ clientSignature[i]
	}

	clientFinal := clientFinalNoProof + ",p=" + base64.StdEncoding.EncodeToString(proof)
	if err := sendMessage(conn, 'p', []byte(clientFinal)); err != nil {
		return err
	}

	// SASLFinal (12).
	mt, body = readMessage(t, conn)
	if mt == 'E' {
		return parseErr(mt, body, "ErrorResponse instead of SASLFinal")
	}
	if mt != 'R' || binary.BigEndian.Uint32(body[:4]) != 12 {
		return parseErr(mt, body, "AuthenticationSASLFinal")
	}
	// Optionally verify v= signature; for this test we trust the server.

	// AuthOk (0).
	mt, body = readMessage(t, conn)
	if mt != 'R' || binary.BigEndian.Uint32(body[:4]) != 0 {
		return parseErr(mt, body, "AuthenticationOk")
	}

	// ReadyForQuery.
	mt, body = readMessage(t, conn)
	if mt != 'Z' {
		return parseErr(mt, body, "ReadyForQuery")
	}
	return nil
}

func sendStartupWithUser(conn net.Conn, user string) error {
	// version 3.0 + "user\0<user>\0\0"
	const protoVer = uint32(196608)
	body := []byte("user")
	body = append(body, 0)
	body = append(body, user...)
	body = append(body, 0, 0)
	totalLen := uint32(8 + len(body))
	buf := make([]byte, 0, totalLen)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], totalLen)
	buf = append(buf, lenBuf[:]...)
	binary.BigEndian.PutUint32(lenBuf[:], protoVer)
	buf = append(buf, lenBuf[:]...)
	buf = append(buf, body...)
	_, err := conn.Write(buf)
	return err
}

func sendMessage(conn net.Conn, msgType byte, body []byte) error {
	totalLen := uint32(4 + len(body))
	buf := make([]byte, 0, 1+totalLen)
	buf = append(buf, msgType)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], totalLen)
	buf = append(buf, lenBuf[:]...)
	buf = append(buf, body...)
	_, err := conn.Write(buf)
	return err
}

func newClientNonce() string {
	raw := make([]byte, 18)
	_, _ = rand.Read(raw)
	return base64.StdEncoding.EncodeToString(raw)
}

// parseServerFirst extracts combinedNonce, base64-encoded salt, and
// iteration count from "r=...,s=...,i=...". Verifies the combinedNonce
// starts with the client nonce we sent (RFC 5802 §5.1).
func parseServerFirst(s, clientNonce string) (combinedNonce, salt string, iter int, err error) {
	for _, kv := range strings.Split(s, ",") {
		switch {
		case strings.HasPrefix(kv, "r="):
			combinedNonce = kv[2:]
		case strings.HasPrefix(kv, "s="):
			salt = kv[2:]
		case strings.HasPrefix(kv, "i="):
			iter, err = strconv.Atoi(kv[2:])
			if err != nil {
				return "", "", 0, err
			}
		}
	}
	if !strings.HasPrefix(combinedNonce, clientNonce) {
		return "", "", 0, errFromString("server nonce does not extend client nonce")
	}
	return combinedNonce, salt, iter, nil
}

func pbkdf2For(password string, salt []byte, iter int) []byte {
	// Same PBKDF2-HMAC-SHA-256 as production; reimplemented here so the
	// test does not import the unexported server package version.
	hLen := sha256.Size
	prf := hmac.New(sha256.New, []byte(password))
	prf.Write(salt)
	var idx [4]byte
	binary.BigEndian.PutUint32(idx[:], 1)
	prf.Write(idx[:])
	u := prf.Sum(nil)
	t := make([]byte, hLen)
	copy(t, u)
	for j := 1; j < iter; j++ {
		prf.Reset()
		prf.Write(u)
		u = prf.Sum(u[:0])
		for k := range t {
			t[k] ^= u[k]
		}
	}
	return t
}

func hmacFor(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

type stringErr string

func (e stringErr) Error() string { return string(e) }

func errFromString(s string) error { return stringErr(s) }

func parseErr(mt byte, body []byte, want string) error {
	if mt == 'E' {
		// Decode an ErrorResponse: NUL-delimited fields like "S<severity>\0M<message>\0...\0"
		var msg string
		for i := 0; i < len(body); {
			if body[i] == 0 {
				break
			}
			fieldType := body[i]
			j := i + 1
			for j < len(body) && body[j] != 0 {
				j++
			}
			val := string(body[i+1 : j])
			if fieldType == 'M' {
				msg = val
			}
			i = j + 1
		}
		if msg != "" {
			return errFromString("ErrorResponse: " + msg + " (expected " + want + ")")
		}
		return errFromString("ErrorResponse (expected " + want + ")")
	}
	return errFromString("expected " + want + ", got msg type " + string(mt))
}
