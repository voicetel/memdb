package server

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ScramAuth authenticates clients via SCRAM-SHA-256 (RFC 5802) over the
// PostgreSQL SASL message flow. Modern psql and pgx prefer SCRAM and will
// downgrade only when the server actively offers MD5 or cleartext, so this
// is what most production deployments need.
//
// Construction is deliberately eager: we run PBKDF2 once at NewScramAuth
// time and cache the StoredKey + ServerKey + salt. Per-connection auth then
// only needs HMACs and a SHA-256, both microsecond-scale, so clients see
// no auth-time PBKDF2 latency.
//
// The plaintext password is never retained — only the SCRAM-derived
// keys — so a memory-dump of a running process does not leak it.
type ScramAuth struct {
	username   string
	storedKey  []byte // 32 bytes — H(ClientKey)
	serverKey  []byte // 32 bytes — HMAC(SaltedPassword, "Server Key")
	salt       []byte // random per ScramAuth instance
	iterations int
}

// scramIterations is the PBKDF2 iteration count baked into every ScramAuth.
// PostgreSQL itself defaults to 4096 (controlled by password_encryption /
// scram_iterations); matching that keeps psql's perceived latency in line
// with what operators are used to. Higher values strengthen offline
// dictionary attacks on a leaked storedKey but slow legitimate auth.
const scramIterations = 4096

// NewScramAuth constructs a SCRAM-SHA-256 authenticator for a single
// (username, password) pair. The password is consumed only during
// construction — only its SCRAM-derived keys are retained.
func NewScramAuth(username, password string) *ScramAuth {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		// crypto/rand failure is fatal: every modern OS provides /dev/urandom
		// or getrandom(2), so a failure here means the kernel CSPRNG is
		// broken. There is no safe fallback.
		panic(fmt.Sprintf("memdb scram: rand.Read: %v", err))
	}
	saltedPassword := pbkdf2HmacSha256([]byte(password), salt, scramIterations, sha256.Size)
	clientKey := hmacSha256(saltedPassword, []byte("Client Key"))
	serverKey := hmacSha256(saltedPassword, []byte("Server Key"))
	storedKey := sha256.Sum256(clientKey)
	return &ScramAuth{
		username:   username,
		storedKey:  storedKey[:],
		serverKey:  serverKey,
		salt:       salt,
		iterations: scramIterations,
	}
}

// Authenticate satisfies the Authenticator interface but is not used for
// SCRAM. The handler detects *ScramAuth via a type assertion and runs the
// multi-round SASL exchange instead. We return false defensively so that
// callers who try to use SCRAM credentials with a server that has fallen
// back to cleartext auth get a clean rejection rather than a silent
// success.
func (a *ScramAuth) Authenticate(username, password string) bool { return false }

// Username reports the configured username so the handler can validate
// the SCRAM-supplied identity against the startup packet.
func (a *ScramAuth) Username() string { return a.username }

// pbkdf2HmacSha256 implements PBKDF2 (RFC 2898 / 8018) with HMAC-SHA-256
// as the PRF. We do this in-tree rather than depend on
// golang.org/x/crypto/pbkdf2 — it is ~30 lines of pure stdlib and avoids
// expanding the dependency surface for a single small primitive.
//
// dkLen is the desired output size in bytes; for SCRAM-SHA-256
// SaltedPassword we always pass sha256.Size (32) and consume one block.
func pbkdf2HmacSha256(password, salt []byte, iterations, dkLen int) []byte {
	prf := hmac.New(sha256.New, password)
	hLen := prf.Size()
	blocks := (dkLen + hLen - 1) / hLen

	out := make([]byte, 0, blocks*hLen)
	t := make([]byte, hLen)
	u := make([]byte, hLen)

	for blockIdx := 1; blockIdx <= blocks; blockIdx++ {
		// U_1 = PRF(P, S || INT(i))
		prf.Reset()
		prf.Write(salt)
		var idx [4]byte
		binary.BigEndian.PutUint32(idx[:], uint32(blockIdx))
		prf.Write(idx[:])
		u = prf.Sum(u[:0])
		copy(t, u)

		// U_2 .. U_c
		for j := 1; j < iterations; j++ {
			prf.Reset()
			prf.Write(u)
			u = prf.Sum(u[:0])
			for k := range t {
				t[k] ^= u[k]
			}
		}
		out = append(out, t...)
	}
	return out[:dkLen]
}

// hmacSha256 returns HMAC-SHA-256(key, data). Convenience wrapper —
// keeps callers free of `hmac.New(sha256.New, ...)` boilerplate.
func hmacSha256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// xorBytes returns a XOR b. Both inputs must be the same length;
// callers in this package only invoke it with two equal-sized 32-byte
// slices (ClientProof XOR ClientSignature) so a panic on length
// mismatch indicates a bug, not malformed wire data.
func xorBytes(a, b []byte) []byte {
	if len(a) != len(b) {
		panic("memdb scram: xor length mismatch")
	}
	out := make([]byte, len(a))
	for i := range a {
		out[i] = a[i] ^ b[i]
	}
	return out
}

// ── SASL message parsers ─────────────────────────────────────────────────────

// parseSASLInitialResponse decodes the body of a PG 'p' message in
// response to AuthenticationSASL. The body is:
//
//	mechanism (NUL-terminated string) || initial-response-len (uint32) || initial-response
//
// We only support SCRAM-SHA-256 here — channel-binding variants
// (SCRAM-SHA-256-PLUS) require TLS export-keying-material plumbing that
// the server doesn't have today.
func parseSASLInitialResponse(body []byte) (mechanism string, initial []byte, err error) {
	nul := -1
	for i, b := range body {
		if b == 0 {
			nul = i
			break
		}
	}
	if nul < 0 {
		return "", nil, errors.New("scram: SASLInitialResponse missing NUL after mechanism")
	}
	mechanism = string(body[:nul])
	rest := body[nul+1:]
	if len(rest) < 4 {
		return "", nil, errors.New("scram: SASLInitialResponse truncated length prefix")
	}
	initialLen := binary.BigEndian.Uint32(rest[:4])
	rest = rest[4:]
	// PG sends -1 (0xFFFFFFFF) for "no initial response", which is illegal
	// for SCRAM but we still need to reject it cleanly rather than treat
	// it as a 4 GB read.
	if initialLen == 0xFFFFFFFF {
		return mechanism, nil, nil
	}
	if int(initialLen) != len(rest) {
		return "", nil, fmt.Errorf("scram: SASLInitialResponse length mismatch (declared %d, have %d)",
			initialLen, len(rest))
	}
	return mechanism, rest, nil
}

// parseClientFirstMessage decodes the GS2 header + client-first-message-bare
// per RFC 5802 §7. Format:
//
//	"n,," (or "y,," / "p=...,authzid,") || client-first-message-bare
//	client-first-message-bare = "n=" username "," "r=" client-nonce
//
// We reject channel-binding requests ("y" and "p=") — the server has no
// way to honour them without TLS exporter plumbing, and a "y" client
// asking us to confirm we don't support CB is fine to refuse outright
// (SCRAM downgrade attacks are a concern only for "y" clients).
func parseClientFirstMessage(payload []byte) (clientFirstBare, clientNonce, username string, err error) {
	s := string(payload)
	// gs2-cb-flag ',' [ authzid ] ','
	parts := strings.SplitN(s, ",", 3)
	if len(parts) < 3 {
		return "", "", "", errors.New("scram: client-first-message missing GS2 header")
	}
	cbFlag := parts[0]
	switch cbFlag {
	case "n":
		// no channel binding requested — supported.
	case "y":
		// client believes we don't support CB. Refusing here closes a
		// theoretical downgrade window; the practical effect is that
		// channel-binding-aware clients still authenticate.
		return "", "", "", errors.New("scram: channel binding requested but not supported")
	default:
		return "", "", "", fmt.Errorf("scram: unsupported GS2 cb-flag %q", cbFlag)
	}
	// parts[1] is authzid; we ignore it (PG convention: empty).
	clientFirstBare = parts[2]

	// client-first-message-bare = "n=" saslname "," "r=" c-nonce [ "," extensions ]
	for _, kv := range strings.Split(clientFirstBare, ",") {
		if strings.HasPrefix(kv, "n=") {
			username = kv[2:]
		} else if strings.HasPrefix(kv, "r=") {
			clientNonce = kv[2:]
		}
	}
	if clientNonce == "" {
		return "", "", "", errors.New("scram: client-first-message missing nonce")
	}
	return clientFirstBare, clientNonce, username, nil
}

// parseClientFinalMessage decodes client-final-message and returns the
// "without-proof" prefix used in the AuthMessage and the raw ClientProof
// bytes. Format:
//
//	"c=" channel-binding "," "r=" combined-nonce "," ... ",p=" base64(proof)
//
// expectedNonce is the client+server combined nonce we sent in
// server-first-message. A mismatch is a fatal error per RFC 5802 §7.
func parseClientFinalMessage(body []byte, expectedNonce string) (withoutProof string, proof []byte, err error) {
	s := string(body)
	pIdx := strings.LastIndex(s, ",p=")
	if pIdx < 0 {
		return "", nil, errors.New("scram: client-final-message missing proof")
	}
	withoutProof = s[:pIdx]
	proofB64 := s[pIdx+3:]

	// Verify nonce echo.
	var nonce string
	for _, kv := range strings.Split(withoutProof, ",") {
		if strings.HasPrefix(kv, "r=") {
			nonce = kv[2:]
			break
		}
	}
	if nonce != expectedNonce {
		return "", nil, errors.New("scram: client-final-message nonce mismatch")
	}

	proof, err = base64.StdEncoding.DecodeString(proofB64)
	if err != nil {
		return "", nil, fmt.Errorf("scram: decode proof: %w", err)
	}
	if len(proof) != sha256.Size {
		return "", nil, fmt.Errorf("scram: proof has wrong length %d", len(proof))
	}
	return withoutProof, proof, nil
}

// ── server-side SCRAM exchange driver ────────────────────────────────────────

// runScramExchange completes the SCRAM-SHA-256 SASL flow against a
// connected client. The caller (handleStartup) is responsible for:
//   - Having sent AuthenticationSASL("SCRAM-SHA-256") already.
//   - Sending AuthenticationOk after this returns nil.
//
// On success the connection has been advanced to the
// post-AuthenticationSASLFinal state; on failure the caller must send
// an ErrorResponse and tear down the conn.
func (h *handler) runScramExchange(auth *ScramAuth) error {
	// 1. Read SASLInitialResponse from client.
	msgType, body, err := h.readMessage()
	if err != nil {
		return fmt.Errorf("scram: read SASLInitialResponse: %w", err)
	}
	if msgType != 'p' {
		return fmt.Errorf("scram: expected 'p' message, got %c", msgType)
	}

	mechanism, initial, err := parseSASLInitialResponse(body)
	if err != nil {
		return err
	}
	if mechanism != "SCRAM-SHA-256" {
		return fmt.Errorf("scram: unsupported mechanism %q", mechanism)
	}
	if len(initial) == 0 {
		return errors.New("scram: client sent empty initial response")
	}

	clientFirstBare, clientNonce, _, err := parseClientFirstMessage(initial)
	if err != nil {
		return err
	}
	// We deliberately ignore the username from client-first-message; PG
	// convention is to take it from the startup packet (h.startupUser),
	// which is what BasicAuth already uses. Trusting the SCRAM "n=" would
	// allow a malicious client to authenticate as a different user than
	// it announced at startup.

	// 2. Generate server nonce, send server-first-message.
	serverNonceRaw := make([]byte, 18)
	if _, err := rand.Read(serverNonceRaw); err != nil {
		return fmt.Errorf("scram: generate server nonce: %w", err)
	}
	serverNonce := base64.StdEncoding.EncodeToString(serverNonceRaw)
	combinedNonce := clientNonce + serverNonce
	saltB64 := base64.StdEncoding.EncodeToString(auth.salt)
	serverFirst := "r=" + combinedNonce + ",s=" + saltB64 + ",i=" + strconv.Itoa(auth.iterations)

	if err := h.sendAuthMessage(11 /* SASLContinue */, []byte(serverFirst)); err != nil {
		return err
	}
	if err := h.bufW.Flush(); err != nil {
		return err
	}

	// 3. Read client-final-message and validate the proof.
	msgType, body, err = h.readMessage()
	if err != nil {
		return fmt.Errorf("scram: read client-final: %w", err)
	}
	if msgType != 'p' {
		return fmt.Errorf("scram: expected 'p' for client-final, got %c", msgType)
	}

	withoutProof, proof, err := parseClientFinalMessage(body, combinedNonce)
	if err != nil {
		return err
	}

	authMessage := clientFirstBare + "," + serverFirst + "," + withoutProof
	clientSignature := hmacSha256(auth.storedKey, []byte(authMessage))
	clientKey := xorBytes(proof, clientSignature)
	derivedStored := sha256.Sum256(clientKey)

	// Constant-time compare: a timing oracle on the StoredKey check would
	// let an attacker forge proofs byte-by-byte against a leaked
	// AuthMessage. For 32 bytes this is a single subtle.ConstantTimeCompare
	// call.
	if subtle.ConstantTimeCompare(derivedStored[:], auth.storedKey) != 1 {
		return errors.New("scram: invalid client proof")
	}

	// 4. Send server-final-message with our signature.
	serverSignature := hmacSha256(auth.serverKey, []byte(authMessage))
	serverFinal := "v=" + base64.StdEncoding.EncodeToString(serverSignature)
	if err := h.sendAuthMessage(12 /* SASLFinal */, []byte(serverFinal)); err != nil {
		return err
	}
	return h.bufW.Flush()
}

// sendAuthMessage writes an AuthenticationRequest message ('R') with the
// given subtype and payload. Subtype 10 = SASL, 11 = SASLContinue,
// 12 = SASLFinal. The 4-byte subtype is stored big-endian per the wire format.
func (h *handler) sendAuthMessage(subtype uint32, payload []byte) error {
	// 'R' [len 4] [subtype 4] payload
	totalLen := 4 + 4 + len(payload)
	buf := make([]byte, 1+totalLen)
	buf[0] = 'R'
	binary.BigEndian.PutUint32(buf[1:5], uint32(totalLen))
	binary.BigEndian.PutUint32(buf[5:9], subtype)
	copy(buf[9:], payload)
	return h.writeRaw(buf)
}

// sendAuthSASLAdvert tells the client which SASL mechanisms we support.
// Per the PG protocol the payload is a sequence of NUL-terminated
// mechanism names followed by a final NUL. We only advertise
// SCRAM-SHA-256.
func (h *handler) sendAuthSASLAdvert() error {
	const mech = "SCRAM-SHA-256"
	payload := make([]byte, 0, len(mech)+2)
	payload = append(payload, mech...)
	payload = append(payload, 0) // terminator after mechanism name
	payload = append(payload, 0) // empty terminator marking end of list
	return h.sendAuthMessage(10, payload)
}
