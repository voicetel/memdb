package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"
)

// TestPBKDF2_RFC6070 verifies our PBKDF2-HMAC-SHA-256 against the
// SHA-256 test vectors from RFC 6070's spirit (the RFC itself uses
// SHA-1 vectors, but the same parameter set is published with SHA-256
// outputs in the IETF draft-ietf-kitten-rfc5802-bis tests). Catching a
// wrong PBKDF2 here is critical because a silently broken KDF would
// still produce self-consistent SCRAM exchanges between this server
// and a (broken) test client — the only way to know we match real
// PG / RFC 5802 implementations is a fixed external vector.
func TestPBKDF2_RFC6070(t *testing.T) {
	cases := []struct {
		password string
		salt     string
		iter     int
		dkLen    int
		want     string // hex
	}{
		// password="password", salt="salt", c=1
		{
			password: "password",
			salt:     "salt",
			iter:     1,
			dkLen:    32,
			want:     "120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b",
		},
		// password="password", salt="salt", c=2
		{
			password: "password",
			salt:     "salt",
			iter:     2,
			dkLen:    32,
			want:     "ae4d0c95af6b46d32d0adff928f06dd02a303f8ef3c251dfd6e2d85a95474c43",
		},
		// password="password", salt="salt", c=4096
		{
			password: "password",
			salt:     "salt",
			iter:     4096,
			dkLen:    32,
			want:     "c5e478d59288c841aa530db6845c4c8d962893a001ce4e11a4963873aa98134a",
		},
	}
	for _, c := range cases {
		got := pbkdf2HmacSha256([]byte(c.password), []byte(c.salt), c.iter, c.dkLen)
		gotHex := hex.EncodeToString(got)
		if gotHex != c.want {
			t.Errorf("pbkdf2(%q, %q, %d) = %s; want %s",
				c.password, c.salt, c.iter, gotHex, c.want)
		}
	}
}

func TestParseClientFirstMessage_OK(t *testing.T) {
	bare, nonce, user, err := parseClientFirstMessage([]byte("n,,n=alice,r=clientNonce123"))
	if err != nil {
		t.Fatal(err)
	}
	if bare != "n=alice,r=clientNonce123" {
		t.Errorf("bare=%q", bare)
	}
	if nonce != "clientNonce123" {
		t.Errorf("nonce=%q", nonce)
	}
	if user != "alice" {
		t.Errorf("user=%q", user)
	}
}

func TestParseClientFirstMessage_RejectsChannelBinding(t *testing.T) {
	_, _, _, err := parseClientFirstMessage([]byte("y,,n=alice,r=nonce"))
	if err == nil || !strings.Contains(err.Error(), "channel binding") {
		t.Fatalf("expected channel-binding rejection, got %v", err)
	}
}

func TestParseClientFirstMessage_MissingNonce(t *testing.T) {
	_, _, _, err := parseClientFirstMessage([]byte("n,,n=alice"))
	if err == nil {
		t.Fatal("expected error for missing nonce")
	}
}

func TestParseClientFinalMessage_OK(t *testing.T) {
	// Manually constructed: c=biws, r=combinedNonce, p=base64(32 bytes)
	proof := bytes.Repeat([]byte{0xAA}, 32)
	proofB64 := base64.StdEncoding.EncodeToString(proof)
	body := []byte("c=biws,r=combinedNonce,p=" + proofB64)

	without, gotProof, err := parseClientFinalMessage(body, "combinedNonce")
	if err != nil {
		t.Fatal(err)
	}
	if without != "c=biws,r=combinedNonce" {
		t.Errorf("withoutProof=%q", without)
	}
	if !bytes.Equal(gotProof, proof) {
		t.Errorf("proof mismatch")
	}
}

func TestParseClientFinalMessage_NonceMismatch(t *testing.T) {
	proof := bytes.Repeat([]byte{0x00}, 32)
	proofB64 := base64.StdEncoding.EncodeToString(proof)
	body := []byte("c=biws,r=wrongNonce,p=" + proofB64)
	_, _, err := parseClientFinalMessage(body, "expected")
	if err == nil || !strings.Contains(err.Error(), "nonce mismatch") {
		t.Fatalf("expected nonce-mismatch error, got %v", err)
	}
}

func TestParseSASLInitialResponse_OK(t *testing.T) {
	const mech = "SCRAM-SHA-256"
	const initial = "n,,n=alice,r=ABC"
	body := append([]byte(mech), 0)
	// 4-byte big-endian length
	body = append(body, byte(len(initial)>>24), byte(len(initial)>>16), byte(len(initial)>>8), byte(len(initial)))
	body = append(body, initial...)

	gotMech, gotInitial, err := parseSASLInitialResponse(body)
	if err != nil {
		t.Fatal(err)
	}
	if gotMech != mech {
		t.Errorf("mech=%q", gotMech)
	}
	if string(gotInitial) != initial {
		t.Errorf("initial=%q", gotInitial)
	}
}

func TestNewScramAuth_Stable(t *testing.T) {
	// Verifying the same plaintext password recomputes to the same StoredKey
	// for the *same* salt — by reconstructing PBKDF2 → ClientKey → StoredKey
	// from outside. This ensures construction matches RFC 5802 §3.
	auth := NewScramAuth("alice", "hunter2")

	salted := pbkdf2HmacSha256([]byte("hunter2"), auth.salt, auth.iterations, 32)
	clientKey := hmacSha256(salted, []byte("Client Key"))
	expectedServerKey := hmacSha256(salted, []byte("Server Key"))
	expectedStored := sha256.Sum256(clientKey)

	if !bytes.Equal(expectedServerKey, auth.serverKey) {
		t.Errorf("serverKey mismatch — NewScramAuth derivation drifted from RFC 5802")
	}
	if !bytes.Equal(expectedStored[:], auth.storedKey) {
		t.Errorf("storedKey mismatch — NewScramAuth derivation drifted from RFC 5802")
	}
	if auth.Authenticate("alice", "hunter2") {
		t.Errorf("ScramAuth.Authenticate must always return false (forces SCRAM path)")
	}
}
