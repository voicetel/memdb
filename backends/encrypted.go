package backends

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
)

// EncryptedBackend wraps any Backend with AES-256-GCM encryption.
// The nonce is prepended to the ciphertext; each flush generates a fresh
// random nonce.
//
// WARNING: The Key field contains raw key material. Do not copy this struct
// by value, print it with %+v, or store it in any log. Use a pointer receiver
// or pass by pointer to avoid accidental key exposure.
type EncryptedBackend struct {
	Inner Backend
	Key   [32]byte
}

func (b *EncryptedBackend) Exists(ctx context.Context) (bool, error) {
	return b.Inner.Exists(ctx)
}

// Authenticated reports true to signal to memdb.WrapBackend that this
// backend already authenticates its payload (via the AES-GCM tag). The
// adapter uses this to skip the redundant 40-byte MDBK SHA-256 header
// it would otherwise prepend to every flushed snapshot — pprof showed
// SHA-256 at ~17% of the encrypted-flush CPU profile, all of it
// duplicating work GCM already did.
func (b *EncryptedBackend) Authenticated() bool { return true }

func (b *EncryptedBackend) Write(ctx context.Context, r io.Reader) error {
	plaintext, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("encrypted backend: read: %w", err)
	}

	block, err := aes.NewCipher(b.Key[:])
	if err != nil {
		return fmt.Errorf("encrypted backend: cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return fmt.Errorf("encrypted backend: gcm: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return fmt.Errorf("encrypted backend: nonce: %w", err)
	}

	ciphertext := gcm.Seal(nonce, nonce, plaintext, nil)
	return b.Inner.Write(ctx, bytes.NewReader(ciphertext))
}

func (b *EncryptedBackend) Read(ctx context.Context) (io.ReadCloser, error) {
	rc, err := b.Inner.Read(ctx)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	ciphertext, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("encrypted backend: read: %w", err)
	}

	block, err := aes.NewCipher(b.Key[:])
	if err != nil {
		return nil, fmt.Errorf("encrypted backend: cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("encrypted backend: gcm: %w", err)
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("encrypted backend: ciphertext too short")
	}
	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]

	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("encrypted backend: decrypt: %w", err)
	}

	return io.NopCloser(bytes.NewReader(plaintext)), nil
}
