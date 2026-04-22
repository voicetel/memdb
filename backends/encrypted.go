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
// The nonce is prepended to the ciphertext.
type EncryptedBackend struct {
	Inner Backend
	Key   [32]byte
}

func (b *EncryptedBackend) Exists(ctx context.Context) (bool, error) {
	return b.Inner.Exists(ctx)
}

func (b *EncryptedBackend) Write(ctx context.Context, r io.Reader) error {
	plaintext, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("encrypted backend: read: %w", err)
	}

	block, err := aes.NewCipher(b.Key[:])
	if err != nil {
		return err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return err
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
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
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
