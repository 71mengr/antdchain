// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package keystore

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"

	"golang.org/x/crypto/scrypt"
)

const (
	// scrypt parameters (same as Ethereum to allow hardware wallet compatibility)
	scryptN      = 1 << 18 // 262144
	scryptR      = 8
	scryptP      = 1
	keyLength    = 32 // AES-256
	saltLength   = 32
	nonceLength  = 12 // GCM standard nonce size
)

var (
	errInvalidKeyLength = errors.New("decryption key must be 32 bytes")
	errCiphertextTooShort = errors.New("ciphertext too short")
	errInvalidNonceSize = errors.New("invalid nonce size")
)

// deriveKey derives an encryption key from a password and salt using scrypt.
func deriveKey(password string, salt []byte) ([]byte, error) {
	return scrypt.Key([]byte(password), salt, scryptN, scryptR, scryptP, keyLength)
}

// encryptData encrypts plaintext using AES-256-GCM with a random nonce.
// Returns ciphertext and nonce.
func encryptData(plaintext, key []byte) ([]byte, []byte, error) {
	if len(key) != keyLength {
		return nil, nil, errInvalidKeyLength
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create GCM mode: %w", err)
	}

	nonce := make([]byte, nonceLength)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	// GCM.Seal appends the authentication tag automatically
	ciphertext := gcm.Seal(nil, nonce, plaintext, nil)
	return ciphertext, nonce, nil
}

// decryptData decrypts ciphertext using AES-256-GCM with the provided nonce.
func decryptData(ciphertext, key, nonce []byte) ([]byte, error) {
	if len(key) != keyLength {
		return nil, errInvalidKeyLength
	}
	if len(ciphertext) < 16 { // GCM tag is at least 12 bytes, we use 16 for safety
		return nil, errCiphertextTooShort
	}
	if len(nonce) != nonceLength {
		return nil, errInvalidNonceSize
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM mode: %w", err)
	}

	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decryption failed (wrong password or corrupted data): %w", err)
	}

	return plaintext, nil
}
