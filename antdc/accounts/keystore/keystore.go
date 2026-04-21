// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package keystore

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/antdaza/antdchain/antdc/crypto/quantum"
	"github.com/antdaza/antdchain/common"
)

const (
	keystoreVersion = 3
	keyTypeMLDSA65  = "ml-dsa-65"
)

// NewAccount generates a new ML‑DSA‑65 key pair, encrypts it with the password,
// and saves the keystore file in the specified directory.
// Returns the 0q quantum address.
func NewAccount(password, keystoreDir string) (common.QuantumAddress, error) {
	if password == "" {
		return common.QuantumAddress{}, errors.New("password cannot be empty")
	}
	if err := ensureDir(keystoreDir); err != nil {
		return common.QuantumAddress{}, fmt.Errorf("failed to create keystore directory: %w", err)
	}

	privKey, pubKey, err := quantum.GenerateKeyPair()
	if err != nil {
		return common.QuantumAddress{}, fmt.Errorf("key generation failed: %w", err)
	}

	addr, err := common.ParseQuantumAddress(quantum.PubKeyToAddress(pubKey))
	if err != nil {
		return common.QuantumAddress{}, fmt.Errorf("failed to derive address: %w", err)
	}

	if err := saveAccount(addr, privKey, password, keystoreDir); err != nil {
		return common.QuantumAddress{}, err
	}

	return addr, nil
}

// ImportAccount imports an existing private key (as raw bytes), encrypts it,
// and saves the keystore file.
func ImportAccount(privKey []byte, password, keystoreDir string) (common.QuantumAddress, error) {
	if len(privKey) == 0 {
		return common.QuantumAddress{}, errors.New("private key is empty")
	}
	if password == "" {
		return common.QuantumAddress{}, errors.New("password cannot be empty")
	}
	if err := ensureDir(keystoreDir); err != nil {
		return common.QuantumAddress{}, fmt.Errorf("failed to create keystore directory: %w", err)
	}

	pubKey, err := quantum.DerivePublicKey(privKey)
	if err != nil {
		return common.QuantumAddress{}, fmt.Errorf("failed to derive public key: %w", err)
	}

	addr, err := common.ParseQuantumAddress(quantum.PubKeyToAddress(pubKey))
	if err != nil {
		return common.QuantumAddress{}, fmt.Errorf("failed to derive address: %w", err)
	}

	// Check if an account with this address already exists
	if fileExists(filepath.Join(keystoreDir, accountFileName(addr))) {
		return common.QuantumAddress{}, fmt.Errorf("account %s already exists", addr.String())
	}

	if err := saveAccount(addr, privKey, password, keystoreDir); err != nil {
		return common.QuantumAddress{}, err
	}

	return addr, nil
}

// Unlock decrypts the keystore file for the given address and returns the raw private key.
func Unlock(addr common.QuantumAddress, password, keystoreDir string) ([]byte, error) {
	if password == "" {
		return nil, errors.New("password cannot be empty")
	}

	path, err := findKeystoreFile(addr, keystoreDir)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read keystore file: %w", err)
	}

	var ks KeyStore
	if err := json.Unmarshal(data, &ks); err != nil {
		return nil, fmt.Errorf("invalid keystore format: %w", err)
	}

	return decryptKey(&ks, password)
}

// ListAccounts returns all quantum addresses found in the keystore directory.
func ListAccounts(keystoreDir string) ([]common.QuantumAddress, error) {
	entries, err := os.ReadDir(keystoreDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read keystore directory: %w", err)
	}

	var addresses []common.QuantumAddress
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "UTC--") {
			continue
		}

		// Parse address from filename: UTC--timestamp--address
		parts := strings.Split(name, "--")
		if len(parts) < 3 {
			continue
		}
		addrStr := parts[2]
		addr, err := common.ParseQuantumAddress(addrStr)
		if err != nil {
			// Log but continue; might be an invalid file
			continue
		}
		addresses = append(addresses, addr)
	}
	return addresses, nil
}

// DeleteAccount removes the keystore file for the given address.
func DeleteAccount(addr common.QuantumAddress, keystoreDir string) error {
	path, err := findKeystoreFile(addr, keystoreDir)
	if err != nil {
		return err
	}
	return os.Remove(path)
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal functions

func saveAccount(addr common.QuantumAddress, privKey []byte, password, dir string) error {
	ks, err := encryptKey(privKey, addr, password)
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(ks, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal keystore: %w", err)
	}

	path := filepath.Join(dir, accountFileName(addr))
	// Write with restricted permissions (owner read/write only)
	if err := os.WriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("failed to write keystore file: %w", err)
	}
	return nil
}

func encryptKey(privKey []byte, addr common.QuantumAddress, password string) (*KeyStore, error) {
	salt := make([]byte, saltLength)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("failed to generate salt: %w", err)
	}

	derivedKey, err := deriveKey(password, salt)
	if err != nil {
		return nil, fmt.Errorf("key derivation failed: %w", err)
	}

	ciphertext, nonce, err := encryptData(privKey, derivedKey)
	if err != nil {
		return nil, fmt.Errorf("encryption failed: %w", err)
	}

	id, err := generateUUID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate UUID: %w", err)
	}

	return &KeyStore{
		Address: addr.String(),
		KeyType: keyTypeMLDSA65,
		Crypto: CryptoJSON{
			Cipher:     "aes-256-gcm",
			CipherText: encodeHex(ciphertext),
			CipherParams: map[string]interface{}{
				"nonce": encodeHex(nonce),
			},
			KDF: "scrypt",
			KDFParams: map[string]interface{}{
				"n":     scryptN,
				"r":     scryptR,
				"p":     scryptP,
				"dklen": keyLength,
				"salt":  encodeHex(salt),
			},
		},
		ID:      id,
		Version: keystoreVersion,
	}, nil
}

func decryptKey(ks *KeyStore, password string) ([]byte, error) {
	if ks.KeyType != keyTypeMLDSA65 {
		return nil, fmt.Errorf("unsupported key type: %s", ks.KeyType)
	}
	if ks.Version != keystoreVersion {
		return nil, fmt.Errorf("unsupported keystore version: %d", ks.Version)
	}

	// Extract KDF parameters
	saltHex, ok := ks.Crypto.KDFParams["salt"].(string)
	if !ok {
		return nil, errors.New("missing salt in kdfparams")
	}
	salt, err := decodeHex(saltHex)
	if err != nil {
		return nil, fmt.Errorf("invalid salt: %w", err)
	}

	// Derive key
	derivedKey, err := deriveKey(password, salt)
	if err != nil {
		return nil, err
	}

	// Decode ciphertext and nonce
	ciphertext, err := decodeHex(ks.Crypto.CipherText)
	if err != nil {
		return nil, fmt.Errorf("invalid ciphertext: %w", err)
	}
	nonceHex, ok := ks.Crypto.CipherParams["nonce"].(string)
	if !ok {
		return nil, errors.New("missing nonce in cipherparams")
	}
	nonce, err := decodeHex(nonceHex)
	if err != nil {
		return nil, fmt.Errorf("invalid nonce: %w", err)
	}

	privKey, err := decryptData(ciphertext, derivedKey, nonce)
	if err != nil {
		return nil, err
	}

	// Verify the private key matches the address (optional but recommended)
	pubKey, err := quantum.DerivePublicKey(privKey)
	if err != nil {
		return nil, fmt.Errorf("failed to derive public key: %w", err)
	}
	recoveredAddr, err := common.ParseQuantumAddress(quantum.PubKeyToAddress(pubKey))
	if err != nil {
		return nil, fmt.Errorf("failed to derive address: %w", err)
	}
	if recoveredAddr.String() != ks.Address {
		return nil, errors.New("address mismatch: keystore corrupted or wrong password")
	}

	return privKey, nil
}

// findKeystoreFile locates the keystore file for a given address.
// It scans the directory for files ending with the address.
func findKeystoreFile(addr common.QuantumAddress, dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", fmt.Errorf("failed to read keystore directory: %w", err)
	}

	addrStr := addr.String()
	var matches []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, addrStr) {
			matches = append(matches, name)
		}
	}

	if len(matches) == 0 {
		return "", fmt.Errorf("no keystore file found for address %s", addrStr)
	}
	if len(matches) > 1 {
		// If multiple, pick the newest by timestamp in filename
		// This is a simple heuristic; we could parse timestamps.
		// For now, return the first one.
	}
	return filepath.Join(dir, matches[0]), nil
}
