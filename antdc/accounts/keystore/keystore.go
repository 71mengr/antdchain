
package keystore

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/antdaza/antdchain/antdc/crypto/quantum"
	"github.com/antdaza/antdchain/common"
)

const (
	keystoreVersion = 3
	keyTypeMLDSA65  = "ml-dsa-65"
)

// NewAccount generates a new ML‑DSA‑65 keypair, encrypts it, and saves the keystore file.
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

// ImportAccount imports a packed 4032‑byte ML‑DSA‑65 private key, encrypts it, and saves the keystore.
func ImportAccount(privKey []byte, password, keystoreDir string) (common.QuantumAddress, error) {
	// Only accept the full packed private key
	if len(privKey) != quantum.MLDSA65PrivateKeySize {
		return common.QuantumAddress{}, fmt.Errorf("invalid private key length: expected %d bytes, got %d", quantum.MLDSA65PrivateKeySize, len(privKey))
	}
	if password == "" {
		return common.QuantumAddress{}, errors.New("password cannot be empty")
	}
	if err := ensureDir(keystoreDir); err != nil {
		return common.QuantumAddress{}, fmt.Errorf("failed to create keystore directory: %w", err)
	}

	// Validate the key by extracting the public part
	pubKey, err := quantum.DerivePublicKey(privKey)
	if err != nil {
		return common.QuantumAddress{}, fmt.Errorf("invalid ML‑DSA‑65 private key: %w", err)
	}

	addr, err := common.ParseQuantumAddress(quantum.PubKeyToAddress(pubKey))
	if err != nil {
		return common.QuantumAddress{}, fmt.Errorf("failed to derive address: %w", err)
	}

	// Avoid accidental overwrites
	if fileExists(filepath.Join(keystoreDir, accountFileName(addr))) {
		return common.QuantumAddress{}, fmt.Errorf("account %s already exists", addr.String())
	}

	if err := saveAccount(addr, privKey, password, keystoreDir); err != nil {
		return common.QuantumAddress{}, err
	}
	return addr, nil
}

// Unlock decrypts the keystore file for the given address and returns the raw private key bytes (4032 bytes).
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

// ListAccounts returns all quantum addresses in the keystore directory.
func ListAccounts(keystoreDir string) ([]common.QuantumAddress, error) {
	entries, err := os.ReadDir(keystoreDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
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
		addr, err := common.ParseQuantumAddress(parts[2])
		if err != nil {
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

// ─── Internal helpers ──────────────────────────────────────────────────────

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

	// Extract salt
	saltHex, ok := ks.Crypto.KDFParams["salt"].(string)
	if !ok {
		return nil, errors.New("missing salt in kdfparams")
	}
	salt, err := decodeHex(saltHex)
	if err != nil {
		return nil, fmt.Errorf("invalid salt: %w", err)
	}

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

	// Verify the key corresponds to the address stored in the file
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

// findKeystoreFile picks the newest file for the given address.
func findKeystoreFile(addr common.QuantumAddress, dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", fmt.Errorf("failed to read keystore directory: %w", err)
	}

	addrStr := addr.String()
	var newestTime int64
	var newestPath string

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "UTC--") || !strings.HasSuffix(name, addrStr) {
			continue
		}

		parts := strings.SplitN(name, "--", 3)
		if len(parts) < 3 {
			continue
		}
		t, err := time.Parse("2006-01-02T15-04-05.000000000Z", parts[1])
		if err != nil {
			continue
		}
		if t.UnixNano() > newestTime {
			newestTime = t.UnixNano()
			newestPath = filepath.Join(dir, name)
		}
	}
	if newestPath == "" {
		return "", fmt.Errorf("no keystore file found for address %s", addrStr)
	}
	return newestPath, nil
}
