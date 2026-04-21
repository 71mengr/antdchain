// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package keystore

// KeyStore represents a single encrypted account file.
// It follows the Web3 Secret Storage Definition with extensions for quantum keys.
type KeyStore struct {
	Address string     `json:"address"`          // 0q Base58Check address
	KeyType string     `json:"keytype"`          // "ml-dsa-65"
	Crypto  CryptoJSON `json:"crypto"`           // Encryption details
	ID      string     `json:"id"`               // UUID v4
	Version int        `json:"version"`          // Format version (3)
}

// CryptoJSON contains all encryption parameters.
type CryptoJSON struct {
	Cipher       string                 `json:"cipher"`                 // "aes-256-gcm"
	CipherText   string                 `json:"ciphertext"`             // hex-encoded encrypted private key
	CipherParams map[string]interface{} `json:"cipherparams"`           // e.g., {"nonce": "..."}
	KDF          string                 `json:"kdf"`                    // "scrypt"
	KDFParams    map[string]interface{} `json:"kdfparams"`              // scrypt parameters
	MAC          string                 `json:"mac,omitempty"`          // optional (GCM provides authentication)
}
