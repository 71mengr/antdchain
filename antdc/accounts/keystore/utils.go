// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package keystore

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/antdaza/antdchain/common"
)

// generateUUID creates a random UUID v4 string.
func generateUUID() (string, error) {
	uuid := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, uuid); err != nil {
		return "", err
	}
	// variant bits (section 4.1.1)
	uuid[8] = uuid[8]&^0xc0 | 0x80
	// version 4 (section 4.1.3)
	uuid[6] = uuid[6]&^0xf0 | 0x40
	return fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:]), nil
}

// accountFileName generates a deterministic filename for a keystore file.
// Format: UTC--<RFC3339Nano with : replaced by ->--<address>
func accountFileName(addr common.QuantumAddress) string {
	ts := time.Now().UTC().Format("2006-01-02T15-04-05.000000000Z")
	// Replace colons to make the filename safe on all filesystems
	ts = strings.ReplaceAll(ts, ":", "-")
	return fmt.Sprintf("UTC--%s--%s", ts, addr.String())
}

// ensureDir creates the keystore directory if it does not exist.
func ensureDir(dir string) error {
	return os.MkdirAll(dir, 0700)
}

// fileExists checks if a file exists and is not a directory.
func fileExists(path string) bool {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

// encodeHex encodes a byte slice as a hex string without 0x prefix.
func encodeHex(data []byte) string {
	return hex.EncodeToString(data)
}

// decodeHex decodes a hex string (with or without 0x prefix).
func decodeHex(s string) ([]byte, error) {
	s = strings.TrimPrefix(s, "0x")
	return hex.DecodeString(s)
}
