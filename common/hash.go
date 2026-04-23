// common/hash.go
package common
import (
    "encoding/hex"
    "strings"

    "golang.org/x/crypto/sha3"
)

const HashLength = 32

type Hash [HashLength]byte

// BytesToHash converts a byte slice to a 32-byte Hash.
// If input is longer than 32 bytes, it takes the last 32 bytes.
// If shorter, it pads with zeros on the left.
func BytesToHash(b []byte) Hash {
    var h Hash
    if len(b) > HashLength {
        b = b[len(b)-HashLength:]
    }
    copy(h[HashLength-len(b):], b)
    return h
}

// HexToHash parses a hex string (with optional 0x prefix) into a Hash.
func HexToHash(s string) Hash {
    s = strings.TrimPrefix(s, "0x")
    b, _ := hex.DecodeString(s)
    return BytesToHash(b)
}

// Hex returns the hex representation of the Hash without 0x prefix.
func (h Hash) Hex() string {
    return hex.EncodeToString(h[:])
}

// String returns the hex representation with 0x prefix (for JSON, etc.).
func (h Hash) String() string {
    return "0x" + h.Hex()
}

// ComputeHash returns the SHA3-256 hash of data.
func ComputeHash(data []byte) Hash {
    return BytesToHash(sha3.New256().Sum(data))
}

// Bytes returns the underlying byte slice of the Hash.
func (h Hash) Bytes() []byte {
    return h[:]
}
