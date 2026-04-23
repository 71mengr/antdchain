// chain/db/keys.go
package db
import (
    "encoding/binary"
    "github.com/antdaza/antdchain/common"
)

// Key prefixes
const (
    prefixHeaderNum   = byte('h')
    prefixHeaderHash  = byte('H')
    prefixBlockHash   = byte('b')
    prefixCanonNum    = byte('c')
)

// HeaderByNumberKey returns key for canonical header at height n
func HeaderByNumberKey(n uint64) []byte {
    key := make([]byte, 1+8)
    key[0] = prefixHeaderNum
    binary.BigEndian.PutUint64(key[1:], n)
    return key
}

// HeaderByHashKey returns key for header lookup by hash
func HeaderByHashKey(hash common.Hash) []byte {
    key := make([]byte, 1+32)
    key[0] = prefixHeaderHash
    copy(key[1:], hash[:])
    return key
}

// BlockByHashKey returns key for full block body + header
func BlockByHashKey(hash common.Hash) []byte {
    key := make([]byte, 1+32)
    key[0] = prefixBlockHash
    copy(key[1:], hash[:])
    return key
}

// CanonicalHashKey returns key that points to the canonical block hash at height n
func CanonicalHashKey(n uint64) []byte {
    key := make([]byte, 1+8)
    key[0] = prefixCanonNum
    binary.BigEndian.PutUint64(key[1:], n)
    return key
}

// encodeNumber encodes a uint64 into big-endian bytes
func encodeNumber(number uint64) []byte {
    enc := make([]byte, 8)
    binary.BigEndian.PutUint64(enc, number)
    return enc
}
