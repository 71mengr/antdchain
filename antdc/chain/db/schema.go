// chain/db/schema.go
package db
import (
    "encoding/binary"
    "github.com/antdaza/antdchain/common"
)

const (
    // Prefixes (single byte is enough for most chains)
    //prefixHeaderNum   = byte('h') // h + 8-byte big-endian number → header RLP
//    prefixHeaderHash  = byte('H') // H + 32-byte hash → header RLP (optional fast lookup)
//    prefixBlockHash   = byte('b') // b + 32-byte hash → full block RLP
  //  prefixCanonNum    = byte('c') // c + 8-byte number → canonical hash at that height
    prefixLastSection = byte('L') // L → last processed section / ancient pointer (future)
)

// headerByNumberKey returns key for canonical header at height n
func headerByNumberKey(n uint64) []byte {
    key := make([]byte, 1+8)
    key[0] = prefixHeaderNum
    binary.BigEndian.PutUint64(key[1:], n)
    return key
}

// headerByHashKey returns key for header lookup by hash
func headerByHashKey(hash common.Hash) []byte {
    key := make([]byte, 1+32)
    key[0] = prefixHeaderHash
    copy(key[1:], hash[:])
    return key
}

// blockByHashKey returns key for full block body + header
func blockByHashKey(hash common.Hash) []byte {
    key := make([]byte, 1+32)
    key[0] = prefixBlockHash
    copy(key[1:], hash[:])
    return key
}

// canonicalHashKey returns key that points to the canonical block hash at height n
func canonicalHashKey(n uint64) []byte {
    key := make([]byte, 1+8)
    key[0] = prefixCanonNum
    binary.BigEndian.PutUint64(key[1:], n)
    return key
}
