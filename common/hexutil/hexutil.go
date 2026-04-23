// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

// Package hexutil provides hex encoding and decoding utilities with 0x prefix
// for use in ANTDChain's quantum‑safe environment.
package hexutil
import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/antdaza/antdchain/common"
)

const uint64Size = 8

var (
	ErrEmptyString   = errors.New("empty hex string")
	ErrMissingPrefix = errors.New("hex string missing 0x prefix")
	ErrSyntax        = errors.New("invalid hex string")
	ErrOddLength     = errors.New("hex string has odd length")
)

// Encode encodes b as a hex string with 0x prefix.
func Encode(b []byte) string {
	return "0x" + hex.EncodeToString(b)
}

// EncodeUint64 encodes i as a hex string with 0x prefix.
func EncodeUint64(i uint64) string {
	enc := make([]byte, 2, 18)
	copy(enc, "0x")
	return string(strconv.AppendUint(enc, i, 16))
}

// EncodeBig encodes bigint as a hex string with 0x prefix.
func EncodeBig(bigint *big.Int) string {
	if bigint == nil {
		return "0x0"
	}
	return "0x" + bigint.Text(16)
}

// EncodeHash encodes a Hash as a hex string with 0x prefix.
func EncodeHash(h common.Hash) string {
	return "0x" + h.Hex()
}

// EncodeQuantumAddress encodes a QuantumAddress as a 0q Base58Check string.
// Note: Quantum addresses use 0q prefix, not 0x.
func EncodeQuantumAddress(addr common.QuantumAddress) string {
	return addr.String()
}

// Decode decodes a hex string with 0x prefix into a byte slice.
func Decode(s string) ([]byte, error) {
	if len(s) == 0 {
		return nil, ErrEmptyString
	}
	if !strings.HasPrefix(s, "0x") {
		return nil, ErrMissingPrefix
	}
	b, err := hex.DecodeString(s[2:])
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSyntax, err)
	}
	return b, nil
}

// MustDecode decodes a hex string with 0x prefix, panicking on error.
func MustDecode(s string) []byte {
	b, err := Decode(s)
	if err != nil {
		panic(err)
	}
	return b
}

// DecodeUint64 decodes a hex string with 0x prefix into a uint64.
func DecodeUint64(s string) (uint64, error) {
	b, err := Decode(s)
	if err != nil {
		return 0, err
	}
	if len(b) > uint64Size {
		return 0, fmt.Errorf("hex number too large, expected at most %d bytes", uint64Size)
	}
	var val uint64
	for _, byt := range b {
		val = val<<8 | uint64(byt)
	}
	return val, nil
}

// MustDecodeUint64 decodes a hex string into a uint64, panicking on error.
func MustDecodeUint64(s string) uint64 {
	val, err := DecodeUint64(s)
	if err != nil {
		panic(err)
	}
	return val
}

// DecodeBig decodes a hex string with 0x prefix into a *big.Int.
func DecodeBig(s string) (*big.Int, error) {
	b, err := Decode(s)
	if err != nil {
		return nil, err
	}
	return new(big.Int).SetBytes(b), nil
}

// MustDecodeBig decodes a hex string into a *big.Int, panicking on error.
func MustDecodeBig(s string) *big.Int {
	val, err := DecodeBig(s)
	if err != nil {
		panic(err)
	}
	return val
}

// DecodeHash decodes a hex string with 0x prefix into a common.Hash.
func DecodeHash(s string) (common.Hash, error) {
	b, err := Decode(s)
	if err != nil {
		return common.Hash{}, err
	}
	if len(b) != common.HashLength {
		return common.Hash{}, fmt.Errorf("invalid hash length: expected %d, got %d", common.HashLength, len(b))
	}
	return common.BytesToHash(b), nil
}

// MustDecodeHash decodes a hex string into a common.Hash, panicking on error.
func MustDecodeHash(s string) common.Hash {
	h, err := DecodeHash(s)
	if err != nil {
		panic(err)
	}
	return h
}

// DecodeQuantumAddress decodes a 0q Base58Check string into a QuantumAddress.
func DecodeQuantumAddress(s string) (common.QuantumAddress, error) {
	return common.ParseQuantumAddress(s)
}

// MustDecodeQuantumAddress decodes a 0q string into a QuantumAddress, panicking on error.
func MustDecodeQuantumAddress(s string) common.QuantumAddress {
	addr, err := DecodeQuantumAddress(s)
	if err != nil {
		panic(err)
	}
	return addr
}

// Bytes is a wrapper around []byte that marshals/unmarshals as hex with 0x prefix.
type Bytes []byte

// MarshalJSON implements json.Marshaler.
func (b Bytes) MarshalJSON() ([]byte, error) {
	return json.Marshal(Encode(b))
}

// UnmarshalJSON implements json.Unmarshaler.
func (b *Bytes) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if s == "" {
		*b = nil
		return nil
	}
	decoded, err := Decode(s)
	if err != nil {
		return err
	}
	*b = decoded
	return nil
}

// Big wraps big.Int to marshal/unmarshal as hex with 0x prefix.
type Big big.Int

// MarshalJSON implements json.Marshaler.
func (b Big) MarshalJSON() ([]byte, error) {
	i := (*big.Int)(&b)
	return json.Marshal(EncodeBig(i))
}

// UnmarshalJSON implements json.Unmarshaler.
func (b *Big) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if s == "" {
		*b = Big(*big.NewInt(0))
		return nil
	}
	decoded, err := DecodeBig(s)
	if err != nil {
		return err
	}
	*b = Big(*decoded)
	return nil
}

// Uint64 wraps uint64 to marshal/unmarshal as hex with 0x prefix.
type Uint64 uint64

// MarshalJSON implements json.Marshaler.
func (u Uint64) MarshalJSON() ([]byte, error) {
	return json.Marshal(EncodeUint64(uint64(u)))
}

// UnmarshalJSON implements json.Unmarshaler.
func (u *Uint64) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if s == "" {
		*u = 0
		return nil
	}
	val, err := DecodeUint64(s)
	if err != nil {
		return err
	}
	*u = Uint64(val)
	return nil
}
