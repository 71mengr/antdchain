package common

import (
"encoding/hex"
"encoding/json"
"errors"
"strings"
)

const QuantumAddressLength = 20

type QuantumAddress [QuantumAddressLength]byte

func NewQuantumAddressFromBytes(b []byte) (QuantumAddress, error) {
var a QuantumAddress
if len(b) != QuantumAddressLength {
return a, errors.New("invalid quantum address length")
}
copy(a[:], b)
return a, nil
}

func (a QuantumAddress) String() string { return a.Hex() }

func (a QuantumAddress) Bytes() []byte {
out := make([]byte, QuantumAddressLength)
copy(out, a[:])
return out
}

func (a QuantumAddress) Hex() string { return hex.EncodeToString(a[:]) }

func (a QuantumAddress) MarshalJSON() ([]byte, error) {
return json.Marshal(a.Hex())
}

func (a *QuantumAddress) UnmarshalJSON(data []byte) error {
var s string
if err := json.Unmarshal(data, &s); err != nil {
return err
}
s = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(s)), "0x")
if len(s) != QuantumAddressLength*2 {
return errors.New("invalid quantum address hex length")
}
decoded, err := hex.DecodeString(s)
if err != nil {
return err
}
copy(a[:], decoded)
return nil
}
