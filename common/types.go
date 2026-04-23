package common
import (
"encoding/json"
"errors"
"strings"

"github.com/antdaza/antdchain/antdc/crypto/quantum"
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

func ParseQuantumAddress(s string) (QuantumAddress, error) {
payload, err := quantum.ExtractPayload(strings.TrimSpace(s))
if err != nil {
return QuantumAddress{}, err
}
return NewQuantumAddressFromBytes(payload)
}

func (a QuantumAddress) String() string { return quantum.EncodeAddress(a[:]) }

func (a QuantumAddress) Bytes() []byte {
out := make([]byte, QuantumAddressLength)
copy(out, a[:])
return out
}

func (a QuantumAddress) MarshalJSON() ([]byte, error) {
return json.Marshal(a.String())
}

func (a *QuantumAddress) UnmarshalJSON(data []byte) error {
var s string
if err := json.Unmarshal(data, &s); err != nil {
return err
}
parsed, err := ParseQuantumAddress(s)
if err != nil {
return err
}
*a = parsed
return nil
}

// BytesToQuantumAddress converts a byte slice to a 20‑byte QuantumAddress.
// If the input is longer than 20 bytes, it takes the last 20 bytes.
// If shorter, it left‑pads with zeros.
func BytesToQuantumAddress(b []byte) QuantumAddress {
    var addr QuantumAddress
    if len(b) > QuantumAddressLength {
        b = b[len(b)-QuantumAddressLength:]
    }
    copy(addr[QuantumAddressLength-len(b):], b)
    return addr
}
