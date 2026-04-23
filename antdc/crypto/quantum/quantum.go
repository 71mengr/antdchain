package quantum
import (
"crypto/rand"
"errors"

"github.com/cloudflare/circl/sign/mldsa/mldsa65"
"github.com/mr-tron/base58"
"golang.org/x/crypto/ripemd160"
"golang.org/x/crypto/sha3"
)

const (
AddressPrefix         = "0q"
PayloadLength         = 20
ChecksumLength        = 4
EncodedPayloadLength  = PayloadLength + ChecksumLength
MLDSA65PrivateKeySize = 4032
MLDSA65PublicKeySize  = 1952
MLDSA65SignatureSize  = 3293
)

func GenerateKeyPair() (privKeyBytes, pubKeyBytes []byte, err error) {
pubKey, privKey, err := mldsa65.GenerateKey(rand.Reader)
if err != nil {
return nil, nil, err
}
privKeyBytes, err = privKey.MarshalBinary()
if err != nil {
return nil, nil, err
}
pubKeyBytes, err = pubKey.MarshalBinary()
if err != nil {
return nil, nil, err
}
return privKeyBytes, pubKeyBytes, nil
}

func Sign(privKeyBytes, message []byte) ([]byte, error) {
privKeyBytes, err := NormalizePrivateKey(privKeyBytes)
if err != nil {
return nil, err
}
privKey := new(mldsa65.PrivateKey)
if err := privKey.UnmarshalBinary(privKeyBytes); err != nil {
return nil, err
}
return privKey.Sign(rand.Reader, message, nil)
}

func Verify(pubKeyBytes, message, signature []byte) bool {
    pubKey := new(mldsa65.PublicKey)
    if err := pubKey.UnmarshalBinary(pubKeyBytes); err != nil {
        return false
    }
    return mldsa65.Verify(pubKey, message, signature, nil)
}

func DerivePublicKey(privKeyBytes []byte) ([]byte, error) {
privKeyBytes, err := NormalizePrivateKey(privKeyBytes)
if err != nil {
return nil, err
}
privKey := new(mldsa65.PrivateKey)
if err := privKey.UnmarshalBinary(privKeyBytes); err != nil {
return nil, err
}
pub, ok := privKey.Public().(*mldsa65.PublicKey)
if !ok {
return nil, errors.New("invalid derived public key type")
}
return pub.MarshalBinary()
}

// NormalizePrivateKey accepts either a packed ML-DSA-65 private key
// (mldsa65.PrivateKeySize bytes) or a seed (mldsa65.SeedSize bytes).
// It always returns a packed private key.
func NormalizePrivateKey(privKeyBytes []byte) ([]byte, error) {
if len(privKeyBytes) == mldsa65.PrivateKeySize {
return append([]byte(nil), privKeyBytes...), nil
}
if len(privKeyBytes) == mldsa65.SeedSize {
var seed [mldsa65.SeedSize]byte
copy(seed[:], privKeyBytes)
_, priv := mldsa65.NewKeyFromSeed(&seed)
return priv.MarshalBinary()
}
return nil, errors.New("invalid private key length: expected packed private key or seed")
}

func PubKeyToAddress(pubKey []byte) string {
sha := sha3.Sum256(pubKey)
r := ripemd160.New()
_, _ = r.Write(sha[:])
payload := r.Sum(nil)
return EncodeAddress(payload)
}

func EncodeAddress(payload []byte) string {
buf := make([]byte, 0, EncodedPayloadLength)
buf = append(buf, payload...)
buf = append(buf, addressChecksum(payload)...)
return AddressPrefix + base58.Encode(buf)
}

func IsValidAddress(addr string) bool {
_, err := ExtractPayload(addr)
return err == nil
}

func ExtractPayload(addr string) ([]byte, error) {
if len(addr) < len(AddressPrefix) || addr[:len(AddressPrefix)] != AddressPrefix {
return nil, errors.New("missing 0q prefix")
}
decoded, err := base58.Decode(addr[len(AddressPrefix):])
if err != nil {
return nil, err
}
if len(decoded) != EncodedPayloadLength {
return nil, errors.New("invalid address length")
}
payload := decoded[:PayloadLength]
givenChecksum := decoded[PayloadLength:]
expectedChecksum := addressChecksum(payload)
for i := range expectedChecksum {
if givenChecksum[i] != expectedChecksum[i] {
return nil, errors.New("checksum mismatch")
}
}
out := make([]byte, PayloadLength)
copy(out, payload)
return out, nil
}

func addressChecksum(payload []byte) []byte {
input := append([]byte(AddressPrefix), payload...)
first := sha3.Sum256(input)
second := sha3.Sum256(first[:])
checksum := make([]byte, ChecksumLength)
copy(checksum, second[:ChecksumLength])
return checksum
}
