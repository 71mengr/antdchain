package quantum

import (
    "crypto/rand"
    "errors"
    "fmt"

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
    MLDSA65SignatureSize  = mldsa65.SignatureSize
)

// GenerateKeyPair creates a new random ML‑DSA‑65 keypair.
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

// DeriveKeyFromSeed deterministically generates an ML‑DSA‑65 keypair from a 32‑byte seed.
func DeriveKeyFromSeed(seed []byte) (privKeyBytes, pubKeyBytes []byte, err error) {
    if len(seed) != 32 {
        return nil, nil, errors.New("seed must be exactly 32 bytes")
    }
    var seedArr [32]byte
    copy(seedArr[:], seed)

    pubKey, privKey := mldsa65.NewKeyFromSeed(&seedArr)

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

// ExtractSeedFromPrivateKey returns the 32‑byte seed if the key was created from one, or nil.
func ExtractSeedFromPrivateKey(privKeyBytes []byte) []byte {
    privKey := new(mldsa65.PrivateKey)
    if err := privKey.UnmarshalBinary(privKeyBytes); err != nil {
        return nil
    }
    return privKey.Seed()
}

// Sign signs a message with the given private key (4032 bytes).
func Sign(privKeyBytes, message []byte) ([]byte, error) {
    if len(privKeyBytes) != MLDSA65PrivateKeySize {
        return nil, fmt.Errorf("invalid private key length: expected %d bytes", MLDSA65PrivateKeySize)
    }
    privKey := new(mldsa65.PrivateKey)
    if err := privKey.UnmarshalBinary(privKeyBytes); err != nil {
        return nil, err
    }
    return privKey.Sign(rand.Reader, message, nil)
}

// Verify verifies an ML‑DSA‑65 signature.
func Verify(pubKeyBytes, message, signature []byte) bool {
    if len(signature) != MLDSA65SignatureSize {
        return false
    }
    pubKey := new(mldsa65.PublicKey)
    if err := pubKey.UnmarshalBinary(pubKeyBytes); err != nil {
        return false
    }
    return mldsa65.Verify(pubKey, message, nil, signature)
}

// DerivePublicKey extracts the public key from a private key.
func DerivePublicKey(privKeyBytes []byte) ([]byte, error) {
    if len(privKeyBytes) != MLDSA65PrivateKeySize {
        return nil, fmt.Errorf("invalid private key length: expected %d bytes", MLDSA65PrivateKeySize)
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

// NormalizePrivateKey accepts either a packed private key (4032 bytes) or a seed (32 bytes)
// and always returns the packed private key.
func NormalizePrivateKey(privKeyBytes []byte) ([]byte, error) {
    if len(privKeyBytes) == MLDSA65PrivateKeySize {
        out := make([]byte, MLDSA65PrivateKeySize)
        copy(out, privKeyBytes)
        return out, nil
    }
    if len(privKeyBytes) == 32 {
        priv, _, err := DeriveKeyFromSeed(privKeyBytes)
        if err != nil {
            return nil, err
        }
        return priv, nil
    }
    return nil, fmt.Errorf("invalid private key length: expected %d (packed) or %d (seed), got %d",
        MLDSA65PrivateKeySize, 32, len(privKeyBytes))
}

// PubKeyToAddress derives the 0q Base58Check address from a public key.
func PubKeyToAddress(pubKey []byte) string {
    sha := sha3.Sum256(pubKey)
    r := ripemd160.New()
    _, _ = r.Write(sha[:])
    payload := r.Sum(nil)
    return EncodeAddress(payload)
}

// EncodeAddress returns the 0q Base58Check string for a 20‑byte payload.
func EncodeAddress(payload []byte) string {
    buf := make([]byte, 0, EncodedPayloadLength)
    buf = append(buf, payload...)
    buf = append(buf, addressChecksum(payload)...)
    return AddressPrefix + base58.Encode(buf)
}

// IsValidAddress checks whether addr is a syntactically valid quantum address.
func IsValidAddress(addr string) bool {
    _, err := ExtractPayload(addr)
    return err == nil
}

// ExtractPayload extracts and validates the 20‑byte payload from a quantum address.
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
