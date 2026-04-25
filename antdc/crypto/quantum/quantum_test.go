package quantum

import "testing"

func TestSignAndVerify(t *testing.T) {
	priv, pub, err := GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair() error = %v", err)
	}

	msg := []byte("antdc-signature-regression")
	sig, err := Sign(priv, msg)
	if err != nil {
		t.Fatalf("Sign() error = %v", err)
	}

	if len(sig) != MLDSA65SignatureSize {
		t.Fatalf("signature length = %d, want %d", len(sig), MLDSA65SignatureSize)
	}

	if ok := Verify(pub, msg, sig); !ok {
		t.Fatalf("Verify() = false, want true")
	}
}
