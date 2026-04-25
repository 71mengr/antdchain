package common

import (
	"testing"

	"github.com/antdaza/antdchain/antdc/crypto/quantum"
)

func TestParseQuantumAddressRejectsWhitespace(t *testing.T) {
	_, pub, err := quantum.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair() error = %v", err)
	}
	addr := quantum.PubKeyToAddress(pub)

	tests := []string{
		" " + addr,
		addr + " ",
		"\n" + addr,
		addr + "\t",
	}

	for _, input := range tests {
		if _, err := ParseQuantumAddress(input); err == nil {
			t.Fatalf("ParseQuantumAddress(%q) unexpectedly succeeded", input)
		}
	}
}

func TestParseQuantumAddressRequiresExactFormat(t *testing.T) {
	_, pub, err := quantum.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair() error = %v", err)
	}
	addr := quantum.PubKeyToAddress(pub)

	if _, err := ParseQuantumAddress(addr); err != nil {
		t.Fatalf("ParseQuantumAddress(valid) error = %v", err)
	}

	modifiedPrefix := "0x" + addr[len(quantum.AddressPrefix):]
	if _, err := ParseQuantumAddress(modifiedPrefix); err == nil {
		t.Fatalf("ParseQuantumAddress(%q) unexpectedly accepted modified prefix", modifiedPrefix)
	}
}
