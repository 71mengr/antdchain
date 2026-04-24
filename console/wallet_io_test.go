package console

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExtractHexPrivateKeyPrefersLongestToken(t *testing.T) {
	t.Parallel()

	key := strings.Repeat("ab", 4032)
	input := "Private key for 0q123\n0x" + key + "\nSeed: 0x" + strings.Repeat("cd", 32)

	got, err := extractHexPrivateKey(input)
	if err != nil {
		t.Fatalf("extractHexPrivateKey returned error: %v", err)
	}
	if got != key {
		t.Fatalf("unexpected key length/content: got %d chars", len(got))
	}
}

func TestLoadImportPrivateKeyInputFromFile(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	file := filepath.Join(tempDir, "mykey.txt")
	key := strings.Repeat("ef", 4032)
	content := "Wallet export\n0x" + key + "\n"
	if err := os.WriteFile(file, []byte(content), 0600); err != nil {
		t.Fatalf("write file: %v", err)
	}

	got, err := loadImportPrivateKeyInput([]string{file})
	if err != nil {
		t.Fatalf("loadImportPrivateKeyInput returned error: %v", err)
	}
	if got != key {
		t.Fatalf("unexpected key content: got len=%d", len(got))
	}
}

func TestExtractHexPrivateKeyRejectsOddLength(t *testing.T) {
	t.Parallel()

	_, err := extractHexPrivateKey("0xabc")
	if err == nil {
		t.Fatalf("expected error for odd-length hex")
	}
}

func TestExtractHexPrivateKeyFromExportTranscriptHas8064HexChars(t *testing.T) {
	t.Parallel()

	const exportedAddress = "0qAhsF8imqZ2JhXTL7Nj1fJHvrKS7ZeHhrc"
	privateKeyHex := strings.Repeat("42", 4032)

	transcript := "wallet> export " + exportedAddress + " > myt.txt\n" +
		"Enter password to decrypt wallet:\n" +
		"�� Private key for " + exportedAddress + ":\n" +
		"0x" + privateKeyHex + "\n"

	got, err := extractHexPrivateKey(transcript)
	if err != nil {
		t.Fatalf("extractHexPrivateKey returned error: %v", err)
	}
	if len(got) != 8064 {
		t.Fatalf("unexpected private key hex length: got %d, want 8064", len(got))
	}
	if got != privateKeyHex {
		t.Fatalf("unexpected private key value extracted")
	}
}

func TestParseOneShotCommandArgs(t *testing.T) {
	t.Parallel()

	got, ok := parseOneShotCommandArgs([]string{"export", "0q123"})
	if !ok {
		t.Fatalf("expected one-shot command to be detected")
	}
	if len(got) != 2 || got[0] != "export" || got[1] != "0q123" {
		t.Fatalf("unexpected parsed args: %#v", got)
	}
}

func TestParseOneShotCommandArgsRejectsFlags(t *testing.T) {
	t.Parallel()

	if _, ok := parseOneShotCommandArgs([]string{"--offline", "export", "0q123"}); ok {
		t.Fatalf("did not expect flag-prefixed input to be treated as one-shot command")
	}
}
