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
