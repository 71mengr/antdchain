package console

import (
	"fmt"
	"os"
	"regexp"
	"strings"
)

var hexTokenRegex = regexp.MustCompile(`(?i)0x[0-9a-f]+|[0-9a-f]+`)

func loadImportPrivateKeyInput(args []string) (string, error) {
	if len(args) == 0 {
		return "", fmt.Errorf("missing private key or file path")
	}

	if len(args) == 1 {
		if b, err := os.ReadFile(args[0]); err == nil {
			return extractHexPrivateKey(string(b))
		}
	}

	return extractHexPrivateKey(strings.Join(args, ""))
}

func extractHexPrivateKey(input string) (string, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return "", fmt.Errorf("private key is empty")
	}

	matches := hexTokenRegex.FindAllString(trimmed, -1)
	if len(matches) == 0 {
		return "", fmt.Errorf("no hexadecimal private key found")
	}

	best := ""
	for _, m := range matches {
		candidate := strings.TrimPrefix(strings.TrimSpace(m), "0x")
		candidate = strings.TrimPrefix(candidate, "0X")
		if len(candidate) > len(best) {
			best = candidate
		}
	}

	if best == "" {
		return "", fmt.Errorf("private key is empty")
	}

	if len(best)%2 != 0 {
		return "", fmt.Errorf("invalid private key hex length: got %d chars; expected an even number of hex chars", len(best))
	}

	return best, nil
}

func stdoutIsTerminal() bool {
	info, err := os.Stdout.Stat()
	if err != nil {
		return true
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}
