package checkpoints

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/antdaza/antdchain/common"
)

func TestStopSavesConfiguredCheckpointPath(t *testing.T) {
	tmp := t.TempDir()
	checkpointDir := filepath.Join(tmp, "checkpoints")
	configPath := filepath.Join(tmp, "checkpoints.json")
	genesisHash := common.BytesToHash([]byte("genesis"))

	cp, err := NewCheckpoints(checkpointDir, configPath, genesisHash)
	if err != nil {
		t.Fatalf("NewCheckpoints failed: %v", err)
	}
	cp.Stop()

	if cp.configPath != configPath {
		t.Fatalf("configPath = %q, want %q", cp.configPath, configPath)
	}
	if _, err := os.Stat(configPath); err != nil {
		t.Fatalf("configured checkpoint file was not saved: %v", err)
	}
	if _, err := os.Stat(filepath.Join(checkpointDir, "checkpoints.json")); !os.IsNotExist(err) {
		t.Fatalf("unexpected checkpoint file in data dir, err=%v", err)
	}
}
