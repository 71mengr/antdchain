package chain

import (
	"fmt"
	"testing"

	"github.com/antdaza/antdchain/antdc/block"
	"github.com/antdaza/antdchain/common"
	"github.com/sirupsen/logrus"
)

type testPruneProvider struct{ height uint64 }

func (p *testPruneProvider) GetBlock(height uint64) (*block.Block, error)          { return nil, nil }
func (p *testPruneProvider) GetBlockByHash(hash common.Hash) (*block.Block, error) { return nil, nil }
func (p *testPruneProvider) GetBlockHeight(hash common.Hash) (uint64, error)       { return 0, nil }
func (p *testPruneProvider) CurrentHeight() uint64                                 { return p.height }
func (p *testPruneProvider) GetBlockHash(height uint64) (common.Hash, error) {
	if height == 0 || height > p.height {
		return common.Hash{}, fmt.Errorf("not found")
	}
	var h common.Hash
	h[31] = byte(height)
	return h, nil
}
func (p *testPruneProvider) HasBlock(height uint64) bool { return height <= p.height }

type testPruneDB struct {
	prunedHeight uint64
	pruned       map[uint64]common.Hash
	records      []PruneRecord
}

func newTestPruneDB() *testPruneDB { return &testPruneDB{pruned: make(map[uint64]common.Hash)} }
func (d *testPruneDB) MarkBlockPruned(height uint64, hash common.Hash) error {
	d.pruned[height] = hash
	return nil
}
func (d *testPruneDB) IsBlockPruned(height uint64) bool                        { _, ok := d.pruned[height]; return ok }
func (d *testPruneDB) GetPrunedHeight() (uint64, error)                        { return d.prunedHeight, nil }
func (d *testPruneDB) SetPrunedHeight(height uint64) error                     { d.prunedHeight = height; return nil }
func (d *testPruneDB) DeleteBlockData(height uint64) error                     { return nil }
func (d *testPruneDB) GetBlockFileInfo(fileNumber int) (*BlockFileInfo, error) { return nil, nil }
func (d *testPruneDB) SaveBlockFileInfo(info *BlockFileInfo) error             { return nil }
func (d *testPruneDB) DeleteBlockFileInfo(fileNumber int) error                { return nil }
func (d *testPruneDB) GetPruneRecords(limit int) ([]PruneRecord, error)        { return d.records, nil }
func (d *testPruneDB) SavePruneRecord(record *PruneRecord) error {
	d.records = append(d.records, *record)
	return nil
}

func TestPruneManagerPruneUpdatesPrunedHeight(t *testing.T) {
	provider := &testPruneProvider{height: MinPruneDepth + 25}
	db := newTestPruneDB()
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)

	pm, err := NewPruneManager(provider, db, t.TempDir(), MinPruneDepth, logger)
	if err != nil {
		t.Fatalf("NewPruneManager() error = %v", err)
	}

	if err := pm.Prune(); err != nil {
		t.Fatalf("Prune() error = %v", err)
	}
	if db.prunedHeight != 25 {
		t.Fatalf("pruned height = %d, want 25", db.prunedHeight)
	}
	if len(db.pruned) != 25 {
		t.Fatalf("pruned block count = %d, want 25", len(db.pruned))
	}
	status := pm.GetPruneStatus()
	if status["enabled"] != true {
		t.Fatalf("enabled status = %v, want true", status["enabled"])
	}
}

func TestReorgManagerStatusDefaults(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	rm := NewReorgManager(nil, logger)
	status := rm.GetReorgStatus()
	if status["reorg_count"] != uint64(0) {
		t.Fatalf("reorg_count = %v, want 0", status["reorg_count"])
	}
	if status["orphan_pool_size"] != 0 {
		t.Fatalf("orphan_pool_size = %v, want 0", status["orphan_pool_size"])
	}
}
