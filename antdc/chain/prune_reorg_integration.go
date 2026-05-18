// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package chain

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/antdaza/antdchain/antdc/block"
	"github.com/antdaza/antdchain/antdc/chain/db"
	"github.com/antdaza/antdchain/common"
	"github.com/cockroachdb/pebble"
)

type pruneBlockProvider struct{ bc *Blockchain }

func (p *pruneBlockProvider) GetBlock(height uint64) (*block.Block, error) {
	blk := p.bc.GetBlock(height)
	if blk == nil {
		return nil, fmt.Errorf("block %d not found", height)
	}
	return blk, nil
}
func (p *pruneBlockProvider) GetBlockByHash(hash common.Hash) (*block.Block, error) {
	return p.bc.GetBlockByHash(hash)
}
func (p *pruneBlockProvider) GetBlockHeight(hash common.Hash) (uint64, error) {
	blk, err := p.bc.GetBlockByHash(hash)
	if err != nil || blk == nil || blk.Header == nil {
		return 0, fmt.Errorf("block %s not found", hash.Hex())
	}
	return blk.Header.Number.Uint64(), nil
}
func (p *pruneBlockProvider) CurrentHeight() uint64 { return p.bc.GetChainHeight() }
func (p *pruneBlockProvider) GetBlockHash(height uint64) (common.Hash, error) {
	return p.bc.GetBlockHash(height)
}
func (p *pruneBlockProvider) HasBlock(height uint64) bool { return p.bc.HasBlockAtHeight(height) }

type reorgChainAdapter struct{ bc *Blockchain }

func (a *reorgChainAdapter) GetBlock(height uint64) (*block.Block, error) {
	blk := a.bc.GetBlock(height)
	if blk == nil {
		return nil, fmt.Errorf("block %d not found", height)
	}
	return blk, nil
}
func (a *reorgChainAdapter) GetBlockByHash(hash common.Hash) (*block.Block, error) {
	return a.bc.GetBlockByHash(hash)
}
func (a *reorgChainAdapter) GetBlockHash(height uint64) (common.Hash, error) {
	return a.bc.GetBlockHash(height)
}
func (a *reorgChainAdapter) HasBlock(hash common.Hash) bool { return a.bc.HasBlock(hash) }
func (a *reorgChainAdapter) HasBlockAtHeight(height uint64) bool {
	return a.bc.HasBlockAtHeight(height)
}
func (a *reorgChainAdapter) CurrentHeight() uint64 { return a.bc.GetChainHeight() }
func (a *reorgChainAdapter) CurrentTip() common.Hash {
	if latest := a.bc.Latest(); latest != nil {
		return latest.Hash()
	}
	return common.Hash{}
}
func (a *reorgChainAdapter) GetBlockParent(blk *block.Block) (*block.Block, error) {
	if blk == nil || blk.Header == nil || blk.Header.Number.Sign() == 0 {
		return nil, nil
	}
	return a.bc.GetBlockByHash(blk.Header.ParentHash)
}
func (a *reorgChainAdapter) GetCumulativeWork(height uint64) (*big.Int, error) {
	total := big.NewInt(0)
	for h := uint64(0); h <= height; h++ {
		blk := a.bc.GetBlock(h)
		if blk == nil || blk.Header == nil {
			return nil, fmt.Errorf("missing canonical block %d", h)
		}
		total.Add(total, a.CalculateBlockWork(blk.Header.Difficulty))
	}
	return total, nil
}
func (a *reorgChainAdapter) CalculateBlockWork(difficulty *big.Int) *big.Int {
	if difficulty == nil || difficulty.Sign() <= 0 {
		return big.NewInt(1)
	}
	return new(big.Int).Set(difficulty)
}
func (a *reorgChainAdapter) AddBlock(blk *block.Block) error {
	a.bc.processingReorgBlock.Store(true)
	defer a.bc.processingReorgBlock.Store(false)
	return a.bc.addBlockInternal(blk)
}
func (a *reorgChainAdapter) ConnectBlock(blk *block.Block) error { return a.AddBlock(blk) }
func (a *reorgChainAdapter) DisconnectBlock(height uint64) error {
	if height == 0 {
		return fmt.Errorf("cannot disconnect genesis")
	}
	return a.bc.revertToHeight(height - 1)
}
func (a *reorgChainAdapter) ValidateBlock(blk *block.Block) error {
	return a.bc.ValidateMinedBlockProposal(blk)
}
func (a *reorgChainAdapter) ValidateBlockContext(blk *block.Block, parent *block.Block) error {
	if blk == nil || blk.Header == nil {
		return fmt.Errorf("nil block")
	}
	if parent == nil && blk.Header.Number.Sign() > 0 {
		var err error
		parent, err = a.bc.GetBlockByHash(blk.Header.ParentHash)
		if err != nil || parent == nil {
			return fmt.Errorf("parent block not found")
		}
	}
	if parent != nil && blk.Header.ParentHash != parent.Hash() {
		return fmt.Errorf("parent hash mismatch")
	}
	return nil
}

func (bc *Blockchain) GetBlockHash(height uint64) (common.Hash, error) {
	return bc.db.GetCanonicalHash(height)
}

func pruneHeightKey() []byte { return []byte("prune:height") }
func prunedBlockKey(height uint64) []byte {
	key := make([]byte, len("prune:block:")+8)
	copy(key, "prune:block:")
	binary.BigEndian.PutUint64(key[len("prune:block:"):], height)
	return key
}
func blockFileInfoKey(fileNumber int) []byte {
	return []byte(fmt.Sprintf("prune:file:%08d", fileNumber))
}
func pruneRecordKey(ts time.Time) []byte {
	return []byte(fmt.Sprintf("prune:record:%020d", ts.UnixNano()))
}

func (bc *Blockchain) MarkBlockPruned(height uint64, hash common.Hash) error {
	return bc.db.DB().Set(prunedBlockKey(height), hash[:], pebble.Sync)
}
func (bc *Blockchain) IsBlockPruned(height uint64) bool {
	_, closer, err := bc.db.DB().Get(prunedBlockKey(height))
	if err == nil {
		closer.Close()
		return true
	}
	return false
}
func (bc *Blockchain) GetPrunedHeight() (uint64, error) {
	data, closer, err := bc.db.DB().Get(pruneHeightKey())
	if err != nil {
		if err == pebble.ErrNotFound || err == db.ErrNotFound {
			return 0, nil
		}
		return 0, err
	}
	defer closer.Close()
	if len(data) != 8 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(data), nil
}
func (bc *Blockchain) SetPrunedHeight(height uint64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, height)
	return bc.db.DB().Set(pruneHeightKey(), buf, pebble.Sync)
}
func (bc *Blockchain) DeleteBlockData(height uint64) error {
	hash, err := bc.db.GetCanonicalHash(height)
	if err != nil || hash == (common.Hash{}) {
		return err
	}
	if bc.ancientStore != nil {
		if blk, _ := bc.db.ReadBlockByHash(hash); blk != nil {
			_ = bc.ancientStore.StoreBlock(blk)
		}
	}
	return bc.db.DB().Delete(db.BlockByHashKey(hash), pebble.Sync)
}
func (bc *Blockchain) GetBlockFileInfo(fileNumber int) (*BlockFileInfo, error) {
	data, closer, err := bc.db.DB().Get(blockFileInfoKey(fileNumber))
	if err != nil {
		if err == pebble.ErrNotFound || err == db.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()
	var info BlockFileInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, err
	}
	return &info, nil
}
func (bc *Blockchain) SaveBlockFileInfo(info *BlockFileInfo) error {
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return bc.db.DB().Set(blockFileInfoKey(info.FileNumber), data, pebble.Sync)
}
func (bc *Blockchain) DeleteBlockFileInfo(fileNumber int) error {
	return bc.db.DB().Delete(blockFileInfoKey(fileNumber), pebble.Sync)
}
func (bc *Blockchain) GetPruneRecords(limit int) ([]PruneRecord, error) {
	iter, err := bc.db.DB().NewIter(&pebble.IterOptions{LowerBound: []byte("prune:record:"), UpperBound: []byte("prune:record;")})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	records := make([]PruneRecord, 0)
	for iter.Last(); iter.Valid() && (limit <= 0 || len(records) < limit); iter.Prev() {
		var rec PruneRecord
		if err := json.Unmarshal(iter.Value(), &rec); err == nil {
			records = append(records, rec)
		}
	}
	return records, iter.Error()
}
func (bc *Blockchain) SavePruneRecord(record *PruneRecord) error {
	if record.PrunedTimestamp.IsZero() {
		record.PrunedTimestamp = time.Now()
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return bc.db.DB().Set(pruneRecordKey(record.PrunedTimestamp), data, pebble.Sync)
}

func (bc *Blockchain) PruneStatus() map[string]interface{} {
	if bc == nil || bc.pruneManager == nil {
		return map[string]interface{}{"enabled": false}
	}
	return bc.pruneManager.GetPruneStatus()
}
func (bc *Blockchain) ReorgStatus() map[string]interface{} {
	if bc == nil || bc.reorgManager == nil {
		return map[string]interface{}{"enabled": false}
	}
	return bc.reorgManager.GetReorgStatus()
}
func (bc *Blockchain) ProcessRemoteBlock(blk *block.Block, fromPeer string) error {
	if bc != nil && bc.reorgManager != nil && !bc.processingReorgBlock.Load() {
		return bc.reorgManager.ProcessNewBlock(blk, fromPeer)
	}
	return bc.addBlockInternal(blk)
}

func (bc *Blockchain) PruneNow() error {
	if bc == nil || bc.pruneManager == nil {
		return fmt.Errorf("prune manager is not enabled")
	}
	return bc.pruneManager.Prune()
}
