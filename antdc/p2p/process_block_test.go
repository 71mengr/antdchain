// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package p2p

import (
	"errors"
	"io"
	"math/big"
	"testing"

	"github.com/antdaza/antdchain/antdc/block"
	"github.com/antdaza/antdchain/antdc/checkpoints"
	"github.com/antdaza/antdchain/antdc/pow"
	"github.com/antdaza/antdchain/antdc/reward"
	"github.com/antdaza/antdchain/antdc/state"
	"github.com/antdaza/antdchain/antdc/tx"
	"github.com/antdaza/antdchain/common"
	"github.com/sirupsen/logrus"
)

func TestProcessBlockDefersSameHeightForkToChainForkChoice(t *testing.T) {
	parentHash := common.BytesToHash([]byte("parent"))
	existing := testP2PBlock(6, parentHash, common.BytesToQuantumAddress([]byte("existing-miner")), 1778464378)
	competitor := testP2PBlock(6, parentHash, common.BytesToQuantumAddress([]byte("fork-miner")), 1778464376)

	chain := &processBlockForkChoiceChain{
		latest: existing,
		blocksByHeight: map[uint64]*block.Block{
			6: existing,
		},
		knownHashes: map[common.Hash]bool{
			parentHash:      true,
			existing.Hash(): true,
		},
	}

	logger := logrus.New()
	logger.SetOutput(io.Discard)

	node := &Node{chain: chain, logger: logger}
	if err := node.processBlock(competitor); err != nil {
		t.Fatalf("processBlock returned error: %v", err)
	}

	if chain.addedBlock != competitor {
		t.Fatalf("expected same-height fork to be passed to chain.AddBlock")
	}
}

func testP2PBlock(height uint64, parentHash common.Hash, miner common.QuantumAddress, timestamp uint64) *block.Block {
	return &block.Block{
		Header: &block.Header{
			ParentHash: parentHash,
			Coinbase:   miner,
			Number:     new(big.Int).SetUint64(height),
			Difficulty: big.NewInt(1_000_000),
			GasLimit:   60_000_000,
			Time:       timestamp,
			Extra:      []byte("ANTDChain-PoW"),
		},
	}
}

type processBlockForkChoiceChain struct {
	latest         *block.Block
	blocksByHeight map[uint64]*block.Block
	knownHashes    map[common.Hash]bool
	addedBlock     *block.Block
	addErr         error
}

func (c *processBlockForkChoiceChain) Latest() *block.Block { return c.latest }
func (c *processBlockForkChoiceChain) State() *state.State  { return nil }
func (c *processBlockForkChoiceChain) GetParentHash(uint64) (common.Hash, error) {
	return common.Hash{}, nil
}
func (c *processBlockForkChoiceChain) Pow() *pow.PoW                         { return nil }
func (c *processBlockForkChoiceChain) Checkpoints() *checkpoints.Checkpoints { return nil }
func (c *processBlockForkChoiceChain) AddBlock(b *block.Block) error {
	c.addedBlock = b
	if c.addErr != nil {
		return c.addErr
	}
	if b == nil {
		return errors.New("nil block")
	}
	return nil
}
func (c *processBlockForkChoiceChain) GetBlock(height uint64) *block.Block {
	return c.blocksByHeight[height]
}
func (c *processBlockForkChoiceChain) GetBlockByHash(common.Hash) (*block.Block, error) {
	return nil, nil
}
func (c *processBlockForkChoiceChain) TruncateTo(uint64) error                            { return nil }
func (c *processBlockForkChoiceChain) TxPool() TxPool                                     { return nil }
func (c *processBlockForkChoiceChain) ValidateTransaction(*tx.Tx) error                   { return nil }
func (c *processBlockForkChoiceChain) IsSyncing() bool                                    { return false }
func (c *processBlockForkChoiceChain) StartSync(uint64)                                   {}
func (c *processBlockForkChoiceChain) StopSync()                                          {}
func (c *processBlockForkChoiceChain) GetRotatingKingManager() reward.RotatingKingManager { return nil }
func (c *processBlockForkChoiceChain) ShouldSyncDatabase() bool                           { return false }
func (c *processBlockForkChoiceChain) MarkDatabaseSynced(uint64)                          {}
func (c *processBlockForkChoiceChain) GetDatabaseSyncHeight() uint64                      { return 0 }
func (c *processBlockForkChoiceChain) HasBlock(hash common.Hash) bool                     { return c.knownHashes[hash] }
