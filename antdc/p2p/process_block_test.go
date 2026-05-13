// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package p2p

import (
	"context"
	"errors"
	"io"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/antdaza/antdchain/antdc/block"
	"github.com/antdaza/antdchain/antdc/checkpoints"
	"github.com/antdaza/antdchain/antdc/pow"
	"github.com/antdaza/antdchain/antdc/reward"
	"github.com/antdaza/antdchain/antdc/state"
	"github.com/antdaza/antdchain/antdc/tx"
	"github.com/antdaza/antdchain/common"
	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/sirupsen/logrus"
)

func TestProcessBlockDefersSameHeightForkToChainForkChoice(t *testing.T) {
	parent := testP2PBlock(5, common.BytesToHash([]byte("grandparent")), common.BytesToQuantumAddress([]byte("parent-miner")), 1778464375)
	parentHash := parent.Hash()
	existing := testP2PBlock(6, parentHash, common.BytesToQuantumAddress([]byte("existing-miner")), 1778464378)
	competitor := testP2PBlock(6, parentHash, common.BytesToQuantumAddress([]byte("fork-miner")), 1778464376)

	chain := &processBlockForkChoiceChain{
		latest: existing,
		blocksByHeight: map[uint64]*block.Block{
			5: parent,
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

func TestProcessBlockRejectsProtocolInvalidSameHeightFork(t *testing.T) {
	parent := testP2PBlock(5, common.BytesToHash([]byte("grandparent-invalid")), common.BytesToQuantumAddress([]byte("parent-miner")), 1778464375)
	parentHash := parent.Hash()
	existing := testP2PBlock(6, parentHash, common.BytesToQuantumAddress([]byte("existing-miner")), 1778464378)
	competitor := testP2PBlock(6, parentHash, common.BytesToQuantumAddress([]byte("fork-miner")), 1778464376)
	protocolErr := errors.New("bad protocol fields")

	chain := &processBlockForkChoiceChain{
		latest: existing,
		blocksByHeight: map[uint64]*block.Block{
			5: parent,
			6: existing,
		},
		knownHashes: map[common.Hash]bool{
			parentHash:      true,
			existing.Hash(): true,
		},
		proposalErr: protocolErr,
	}

	logger := logrus.New()
	logger.SetOutput(io.Discard)

	node := &Node{chain: chain, logger: logger}
	err := node.processBlock(competitor)
	if err == nil {
		t.Fatalf("expected processBlock to reject protocol-invalid fork")
	}
	if !errors.Is(err, protocolErr) {
		t.Fatalf("expected protocol error %v, got %v", protocolErr, err)
	}
	if chain.addedBlock != nil {
		t.Fatalf("protocol-invalid same-height fork was passed to AddBlock")
	}
	if chain.proposalChecks != 1 {
		t.Fatalf("expected one proposal validation, got %d", chain.proposalChecks)
	}
}

func TestProcessBlockOrphansProtocolLoser(t *testing.T) {
	parent := testP2PBlock(7, common.BytesToHash([]byte("grandparent-2")), common.BytesToQuantumAddress([]byte("parent-miner")), 1778464375)
	parentHash := parent.Hash()
	winner := testP2PBlock(8, parentHash, common.BytesToQuantumAddress([]byte("winner-miner")), 1778464376)
	loser := testP2PBlock(8, parentHash, common.BytesToQuantumAddress([]byte("loser-miner")), 1778464380)

	chain := &processBlockForkChoiceChain{
		latest: winner,
		blocksByHeight: map[uint64]*block.Block{
			7: parent,
			8: winner,
		},
		knownHashes: map[common.Hash]bool{
			parentHash:    true,
			winner.Hash(): true,
		},
	}

	logger := logrus.New()
	logger.SetOutput(io.Discard)

	node := &Node{chain: chain, logger: logger}
	if err := node.processBlock(loser); err != nil {
		t.Fatalf("processBlock returned error: %v", err)
	}
	if chain.addedBlock != nil {
		t.Fatalf("expected protocol loser to stay out of AddBlock")
	}

	node.orphanPoolMu.Lock()
	entry, ok := node.orphanPool[loser.Hash()]
	node.orphanPoolMu.Unlock()
	if !ok {
		t.Fatalf("expected losing block to be stored in orphan pool")
	}
	if time.Until(entry.expiresAt) <= 0 || time.Until(entry.expiresAt) > orphanBlockTTL {
		t.Fatalf("expected orphan expiry within %s, got %s", orphanBlockTTL, time.Until(entry.expiresAt))
	}
}

func TestProcessBlockDefersMissingParentOrphanAndTriggersSync(t *testing.T) {
	genesis := testP2PBlock(0, common.Hash{}, common.BytesToQuantumAddress([]byte("genesis-miner")), 1778464375)
	parent := testP2PBlock(1, genesis.Hash(), common.BytesToQuantumAddress([]byte("parent-miner")), 1778464376)
	missingParent := testP2PBlock(2, parent.Hash(), common.BytesToQuantumAddress([]byte("missing-parent-miner")), 1778464377)
	orphan := testP2PBlock(3, missingParent.Hash(), common.BytesToQuantumAddress([]byte("orphan-miner")), 1778464378)

	chain := &processBlockForkChoiceChain{
		latest: parent,
		blocksByHeight: map[uint64]*block.Block{
			0: genesis,
			1: parent,
		},
		knownHashes: map[common.Hash]bool{
			genesis.Hash(): true,
			parent.Hash():  true,
		},
	}

	logger := logrus.New()
	logger.SetOutput(io.Discard)

	node := &Node{chain: chain, logger: logger}
	if err := node.processBlock(orphan); err != nil {
		t.Fatalf("expected missing-parent orphan to be deferred without error, got %v", err)
	}
	if chain.addedBlock != nil {
		t.Fatalf("missing-parent orphan was passed to AddBlock")
	}
	if !chain.syncing || chain.syncTarget != 3 {
		t.Fatalf("expected sync target 3 after orphan ahead of tip, syncing=%v target=%d", chain.syncing, chain.syncTarget)
	}

	node.orphanPoolMu.Lock()
	_, ok := node.orphanPool[orphan.Hash()]
	node.orphanPoolMu.Unlock()
	if !ok {
		t.Fatalf("expected missing-parent block to be stored in orphan pool")
	}
}

func TestTwoNodesFindHeightAndBlock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := logrus.New()
	logger.SetOutput(io.Discard)

	parent := testP2PBlock(0, common.Hash{}, common.BytesToQuantumAddress([]byte("genesis-miner")), 1778464375)
	blk := testP2PBlock(1, parent.Hash(), common.BytesToQuantumAddress([]byte("sync-miner")), 1778464376)

	servingChain := &processBlockForkChoiceChain{
		latest: blk,
		blocksByHeight: map[uint64]*block.Block{
			0: parent,
			1: blk,
		},
		knownHashes: map[common.Hash]bool{
			parent.Hash(): true,
			blk.Hash():    true,
		},
	}
	requestingChain := &processBlockForkChoiceChain{
		latest: parent,
		blocksByHeight: map[uint64]*block.Block{
			0: parent,
		},
		knownHashes: map[common.Hash]bool{
			parent.Hash(): true,
		},
	}

	servingHost, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("create serving host: %v", err)
	}
	defer servingHost.Close()

	requestingHost, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("create requesting host: %v", err)
	}
	defer requestingHost.Close()

	servingNode := &Node{host: servingHost, chain: servingChain, logger: logger, ctx: ctx}
	requestingNode := &Node{host: requestingHost, chain: requestingChain, logger: logger, ctx: ctx}
	servingHost.SetStreamHandler("/antdchain/sync/1.0.0", servingNode.handleStream)

	servingInfo := peer.AddrInfo{ID: servingHost.ID(), Addrs: servingHost.Addrs()}
	if err := requestingHost.Connect(ctx, servingInfo); err != nil {
		t.Fatalf("connect requesting node to serving node: %v", err)
	}

	height, err := requestingNode.GetPeerHeight(servingHost.ID())
	if err != nil {
		t.Fatalf("get peer height: %v", err)
	}
	if height != 1 {
		t.Fatalf("expected peer height 1, got %d", height)
	}

	found, err := requestingNode.RequestBlockSync(servingHost.ID(), 1)
	if err != nil {
		t.Fatalf("request block: %v", err)
	}
	if found.Hash() != blk.Hash() {
		t.Fatalf("expected requested block hash %s, got %s", blk.Hash(), found.Hash())
	}

	missing, err := requestingNode.RequestBlockSync(servingHost.ID(), 2)
	if err == nil {
		t.Fatalf("expected missing block request to fail, got block %v", missing)
	}
}

func TestTriggerImmediateSyncDoesNotPreMarkSyncing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := logrus.New()
	logger.SetOutput(io.Discard)

	genesis := testP2PBlock(0, common.Hash{}, common.BytesToQuantumAddress([]byte("genesis-miner")), 1778464375)
	parent := testP2PBlock(1, genesis.Hash(), common.BytesToQuantumAddress([]byte("parent-miner")), 1778464376)
	blk := testP2PBlock(2, parent.Hash(), common.BytesToQuantumAddress([]byte("sync-miner")), 1778464377)

	servingChain := &processBlockForkChoiceChain{
		latest: blk,
		blocksByHeight: map[uint64]*block.Block{
			0: genesis,
			1: parent,
			2: blk,
		},
		knownHashes: map[common.Hash]bool{
			genesis.Hash(): true,
			parent.Hash():  true,
			blk.Hash():     true,
		},
	}
	requestingChain := &processBlockForkChoiceChain{
		latest: parent,
		blocksByHeight: map[uint64]*block.Block{
			0: genesis,
			1: parent,
		},
		knownHashes: map[common.Hash]bool{
			genesis.Hash(): true,
			parent.Hash():  true,
		},
	}

	servingHost, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("create serving host: %v", err)
	}
	defer servingHost.Close()

	requestingHost, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("create requesting host: %v", err)
	}
	defer requestingHost.Close()

	servingNode := &Node{host: servingHost, chain: servingChain, logger: logger, ctx: ctx}
	requestingNode := &Node{host: requestingHost, chain: requestingChain, logger: logger, ctx: ctx}
	servingHost.SetStreamHandler("/antdchain/sync/1.0.0", servingNode.handleStream)

	servingInfo := peer.AddrInfo{ID: servingHost.ID(), Addrs: servingHost.Addrs()}
	if err := requestingHost.Connect(ctx, servingInfo); err != nil {
		t.Fatalf("connect requesting node to serving node: %v", err)
	}

	requestingNode.triggerImmediateSync()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if latest := requestingChain.Latest(); latest != nil && latest.Header.Number.Uint64() == 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	latest := requestingChain.Latest()
	if latest == nil {
		t.Fatal("expected requesting chain to sync block 2, latest block is nil")
	}
	t.Fatalf("expected requesting chain to sync block 2, got height %d", latest.Header.Number.Uint64())
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
	mu             sync.Mutex
	latest         *block.Block
	blocksByHeight map[uint64]*block.Block
	knownHashes    map[common.Hash]bool
	addedBlock     *block.Block
	addErr         error
	proposalErr    error
	proposalChecks int
	syncing        bool
	syncTarget     uint64
}

func (c *processBlockForkChoiceChain) Latest() *block.Block {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.latest
}
func (c *processBlockForkChoiceChain) State() *state.State { return nil }
func (c *processBlockForkChoiceChain) GetParentHash(uint64) (common.Hash, error) {
	return common.Hash{}, nil
}
func (c *processBlockForkChoiceChain) Pow() *pow.PoW                         { return nil }
func (c *processBlockForkChoiceChain) Checkpoints() *checkpoints.Checkpoints { return nil }
func (c *processBlockForkChoiceChain) ValidateMinedBlockProposal(*block.Block) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.proposalChecks++
	return c.proposalErr
}

func (c *processBlockForkChoiceChain) AddBlock(b *block.Block) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.addedBlock = b
	if c.addErr != nil {
		return c.addErr
	}
	if b == nil {
		return errors.New("nil block")
	}
	if c.blocksByHeight == nil {
		c.blocksByHeight = make(map[uint64]*block.Block)
	}
	if c.knownHashes == nil {
		c.knownHashes = make(map[common.Hash]bool)
	}
	c.latest = b
	c.blocksByHeight[b.Header.Number.Uint64()] = b
	c.knownHashes[b.Hash()] = true
	return nil
}
func (c *processBlockForkChoiceChain) GetBlock(height uint64) *block.Block {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.blocksByHeight[height]
}
func (c *processBlockForkChoiceChain) GetBlockByHash(common.Hash) (*block.Block, error) {
	return nil, nil
}
func (c *processBlockForkChoiceChain) TruncateTo(uint64) error          { return nil }
func (c *processBlockForkChoiceChain) TxPool() TxPool                   { return nil }
func (c *processBlockForkChoiceChain) ValidateTransaction(*tx.Tx) error { return nil }
func (c *processBlockForkChoiceChain) IsSyncing() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.syncing
}
func (c *processBlockForkChoiceChain) StartSync(target uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.syncing = true
	c.syncTarget = target
}
func (c *processBlockForkChoiceChain) StopSync() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.syncing = false
}
func (c *processBlockForkChoiceChain) GetRotatingKingManager() reward.RotatingKingManager { return nil }
func (c *processBlockForkChoiceChain) ShouldSyncDatabase() bool                           { return false }
func (c *processBlockForkChoiceChain) MarkDatabaseSynced(uint64)                          {}
func (c *processBlockForkChoiceChain) GetDatabaseSyncHeight() uint64                      { return 0 }
func (c *processBlockForkChoiceChain) HasBlock(hash common.Hash) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.knownHashes[hash]
}
