package chain

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/antdaza/antdchain/antdc/block"
	"github.com/antdaza/antdchain/common"
	"github.com/sirupsen/logrus"
)

type sameHeightTestChain struct {
	canonical        map[uint64]*block.Block
	byHash           map[common.Hash]*block.Block
	tipHeight        uint64
	failDisconnectAt uint64
	connectCalls     int
}

func newSameHeightTestChain(t *testing.T, tip *block.Block) *sameHeightTestChain {
	t.Helper()

	chain := &sameHeightTestChain{
		canonical: make(map[uint64]*block.Block),
		byHash:    make(map[common.Hash]*block.Block),
	}

	for current := tip; current != nil; {
		height := current.Header.Number.Uint64()
		chain.canonical[height] = current
		chain.byHash[current.Hash()] = current
		if height == 0 {
			break
		}
		parent := chain.byHash[current.Header.ParentHash]
		current = parent
	}
	chain.tipHeight = tip.Header.Number.Uint64()

	return chain
}

func (c *sameHeightTestChain) GetBlock(height uint64) (*block.Block, error) {
	blk := c.canonical[height]
	if blk == nil {
		return nil, fmt.Errorf("block %d not found", height)
	}
	return blk, nil
}

func (c *sameHeightTestChain) GetBlockByHash(hash common.Hash) (*block.Block, error) {
	blk := c.byHash[hash]
	if blk == nil {
		return nil, fmt.Errorf("block %s not found", hash.String())
	}
	return blk, nil
}

func (c *sameHeightTestChain) GetBlockHash(height uint64) (common.Hash, error) {
	blk := c.canonical[height]
	if blk == nil {
		return common.Hash{}, fmt.Errorf("block %d not found", height)
	}
	return blk.Hash(), nil
}

func (c *sameHeightTestChain) HasBlock(hash common.Hash) bool {
	_, ok := c.byHash[hash]
	return ok
}

func (c *sameHeightTestChain) HasBlockAtHeight(height uint64) bool {
	return c.canonical[height] != nil
}

func (c *sameHeightTestChain) CurrentHeight() uint64 { return c.tipHeight }

func (c *sameHeightTestChain) CurrentTip() common.Hash {
	blk := c.canonical[c.tipHeight]
	if blk == nil {
		return common.Hash{}
	}
	return blk.Hash()
}

func (c *sameHeightTestChain) GetBlockParent(blk *block.Block) (*block.Block, error) {
	return c.GetBlockByHash(blk.Header.ParentHash)
}

func (c *sameHeightTestChain) GetCumulativeWork(height uint64) (*big.Int, error) {
	total := big.NewInt(0)
	for h := uint64(0); h <= height; h++ {
		blk := c.canonical[h]
		if blk == nil {
			return nil, fmt.Errorf("missing canonical block %d", h)
		}
		total.Add(total, c.CalculateBlockWork(blk.Header.Difficulty))
	}
	return total, nil
}

func (c *sameHeightTestChain) CalculateBlockWork(difficulty *big.Int) *big.Int {
	if difficulty == nil || difficulty.Sign() <= 0 {
		return big.NewInt(1)
	}
	return new(big.Int).Set(difficulty)
}

func (c *sameHeightTestChain) AddBlock(blk *block.Block) error {
	return c.ConnectBlock(blk)
}

func (c *sameHeightTestChain) ConnectBlock(blk *block.Block) error {
	c.connectCalls++
	height := blk.Header.Number.Uint64()
	if height > 0 {
		parent := c.canonical[height-1]
		if parent == nil || parent.Hash() != blk.Header.ParentHash {
			return fmt.Errorf("parent mismatch at height %d", height)
		}
	}
	c.canonical[height] = blk
	c.byHash[blk.Hash()] = blk
	if height >= c.tipHeight {
		c.tipHeight = height
	}
	return nil
}

func (c *sameHeightTestChain) DisconnectBlock(height uint64) error {
	if c.failDisconnectAt == height {
		return fmt.Errorf("forced disconnect failure at height %d", height)
	}
	if height == 0 {
		return fmt.Errorf("cannot disconnect genesis")
	}
	for h := height; h <= c.tipHeight; h++ {
		delete(c.canonical, h)
	}
	c.tipHeight = height - 1
	return nil
}

func (c *sameHeightTestChain) ValidateBlock(blk *block.Block) error { return nil }

func (c *sameHeightTestChain) ValidateBlockContext(blk *block.Block, parent *block.Block) error {
	if blk == nil || blk.Header == nil {
		return fmt.Errorf("nil block")
	}
	if blk.Header.Number.Sign() == 0 {
		return nil
	}
	if parent == nil {
		var err error
		parent, err = c.GetBlockByHash(blk.Header.ParentHash)
		if err != nil {
			return err
		}
	}
	if parent.Hash() != blk.Header.ParentHash {
		return fmt.Errorf("parent hash mismatch")
	}
	return nil
}

func TestProcessNewBlockSameHeightForkReorgsToMoreWork(t *testing.T) {
	prefix := testReorgChainPrefix(t, 5)
	parent := prefix[5]
	existing := testReorgBlock(t, parent, 6, 1, "miner-1")
	candidate := testReorgBlock(t, parent, 6, 2, "miner-2")
	chain := newSameHeightTestChain(t, existing)
	for height, blk := range prefix {
		chain.canonical[uint64(height)] = blk
		chain.byHash[blk.Hash()] = blk
	}

	rm := NewReorgManager(chain, testReorgLogger())

	if err := rm.ProcessNewBlock(candidate, "miner-2"); err != nil {
		t.Fatalf("ProcessNewBlock() error = %v", err)
	}

	got := chain.canonical[6]
	if got == nil || got.Hash() != candidate.Hash() {
		t.Fatalf("canonical block at height 6 = %v, want candidate %s", got, candidate.Hash())
	}
	if rm.reorgCount != 1 {
		t.Fatalf("reorgCount = %d, want 1", rm.reorgCount)
	}
}

func TestPerformReorgDoesNotReconnectBlocksThatFailedToDisconnect(t *testing.T) {
	prefix := testReorgChainPrefix(t, 5)
	parent := prefix[5]
	existing := testReorgBlock(t, parent, 6, 1, "miner-1")
	candidate := testReorgBlock(t, parent, 6, 2, "miner-2")
	chain := newSameHeightTestChain(t, existing)
	for height, blk := range prefix {
		chain.canonical[uint64(height)] = blk
		chain.byHash[blk.Hash()] = blk
	}
	chain.failDisconnectAt = 6

	rm := NewReorgManager(chain, testReorgLogger())
	fork := &ChainFork{
		ForkHeight:     5,
		ActiveChain:    []*block.Block{existing},
		CandidateChain: []*block.Block{candidate},
		ActiveWork:     big.NewInt(1),
		CandidateWork:  big.NewInt(2),
	}

	if err := rm.performReorg(fork); err == nil {
		t.Fatal("performReorg() error = nil, want disconnect failure")
	}
	if chain.connectCalls != 0 {
		t.Fatalf("ConnectBlock calls after failed disconnect = %d, want 0", chain.connectCalls)
	}
	if got := chain.canonical[6]; got == nil || got.Hash() != existing.Hash() {
		t.Fatalf("canonical block at height 6 = %v, want existing %s", got, existing.Hash())
	}
}

func TestProcessNewBlockSameHeightForkKeepsExistingOnEqualOrLessWork(t *testing.T) {
	prefix := testReorgChainPrefix(t, 5)
	parent := prefix[5]
	existing := testReorgBlock(t, parent, 6, 2, "miner-1")
	candidate := testReorgBlock(t, parent, 6, 2, "miner-2")
	chain := newSameHeightTestChain(t, existing)
	for height, blk := range prefix {
		chain.canonical[uint64(height)] = blk
		chain.byHash[blk.Hash()] = blk
	}

	rm := NewReorgManager(chain, testReorgLogger())

	if err := rm.ProcessNewBlock(candidate, "miner-2"); err != nil {
		t.Fatalf("ProcessNewBlock() error = %v", err)
	}

	got := chain.canonical[6]
	if got == nil || got.Hash() != existing.Hash() {
		t.Fatalf("canonical block at height 6 = %v, want existing %s", got, existing.Hash())
	}
	if rm.reorgCount != 0 {
		t.Fatalf("reorgCount = %d, want 0", rm.reorgCount)
	}
}

func testReorgChainPrefix(t *testing.T, tipHeight uint64) []*block.Block {
	t.Helper()

	blocks := make([]*block.Block, tipHeight+1)
	for height := uint64(0); height <= tipHeight; height++ {
		var parent *block.Block
		if height > 0 {
			parent = blocks[height-1]
		}
		blocks[height] = testReorgBlock(t, parent, height, 1, fmt.Sprintf("prefix-%d", height))
	}
	return blocks
}

func testReorgBlock(t *testing.T, parent *block.Block, height uint64, difficulty int64, extra string) *block.Block {
	t.Helper()

	var parentHash common.Hash
	if parent != nil {
		parentHash = parent.Hash()
	}

	blk, err := block.NewBlock(&block.Header{
		ParentHash: parentHash,
		Difficulty: big.NewInt(difficulty),
		Number:     new(big.Int).SetUint64(height),
		GasLimit:   1,
		Time:       uint64(time.Now().Unix()) + height,
		Extra:      []byte(extra),
	}, nil, nil)
	if err != nil {
		t.Fatalf("NewBlock() error = %v", err)
	}
	return blk
}

func testReorgLogger() *logrus.Logger {
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	return logger
}
