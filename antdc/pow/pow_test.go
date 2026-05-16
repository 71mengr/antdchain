// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package pow

import (
	"context"
	"math/big"
	"testing"

	"github.com/antdaza/antdchain/common"
	"github.com/antdaza/antdchain/difficulty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPoWDefaults(t *testing.T) {
	engine := NewPoW()
	require.NotNil(t, engine)

	assert.Equal(t, big.NewInt(difficulty.BaseDifficulty), engine.GetDifficulty())
	assert.NotNil(t, engine.GetTarget())
}

func TestCalculateExpectedDifficultyBootstrapHeight(t *testing.T) {
	engine := NewPoW()

	diff := engine.CalculateExpectedDifficulty(1, 1000, 1012)

	assert.Equal(t, big.NewInt(difficulty.BaseDifficulty*4), diff)
}

func TestCalculateExpectedDifficultyHandlesNonIncreasingTime(t *testing.T) {
	engine := NewPoW()

	diff := engine.CalculateExpectedDifficulty(1, 1000, 1000)

	assert.Equal(t, difficulty.FromWindow(big.NewInt(difficulty.BaseDifficulty), 1, []uint64{1}), diff)
}

func TestCalculateDifficultyMovesEveryBlock(t *testing.T) {
	currentDifficulty := big.NewInt(difficulty.BaseDifficulty)
	for height := uint64(1); height <= 5; height++ {
		next := difficulty.FromWindow(currentDifficulty, height, []uint64{difficulty.BlockTimeTarget})
		assert.NotEqual(t, 0, next.Cmp(currentDifficulty), "height %d reused the parent difficulty", height)
		currentDifficulty = next
	}
}

func TestMinerSpecificDifficultyIsUniqueAtSameHeight(t *testing.T) {
	base := difficulty.FromWindow(big.NewInt(difficulty.BaseDifficulty), 5, []uint64{difficulty.BlockTimeTarget})
	minerA := common.BytesToQuantumAddress([]byte("miner-a"))
	minerB := common.BytesToQuantumAddress([]byte("miner-b"))

	diffA := difficulty.ForMiner(base, minerA)
	diffB := difficulty.ForMiner(base, minerB)

	assert.NotEqual(t, 0, diffA.Cmp(diffB))
	assert.Equal(t, base, difficulty.Normalize(diffA))
	assert.Equal(t, base, difficulty.Normalize(diffB))
	assert.Equal(t, 0, difficulty.Target(diffA).Cmp(difficulty.Target(diffB)))
}

func TestCalculateExpectedDifficultyForMinerUsesMinerSuffix(t *testing.T) {
	engine := NewPoW()
	minerA := common.BytesToQuantumAddress([]byte("miner-a"))
	minerB := common.BytesToQuantumAddress([]byte("miner-b"))

	diffA := engine.CalculateExpectedDifficultyForMiner(5, 1000, 1000+difficulty.BlockTimeTarget, minerA)
	diffB := engine.CalculateExpectedDifficultyForMiner(5, 1000, 1000+difficulty.BlockTimeTarget, minerB)

	assert.NotEqual(t, 0, diffA.Cmp(diffB))
	assert.Equal(t, engine.CalculateExpectedDifficulty(5, 1000, 1000+difficulty.BlockTimeTarget), difficulty.Normalize(diffA))
	assert.Equal(t, engine.CalculateExpectedDifficulty(5, 1000, 1000+difficulty.BlockTimeTarget), difficulty.Normalize(diffB))
}

func TestDisplayDifficultyKeepsMinerSpecificValue(t *testing.T) {
	base := difficulty.FromWindow(big.NewInt(difficulty.BaseDifficulty), 7, []uint64{difficulty.BlockTimeTarget})
	miner := common.BytesToQuantumAddress([]byte("display-miner"))
	diff := difficulty.ForMiner(base, miner)

	assert.Equal(t, diff.String(), difficulty.Display(diff))
	assert.NotEqual(t, difficulty.Normalize(diff).String(), difficulty.Display(diff))
}

func TestAutoRegisterIfEligible(t *testing.T) {
	engine := NewPoW()
	addr, err := common.ParseQuantumAddress("0qANA3c85k94LTyTXLGDdEzmLE32b1qhYZF")
	require.NoError(t, err)

	engine.AutoRegisterIfEligible(addr, big.NewInt(0), nil)

	eligible, err := engine.VerifyMinerEligibility(addr, common.Hash{}, 1, 0)
	require.NoError(t, err)
	assert.True(t, eligible)
	assert.False(t, engine.IsKing(addr))
}

func TestGetNextMinerIgnoresLocalPenaltyState(t *testing.T) {
	engineA := NewPoW()
	engineB := NewPoW()

	addr1 := common.BytesToQuantumAddress([]byte("validator-1"))
	addr2 := common.BytesToQuantumAddress([]byte("validator-2"))
	parent := common.BytesToHash([]byte("genesis"))

	stake1 := big.NewInt(0)
	stake2 := big.NewInt(0)

	engineA.AutoRegisterIfEligible(addr1, stake1, nil)
	engineA.AutoRegisterIfEligible(addr2, stake2, nil)
	engineB.AutoRegisterIfEligible(addr1, stake1, nil)
	engineB.AutoRegisterIfEligible(addr2, stake2, nil)

	// Introduce local-only state drift on engineB.
	engineB.RecordMissedBlock(addr1)

	minerA, err := engineA.GetNextMiner(parent, 1)
	require.NoError(t, err)
	minerB, err := engineB.GetNextMiner(parent, 1)
	require.NoError(t, err)

	assert.Equal(t, minerA, minerB)
}

func TestAnyMinerCanMineAndVerifyBlock(t *testing.T) {
	t.Parallel()

	engine := NewPoW()
	miners := []common.QuantumAddress{
		common.BytesToQuantumAddress([]byte("miner-one")),
		common.BytesToQuantumAddress([]byte("miner-two")),
		common.BytesToQuantumAddress([]byte("miner-three")),
	}

	for i, miner := range miners {
		eligible, err := engine.VerifyMinerEligibility(miner, common.Hash{}, uint64(i+1), 1)
		require.NoError(t, err)
		assert.True(t, eligible)

		header := &BlockHeader{
			ParentHash: common.BytesToHash([]byte("parent")),
			Coinbase:   miner,
			Root:       common.BytesToHash([]byte("root")),
			TxHash:     common.BytesToHash([]byte("txs")),
			Number:     uint64(i + 1),
			Difficulty: big.NewInt(difficulty.MinDifficulty),
			Time:       uint64(i + 1),
			Extra:      []byte("ANTDChain-PoW"),
		}

		require.NoError(t, engine.MineBlock(context.Background(), header))
		assert.True(t, engine.Verify(header))
	}
}

func TestMineBlockReturnsWhenContextCanceled(t *testing.T) {
	t.Parallel()

	engine := NewPoW()
	header := &BlockHeader{
		ParentHash: common.BytesToHash([]byte("parent")),
		Coinbase:   common.BytesToQuantumAddress([]byte("miner")),
		Root:       common.BytesToHash([]byte("root")),
		TxHash:     common.BytesToHash([]byte("txs")),
		Number:     1,
		Difficulty: new(big.Int).Set(difficulty.MaxDifficulty),
		Time:       1,
		Extra:      []byte("ANTDChain-PoW"),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, engine.MineBlock(ctx, header), context.Canceled)
}
