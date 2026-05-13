// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package pow

import (
	"context"
	"math/big"
	"testing"

	"github.com/antdaza/antdchain/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPoWDefaults(t *testing.T) {
	engine := NewPoW()
	require.NotNil(t, engine)

	assert.Equal(t, big.NewInt(BaseDifficulty), engine.GetDifficulty())
	assert.NotNil(t, engine.GetTarget())
}

func TestCalculateExpectedDifficultyBootstrapHeight(t *testing.T) {
	engine := NewPoW()

	diff := engine.CalculateExpectedDifficulty(1, 1000, 1012)

	assert.Equal(t, big.NewInt(BaseDifficulty*4), diff)
}

func TestCalculateDifficultyMovesEveryBlock(t *testing.T) {
	difficulty := big.NewInt(BaseDifficulty)
	for height := uint64(1); height <= 5; height++ {
		next := CalculateDifficultyFromWindow(difficulty, height, []uint64{BlockTimeTarget})
		assert.NotEqual(t, 0, next.Cmp(difficulty), "height %d reused the parent difficulty", height)
		difficulty = next
	}
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
			Difficulty: big.NewInt(MinDifficulty),
			Time:       uint64(i + 1),
			Extra:      []byte("ANTDChain-PoW"),
		}

		require.NoError(t, engine.MineBlock(context.Background(), header))
		assert.True(t, engine.Verify(header))
	}
}
