// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package pow

import (
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

	assert.Equal(t, big.NewInt(1_000_000), diff)
}

func TestAutoRegisterIfEligible(t *testing.T) {
	engine := NewPoW()
	addr, err := common.ParseQuantumAddress("0qANA3c85k94LTyTXLGDdEzmLE32b1qhYZF")
	require.NoError(t, err)

	engine.AutoRegisterIfEligible(addr, new(big.Int).Set(MinStakeAmount), nil)

	assert.True(t, engine.IsKing(addr))
}

func TestGetNextMinerIgnoresLocalPenaltyState(t *testing.T) {
	engineA := NewPoW()
	engineB := NewPoW()

	addr1 := common.BytesToQuantumAddress([]byte("validator-1"))
	addr2 := common.BytesToQuantumAddress([]byte("validator-2"))
	parent := common.BytesToHash([]byte("genesis"))

	stake1 := new(big.Int).Set(MinStakeAmount)
	stake2 := new(big.Int).Mul(MinStakeAmount, big.NewInt(2))

	engineA.AutoRegisterIfEligible(addr1, stake1, nil)
	engineA.AutoRegisterIfEligible(addr2, stake2, nil)
	engineB.AutoRegisterIfEligible(addr1, stake1, nil)
	engineB.AutoRegisterIfEligible(addr2, stake2, nil)

	// Introduce local-only state drift on engineB.
	engineB.RecordMissedBlock(addr1)
	engineB.SetPriorityMiner(addr1, 50)

	minerA, err := engineA.GetNextMiner(parent, 1)
	require.NoError(t, err)
	minerB, err := engineB.GetNextMiner(parent, 1)
	require.NoError(t, err)

	assert.Equal(t, minerA, minerB)
}
