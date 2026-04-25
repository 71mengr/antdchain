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
