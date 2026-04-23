// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package chain
import (
    "math/big"
    "github.com/antdaza/antdchain/antdc/rotatingking"

    )
// RotatingKingManager interface for managing rotating kings
type RotatingKingManager interface {
    GetCurrentKing() common.QuantumAddress
    GetCurrentKingIndex() int
    GetKingAddresses() []common.QuantumAddress
    IsKing(common.QuantumAddress) bool
    ForceRotate(int, string) error
    GetRotationInterval() uint64
    GetRotationHistory(int) []interface{}
    GetKingRewardMultiplier() *big.Float
}


var _ rotatingking.BlockchainProvider = (*blockchainProviderWrapper)(nil)
