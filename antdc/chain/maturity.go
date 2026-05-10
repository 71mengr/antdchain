// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package chain

import (
	"math/big"

	"github.com/antdaza/antdchain/common"
)

// txMaturityConfirmations returns the number of confirmations required before
// received transaction value can be used in a later spend.
func (bc *Blockchain) txMaturityConfirmations() uint64 {
	if bc != nil && bc.minConfirmations > 0 {
		return bc.minConfirmations
	}
	return DefaultMinConfirmations
}

func (bc *Blockchain) spendableBalanceAtHeight(addr common.QuantumAddress, currentHeight uint64, pendingIncoming map[common.QuantumAddress]*big.Int) *big.Int {
	if bc == nil || bc.state == nil {
		return big.NewInt(0)
	}

	balance := new(big.Int).Set(bc.state.GetBalance(addr))
	immature := bc.immatureIncomingBalance(addr, currentHeight, pendingIncoming)
	spendable := new(big.Int).Sub(balance, immature)
	if spendable.Sign() < 0 {
		return big.NewInt(0)
	}
	return spendable
}

func (bc *Blockchain) immatureIncomingBalance(addr common.QuantumAddress, currentHeight uint64, pendingIncoming map[common.QuantumAddress]*big.Int) *big.Int {
	immature := big.NewInt(0)
	if pendingIncoming != nil {
		if amount := pendingIncoming[addr]; amount != nil {
			immature.Add(immature, amount)
		}
	}

	confirmationsRequired := bc.txMaturityConfirmations()
	if confirmationsRequired <= 1 || bc == nil {
		return immature
	}

	lookback := confirmationsRequired - 1
	startHeight := uint64(0)
	if currentHeight > lookback {
		startHeight = currentHeight - lookback + 1
	}

	for height := startHeight; height <= currentHeight; height++ {
		blk, err := bc.getBlockByNumber(height)
		if err != nil || blk == nil {
			continue
		}
		for _, transaction := range blk.Txs {
			if transaction == nil || transaction.To == nil || transaction.Value == nil {
				continue
			}
			to := common.BytesToQuantumAddress(transaction.To.Bytes())
			from := common.BytesToQuantumAddress(transaction.From.Bytes())
			if to == addr && from != addr {
				immature.Add(immature, transaction.Value)
			}
		}
	}

	return immature
}
