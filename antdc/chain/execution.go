// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package chain

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"math/big"
	"sort"

	"github.com/antdaza/antdchain/antdc/block"
	"github.com/antdaza/antdchain/antdc/tx"
	"github.com/antdaza/antdchain/antdc/vm"
	"github.com/antdaza/antdchain/common"
)

// executeBlockTransactions executes all transactions in the block
func (bc *Blockchain) executeBlockTransactions(b *block.Block) (*big.Int, uint64, error) {
	bc.stateMu.Lock()
	defer bc.stateMu.Unlock()

	v := vm.NewVM(bc.state, b.Header.GasLimit)
	ctx := context.Background()

	totalFees := big.NewInt(0)
	totalGasUsed := uint64(0)

	for i, transaction := range b.Txs {
		// Validate transaction before execution
		if err := bc.validateTransactionForExecution(transaction, i); err != nil {
			return nil, 0, fmt.Errorf("transaction %d invalid: %w", i, err)
		}

		// Execute transaction
		_, _, execErr := v.Execute(ctx, transaction)
		if execErr != nil {
			return nil, 0, fmt.Errorf("transaction %d execution error: %w", i, execErr)
		}

		// Update totals using the block-committed gas amount.
		// Block headers store GasUsed as the sum of transaction gas limits,
		// so reward-fee accounting must use the same value that miners used
		// when computing the header state root.
		committedGas := transactionCommittedGas(transaction)
		totalGasUsed += committedGas
		txFee := new(big.Int).Mul(transaction.GasPrice, big.NewInt(int64(committedGas)))
		totalFees.Add(totalFees, txFee)

		// Log execution progress for large blocks
		if i > 0 && i%100 == 0 {
			log.Printf("[blockchain] Executed %d/%d transactions in block %d",
				i, len(b.Txs), b.Header.Number.Uint64())
		}
	}

	return totalFees, totalGasUsed, nil
}


func transactionCommittedGas(t *tx.Tx) uint64 {
	if t == nil {
		return 0
	}
	return t.Gas
}

// sortAndValidateTransactions sorts transactions by sender and nonce
func (bc *Blockchain) sortAndValidateTransactions(txs []*tx.Tx) ([]*tx.Tx, error) {
	if len(txs) == 0 {
		return txs, nil
	}

	// Group transactions by sender while rejecting nil/duplicate txs.
	txsBySender := make(map[common.QuantumAddress][]*tx.Tx)
	seenTxHashes := make(map[common.Hash]struct{}, len(txs))
	for i, currentTx := range txs {
		if currentTx == nil {
			return nil, fmt.Errorf("nil transaction at index %d", i)
		}
		txHash := currentTx.Hash()
		if _, exists := seenTxHashes[txHash]; exists {
			return nil, fmt.Errorf("duplicate transaction detected: %s", txHash.Hex())
		}
		seenTxHashes[txHash] = struct{}{}
		txsBySender[currentTx.From] = append(txsBySender[currentTx.From], currentTx)
	}

	// Process senders in deterministic order to avoid consensus splits.
	senders := make([]common.QuantumAddress, 0, len(txsBySender))
	for sender := range txsBySender {
		senders = append(senders, sender)
	}
	sort.Slice(senders, func(i, j int) bool {
		return bytes.Compare(senders[i][:], senders[j][:]) < 0
	})

	validTxs := make([]*tx.Tx, 0, len(txs))

	// Process each sender's transactions in strict nonce order.
	for _, sender := range senders {
		senderTxs := txsBySender[sender]
		sort.Slice(senderTxs, func(i, j int) bool {
			return senderTxs[i].Nonce < senderTxs[j].Nonce
		})

		senderAddress := common.BytesToQuantumAddress(sender.Bytes())
		currentNonce := bc.state.GetNonce(senderAddress)
		currentBalance := new(big.Int).Set(bc.state.GetBalance(senderAddress))

		for txIndex, senderTx := range senderTxs {
			// Strict contiguous nonce sequence.
			if senderTx.Nonce != currentNonce {
				return nil, fmt.Errorf("nonce mismatch for %s at sender index %d: expected %d, got %d",
					sender.String(), txIndex, currentNonce, senderTx.Nonce)
			}

			txCost := new(big.Int).Add(
				senderTx.Value,
				new(big.Int).Mul(senderTx.GasPrice, big.NewInt(int64(senderTx.Gas))),
			)
			if currentBalance.Cmp(txCost) < 0 {
				return nil, fmt.Errorf("insufficient balance for %s: need %s, have %s",
					sender.String(), formatBalance(txCost), formatBalance(currentBalance))
			}

			// Advance simulated sender state so later txs cannot overspend.
			validTxs = append(validTxs, senderTx)
			currentNonce++
			currentBalance.Sub(currentBalance, txCost)
		}
	}

	return validTxs, nil
}

// validateTransactionForExecution validates a single transaction
func (bc *Blockchain) validateTransactionForExecution(t *tx.Tx, index int) error {
	if t == nil {
		return fmt.Errorf("transaction %d is nil", index)
	}

	// Signature verification
	valid, err := t.Verify()
	if err != nil {
		return fmt.Errorf("transaction %d verification error: %w", index, err)
	}
	if !valid {
		return fmt.Errorf("transaction %d has invalid signature", index)
	}

	// Nonce validation
	from := common.BytesToQuantumAddress(t.From.Bytes())
	expectedNonce := bc.state.GetNonce(from)
	if t.Nonce != expectedNonce {
		return fmt.Errorf("transaction %d invalid nonce: expected %d, got %d",
			index, expectedNonce, t.Nonce)
	}

	// Balance check
	balance := bc.state.GetBalance(from)
	totalCost := new(big.Int).Add(
		t.Value,
		new(big.Int).Mul(t.GasPrice, big.NewInt(int64(t.Gas))),
	)
	if balance.Cmp(totalCost) < 0 {
		return fmt.Errorf("transaction %d insufficient balance: have %s, need %s",
			index, formatBalance(balance), formatBalance(totalCost))
	}

	return nil
}

// applyBlockTransactions applies transactions to state
func (bc *Blockchain) applyBlockTransactions(b *block.Block) error {
	// Apply all transactions in the block
	for _, tx := range b.Txs {
		if err := bc.applyTransaction(tx); err != nil {
			return fmt.Errorf("failed to apply tx %s: %w", tx.Hash().String(), err)
		}
	}
	return nil
}

// applyTransaction applies a single transaction to state
func (bc *Blockchain) applyTransaction(t *tx.Tx) error {
	if t == nil {
		return errors.New("nil transaction")
	}

	// Get current state
	state := bc.State()
	sender := t.From
	senderAddress := common.BytesToQuantumAddress(sender.Bytes())
	senderNonce := state.GetNonce(senderAddress)

	// Validate nonce
	if t.Nonce != senderNonce {
		return fmt.Errorf("invalid nonce for %s: got %d, want %d",
			sender.String(), t.Nonce, senderNonce)
	}

	// Calculate total cost
	gasCost := new(big.Int).Mul(t.GasPrice, big.NewInt(int64(t.Gas)))
	totalCost := new(big.Int).Add(t.Value, gasCost)

	// Check sender balance
	senderBalance := state.GetBalance(senderAddress)
	if senderBalance.Cmp(totalCost) < 0 {
		return fmt.Errorf("insufficient balance for %s: have %s, need %s",
			sender.String(),
			formatWei(senderBalance),
			formatWei(totalCost))
	}

	// Deduct total cost from sender
	newSenderBalance := new(big.Int).Sub(senderBalance, totalCost)
	state.SetBalance(senderAddress, newSenderBalance)

	// Update sender nonce
	state.SetNonce(senderAddress, senderNonce+1)

	// Handle transaction type
	if t.To == nil {
		// Contract creation
		return bc.applyContractCreation(t, gasCost)
	} else {
		// Regular transfer or contract call
		return bc.applyTransferOrCall(t, gasCost)
	}
}

// applyContractCreation handles contract creation
func (bc *Blockchain) applyContractCreation(t *tx.Tx, gasCost *big.Int) error {
	state := bc.State()

	// Get sender's current nonce (after increment)
	senderNonce := state.GetNonce(common.BytesToQuantumAddress(t.From.Bytes())) - 1

	// Create simple contract address
	contractAddrBytes := sha256.Sum256(append(t.From.Bytes(),
		[]byte(fmt.Sprintf("%d", senderNonce))...))
	contractAddr := common.BytesToQuantumAddress(contractAddrBytes[:20])

	// Check if contract already exists
	if state.GetBalance(contractAddr).Sign() > 0 || len(state.GetCode(contractAddr)) > 0 {
		return fmt.Errorf("contract address already exists: %s", contractAddr.String())
	}

	// Set initial balance
	state.SetBalance(contractAddr, t.Value)

	// Store contract code if any
	if len(t.Data) > 0 {
		state.SetCode(contractAddr, t.Data)
	}

	log.Printf("[blockchain] Contract created: %s by %s with value %s",
		contractAddr.String()[:10], t.From.String()[:10], formatWei(t.Value))

	return nil
}

// applyTransferOrCall handles transfers and contract calls
func (bc *Blockchain) applyTransferOrCall(t *tx.Tx, gasCost *big.Int) error {
	state := bc.State()
	to := common.BytesToQuantumAddress(t.To.Bytes())

	// Check if recipient exists
	hasBalance := state.GetBalance(to).Sign() > 0
	hasCode := len(state.GetCode(to)) > 0

	// Initialize account if doesn't exist
	if !hasBalance && !hasCode {
		state.SetBalance(to, big.NewInt(0))
		state.SetNonce(to, 0)
	}

	// Check if it's a contract call
	if len(t.Data) > 0 {
		contractCode := state.GetCode(to)
		if len(contractCode) > 0 {
			// Contract call - transfer value
			currentBalance := state.GetBalance(to)
			newBalance := new(big.Int).Add(currentBalance, t.Value)
			state.SetBalance(to, newBalance)
			log.Printf("[blockchain] Contract call to %s: value %s, data %d bytes",
				to.String()[:10], formatWei(t.Value), len(t.Data))
		} else {
			// Data to non-contract
			currentBalance := state.GetBalance(to)
			newBalance := new(big.Int).Add(currentBalance, t.Value)
			state.SetBalance(to, newBalance)
			log.Printf("[blockchain] Data tx to EOA %s: value %s, data %d bytes",
				to.String()[:10], formatWei(t.Value), len(t.Data))
		}
	} else {
		// Simple transfer
		currentBalance := state.GetBalance(to)
		newBalance := new(big.Int).Add(currentBalance, t.Value)
		state.SetBalance(to, newBalance)
		log.Printf("[blockchain] Transfer: %s → %s: %s ANTD",
			t.From.String()[:10], to.String()[:10], formatWei(t.Value))
	}

	return nil
}
