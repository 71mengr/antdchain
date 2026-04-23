// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package chain
import (
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
chaincommon "github.com/antdaza/antdchain/common"

)

// executeBlockTransactions executes all transactions in the block
func (bc *Blockchain) executeBlockTransactions(b *block.Block) (*big.Int, uint64, error) {
bc.stateMu.Lock()
defer bc.stateMu.Unlock()

v := vm.NewVM(bc.state, b.Header.GasLimit)
ctx := context.Background()

totalFees := big.NewInt(0)
totalGasUsed := uint64(0)

// Sort transactions by nonce to ensure deterministic execution order
sortedTxs := bc.sortAndValidateTransactions(b.Txs)
if len(sortedTxs) != len(b.Txs) {
return nil, 0, errors.New("transaction validation failed - some txs rejected")
}

for i, transaction := range sortedTxs {
// Validate transaction before execution
if err := bc.validateTransactionForExecution(transaction, i); err != nil {
return nil, 0, fmt.Errorf("transaction %d invalid: %w", i, err)
}

// Execute transaction
_, gas, execErr := v.Execute(ctx, transaction)
if execErr != nil {
return nil, 0, fmt.Errorf("transaction %d execution error: %w", i, execErr)
}

// Update totals
totalGasUsed += gas
txFee := new(big.Int).Mul(transaction.GasPrice, big.NewInt(int64(gas)))
totalFees.Add(totalFees, txFee)

// Log execution progress for large blocks
if i > 0 && i%100 == 0 {
log.Printf("[blockchain] Executed %d/%d transactions in block %d",
i, len(sortedTxs), b.Header.Number.Uint64())
}
}

return totalFees, totalGasUsed, nil
}

// sortAndValidateTransactions sorts transactions by sender and nonce
func (bc *Blockchain) sortAndValidateTransactions(txs []*tx.Tx) []*tx.Tx {
if len(txs) == 0 {
return txs
}

// Group transactions by sender
txsBySender := make(map[chaincommon.QuantumAddress][]*tx.Tx)
for _, tx := range txs {
if tx != nil {
txsBySender[tx.From] = append(txsBySender[tx.From], tx)
}
}

var validTxs []*tx.Tx

// Process each sender's transactions in nonce order
for sender, senderTxs := range txsBySender {
// Sort by nonce
sort.Slice(senderTxs, func(i, j int) bool {
return senderTxs[i].Nonce < senderTxs[j].Nonce
})

// Get current state for this sender
senderAddress := common.BytesToQuantumAddress(sender.Bytes())
currentNonce := bc.state.GetNonce(senderAddress)
currentBalance := bc.state.GetBalance(senderAddress)

// Validate and add transactions in sequence
for _, tx := range senderTxs {
// Check nonce sequence
if tx.Nonce != currentNonce {
log.Printf("[blockchain] Nonce mismatch for %s: expected %d, got %d",
sender.String()[:8], currentNonce, tx.Nonce)
break // Stop processing this sender's transactions
}

// Check balance
txCost := new(big.Int).Add(
tx.Value,
new(big.Int).Mul(tx.GasPrice, big.NewInt(int64(tx.Gas))),
)

if currentBalance.Cmp(txCost) < 0 {
log.Printf("[blockchain] Insufficient balance for %s: need %s, have %s",
sender.String()[:8], formatBalance(txCost), formatBalance(currentBalance))
break
}

// All checks passed
validTxs = append(validTxs, tx)
currentNonce++
currentBalance.Sub(currentBalance, txCost)
}
}

// Sort by gas price for miner revenue optimization
sort.Slice(validTxs, func(i, j int) bool {
return validTxs[i].GasPrice.Cmp(validTxs[j].GasPrice) > 0
})

return validTxs
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
return fmt.Errorf("failed to apply tx %s: %w", tx.Hash().Hex(), err)
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
return fmt.Errorf("contract address already exists: %s", contractAddr.Hex())
}

// Set initial balance
state.SetBalance(contractAddr, t.Value)

// Store contract code if any
if len(t.Data) > 0 {
state.SetCode(contractAddr, t.Data)
}

log.Printf("[blockchain] Contract created: %s by %s with value %s",
contractAddr.Hex()[:10], t.From.String()[:10], formatWei(t.Value))

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
to.Hex()[:10], formatWei(t.Value), len(t.Data))
} else {
// Data to non-contract
currentBalance := state.GetBalance(to)
newBalance := new(big.Int).Add(currentBalance, t.Value)
state.SetBalance(to, newBalance)
log.Printf("[blockchain] Data tx to EOA %s: value %s, data %d bytes",
to.Hex()[:10], formatWei(t.Value), len(t.Data))
}
} else {
// Simple transfer
currentBalance := state.GetBalance(to)
newBalance := new(big.Int).Add(currentBalance, t.Value)
state.SetBalance(to, newBalance)
log.Printf("[blockchain] Transfer: %s → %s: %s ANTD",
t.From.String()[:10], to.Hex()[:10], formatWei(t.Value))
}

return nil
}
