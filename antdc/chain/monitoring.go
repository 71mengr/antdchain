// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package chain

import (
"fmt"
"github.com/antdaza/antdchain/antdc/block"
"github.com/antdaza/antdchain/antdc/monitoring"
"github.com/antdaza/antdchain/antdc/state"
"github.com/antdaza/antdchain/antdc/tx"
"github.com/ethereum/go-ethereum/common"
"math/big"
)

// BlockWrapper wraps block for monitoring
type BlockWrapper struct{ block *block.Block }

func (w *BlockWrapper) GetHeader() monitoring.HeaderProvider {
return &HeaderWrapper{w.block.Header}
}

func (w *BlockWrapper) GetTransactions() []monitoring.TransactionProvider {
out := make([]monitoring.TransactionProvider, len(w.block.Txs))
for i := range w.block.Txs {
out[i] = &TransactionWrapper{w.block.Txs[i]}
}
return out
}

// TransactionWrapper wraps transaction for monitoring
type TransactionWrapper struct{ tx *tx.Tx }

func (w *TransactionWrapper) GetHash() common.Hash { return w.tx.Hash() }
func (w *TransactionWrapper) GetFrom() common.Address {
return common.BytesToAddress(w.tx.From.Bytes())
}
func (w *TransactionWrapper) GetTo() *common.Address {
if w.tx.To == nil {
return nil
}
to := common.BytesToAddress(w.tx.To.Bytes())
return &to
}
func (w *TransactionWrapper) GetValue() *big.Int    { return w.tx.Value }
func (w *TransactionWrapper) GetNonce() uint64      { return w.tx.Nonce }
func (w *TransactionWrapper) GetGas() uint64        { return w.tx.Gas }
func (w *TransactionWrapper) GetGasPrice() *big.Int { return w.tx.GasPrice }
func (w *TransactionWrapper) GetData() []byte       { return w.tx.Data }

// HeaderWrapper wraps header for monitoring
type HeaderWrapper struct{ header *block.Header }

func (w *HeaderWrapper) GetTime() uint64     { return w.header.Time }
func (w *HeaderWrapper) GetNumber() *big.Int { return w.header.Number }

// StateWrapper wraps state for monitoring
type StateWrapper struct{ state *state.State }

func (w *StateWrapper) GetBalance(addr common.Address) *big.Int {
if w.state == nil {
return big.NewInt(0)
}
return w.state.GetBalance(addr)
}

func (w *StateWrapper) GetNonce(addr common.Address) uint64 {
if w.state == nil {
return 0
}
return w.state.GetNonce(addr)
}

// blockchainAdapter adapts Blockchain for monitoring
type blockchainAdapter struct {
*Blockchain
}

func (b *blockchainAdapter) GetStateForMonitoring() monitoring.StateProvider {
return &StateWrapper{b.Blockchain.State()}
}

func (b *blockchainAdapter) State() monitoring.StateProvider {
return &StateWrapper{b.Blockchain.State()}
}

func (b *blockchainAdapter) GetChainHeight() uint64 {
return b.Blockchain.GetChainHeight()
}

func (b *blockchainAdapter) GetBlockByNumber(number uint64) (monitoring.BlockProvider, error) {
block := b.Blockchain.GetBlock(number)
if block == nil {
return nil, fmt.Errorf("block not found at height %d", number)
}
return &BlockWrapper{block: block}, nil
}
