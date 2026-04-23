// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package p2p
import (

    "github.com/antdaza/antdchain/antdc/block"
    "github.com/antdaza/antdchain/antdc/checkpoints"
    "github.com/antdaza/antdchain/antdc/pow"
    "github.com/antdaza/antdchain/antdc/state"
    "github.com/antdaza/antdchain/antdc/tx"
    "github.com/antdaza/antdchain/antdc/reward"
    "github.com/antdaza/antdchain/common"
)

// Chain is the interface required by P2P and TxPool
type Chain interface {
    Latest() *block.Block
    State() *state.State
    GetParentHash(height uint64) (common.Hash, error)
    Pow() *pow.PoW
    Checkpoints() *checkpoints.Checkpoints

    AddBlock(*block.Block) error
    GetBlock(height uint64) *block.Block
    GetBlockByHash(hash common.Hash) (*block.Block, error)
    TruncateTo(height uint64) error

    TxPool() TxPool
    ValidateTransaction(*tx.Tx) error
    IsSyncing() bool
    // optional but nice:
    StartSync(target uint64)
    StopSync()
    GetRotatingKingManager() reward.RotatingKingManager

    ShouldSyncDatabase() bool
    MarkDatabaseSynced(height uint64)
    GetDatabaseSyncHeight() uint64
    HasBlock(common.Hash) bool
}

// TxPool interface used by P2P
type TxPool interface {
    AddTx(*tx.Tx, Chain) error
    GetPending() []*tx.Tx
    GetPendingTransactions() []*tx.Tx
    GetConfirmedTxs(Chain, uint64) []*tx.Tx
    CleanupStale(Chain, uint64)
    RemoveTx(common.Hash)
    RemoveTxs([]*tx.Tx)
    GetTransactionCounts() int
    Size() int
    GetPendingTransactionsByNonce(addr common.QuantumAddress) []*tx.Tx
    GetNextNonce(addr common.QuantumAddress, bc Chain) uint64
}
