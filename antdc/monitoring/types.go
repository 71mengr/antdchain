// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package monitoring
import (
    "encoding/json"
    "math/big"
    "time"

    "github.com/antdaza/antdchain/common"
)

// MonitorConfig holds monitoring configuration
type MonitorConfig struct {
    MainKingAddress   common.QuantumAddress
    AlertThreshold    *big.Int // Alert if any tx exceeds this amount
    MaxSupply         *big.Int // Maximum allowed total supply
    CheckInterval     time.Duration
    EnableRealTime    bool
    LogFile           string
    WebhookURL        string // For external notifications
}

// TransactionAlert represents a monitoring alert
type TransactionAlert struct {
    Type        string          `json:"type"`
    Severity    string          `json:"severity"` // "low", "medium", "high", "critical"
    Message     string          `json:"message"`
    TxHash      common.Hash     `json:"tx_hash"`
    Amount      *big.Int        `json:"amount"`
    From        common.QuantumAddress  `json:"from"`
    To          common.QuantumAddress  `json:"to"`
    BlockNumber uint64          `json:"block_number"`
    Timestamp   time.Time       `json:"timestamp"`
    Data        json.RawMessage `json:"data,omitempty"`
}

// SupplyStats represents supply statistics
type SupplyStats struct {
    TotalSupply     string                      `json:"total_supply"`
    UniqueAddresses int                         `json:"unique_addresses"`
    TopHolders      []Holder                    `json:"top_holders"`
    Distribution    map[common.QuantumAddress]string   `json:"distribution"`
    AlertCounts     map[string]int              `json:"alert_counts"`
    LastUpdated     time.Time                   `json:"last_updated"`
}

// Holder represents an address with balance
type Holder struct {
    Address string `json:"address"`
    Balance string `json:"balance"`
    Percent string `json:"percent"`
}

// Defines the interface that monitoring needs from blockchain
type BlockchainProvider interface {
    GetChainHeight() uint64
    GetBlockByNumber(number uint64) (BlockProvider, error)
    State() StateProvider
    GetStateForMonitoring() StateProvider
}

// Defines the interface for block access
type BlockProvider interface {
    GetTransactions() []TransactionProvider
    GetHeader() HeaderProvider
}

// Defines the interface for transaction access
type TransactionProvider interface {
    GetHash() common.Hash
    GetFrom() common.QuantumAddress
    GetTo() *common.QuantumAddress
    GetValue() *big.Int
    GetNonce() uint64
    GetGas() uint64
    GetGasPrice() *big.Int
    GetData() []byte
}

// Defines the interface for header access
type HeaderProvider interface {
    GetTime() uint64
    GetNumber() *big.Int
}

// Defines the interface for state access
type StateProvider interface {
    GetBalance(address common.QuantumAddress) *big.Int
    GetNonce(address common.QuantumAddress) uint64
}
