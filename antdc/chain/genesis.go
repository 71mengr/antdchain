// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package chain
import (
"encoding/json"
"fmt"
"log"
"math/big"
"os"
"path/filepath"

"github.com/antdaza/antdchain/antdc/block"
"github.com/antdaza/antdchain/antdc/pow"
"github.com/antdaza/antdchain/antdc/state"
"github.com/antdaza/antdchain/common"

)

// Genesis parameters
const (
GenesisTimestamp          = 1763731821 // Fixed genesis time (November 2025)
GenesisDifficulty         = 1000000    // Fixed initial difficulty
GenesisGasLimit           = 60_000_000 // High enough for initial blocks
GenesisExtraData          = "ANTDChain Genesis — November 2025"
GenesisMainKing           = "0q5E2PeUs72XQrN5FKWwMwPnM2Z5FjTD5jY"
GenesisMainKingBalanceStr = "60000000000000000000000000"
)

func EnsureGenesisBlock(statePath string, miner common.QuantumAddress) (*block.Block, error) {
// Try to load existing genesis block
genesis, err := loadGenesisBlock(statePath)
if err == nil && genesis != nil {
log.Printf("[genesis] Using existing genesis block: %s", genesis.Hash().String())
return genesis, nil
}

// No valid genesis found → create fixed one
log.Printf("[genesis] No valid genesis found — creating fixed genesis block")

parsedMainKing, err := common.ParseQuantumAddress(GenesisMainKing)
if err != nil {
return nil, fmt.Errorf("invalid genesis main king quantum address: %w", err)
}
mainKing := common.QuantumAddress(parsedMainKing)

// Initialize state
s, err := state.NewState(statePath)
if err != nil {
return nil, fmt.Errorf("failed to create state for genesis: %w", err)
}
defer s.Close()

// Set main king balance
mainKingBalance := new(big.Int)
mainKingBalance.SetString(GenesisMainKingBalanceStr, 10)
s.AddBalance(mainKing, mainKingBalance)

// Create genesis header using the existing NewHeader function
header, err := block.NewHeader(
nil, // no parent for genesis
mainKing,
s.Root(),
common.Hash{}, // no txs
big.NewInt(0), // block 0
GenesisGasLimit,
pow.NewPoW(), // pass PoS engine for consistency
)
if err != nil {
return nil, fmt.Errorf("failed to create genesis header: %w", err)
}

header.Time = GenesisTimestamp
header.Difficulty = big.NewInt(GenesisDifficulty)
header.Extra = []byte(GenesisExtraData)
header.ParentHash = common.Hash{} // explicit zero
// Ensure other fields are properly set
header.UncleHash = common.Hash{}
header.ReceiptHash = common.Hash{}
header.MixDigest = common.Hash{}
header.Nonce = block.BlockNonce{}
header.Bloom = make([]byte, 256) // Empty bloom filter

// Create genesis block (no txs, no uncles)
genesis, err = block.NewBlock(header, nil, nil)
if err != nil {
return nil, fmt.Errorf("failed to create genesis block: %w", err)
}

// Persist it
if err := persistGenesisBlock(statePath, genesis); err != nil {
return nil, fmt.Errorf("failed to persist genesis block: %w", err)
}

balanceFloat := new(big.Float).SetInt(mainKingBalance)
balanceAntd := new(big.Float).Quo(balanceFloat, big.NewFloat(1e18))

// Verify hash consistency
genesisHash := genesis.Hash()
log.Printf("[genesis] Created FIXED genesis block:")
log.Printf("   Hash:      %s", genesisHash.String())
log.Printf("   Miner:     %s", mainKing.String())
log.Printf("   Balance:   %.0f ANTD", balanceAntd)
log.Printf("   Timestamp: %d", header.Time)
log.Printf("   StateRoot: %s", s.Root().String())
log.Printf("   Difficulty: %s", header.Difficulty.String())

return genesis, nil
}

// loadGenesisBlock tries to load an existing genesis block from disk
func loadGenesisBlock(statePath string) (*block.Block, error) {
dir := filepath.Join(statePath, "blocks")
genesisFiles := []string{
filepath.Join(dir, "genesis.json"),
filepath.Join(dir, "genesis_fixed.json"),
}

for _, file := range genesisFiles {
data, err := os.ReadFile(file)
if err != nil {
continue
}

var genesisData struct {
Block *block.Block `json:"block"`
}
if err := json.Unmarshal(data, &genesisData); err != nil {
continue
}

if genesisData.Block != nil && genesisData.Block.Header != nil &&
genesisData.Block.Header.Number.Uint64() == 0 {
return genesisData.Block, nil
}
}

return nil, os.ErrNotExist
}

// saves the genesis block to disk
func persistGenesisBlock(statePath string, genesis *block.Block) error {
dir := filepath.Join(statePath, "blocks")
if err := os.MkdirAll(dir, os.ModePerm); err != nil {
return err
}

// Save as clean JSON
data := map[string]interface{}{
"block": genesis,
"hash":  genesis.Hash().String(),
"info": map[string]interface{}{
"miner":      genesis.Header.Coinbase.String(),
"timestamp":  genesis.Header.Time,
"difficulty": genesis.Header.Difficulty.String(),
"extra":      string(genesis.Header.Extra),
"stateRoot":  genesis.Header.Root.String(),
},
}

jsonData, err := json.MarshalIndent(data, "", "  ")
if err != nil {
return err
}

genesisPath := filepath.Join(dir, "genesis_fixed.json")
if err := os.WriteFile(genesisPath, jsonData, 0644); err != nil {
return err
}

log.Printf("[genesis] Persisted genesis block to: %s", genesisPath)
return nil
}
