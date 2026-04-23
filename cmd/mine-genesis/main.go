// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package main
import (
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"os"

	"github.com/antdaza/antdchain/antdc/block"
	"github.com/antdaza/antdchain/antdc/pow"
)

type Genesis struct {
	Header struct {
		Number     uint64      `json:"number"`
		Difficulty string      `json:"difficulty"`
		GasLimit   uint64      `json:"gasLimit"`
		Time       uint64      `json:"time"`
		Nonce      common.Hash `json:"nonce"`
		MixDigest  common.Hash `json:"mixDigest"`
		Extra      []byte      `json:"extra"`
	} `json:"header"`
	Alloc map[common.QuantumAddress]struct {
		Balance string `json:"balance"`
	} `json:"alloc"`
}

func main() {
	// Create genesis block with initial values
	genesisBlock := block.NewGenesisBlock()
	
	// Set genesis difficulty (adjust as needed - lower for faster mining)
	difficulty, _ := new(big.Int).SetString("1000000", 10) // Start with low difficulty
	
	// Initialize PoW
	p, err := pow.NewPoW(difficulty, []byte("antdchain-genesis-seed"))
	if err != nil {
		log.Fatal("Failed to initialize PoW:", err)
	}
	defer p.Release()

	fmt.Printf("Mining genesis block with difficulty %s...\n", difficulty.String())
	
	// Mine the genesis block
	err = p.Mine(genesisBlock, 1000000000) // 1 billion nonce attempts
	if err != nil {
		log.Fatal("Failed to mine genesis block:", err)
	}

	fmt.Printf("Genesis block mined successfully!\n")
	fmt.Printf("Nonce: %s\n", genesisBlock.Header.Nonce.Hex())
	fmt.Printf("MixDigest: %s\n", genesisBlock.Header.MixDigest.Hex())
	fmt.Printf("Block Hash: %s\n", genesisBlock.Hash().Hex())

	// Create genesis JSON
	genesis := Genesis{}
	genesis.Header.Number = 0
	genesis.Header.Difficulty = difficulty.String()
	genesis.Header.GasLimit = genesisBlock.Header.GasLimit
	genesis.Header.Time = genesisBlock.Header.Time
	genesis.Header.Nonce = genesisBlock.Header.Nonce
	genesis.Header.MixDigest = genesisBlock.Header.MixDigest
	genesis.Header.Extra = genesisBlock.Header.Extra
	
	// Add some initial allocations
	genesis.Alloc = map[common.QuantumAddress]struct {
		Balance string `json:"balance"`
	}{
		common.ParseQuantumAddress("0q5E2PeUs72XQrN5FKWwMwPnM2Z5FjTD5jY"): {
			Balance: "1000000000000000000000000", // 1,000,000 ANTD
		},
	}

	// Write genesis.json
	genesisData, err := json.MarshalIndent(genesis, "", "  ")
	if err != nil {
		log.Fatal("Failed to marshal genesis:", err)
	}

	err = os.WriteFile("genesis.json", genesisData, 0644)
	if err != nil {
		log.Fatal("Failed to write genesis.json:", err)
	}

	fmt.Printf("genesis.json created successfully!\n")
}
