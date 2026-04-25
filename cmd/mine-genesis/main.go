// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/antdaza/antdchain/antdc/chain"
	"github.com/antdaza/antdchain/common"
)

func main() {
	statePath := filepath.Join(".", "state")
	if err := os.MkdirAll(statePath, 0o755); err != nil {
		log.Fatalf("failed to create state directory: %v", err)
	}

	miner, err := common.ParseQuantumAddress(chain.GenesisMainKing)
	if err != nil {
		log.Fatalf("failed to parse genesis miner address: %v", err)
	}
	genesis, err := chain.EnsureGenesisBlock(statePath, miner)
	if err != nil {
		log.Fatalf("failed to ensure genesis block: %v", err)
	}

	fmt.Printf("Genesis block ready at %s\n", filepath.Join(statePath, "blocks", "genesis_fixed.json"))
	fmt.Printf("Genesis hash: %s\n", genesis.Hash().Hex())
}
