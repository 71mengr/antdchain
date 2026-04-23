// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package chain
import (
//    "encoding/json"
    "fmt"
  //  "log"
    "os"
    "path/filepath"
    //"strings"
 //    "github.com/ethereum/go-ethereum/common"
    "github.com/antdaza/antdchain/antdc/block"
    "github.com/antdaza/antdchain/antdc/state"
)

// persistBlockAsync persists block to disk asynchronously
/*func (bc *Blockchain) persistBlockAsync(b *block.Block) {
    if err := persistBlockJSON(bc.statePath, b); err != nil {
        log.Printf("[blockchain] Failed to persist block %d: %v", b.Header.Number.Uint64(), err)
    }
}*/

// persistBlockJSON persists a block to disk in JSON format
func persistBlockJSON(path string, b *block.Block) error {
    dir := filepath.Join(path, "blocks")
    if err := os.MkdirAll(dir, os.ModePerm); err != nil {
        return err
    }

    data, err := b.Serialize()
    if err != nil {
        return err
    }

    filename := fmt.Sprintf("block_%d_%s.json", b.Header.Number.Uint64(), b.Hash().Hex()[:8])
    return os.WriteFile(filepath.Join(dir, filename), data, 0644)
}
/*
// loadExistingBlocks loads blocks from disk
func loadExistingBlocks(path string) (map[common.Hash]*block.Block, map[uint64]common.Hash, *block.Block, error) {
    blocks := make(map[common.Hash]*block.Block)
    headers := make(map[uint64]common.Hash)
    dir := filepath.Join(path, "blocks")

    // Check if directory exists
    if _, err := os.Stat(dir); os.IsNotExist(err) {
        log.Printf("[blockchain] Blocks directory does not exist: %s", dir)
        return blocks, headers, nil, nil
    }

    // First, try to load the fixed genesis
    fixedGenesisPath := filepath.Join(dir, "genesis_fixed.json")
    if _, err := os.Stat(fixedGenesisPath); err == nil {
        data, err := os.ReadFile(fixedGenesisPath)
        if err == nil {
            var genesisData map[string]interface{}
            if json.Unmarshal(data, &genesisData) == nil {
                log.Printf("[blockchain] Found fixed genesis file")
            }
        }
    }

    // Read directory
    files, err := os.ReadDir(dir)
    if err != nil {
        log.Printf("[blockchain] Failed to read blocks directory: %v", err)
        return nil, nil, nil, err
    }

    log.Printf("[blockchain] Found %d files in blocks directory", len(files))

    var latest *block.Block
    var maxHeight uint64 = 0
    var loadedCount int = 0

    // Load all JSON files
    for _, f := range files {
        if !strings.HasSuffix(f.Name(), ".json") {
            continue
        }

        filePath := filepath.Join(dir, f.Name())
        data, err := os.ReadFile(filePath)
        if err != nil {
            log.Printf("[blockchain] Warning: Failed to read %s: %v", f.Name(), err)
            continue
        }

        block, err := block.Deserialize(data)
        if err != nil {
            log.Printf("[blockchain] Warning: Failed to deserialize %s: %v", f.Name(), err)
            continue
        }

        if block == nil || block.Header == nil || block.Header.Number == nil {
            log.Printf("[blockchain] Warning: Invalid block in %s", f.Name())
            continue
        }

        hash := block.Hash()
        height := block.Header.Number.Uint64()

        // Store block
        blocks[hash] = block
        headers[height] = hash
        loadedCount++

        // Update latest
        if height > maxHeight {
            maxHeight = height
            latest = block
        }

        log.Printf("[blockchain] Loaded block #%d: %s", height, hash.Hex())
    }

    log.Printf("[blockchain] Successfully loaded %d/%d blocks", loadedCount, len(files))

    if latest != nil {
        log.Printf("[blockchain] Chain tip: #%d (%s)",
            latest.Header.Number.Uint64(), latest.Hash().Hex())
    }

    return blocks, headers, latest, nil
}*/

// rebuildStateFromHeaders rebuilds state from headers
func (bc *Blockchain) rebuildStateFromHeaders() error {
    bc.stateMu.Lock()
    defer bc.stateMu.Unlock()

    if bc.state != nil {
        bc.state.Close()
    }

    s, err := state.NewState(bc.statePath)
    if err != nil {
        return err
    }

    bc.state = s
    return nil
}
