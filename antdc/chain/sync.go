// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package chain

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/big"
	"sort"
	"time"

	"github.com/antdaza/antdchain/antdc/block"
	"github.com/antdaza/antdchain/antdc/chain/db"
	"github.com/antdaza/antdchain/antdc/checkpoints"
	"github.com/antdaza/antdchain/common"
	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/rlp"
)

// AddBlock adds a new block to the blockchain with proper fork handling
func (bc *Blockchain) AddBlock(b *block.Block) error {
	if b == nil || b.Header == nil {
		return errors.New("nil block or header")
	}

	blockHash := b.Hash()
	blockHeight := b.Header.Number.Uint64()
	parentHash := b.Header.ParentHash

	log.Printf("[blockchain] AddBlock: height=%d hash=%s parent=%s timestamp=%d",
		blockHeight, blockHash.Hex()[:12], parentHash.Hex()[:12], b.Header.Time)

	// ==============================================
	// GENESIS CHECKPOINT VALIDATION ONLY
	// ==============================================
	if bc.checkpointManager != nil && blockHeight == 0 {
		// Only validate genesis block against checkpoint
		err := bc.checkpointManager.ValidateBlock(blockHeight, blockHash)
		if err != nil {
			log.Printf("[blockchain] ❌ GENESIS BLOCK CHECKPOINT FAILURE: %v", err)
			return fmt.Errorf("genesis block checkpoint validation failed: %w", err)
		}
		log.Printf("[blockchain] ✅ Genesis block checkpoint validated")
	}
	// Skip checkpoint validation for non-genesis blocks during normal operation
	// Checkpoints are only for finalized blocks, not every block

	// Prevent concurrent processing of the same block
	bc.blockSubmitMu.Lock()
	defer bc.blockSubmitMu.Unlock()

	// Get current tip
	currentTip := bc.latest.Load()
	currentHeight := uint64(0)
	var currentTipHash common.Hash
	if currentTip != nil {
		currentHeight = currentTip.Header.Number.Uint64()
		currentTipHash = currentTip.Hash()
	}

	log.Printf("[blockchain] Current chain state: height=%d tip=%s",
		currentHeight, currentTipHash.Hex()[:12])

	// Reject duplicate by hash
	if bc.HasBlock(blockHash) {
		log.Printf("[blockchain] Rejecting duplicate block: %s", blockHash.Hex()[:12])
		return fmt.Errorf("duplicate block %s", blockHash.Hex()[:12])
	}

	// Reject if below current height (stale)
	if blockHeight < currentHeight {
		log.Printf("[blockchain] Rejecting stale block: height=%d (current=%d)",
			blockHeight, currentHeight)
		return fmt.Errorf("stale block at height %d (current %d)", blockHeight, currentHeight)
	}

	// ==============================================
	// PARENT VALIDATION
	// ==============================================
	var parentBlock *block.Block

	if blockHeight == 0 {
		// Genesis block has no parent
		parentBlock = nil
	} else if blockHeight == 1 {
		// Block 1 must have genesis as parent
		genesisBlock := bc.GetBlock(0)
		if genesisBlock == nil {
			log.Printf("[blockchain] ERROR: Genesis block not found!")
			return errors.New("genesis block not found")
		}

		genesisHash := genesisBlock.Hash()
		if parentHash != genesisHash {
			log.Printf("[blockchain] ERROR: Block 1 must have genesis as parent")
			log.Printf("[blockchain]   Expected: %s (genesis)", genesisHash.Hex()[:12])
			log.Printf("[blockchain]   Got:      %s", parentHash.Hex()[:12])
			return fmt.Errorf("block 1 must have genesis as parent")
		}

		parentBlock = genesisBlock
		log.Printf("[blockchain] Block 1 parent is genesis: %s", genesisHash.Hex()[:12])
	} else {
		// Normal parent validation for height > 1
		parentBlock = bc.GetBlock(blockHeight - 1)

		if parentBlock != nil {
			// Check if parent hash matches
			if parentBlock.Hash() != parentHash {
				log.Printf("[blockchain] ⚠️ PARENT HASH MISMATCH at height %d", blockHeight-1)
				log.Printf("[blockchain]   Expected: %s", parentHash.Hex()[:12])
				log.Printf("[blockchain]   Got:      %s", parentBlock.Hash().Hex()[:12])

				// Try to find the correct parent by hash
				correctParent, err := bc.db.ReadBlockByHash(parentHash)
				if err == nil && correctParent != nil && correctParent.Hash() == parentHash {
					log.Printf("[blockchain] Found correct parent by hash at height %d",
						correctParent.Header.Number.Uint64())
					parentBlock = correctParent

					// Update cache with correct block
					bc.cacheMu.Lock()
					bc.blockByNumberCache.Add(blockHeight-1, parentBlock)
					bc.blockByHashCache.Add(parentHash, parentBlock)
					bc.cacheMu.Unlock()
				} else {
					if err == nil && correctParent != nil {
						log.Printf("[blockchain] ⚠️ Parent lookup returned non-matching block: requested=%s returned=%s",
							parentHash.Hex()[:12], correctParent.Hash().Hex()[:12])
					}
					// If the incoming block advances beyond our tip, prefer sync over
					// hard fork rejection so we can fetch the missing parent branch.
					if blockHeight > currentHeight {
						if !bc.IsSyncing() {
							log.Printf("[blockchain] Parent branch missing for height %d (current=%d) — triggering sync",
								blockHeight, currentHeight)
							go bc.triggerSyncFromBlock(b)
						} else {
							log.Printf("[blockchain] Parent branch missing for height %d while already syncing (current=%d)",
								blockHeight, currentHeight)
						}
						return fmt.Errorf("parent branch missing at height %d; syncing", blockHeight-1)
					}

					// This is a fork - different block at same height
					log.Printf("[blockchain] ❌ FORK DETECTED at height %d", blockHeight-1)
					return fmt.Errorf("fork detected: different block at height %d", blockHeight-1)
				}
			}
		} else {
			log.Printf("[blockchain] Parent not found at height %d", blockHeight-1)

			// Try to get parent by hash
			parentBlock, err := bc.db.ReadBlockByHash(parentHash)
			if err != nil || parentBlock == nil {
				if blockHeight > currentHeight {
					if !bc.IsSyncing() {
						log.Printf("[blockchain] Missing parent %s for height %d — triggering sync",
							parentHash.Hex()[:12], blockHeight)
						go bc.triggerSyncFromBlock(b)
					} else {
						log.Printf("[blockchain] Missing parent %s for height %d while already syncing",
							parentHash.Hex()[:12], blockHeight)
					}
				}
				return fmt.Errorf("parent block at height %d not found", blockHeight-1)
			}
			if parentBlock.Hash() != parentHash {
				return fmt.Errorf("parent hash lookup mismatch: requested %s, got %s",
					parentHash.Hex(), parentBlock.Hash().Hex())
			}

			// Verify parent height
			if parentBlock.Header.Number.Uint64() != blockHeight-1 {
				log.Printf("[blockchain] ⚠️ Parent has unexpected height: %d (expected %d)",
					parentBlock.Header.Number.Uint64(), blockHeight-1)
				// Still use it - might be from a reorg
			}

			// Update cache
			bc.cacheMu.Lock()
			bc.blockByNumberCache.Add(blockHeight-1, parentBlock)
			bc.blockByHashCache.Add(parentHash, parentBlock)
			bc.cacheMu.Unlock()
		}
	}

	if parentBlock != nil && blockHeight > 0 {
		log.Printf("[blockchain] Parent validated: height=%d hash=%s",
			parentBlock.Header.Number.Uint64(), parentBlock.Hash().Hex()[:12])
	}

	// Fast fork pre-check for same-height competitors.
	// Avoid mutating state for losing fork candidates.
	if blockHeight == currentHeight && currentTip != nil && currentTipHash != blockHash {
		log.Printf("[blockchain] Fork detected at height %d: current=%s, new=%s",
			blockHeight, currentTipHash.Hex()[:12], blockHash.Hex()[:12])

		if !isBetterBlockCandidate(b, currentTip) {
			log.Printf("[blockchain] Fork rejected: current block is better or equal by fork-choice")
			return fmt.Errorf("fork block rejected by fork-choice rule")
		}

		log.Printf("[blockchain] Reorg candidate accepted by fork-choice rule")
		return bc.reorganizeAtHeight(blockHeight, b)
	}

	// ==============================================
	// BLOCK VALIDATION
	// ==============================================
	if err := bc.validateAndExecuteBlock(b, parentBlock); err != nil {
		log.Printf("[blockchain] Block validation failed: %v", err)
		return fmt.Errorf("block validation failed: %w", err)
	}

	isSyncing := bc.IsSyncing()

	// ==============================================
	// CHAIN EXTENSION LOGIC
	// ==============================================

	// Direct extension — fast path
	if blockHeight == currentHeight+1 && parentHash == currentTipHash {
		log.Printf("[blockchain] Direct chain extension: %d -> %d", currentHeight, blockHeight)
		return bc.handleDirectExtension(b, isSyncing)
	}

	// During sync — accept if parent exists (gap filling)
	if isSyncing {
		if parentBlock == nil && blockHeight > 0 {
			log.Printf("[blockchain] Orphan block during sync: parent missing")
			return fmt.Errorf("orphan block during sync: parent height %d missing", blockHeight-1)
		}

		if blockHeight == currentHeight+1 {
			log.Printf("[blockchain] Filling sync gap: %d -> %d", currentHeight, blockHeight)
			return bc.handleDirectExtension(b, true)
		}

		log.Printf("[blockchain] Sync block ahead: current=%d, new=%d", currentHeight, blockHeight)
		return bc.handleSyncModeBlock(b, blockHeight, parentBlock) // Remove unused blockHash parameter
	}

	// Same height fork
	if blockHeight == currentHeight {
		if currentTipHash == blockHash {
			log.Printf("[blockchain] Duplicate block at height %d", blockHeight)
			return nil
		}
		return fmt.Errorf("same-height fork should have been handled earlier")
	}

	// Block is ahead — trigger sync
	if blockHeight > currentHeight+1 {
		log.Printf("[blockchain] Block %d is ahead (current %d) — triggering sync",
			blockHeight, currentHeight)
		go bc.triggerSyncFromBlock(b)
		return fmt.Errorf("block ahead — syncing (current=%d, new=%d)", currentHeight, blockHeight)
	}

	log.Printf("[blockchain] Unexpected block state: height=%d, current=%d, tip=%s",
		blockHeight, currentHeight, currentTipHash.Hex()[:12])
	return fmt.Errorf("unexpected block state")
}

// handleDirectExtension handles direct chain extension
func (bc *Blockchain) handleDirectExtension(b *block.Block, duringSync bool) error {
	bc.stateMu.Lock()
	defer bc.stateMu.Unlock()

	blockHeight := b.Header.Number.Uint64()
	blockHash := b.Hash()

	// Add to database FIRST (this is critical)
	if err := bc.db.WriteBlock(b); err != nil {
		return fmt.Errorf("failed to write block to database: %w", err)
	}

	// Mark this height as canonical for direct extension.
	if err := bc.db.WriteCanonicalHash(blockHeight, blockHash); err != nil {
		return fmt.Errorf("failed to write canonical hash: %w", err)
	}

	// Update canonical tip in database
	if err := bc.db.WriteHeadBlockHash(blockHash); err != nil {
		log.Printf("[blockchain] Warning: failed to update head block hash: %v", err)
		// Continue anyway - we can recover from this
	}

	// Update latest block pointer
	bc.latest.Store(b)
	bc.lastCanonicalHeight.Store(blockHeight)

	// Commit state after block acceptance
	root, err := bc.state.Commit(b.Header.Number.Uint64())
	if err != nil {
		return fmt.Errorf("failed to commit state for block %d: %w", blockHeight, err)
	}
	if root != b.Header.Root {
		return fmt.Errorf("committed state root mismatch: %s != %s", root.Hex(), b.Header.Root.Hex())
	}
	// Refresh the state reference to the new root
	if err := bc.state.Reset(root); err != nil {
		return fmt.Errorf("failed to reset state to new root: %w", err)
	}
	log.Printf("[blockchain] State committed for block %d (root: %s)", blockHeight, root.Hex()[:12])

	// Update cache
	bc.cacheMu.Lock()
	bc.blockByNumberCache.Add(blockHeight, b)
	bc.blockByHashCache.Add(blockHash, b)
	bc.cacheMu.Unlock()

	// ==============================================
	// CREATE CHECKPOINT ONLY FOR FINALIZED BLOCKS
	// ==============================================
	if bc.checkpointManager != nil && !duringSync {
		// Only create checkpoints for finalized blocks (e.g., every 1000 blocks)
		if blockHeight >= 1000 && blockHeight%1000 == 0 {
			log.Printf("[blockchain] Creating checkpoint at finalized height %d", blockHeight)
			go func() {
				if err := bc.createCheckpointFromBlock(b); err != nil {
					log.Printf("[blockchain] Failed to create checkpoint at height %d: %v", blockHeight, err)
				}
			}()
		}
	}

	if bc.rotatingKingManager != nil {
		go bc.syncRotatingKingForBlock(blockHeight)
	}

	// Check if sync completed
	if duringSync {
		bc.maybeCompleteSync(blockHeight)
	}

	// Note: persistBlockAsync is no longer needed since db.WriteBlock already persists
	// But keep it if it does additional processing
	go bc.persistBlockAsync(b)

	// Cleanup mined txs
	if len(b.Txs) > 0 && bc.txPool != nil {
		bc.txPool.CleanupMinedTransactions(b.Txs)
	}

	log.Printf("[blockchain] Accepted block → height=%d hash=%s txs=%d",
		blockHeight, blockHash.Hex()[:12], len(b.Txs))

	go bc.notifyNewBlock(b)

	return nil
}

// handleSyncModeBlock handles blocks during sync mode
func (bc *Blockchain) handleSyncModeBlock(b *block.Block, blockHeight uint64, parentBlock *block.Block) error {
	blockHash := b.Hash() // Declare blockHash here

	// NOTE: AddBlock already calls validateAndExecuteBlock before dispatching here.
	// Re-running execution would mutate state twice and cause nonce/root mismatches.

	// Add to database first
	if err := bc.db.WriteBlock(b); err != nil {
		return fmt.Errorf("failed to write sync block to database: %w", err)
	}

	// Keep canonical mapping aligned with accepted sync chain progression.
	if err := bc.db.WriteCanonicalHash(blockHeight, blockHash); err != nil {
		return fmt.Errorf("failed to write canonical hash during sync: %w", err)
	}

	// Update canonical tip if this advances the chain
	currentTip := bc.latest.Load()
	currentHeight := uint64(0)
	if currentTip != nil {
		currentHeight = currentTip.Header.Number.Uint64()
	}

	if blockHeight > currentHeight {
		if err := bc.db.WriteHeadBlockHash(blockHash); err != nil {
			log.Printf("[blockchain] Warning: failed to update head block hash during sync: %v", err)
		}
	}

	// Add to chain (write lock)
	bc.stateMu.Lock()

	// Update latest block pointer
	bc.latest.Store(b)
	bc.lastCanonicalHeight.Store(blockHeight)

	// Commit state after block acceptance
	root, err := bc.state.Commit(b.Header.Number.Uint64())
	if err != nil {
		bc.stateMu.Unlock()
		return fmt.Errorf("failed to commit state for block %d: %w", blockHeight, err)
	}
	if root != b.Header.Root {
		bc.stateMu.Unlock()
		return fmt.Errorf("committed state root mismatch: %s != %s", root.Hex(), b.Header.Root.Hex())
	}
	// Refresh the state reference to the new root
	if err := bc.state.Reset(root); err != nil {
		bc.stateMu.Unlock()
		return fmt.Errorf("failed to reset state to new root: %w", err)
	}
	log.Printf("[blockchain] State committed for block %d (root: %s)", blockHeight, root.Hex()[:12])

	// Update cache
	bc.cacheMu.Lock()
	bc.blockByNumberCache.Add(blockHeight, b)
	bc.blockByHashCache.Add(blockHash, b)
	bc.cacheMu.Unlock()

	if bc.rotatingKingManager != nil {
		go bc.syncRotatingKingForBlock(blockHeight)
	}

	// Check if sync completed
	bc.maybeCompleteSync(blockHeight)
	bc.stateMu.Unlock()

	// Note: persistBlockAsync is now redundant but kept for compatibility
	go bc.persistBlockAsync(b)

	// Update rotating king database
	if bc.rotatingKingManager != nil {
		go bc.updateRotatingKingForBlock(b, blockHeight)
	}

	// Clean up mined transactions from pool
	if bc.txPool != nil && len(b.Txs) > 0 {
		bc.txPool.CleanupMinedTransactions(b.Txs)
		log.Printf("[blockchain] Cleaned up %d mined transactions from pool during sync", len(b.Txs))
	}

	// Log success
	log.Printf("[blockchain] Added sync block → height=%d hash=%s",
		blockHeight, blockHash.Hex())

	go bc.notifyNewBlock(b)

	return nil
}

// reorganizeAtHeight performs chain reorganization
func (bc *Blockchain) reorganizeAtHeight(height uint64, newBlock *block.Block) error {
	bc.stateMu.Lock()
	defer bc.stateMu.Unlock()

	// Get old block from cache or database
	oldBlock := bc.GetBlock(height)
	if oldBlock == nil {
		return fmt.Errorf("no block to replace at height %d", height)
	}

	oldHash := oldBlock.Hash()
	newHash := newBlock.Hash()

	log.Printf("[blockchain] Reorg: replacing block %s with %s at height %d",
		oldHash.Hex()[:12], newHash.Hex()[:12], height)

	parent := bc.GetBlock(height - 1)
	if parent == nil {
		return fmt.Errorf("missing parent block at height %d for reorg", height-1)
	}

	// Rebuild state from parent before validating/executing replacement block.
	if err := bc.revertToHeight(height - 1); err != nil {
		return fmt.Errorf("failed to revert state for reorg at height %d: %w", height, err)
	}

	if err := bc.validateAndExecuteBlock(newBlock, parent); err != nil {
		return fmt.Errorf("replacement block validation failed at height %d: %w", height, err)
	}

	// Write new block to database
	if err := bc.db.WriteBlock(newBlock); err != nil {
		return fmt.Errorf("failed to write new block to database during reorg: %w", err)
	}

	// Update canonical hash in database
	if err := bc.db.WriteCanonicalHash(height, newHash); err != nil {
		log.Printf("[blockchain] Warning: failed to update canonical hash during reorg: %v", err)
	}

	// Update head if this was the tip
	currentTip := bc.latest.Load()
	if currentTip != nil && currentTip.Hash() == oldHash {
		if err := bc.db.WriteHeadBlockHash(newHash); err != nil {
			log.Printf("[blockchain] Warning: failed to update head block hash during reorg: %v", err)
		}
	}

	// Update cache
	bc.cacheMu.Lock()
	// Remove old block from cache
	bc.blockByNumberCache.Remove(height)
	bc.blockByHashCache.Remove(oldHash)
	// Add new block to cache
	bc.blockByNumberCache.Add(height, newBlock)
	bc.blockByHashCache.Add(newHash, newBlock)
	bc.cacheMu.Unlock()

	// Update tip if this was the tip
	if currentTip != nil && currentTip.Hash() == oldHash {
		bc.latest.Store(newBlock)
	}

	// Commit new canonical state for the replacement block.
	root, err := bc.state.Commit(newBlock.Header.Number.Uint64())
	if err != nil {
		return fmt.Errorf("failed to commit replacement block state at height %d: %w", height, err)
	}
	if root != newBlock.Header.Root {
		return fmt.Errorf("replacement block state root mismatch at height %d: %s != %s", height, root.Hex(), newBlock.Header.Root.Hex())
	}
	if err := bc.state.Reset(root); err != nil {
		return fmt.Errorf("failed to reset state after replacement block at height %d: %w", height, err)
	}

	// Note: persistBlockAsync is no longer needed for persistence but may do other things
	go bc.persistBlockAsync(newBlock)

	// Cleanup txs from old and new blocks in pool
	if bc.txPool != nil {
		bc.txPool.CleanupMinedTransactions(oldBlock.Txs)
		bc.txPool.CleanupMinedTransactions(newBlock.Txs)
	}

	log.Printf("[blockchain] Reorg successful at height %d", height)
	return nil
}

// triggerSyncFromBlock triggers sync from a block
func (bc *Blockchain) triggerSyncFromBlock(b *block.Block) {
	height := b.Header.Number.Uint64()
	if height <= bc.GetChainHeight() {
		return
	}
	bc.StartSync(height)
}

// maybeCompleteSync checks if sync is complete
func (bc *Blockchain) maybeCompleteSync(height uint64) {
	if !bc.syncing.Load() {
		return
	}

	target := bc.syncTarget.Load()
	if height >= target {
		if bc.syncing.Swap(false) {
			log.Printf("[blockchain] Sync completed! Reached height %d (target was %d)", height, target)
		}
	}
}

// ProcessSyncBatch processes multiple blocks during sync
func (bc *Blockchain) ProcessSyncBatch(blocks []*block.Block) error {
	if len(blocks) == 0 {
		return nil
	}

	// Sort blocks by height
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Header.Number.Uint64() < blocks[j].Header.Number.Uint64()
	})

	// Validate blocks in order
	for i, blk := range blocks {
		blockHeight := blk.Header.Number.Uint64()

		// Skip checkpoint validation during sync
		// We just want to get the chain data

		// Validate parent exists
		if blockHeight > 0 {
			var parentBlock *block.Block
			if i == 0 {
				// First block in batch
				parentBlock = bc.GetBlock(blockHeight - 1)
			} else {
				// Previous block in batch
				parentBlock = blocks[i-1]
			}

			if parentBlock == nil {
				return fmt.Errorf("missing parent for block %d", blockHeight)
			}

			// Quick validation only during sync
			if blk.Header.ParentHash != parentBlock.Hash() {
				return fmt.Errorf("parent hash mismatch for block %d", blockHeight)
			}
		}
	}

	// Use batch for database writes during sync
	batch := bc.db.DB().NewBatch()
	defer batch.Close()

	for _, block := range blocks {
		blockHeight := block.Header.Number.Uint64()
		blockHash := block.Hash()

		// Write block to batch using RLP encoding
		blockData, err := rlp.EncodeToBytes(block)
		if err != nil {
			return fmt.Errorf("failed to encode block %d: %w", blockHeight, err)
		}

		batch.Set(db.BlockByHashKey(blockHash), blockData, pebble.Sync)

		// Write header using RLP encoding
		headerData, err := rlp.EncodeToBytes(block.Header)
		if err != nil {
			return fmt.Errorf("failed to encode header for block %d: %w", blockHeight, err)
		}
		batch.Set(db.HeaderByNumberKey(blockHeight), headerData, pebble.Sync)
		batch.Set(db.HeaderByHashKey(blockHash), headerData, pebble.Sync)

		// Update canonical pointer
		batch.Set(db.CanonicalHashKey(blockHeight), blockHash[:], pebble.Sync)
	}

	// Commit batch to database
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("failed to commit sync batch: %w", err)
	}

	// Update head block hash after batch commit
	if len(blocks) > 0 {
		latestBlock := blocks[len(blocks)-1]
		if err := bc.db.WriteHeadBlockHash(latestBlock.Hash()); err != nil {
			log.Printf("[blockchain] Warning: failed to update head block hash after batch sync: %v", err)
		}
	}

	// Update cache and latest pointer
	bc.stateMu.Lock()
	for _, block := range blocks {
		blockHeight := block.Header.Number.Uint64()
		blockHash := block.Hash()

		bc.cacheMu.Lock()
		bc.blockByNumberCache.Add(blockHeight, block)
		bc.blockByHashCache.Add(blockHash, block)
		bc.cacheMu.Unlock()
	}

	// Update latest block pointer
	latestBlock := blocks[len(blocks)-1]
	bc.latest.Store(latestBlock)
	bc.lastCanonicalHeight.Store(latestBlock.Header.Number.Uint64())
	bc.stateMu.Unlock()

	log.Printf("[blockchain] Processed sync batch of %d blocks", len(blocks))
	return nil
}

// StartSyncWithDatabase starts sync with database integration
func (bc *Blockchain) StartSyncWithDatabase(targetHeight uint64) error {
	log.Printf("[blockchain] Starting sync to height %d with database integration", targetHeight)

	bc.syncing.Store(true)
	bc.syncTarget.Store(targetHeight)

	// Start rotating king database sync in parallel
	if bc.rotatingKingManager != nil {
		go bc.syncRotatingKingDatabase(targetHeight)
	}

	log.Printf("[blockchain] Sync mode ENABLED → target height = %d", targetHeight)
	return nil
}

// notifyNewBlock notifies miners of new block
func (bc *Blockchain) notifyNewBlock(b *block.Block) {
	log.Printf("[blockchain] New block notification: height=%d hash=%s",
		b.Header.Number.Uint64(), b.Hash().Hex()[:10])
}

// syncRotatingKingForBlock syncs rotating king database for a block
func (bc *Blockchain) syncRotatingKingForBlock(blockHeight uint64) {
	if bc.rotatingKingManager != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := bc.rotatingKingManager.SyncBlocks(ctx, blockHeight); err != nil {
			log.Printf("[blockchain] Failed to sync rotating king for block %d: %v", blockHeight, err)
		}
	}
}

// syncRotatingKingDatabase syncs rotating king database
func (bc *Blockchain) syncRotatingKingDatabase(targetHeight uint64) {
	if bc.rotatingKingManager != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := bc.rotatingKingManager.SyncBlocks(ctx, targetHeight); err != nil {
			log.Printf("[blockchain] Failed to sync rotating king database: %v", err)
		}
	}
}

// updateRotatingKingForBlock updates rotating king for a block
func (bc *Blockchain) updateRotatingKingForBlock(b *block.Block, blockHeight uint64) {
	if bc.rotatingKingManager != nil {
		// Check if rotation should happen
		if bc.rotatingKingManager.ShouldRotate(blockHeight) {
			if err := bc.rotatingKingManager.RotateToNextKing(blockHeight, b.Hash()); err != nil {
				log.Printf("[blockchain] Failed to rotate king at block %d: %v", blockHeight, err)
			}
		}
	}
}

// persistBlockAsync - kept for compatibility
func (bc *Blockchain) persistBlockAsync(b *block.Block) {
	// This is now a no-op since persistence is handled by db.WriteBlock
	// But kept for compatibility with code that might call it
	log.Printf("[blockchain] Block %d persisted to database", b.Header.Number.Uint64())
}

// ==============================================
// CHECKPOINT HELPER METHODS
// ==============================================

// shouldCreateCheckpoint determines if a checkpoint should be created at this height
func (bc *Blockchain) shouldCreateCheckpoint(height uint64) bool {
	// Only create checkpoints for:
	// 1. Genesis block (height 0) - REQUIRED
	// 2. Every 1000 blocks (adjustable)
	// 3. Special heights (like 1 for initial validation, but be careful)

	if height == 0 {
		return true // Genesis always needs checkpoint
	}

	// Don't create checkpoint at height 1 - it's too early
	if height == 1 {
		return false // Disable for now
	}

	// Create checkpoints every 1000 blocks
	if height%1000 == 0 {
		return true
	}

	// Major checkpoints every 10000 blocks
	if height%10000 == 0 {
		return true
	}

	return false
}

// createCheckpointFromBlock creates a checkpoint from a block
func (bc *Blockchain) createCheckpointFromBlock(b *block.Block) error {
	if bc.checkpointManager == nil {
		return errors.New("checkpoint manager not initialized")
	}

	blockHeight := b.Header.Number.Uint64()

	// Only create checkpoints for finalized blocks
	// Blocks need multiple confirmations before being checkpointed
	if blockHeight < 1000 && blockHeight != 0 {
		log.Printf("[blockchain] Skipping checkpoint for non-finalized block %d", blockHeight)
		return nil
	}

	blockHash := b.Hash()

	// Get rotating king address
	var rotatingKing common.QuantumAddress
	if bc.rotatingKingManager != nil {
		rotatingKing = bc.rotatingKingManager.GetCurrentKing()
	}

	// Get parent hash
	parentHash := b.Header.ParentHash
	if blockHeight == 0 {
		parentHash = common.Hash{} // Genesis has no parent
	}

	// Get miner/coinbase
	miner := b.Header.Coinbase

	// Get transaction count
	txCount := len(b.Txs)

	// Estimate gas used
	gasUsed := uint64(0)
	if len(b.Txs) > 0 {
		// TODO: 21000 per transaction
		gasUsed = uint64(len(b.Txs) * 21000)
	}

	// Add the checkpoint
	err := bc.checkpointManager.AddCheckpoint(
		blockHeight,
		blockHash,
		parentHash,
		miner,
		rotatingKing,
		txCount,
		gasUsed,
	)

	if err != nil {
		return fmt.Errorf("failed to add checkpoint at height %d: %w", blockHeight, err)
	}

	log.Printf("[blockchain] Checkpoint created at finalized height %d (hash: %s)",
		blockHeight, blockHash.Hex()[:12])

	return nil
}

// ValidateBlockAgainstCheckpoints validates a block against any existing checkpoints
func (bc *Blockchain) ValidateBlockAgainstCheckpoints(height uint64, hash common.Hash) error {
	if bc.checkpointManager == nil {
		return nil // No checkpoint manager, skip validation
	}

	// Only validate against existing checkpoints
	// Don't reject blocks that don't have checkpoints
	if _, exists := bc.checkpointManager.GetCheckpoint(height); exists {
		return bc.checkpointManager.ValidateBlock(height, hash)
	}

	return nil
}

// GetCheckpointManager returns the checkpoint manager
func (bc *Blockchain) GetCheckpointManager() *checkpoints.Checkpoints {
	return bc.checkpointManager
}

// SetCheckpointManager sets the checkpoint manager
func (bc *Blockchain) SetCheckpointManager(cp *checkpoints.Checkpoints) {
	bc.checkpointManager = cp
	log.Printf("[blockchain] Checkpoint manager configured")
}

// GetBlock implementation
func (bc *Blockchain) GetBlock(height uint64) *block.Block {
	// Check cache first
	bc.cacheMu.RLock()
	if cached, found := bc.blockByNumberCache.Get(height); found {
		cachedBlock := cached.(*block.Block)
		bc.cacheMu.RUnlock()

		if cachedBlock.Header.Number.Uint64() == height {
			return cachedBlock
		}
		// Cache corruption - clear it
		bc.cacheMu.Lock()
		bc.blockByNumberCache.Remove(height)
		bc.cacheMu.Unlock()
	} else {
		bc.cacheMu.RUnlock()
	}

	// Get from database
	// 1. Get canonical hash for this height
	canonicalHash, err := bc.db.GetCanonicalHash(height)
	if err != nil || canonicalHash == (common.Hash{}) {
		if err != nil {
			log.Printf("[blockchain] No canonical hash for height %d: %v", height, err)
		} else {
			log.Printf("[blockchain] No canonical hash for height %d", height)
		}
		return nil
	}

	// 2. Get block by hash
	block, err := bc.db.ReadBlockByHash(canonicalHash)
	if err != nil {
		log.Printf("[blockchain] Failed to read block %s at height %d: %v",
			canonicalHash.Hex()[:12], height, err)
		return nil
	}

	// Verify height
	if block.Header.Number.Uint64() != height {
		log.Printf("[blockchain] ❌ Database corruption: block at hash %s has height %d, expected %d",
			canonicalHash.Hex()[:12], block.Header.Number.Uint64(), height)
		return nil
	}

	// Update cache
	bc.cacheMu.Lock()
	bc.blockByNumberCache.Add(height, block)
	bc.blockByHashCache.Add(canonicalHash, block)
	bc.cacheMu.Unlock()

	log.Printf("[blockchain] GetBlock from DB: height=%d hash=%s", height, canonicalHash.Hex()[:12])

	return block
}

// GetBlockByHash implementation
func (bc *Blockchain) GetBlockByHash(hash common.Hash) (*block.Block, error) {
	if hash == (common.Hash{}) {
		return nil, errors.New("empty hash")
	}

	// Check cache first
	bc.cacheMu.RLock()
	if cached, found := bc.blockByHashCache.Get(hash); found {
		bc.cacheMu.RUnlock()
		if block, ok := cached.(*block.Block); ok {
			return block, nil
		}
	} else {
		bc.cacheMu.RUnlock()
	}

	// Get from database
	block, err := bc.db.ReadBlockByHash(hash)
	if err != nil {
		return nil, fmt.Errorf("block not found by hash %s: %w", hash.Hex()[:12], err)
	}

	// Update cache
	bc.cacheMu.Lock()
	bc.blockByHashCache.Add(hash, block)
	bc.blockByNumberCache.Add(block.Header.Number.Uint64(), block)
	bc.cacheMu.Unlock()

	return block, nil
}

type orphanBlock struct {
	block      *block.Block
	receivedAt time.Time
}

func (bc *Blockchain) storeOrphanBlock(b *block.Block) {
	bc.orphanMu.Lock()
	defer bc.orphanMu.Unlock()

	if bc.orphanBlocks == nil {
		bc.orphanBlocks = make(map[common.Hash]*orphanBlock)
	}

	blockHash := b.Hash()
	bc.orphanBlocks[blockHash] = &orphanBlock{
		block:      b,
		receivedAt: time.Now(),
	}

	log.Printf("[blockchain] Stored orphan block: height=%d hash=%s",
		b.Header.Number.Uint64(), blockHash.Hex()[:12])

	// Cleanup old orphans if needed
	if len(bc.orphanBlocks) > 100 {
		bc.cleanupOldOrphans()
	}
}

func (bc *Blockchain) cleanupOldOrphans() {
	maxAge := 30 * time.Minute
	now := time.Now()

	for hash, orphan := range bc.orphanBlocks {
		if now.Sub(orphan.receivedAt) > maxAge {
			delete(bc.orphanBlocks, hash)
		}
	}
}

func isBetterBlockCandidate(newBlock, currentBlock *block.Block) bool {
	if newBlock == nil || newBlock.Header == nil || currentBlock == nil || currentBlock.Header == nil {
		return false
	}

	newDifficulty := new(big.Int)
	if newBlock.Header.Difficulty != nil {
		newDifficulty.Set(newBlock.Header.Difficulty)
	}

	currentDifficulty := new(big.Int)
	if currentBlock.Header.Difficulty != nil {
		currentDifficulty.Set(currentBlock.Header.Difficulty)
	}

	if newDifficulty.Cmp(currentDifficulty) > 0 {
		return true
	}
	if newDifficulty.Cmp(currentDifficulty) < 0 {
		return false
	}

	// Deterministic tie-breaker: lower block hash wins.
	return newBlock.Hash().Hex() < currentBlock.Hash().Hex()
}
