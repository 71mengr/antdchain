// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package chain

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/antdaza/antdchain/antdc/block"
	"github.com/antdaza/antdchain/common"
	"github.com/sirupsen/logrus"
)

// ============================================================================
// BITCOIN-STYLE CHAIN REORGANIZATION
// ============================================================================
// Features:
// - Maximum reorg depth (Bitcoin: 6 blocks)
// - Fork detection and resolution
// - Active chain vs candidate chain comparison
// - Orphan block handling
// - Best chain selection based on work (cumulative difficulty)
// ============================================================================

const (
	// Bitcoin-style reorg constants
	ManagerMaxReorgDepth    = 6    // Maximum blocks to reorganize (Bitcoin: 6)
	MaxForkBlocks           = 1000 // Maximum blocks to consider in fork
	MinWorkForReorg         = 3    // Minimum work difference to trigger reorg (blocks)
	
	// Orphan block handling
	MaxOrphanBlocks         = 100
	OrphanBlockExpiry       = 2 * time.Hour
	OrphanCheckInterval     = 10 * time.Minute
	
	// Fork detection
	ForkCheckInterval       = 5 * time.Second
	MaxForkDepth            = 100
)

var (
	ErrReorgInProgress      = errors.New("reorganization already in progress")
	ErrReorgDepthExceeded   = errors.New("reorg depth exceeds maximum")
	ErrForkTooDeep          = errors.New("fork is too deep to reorganize")
	ErrInvalidReorgTarget   = errors.New("invalid reorg target")
	ErrOrphanBlockLimit     = errors.New("orphan block limit reached")
)

// ChainFork represents a fork in the blockchain
type ChainFork struct {
	ForkHeight    uint64           `json:"forkHeight"`    // Height where chains diverge
	CommonAncestor common.Hash     `json:"commonAncestor"` // Hash of common ancestor
	ActiveChain   []*block.Block   `json:"-"`              // Active chain blocks (newest first)
	CandidateChain []*block.Block  `json:"-"`              // Candidate chain blocks (newest first)
	ActiveWork    *big.Int         `json:"activeWork"`    // Cumulative work of active chain
	CandidateWork *big.Int         `json:"candidateWork"` // Cumulative work of candidate chain
	DetectedAt    time.Time        `json:"detectedAt"`
}

// OrphanBlock represents a block with missing parent
type OrphanBlock struct {
	Block      *block.Block
	ReceivedAt time.Time
	ExpiresAt  time.Time
	FromPeer   string
}

// ReorgManager handles blockchain reorganizations (Bitcoin Core style)
type ReorgManager struct {
	chain       FullBlockChain
	orphanPool  map[common.Hash]*OrphanBlock
	activeForks map[uint64]*ChainFork
	mu          sync.RWMutex
	
	logger      *logrus.Logger
	
	// Statistics
	reorgCount       uint64
	lastReorgHeight  uint64
	lastReorgTime    time.Time
	maxReorgDepthObserved int
	
	// Context
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// FullBlockChain interface for chain operations needed for reorg
type FullBlockChain interface {
	// Block retrieval
	GetBlock(height uint64) (*block.Block, error)
	GetBlockByHash(hash common.Hash) (*block.Block, error)
	GetBlockHash(height uint64) (common.Hash, error)
	HasBlock(hash common.Hash) bool
	HasBlockAtHeight(height uint64) bool
	
	// Chain state
	CurrentHeight() uint64
	CurrentTip() common.Hash
	GetBlockParent(block *block.Block) (*block.Block, error)
	
	// Work calculation
	GetCumulativeWork(height uint64) (*big.Int, error)
	CalculateBlockWork(difficulty *big.Int) *big.Int
	
	// Chain operations
	AddBlock(block *block.Block) error
	ConnectBlock(block *block.Block) error
	DisconnectBlock(height uint64) error
	
	// Validation
	ValidateBlock(block *block.Block) error
	ValidateBlockContext(block *block.Block, parent *block.Block) error
}

// NewReorgManager creates a new Bitcoin-style reorganization manager
func NewReorgManager(chain FullBlockChain, logger *logrus.Logger) *ReorgManager {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &ReorgManager{
		chain:       chain,
		orphanPool:  make(map[common.Hash]*OrphanBlock),
		activeForks: make(map[uint64]*ChainFork),
		logger:      logger,
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Start starts the reorg manager background tasks
func (rm *ReorgManager) Start() {
	rm.logger.Info("Starting reorganization manager")
	
	// Start orphan block cleanup
	rm.wg.Add(1)
	go rm.orphanCleanupLoop()
	
	// Start fork detection
	rm.wg.Add(1)
	go rm.forkDetectionLoop()
}

// Stop stops the reorg manager
func (rm *ReorgManager) Stop() {
	rm.logger.Info("Stopping reorganization manager...")
	rm.cancel()
	rm.wg.Wait()
	rm.logger.Info("Reorganization manager stopped")
}

// ProcessNewBlock processes a new block (Bitcoin Core algorithm)
func (rm *ReorgManager) ProcessNewBlock(blk *block.Block, fromPeer string) error {
	if blk == nil || blk.Header == nil {
		return errors.New("nil block")
	}
	
	height := blk.Header.Number.Uint64()
	hash := blk.Hash()
	
	rm.logger.Debugf("Processing new block %d (%s) from %s", height, hash.String()[:12], fromPeer)
	
	// Check if we already have this block
	if rm.chain.HasBlock(hash) {
		rm.logger.Debugf("Block %d already known", height)
		return nil
	}
	
	// Check if block connects to current tip
	currentTip := rm.chain.CurrentTip()
	if blk.Header.ParentHash == currentTip {
		// Fast path: block extends current tip
		return rm.extendChain(blk)
	}
	
	// Check if block connects to an orphan we have
	if _, exists := rm.orphanPool[blk.Header.ParentHash]; exists {
		rm.logger.Debugf("Block connects to orphan parent %s", blk.Header.ParentHash.String()[:12])
		rm.addOrphanBlock(blk, fromPeer)
		return rm.tryResolveOrphans(blk.Header.ParentHash)
	}
	
	// Check if parent exists in main chain
	parent, err := rm.chain.GetBlockByHash(blk.Header.ParentHash)
	if err == nil && parent != nil {
		// Parent exists but not tip - potential fork
		return rm.handlePotentialFork(blk, parent)
	}
	
	// Block is orphan (parent unknown)
	rm.addOrphanBlock(blk, fromPeer)
	return nil
}

// extendChain extends the current chain (fast path)
func (rm *ReorgManager) extendChain(blk *block.Block) error {
	rm.logger.Debugf("Extending chain with block %d", blk.Header.Number.Uint64())
	
	// Validate the block
	if err := rm.chain.ValidateBlock(blk); err != nil {
		return fmt.Errorf("block validation failed: %w", err)
	}
	
	// Add to chain
	if err := rm.chain.AddBlock(blk); err != nil {
		return fmt.Errorf("failed to add block: %w", err)
	}
	
	rm.logger.Infof("Chain extended to height %d", blk.Header.Number.Uint64())
	return nil
}

// handlePotentialFork handles a potential chain fork (Bitcoin Core algorithm)
func (rm *ReorgManager) handlePotentialFork(blk *block.Block, parent *block.Block) error {
	height := blk.Header.Number.Uint64()
	
	rm.logger.Warnf("Potential fork detected at height %d", height)
	
	// Check if this block is on a different branch
	currentTipHash := rm.chain.CurrentTip()
	currentTipBlock, _ := rm.chain.GetBlockByHash(currentTipHash)
	
	if currentTipBlock != nil && parent.Hash() == currentTipHash {
		// This is actually extending the tip (parent was cached but tip changed)
		return rm.extendChain(blk)
	}
	
	// Find the fork point
	forkHeight, err := rm.findForkPoint(parent, currentTipBlock)
	if err != nil {
		// Couldn't find common ancestor, add as orphan
		rm.addOrphanBlock(blk, "fork-detection")
		return nil
	}
	
	// Check if reorg is needed
	fork := &ChainFork{
		ForkHeight:     forkHeight,
		DetectedAt:     time.Now(),
	}
	
	// Build candidate chain
	candidateChain, err := rm.buildCandidateChain(blk, forkHeight)
	if err != nil {
		rm.logger.Warnf("Failed to build candidate chain: %v", err)
		return nil
	}
	
	// Build active chain
	activeChain, err := rm.buildActiveChain(currentTipBlock, forkHeight)
	if err != nil {
		rm.logger.Warnf("Failed to build active chain: %v", err)
		return nil
	}
	
	fork.ActiveChain = activeChain
	fork.CandidateChain = candidateChain
	
	// Calculate cumulative work
	fork.ActiveWork = rm.calculateChainWork(activeChain)
	fork.CandidateWork = rm.calculateChainWork(candidateChain)
	
	// Compare work (Bitcoin: most work chain wins)
	workDiff := new(big.Int).Sub(fork.CandidateWork, fork.ActiveWork)
	minWork := big.NewInt(int64(MinWorkForReorg))
	
	if workDiff.Cmp(minWork) > 0 {
		rm.logger.Warnf("⛓️ REORGANIZATION NEEDED: candidate work %s > active work %s (diff=%s blocks)",
			fork.CandidateWork.String(), fork.ActiveWork.String(),
			new(big.Int).Div(workDiff, big.NewInt(1)))
		
		return rm.performReorg(fork)
	}
	
	rm.logger.Infof("Candidate chain has less work (diff=%s), ignoring", workDiff.String())
	
	// Store fork info for potential future reorg
	rm.mu.Lock()
	rm.activeForks[forkHeight] = fork
	rm.mu.Unlock()
	
	return nil
}

// findForkPoint finds the common ancestor of two chains
func (rm *ReorgManager) findForkPoint(parent, tip *block.Block) (uint64, error) {
	// Start from parent and walk back until we find a common block
	currentParent := parent
	
	for depth := 0; depth < MaxForkDepth; depth++ {
		if currentParent == nil {
			break
		}
		
		// Check if this block is in active chain
		if rm.chain.HasBlock(currentParent.Hash()) {
			// Verify this block is actually on the active chain at its height
			activeHash, err := rm.chain.GetBlockHash(currentParent.Header.Number.Uint64())
			if err == nil && activeHash == currentParent.Hash() {
				return currentParent.Header.Number.Uint64(), nil
			}
		}
		
		// Move to parent
		nextParent, err := rm.chain.GetBlockByHash(currentParent.Header.ParentHash)
		if err != nil {
			break
		}
		currentParent = nextParent
	}
	
	return 0, errors.New("no common ancestor found")
}

// buildCandidateChain builds the chain from fork point to candidate tip
func (rm *ReorgManager) buildCandidateChain(tip *block.Block, forkHeight uint64) ([]*block.Block, error) {
	chain := make([]*block.Block, 0)
	current := tip
	
	for current.Header.Number.Uint64() > forkHeight {
		chain = append([]*block.Block{current}, chain...)
		
		parent, err := rm.chain.GetBlockByHash(current.Header.ParentHash)
		if err != nil {
			return nil, err
		}
		current = parent
	}
	
	return chain, nil
}

// buildActiveChain builds the chain from fork point to active tip
func (rm *ReorgManager) buildActiveChain(tip *block.Block, forkHeight uint64) ([]*block.Block, error) {
	chain := make([]*block.Block, 0)
	current := tip
	
	for current.Header.Number.Uint64() > forkHeight {
		chain = append([]*block.Block{current}, chain...)
		
		parent, err := rm.chain.GetBlockByHash(current.Header.ParentHash)
		if err != nil {
			return nil, err
		}
		current = parent
	}
	
	return chain, nil
}

// calculateChainWork calculates total cumulative work for a chain
func (rm *ReorgManager) calculateChainWork(chain []*block.Block) *big.Int {
	totalWork := big.NewInt(0)
	
	for _, blk := range chain {
		blockWork := rm.chain.CalculateBlockWork(blk.Header.Difficulty)
		totalWork.Add(totalWork, blockWork)
	}
	
	return totalWork
}

// performReorg performs a chain reorganization (Bitcoin Core algorithm)
func (rm *ReorgManager) performReorg(fork *ChainFork) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	
	rm.logger.Warnf("⛓️ STARTING REORGANIZATION: height %d, disconnecting %d blocks, connecting %d blocks",
		fork.ForkHeight, len(fork.ActiveChain), len(fork.CandidateChain))
	
	// Validate reorg depth
	if len(fork.ActiveChain) > ManagerMaxReorgDepth {
		rm.logger.Warnf("Reorg depth %d exceeds maximum %d", len(fork.ActiveChain), ManagerMaxReorgDepth)
		return ErrReorgDepthExceeded
	}
	
	// Step 1: Disconnect blocks from active chain (newest to oldest)
	for i := len(fork.ActiveChain) - 1; i >= 0; i-- {
		blk := fork.ActiveChain[i]
		height := blk.Header.Number.Uint64()
		
		rm.logger.Debugf("Disconnecting block %d (%s)", height, blk.Hash().String()[:12])
		
		if err := rm.chain.DisconnectBlock(height); err != nil {
			rm.logger.Errorf("Failed to disconnect block %d: %v", height, err)
			// Attempt to reconnect and abort reorg
			rm.emergencyRollback(fork)
			return fmt.Errorf("disconnect failed at height %d: %w", height, err)
		}
	}
	
	// Step 2: Connect candidate chain blocks (oldest to newest)
	for i, blk := range fork.CandidateChain {
		height := blk.Header.Number.Uint64()
		
		rm.logger.Debugf("Connecting candidate block %d (%s)", height, blk.Hash().String()[:12])
		
		// Validate context before connecting
		if err := rm.chain.ValidateBlockContext(blk, nil); err != nil {
			rm.logger.Errorf("Candidate block %d failed context validation: %v", height, err)
			// Attempt to reconnect original chain
			rm.emergencyRollback(fork)
			return fmt.Errorf("candidate validation failed at height %d: %w", height, err)
		}
		
		if err := rm.chain.ConnectBlock(blk); err != nil {
			rm.logger.Errorf("Failed to connect candidate block %d: %v", height, err)
			// Attempt to reconnect original chain from this point
			rm.emergencyRollbackFrom(fork, i)
			return fmt.Errorf("connect failed at height %d: %w", height, err)
		}
	}
	
	// Update statistics
	rm.reorgCount++
	rm.lastReorgHeight = fork.CandidateChain[len(fork.CandidateChain)-1].Header.Number.Uint64()
	rm.lastReorgTime = time.Now()
	if len(fork.ActiveChain) > rm.maxReorgDepthObserved {
		rm.maxReorgDepthObserved = len(fork.ActiveChain)
	}
	
	rm.logger.Warnf("✅ REORGANIZATION COMPLETE: new tip at height %d, reorg depth=%d, total reorgs=%d",
		rm.lastReorgHeight, len(fork.ActiveChain), rm.reorgCount)
	
	// Clean up fork tracking
	delete(rm.activeForks, fork.ForkHeight)
	
	return nil
}

// emergencyRollback rolls back a failed reorg attempt
func (rm *ReorgManager) emergencyRollback(fork *ChainFork) {
	rm.logger.Error("�� EMERGENCY ROLLBACK: Attempting to restore original chain")
	
	// Reconnect original blocks
	for i, blk := range fork.ActiveChain {
		if err := rm.chain.ConnectBlock(blk); err != nil {
			rm.logger.Errorf("CRITICAL: Failed to restore block %d: %v", 
				blk.Header.Number.Uint64(), err)
		} else {
			rm.logger.Infof("Restored original block %d", blk.Header.Number.Uint64())
		}
		
		// Stop if we've restored enough
		if i >= len(fork.CandidateChain) {
			break
		}
	}
}

// emergencyRollbackFrom rolls back from a specific point
func (rm *ReorgManager) emergencyRollbackFrom(fork *ChainFork, candidateIndex int) {
	rm.logger.Errorf("�� EMERGENCY ROLLBACK from candidate index %d", candidateIndex)
	
	// Disconnect any candidate blocks that were connected
	for i := candidateIndex; i >= 0; i-- {
		blk := fork.CandidateChain[i]
		if err := rm.chain.DisconnectBlock(blk.Header.Number.Uint64()); err != nil {
			rm.logger.Errorf("Failed to disconnect candidate block %d during rollback: %v",
				blk.Header.Number.Uint64(), err)
		}
	}
	
	// Reconnect original blocks up to the failure point
	for i := len(fork.ActiveChain) - 1; i >= candidateIndex && i >= 0; i-- {
		blk := fork.ActiveChain[i]
		if err := rm.chain.ConnectBlock(blk); err != nil {
			rm.logger.Errorf("CRITICAL: Failed to restore original block %d: %v",
				blk.Header.Number.Uint64(), err)
		}
	}
}

// addOrphanBlock adds a block to the orphan pool
func (rm *ReorgManager) addOrphanBlock(blk *block.Block, fromPeer string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	
	// Limit orphan pool size
	if len(rm.orphanPool) >= MaxOrphanBlocks {
		// Remove oldest orphan
		var oldestHash common.Hash
		var oldestTime time.Time
		for hash, orphan := range rm.orphanPool {
			if oldestTime.IsZero() || orphan.ReceivedAt.Before(oldestTime) {
				oldestTime = orphan.ReceivedAt
				oldestHash = hash
			}
		}
		delete(rm.orphanPool, oldestHash)
		rm.logger.Debugf("Removed oldest orphan %s", oldestHash.String()[:12])
	}
	
	rm.orphanPool[blk.Hash()] = &OrphanBlock{
		Block:      blk,
		ReceivedAt: time.Now(),
		ExpiresAt:  time.Now().Add(OrphanBlockExpiry),
		FromPeer:   fromPeer,
	}
	
	rm.logger.Debugf("Added orphan block %d (%s) from %s (total orphans: %d)",
		blk.Header.Number.Uint64(), blk.Hash().String()[:12], fromPeer, len(rm.orphanPool))
}

// tryResolveOrphans attempts to resolve orphan blocks
func (rm *ReorgManager) tryResolveOrphans(parentHash common.Hash) error {
	rm.mu.Lock()
	orphansToProcess := make([]*OrphanBlock, 0)
	
	for hash, orphan := range rm.orphanPool {
		if orphan.Block.Header.ParentHash == parentHash {
			orphansToProcess = append(orphansToProcess, orphan)
			delete(rm.orphanPool, hash)
		}
	}
	rm.mu.Unlock()
	
	for _, orphan := range orphansToProcess {
		rm.logger.Debugf("Processing resolved orphan block %d", orphan.Block.Header.Number.Uint64())
		
		if err := rm.ProcessNewBlock(orphan.Block, orphan.FromPeer); err != nil {
			rm.logger.Warnf("Failed to process resolved orphan: %v", err)
		}
	}
	
	return nil
}

// orphanCleanupLoop removes expired orphan blocks
func (rm *ReorgManager) orphanCleanupLoop() {
	defer rm.wg.Done()
	
	ticker := time.NewTicker(OrphanCheckInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-rm.ctx.Done():
			return
		case <-ticker.C:
			rm.cleanupExpiredOrphans()
		}
	}
}

func (rm *ReorgManager) cleanupExpiredOrphans() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	
	now := time.Now()
	expired := make([]common.Hash, 0)
	
	for hash, orphan := range rm.orphanPool {
		if orphan.ExpiresAt.Before(now) {
			expired = append(expired, hash)
		}
	}
	
	for _, hash := range expired {
		delete(rm.orphanPool, hash)
	}
	
	if len(expired) > 0 {
		rm.logger.Debugf("Cleaned up %d expired orphan blocks", len(expired))
	}
}

// forkDetectionLoop periodically checks for and resolves forks
func (rm *ReorgManager) forkDetectionLoop() {
	defer rm.wg.Done()
	
	ticker := time.NewTicker(ForkCheckInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-rm.ctx.Done():
			return
		case <-ticker.C:
			rm.detectAndResolveForks()
		}
	}
}

func (rm *ReorgManager) detectAndResolveForks() {
	rm.mu.RLock()
	forks := make([]*ChainFork, 0, len(rm.activeForks))
	for _, fork := range rm.activeForks {
		forks = append(forks, fork)
	}
	rm.mu.RUnlock()
	
	for _, fork := range forks {
		// Re-check work difference
		if fork.CandidateWork.Cmp(fork.ActiveWork) > 0 {
			rm.logger.Warnf("Detected fork with more work at height %d, triggering reorg", fork.ForkHeight)
			if err := rm.performReorg(fork); err != nil {
				rm.logger.Errorf("Failed to resolve fork: %v", err)
			}
		}
	}
}

// GetReorgStatus returns current reorg statistics
func (rm *ReorgManager) GetReorgStatus() map[string]interface{} {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	
	status := map[string]interface{}{
		"reorg_count":              rm.reorgCount,
		"last_reorg_height":        rm.lastReorgHeight,
		"last_reorg_time":          rm.lastReorgTime,
		"max_reorg_depth_observed": rm.maxReorgDepthObserved,
		"orphan_pool_size":         len(rm.orphanPool),
		"active_forks":             len(rm.activeForks),
		"max_reorg_depth":          MaxReorgDepth,
	}
	
	return status
}

// GetOrphanPool returns a copy of the orphan pool
func (rm *ReorgManager) GetOrphanPool() map[string]interface{} {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	
	orphans := make(map[string]interface{})
	for hash, orphan := range rm.orphanPool {
		orphans[hash.String()[:12]] = map[string]interface{}{
			"height":      orphan.Block.Header.Number.Uint64(),
			"received_at": orphan.ReceivedAt,
			"expires_at":  orphan.ExpiresAt,
			"from_peer":   orphan.FromPeer,
		}
	}
	
	return orphans
}
