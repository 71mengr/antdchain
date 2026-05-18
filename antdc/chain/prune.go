// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package chain

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/antdaza/antdchain/antdc/block"
	"github.com/antdaza/antdchain/common"
	"github.com/sirupsen/logrus"
)

// ============================================================================
// BLOCKCHAIN PRUNING
// ============================================================================
// Features:
// - Prune old blocks while keeping UTXO set
// - Configurable prune depth (default: 288 blocks = ~12 hours at 2.5 min blocks)
// - Automatic prune on startup and periodically
// - Prune undo data for reorganization
// - Block file management (blk*.dat, rev*.dat)
// ============================================================================

const (
	// Pruning constants (defaults adapted for 2.5 min blocks)
	DefaultPruneDepth        = 288   // Keep last 288 blocks (~12 hours)
	MinPruneDepth            = 100   // Minimum blocks to keep
	MaxPruneDepth            = 10000 // Maximum blocks to keep
	
	// Block file sizes (128 MiB)
	MaxBlockFileSize         = 128 * 1024 * 1024 // 128 MiB
	PruneCheckInterval       = 10 * time.Minute
	StartupPruneDelay        = 30 * time.Second
	
	// Block file naming
	BlockFilePrefix          = "blk"
	RevFilePrefix            = "rev"
	BlockFileExtension       = ".dat"
	IndexFileExtension       = ".idx"
	
	// Pruning flags
	PruneModeManual          = "manual"
	PruneModeAuto            = "auto"
	PruneModeDisabled        = "disabled"
)

var (
	ErrPruneInProgress      = errors.New("pruning already in progress")
	ErrPruneDepthTooLow     = errors.New("prune depth too low")
	ErrPruneDepthTooHigh    = errors.New("prune depth too high")
	ErrBlockFileNotFound    = errors.New("block file not found")
	ErrPruneTargetInvalid   = errors.New("invalid prune target height")
)

// BlockFileInfo stores metadata about a block file (blk*.dat)
type BlockFileInfo struct {
	FileNumber    int       `json:"fileNumber"`    // File sequence number
	FilePath      string    `json:"filePath"`      // Full path to file
	FileSize      int64     `json:"fileSize"`      // Size in bytes
	FirstBlock    uint64    `json:"firstBlock"`    // First block height in file
	LastBlock     uint64    `json:"lastBlock"`     // Last block height in file
	BlockCount    int       `json:"blockCount"`    // Number of blocks in file
	CreatedAt     time.Time `json:"createdAt"`
	IsPrunable    bool      `json:"isPrunable"`    // Can this file be pruned?
}

// PruneRecord tracks what has been pruned
type PruneRecord struct {
	PrunedHeight    uint64    `json:"prunedHeight"`    // Highest height pruned
	PrunedTimestamp time.Time `json:"prunedTimestamp"`
	PrunedFiles     []int     `json:"prunedFiles"`     // File numbers removed
	BytesFreed      int64     `json:"bytesFreed"`      // Total bytes freed
}

// PruneManager handles blockchain pruning
type PruneManager struct {
	chain        BlockProvider
	db           PruneDatabase
	logger       *logrus.Logger
	mu           sync.RWMutex
	
	// Configuration
	pruneDepth   uint64
	pruneMode    string
	dataDir      string
	enabled      bool
	
	// State
	isPruning    bool
	lastPruneTime time.Time
	pruneRecords []PruneRecord
	blockFiles   map[int]*BlockFileInfo
	blockFileMu  sync.RWMutex
	
	// Statistics
	totalBytesPruned int64
	prunedBlockCount uint64
	
	// Context
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// PruneDatabase interface for pruning-related DB operations
type PruneDatabase interface {
	// MarkBlockPruned marks a block as pruned
	MarkBlockPruned(height uint64, hash common.Hash) error
	
	// IsBlockPruned returns true if block data is pruned
	IsBlockPruned(height uint64) bool
	
	// GetPrunedHeight returns the highest pruned height
	GetPrunedHeight() (uint64, error)
	
	// SetPrunedHeight stores the highest pruned height
	SetPrunedHeight(height uint64) error
	
	// DeleteBlockData removes block data from database
	DeleteBlockData(height uint64) error
	
	// GetBlockFileInfo retrieves block file metadata
	GetBlockFileInfo(fileNumber int) (*BlockFileInfo, error)
	
	// SaveBlockFileInfo stores block file metadata
	SaveBlockFileInfo(info *BlockFileInfo) error
	
	// DeleteBlockFileInfo removes block file metadata
	DeleteBlockFileInfo(fileNumber int) error
	
	// GetPruneRecords retrieves historical prune records
	GetPruneRecords(limit int) ([]PruneRecord, error)
	
	// SavePruneRecord stores a prune record
	SavePruneRecord(record *PruneRecord) error
}

// BlockProvider provides block access for pruning
type BlockProvider interface {
	GetBlock(height uint64) (*block.Block, error)
	GetBlockByHash(hash common.Hash) (*block.Block, error)
	GetBlockHeight(hash common.Hash) (uint64, error)
	CurrentHeight() uint64
	GetBlockHash(height uint64) (common.Hash, error)
	HasBlock(height uint64) bool
}

// NewPruneManager creates a new prune manager
func NewPruneManager(
	chain BlockProvider,
	db PruneDatabase,
	dataDir string,
	pruneDepth uint64,
	logger *logrus.Logger,
) (*PruneManager, error) {
	
	if pruneDepth < MinPruneDepth {
		return nil, ErrPruneDepthTooLow
	}
	if pruneDepth > MaxPruneDepth {
		return nil, ErrPruneDepthTooHigh
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	pm := &PruneManager{
		chain:          chain,
		db:             db,
		logger:         logger,
		pruneDepth:     pruneDepth,
		pruneMode:      PruneModeAuto,
		dataDir:        dataDir,
		enabled:        true,
		blockFiles:     make(map[int]*BlockFileInfo),
		pruneRecords:   make([]PruneRecord, 0),
		ctx:            ctx,
		cancel:         cancel,
	}
	
	// Load existing block files
	if err := pm.loadBlockFiles(); err != nil {
		pm.logger.Warnf("Failed to load block files: %v", err)
	}
	
	// Load prune records
	if err := pm.loadPruneRecords(); err != nil {
		pm.logger.Warnf("Failed to load prune records: %v", err)
	}
	
	return pm, nil
}

// Start starts the prune manager background tasks
func (pm *PruneManager) Start() {
	if !pm.enabled {
		pm.logger.Info("Pruning is disabled")
		return
	}
	
	pm.logger.Infof("Starting prune manager: depth=%d blocks, mode=%s", 
		pm.pruneDepth, pm.pruneMode)
	
	// Initial prune after delay
	pm.wg.Add(1)
	go func() {
		defer pm.wg.Done()
		time.Sleep(StartupPruneDelay)
		if err := pm.Prune(); err != nil {
			pm.logger.Warnf("Initial prune failed: %v", err)
		}
	}()
	
	// Periodic prune checks
	pm.wg.Add(1)
	go pm.periodicPruneLoop()
}

// Stop stops the prune manager
func (pm *PruneManager) Stop() {
	pm.logger.Info("Stopping prune manager...")
	pm.cancel()
	pm.wg.Wait()
	pm.logger.Info("Prune manager stopped")
}

// periodicPruneLoop runs prune checks periodically
func (pm *PruneManager) periodicPruneLoop() {
	defer pm.wg.Done()
	
	ticker := time.NewTicker(PruneCheckInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-pm.ctx.Done():
			return
		case <-ticker.C:
			if err := pm.Prune(); err != nil {
				pm.logger.Debugf("Periodic prune: %v", err)
			}
		}
	}
}

// Prune performs the actual pruning (algorithm)
func (pm *PruneManager) Prune() error {
	pm.mu.Lock()
	if pm.isPruning {
		pm.mu.Unlock()
		return ErrPruneInProgress
	}
	pm.isPruning = true
	pm.mu.Unlock()
	
	defer func() {
		pm.mu.Lock()
		pm.isPruning = false
		pm.lastPruneTime = time.Now()
		pm.mu.Unlock()
	}()
	
	currentHeight := pm.chain.CurrentHeight()
	if currentHeight <= pm.pruneDepth {
		pm.logger.Debugf("Chain too short for pruning: height=%d, depth=%d", 
			currentHeight, pm.pruneDepth)
		return nil
	}
	
	// Calculate prune target (keep last N blocks)
	pruneTarget := currentHeight - pm.pruneDepth
	
	// Get current pruned height
	prunedHeight, err := pm.db.GetPrunedHeight()
	if err != nil {
		prunedHeight = 0
	}
	
	if pruneTarget <= prunedHeight {
		pm.logger.Debugf("No new blocks to prune: target=%d, pruned=%d", 
			pruneTarget, prunedHeight)
		return nil
	}
	
	pm.logger.Infof("Pruning blocks from height %d to %d (keeping last %d blocks)",
		prunedHeight+1, pruneTarget, pm.pruneDepth)
	
	// Collect blocks to prune
	blocksToPrune := make([]uint64, 0)
	for height := prunedHeight + 1; height <= pruneTarget; height++ {
		if pm.db.IsBlockPruned(height) {
			continue
		}
		blocksToPrune = append(blocksToPrune, height)
	}
	
	if len(blocksToPrune) == 0 {
		return nil
	}
	
	// Prune blocks in batches
	batchSize := 100
	var prunedFiles []int
	var bytesFreed int64
	
	for i := 0; i < len(blocksToPrune); i += batchSize {
		end := i + batchSize
		if end > len(blocksToPrune) {
			end = len(blocksToPrune)
		}
		
		batch := blocksToPrune[i:end]
		if err := pm.pruneBlockBatch(batch); err != nil {
			pm.logger.Errorf("Failed to prune batch: %v", err)
			continue
		}
		
		// Update pruned height
		lastPruned := batch[len(batch)-1]
		if err := pm.db.SetPrunedHeight(lastPruned); err != nil {
			pm.logger.Warnf("Failed to update pruned height: %v", err)
		}
		
		pm.prunedBlockCount += uint64(len(batch))
	}
	
	// Prune block files
	prunedFiles, bytesFreed = pm.pruneBlockFiles()
	
	// Record pruning stats
	record := PruneRecord{
		PrunedHeight:    pruneTarget,
		PrunedTimestamp: time.Now(),
		PrunedFiles:     prunedFiles,
		BytesFreed:      bytesFreed,
	}
	
	if err := pm.db.SavePruneRecord(&record); err != nil {
		pm.logger.Warnf("Failed to save prune record: %v", err)
	}
	
	pm.pruneRecords = append(pm.pruneRecords, record)
	pm.totalBytesPruned += bytesFreed
	
	pm.logger.Infof("Pruning complete: removed %d blocks, freed %d MB, total freed %d MB",
		len(blocksToPrune), bytesFreed/(1024*1024), pm.totalBytesPruned/(1024*1024))
	
	return nil
}

// pruneBlockBatch prunes a batch of blocks
func (pm *PruneManager) pruneBlockBatch(heights []uint64) error {
	for _, height := range heights {
		// Get block hash
		hash, err := pm.chain.GetBlockHash(height)
		if err != nil {
			pm.logger.Debugf("Cannot prune height %d: hash not found", height)
			continue
		}
		
		// Mark as pruned in DB
		if err := pm.db.MarkBlockPruned(height, hash); err != nil {
			return fmt.Errorf("failed to mark block %d as pruned: %w", height, err)
		}
		
		// Delete block data from database
		if err := pm.db.DeleteBlockData(height); err != nil {
			pm.logger.Warnf("Failed to delete block data at height %d: %v", height, err)
		}
		
		// Note: Block files are kept for potential reorgs until they're fully pruned
	}
	
	return nil
}

// pruneBlockFiles prunes old block files.
func (pm *PruneManager) pruneBlockFiles() ([]int, int64) {
	pm.blockFileMu.Lock()
	defer pm.blockFileMu.Unlock()
	
	var prunedFiles []int
	var totalFreed int64
	
	// Sort files by number
	fileNumbers := make([]int, 0, len(pm.blockFiles))
	for num := range pm.blockFiles {
		fileNumbers = append(fileNumbers, num)
	}
	sort.Ints(fileNumbers)
	
	// Find prunable files (all blocks in file are pruned)
	for _, fileNum := range fileNumbers {
		info := pm.blockFiles[fileNum]
		if !info.IsPrunable {
			continue
		}
		
		// Check if all blocks in this file are pruned
		prunedHeight, _ := pm.db.GetPrunedHeight()
		if info.LastBlock <= prunedHeight {
			// Prune this file
			if err := os.Remove(info.FilePath); err != nil {
				pm.logger.Warnf("Failed to remove block file %s: %v", info.FilePath, err)
				continue
			}
			
			// Also remove rev file
			revPath := pm.getRevFilePath(info.FileNumber)
			os.Remove(revPath)
			
			totalFreed += info.FileSize
			prunedFiles = append(prunedFiles, fileNum)
			
			// Delete metadata
			pm.db.DeleteBlockFileInfo(fileNum)
			delete(pm.blockFiles, fileNum)
			
			pm.logger.Debugf("Pruned block file %s (%d MB)", 
				info.FilePath, info.FileSize/(1024*1024))
		}
	}
	
	return prunedFiles, totalFreed
}

// getBlockFilePath returns the path for a block file (blk00000.dat)
func (pm *PruneManager) getBlockFilePath(fileNumber int) string {
	filename := fmt.Sprintf("%s%05d%s", BlockFilePrefix, fileNumber, BlockFileExtension)
	return filepath.Join(pm.dataDir, filename)
}

// getRevFilePath returns the path for a rev file (rev00000.dat)
func (pm *PruneManager) getRevFilePath(fileNumber int) string {
	filename := fmt.Sprintf("%s%05d%s", RevFilePrefix, fileNumber, BlockFileExtension)
	return filepath.Join(pm.dataDir, filename)
}

// loadBlockFiles scans existing block files
func (pm *PruneManager) loadBlockFiles() error {
	files, err := os.ReadDir(pm.dataDir)
	if err != nil {
		return err
	}
	
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		
		name := file.Name()
		if len(name) >= 8 && name[:3] == BlockFilePrefix && name[len(name)-4:] == BlockFileExtension {
			var fileNum int
			fmt.Sscanf(name[3:8], "%05d", &fileNum)
			
			info, err := os.Stat(filepath.Join(pm.dataDir, name))
			if err != nil {
				continue
			}
			
			// Try to load metadata from DB
			dbInfo, _ := pm.db.GetBlockFileInfo(fileNum)
			if dbInfo != nil {
				pm.blockFiles[fileNum] = dbInfo
			} else {
				pm.blockFiles[fileNum] = &BlockFileInfo{
					FileNumber: fileNum,
					FilePath:   filepath.Join(pm.dataDir, name),
					FileSize:   info.Size(),
					CreatedAt:  info.ModTime(),
					IsPrunable: true,
				}
			}
		}
	}
	
	return nil
}

// loadPruneRecords loads historical prune records
func (pm *PruneManager) loadPruneRecords() error {
	records, err := pm.db.GetPruneRecords(100)
	if err != nil {
		return err
	}
	pm.pruneRecords = records
	
	// Calculate total bytes pruned
	for _, record := range records {
		pm.totalBytesPruned += record.BytesFreed
	}
	
	return nil
}

// GetPruneStatus returns current pruning status
func (pm *PruneManager) GetPruneStatus() map[string]interface{} {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	
	prunedHeight, _ := pm.db.GetPrunedHeight()
	currentHeight := pm.chain.CurrentHeight()
	
	status := map[string]interface{}{
		"enabled":           pm.enabled,
		"prune_depth":       pm.pruneDepth,
		"prune_mode":        pm.pruneMode,
		"is_pruning":        pm.isPruning,
		"pruned_height":     prunedHeight,
		"current_height":    currentHeight,
		"blocks_kept":       currentHeight - prunedHeight,
		"total_blocks_pruned": pm.prunedBlockCount,
		"total_bytes_freed":   pm.totalBytesPruned,
		"total_mb_freed":      pm.totalBytesPruned / (1024 * 1024),
		"block_files":         len(pm.blockFiles),
		"last_prune_time":     pm.lastPruneTime,
		"prune_records":       len(pm.pruneRecords),
	}
	
	return status
}

// SetPruneDepth changes the prune depth (requires restart or next prune)
func (pm *PruneManager) SetPruneDepth(depth uint64) error {
	if depth < MinPruneDepth {
		return ErrPruneDepthTooLow
	}
	if depth > MaxPruneDepth {
		return ErrPruneDepthTooHigh
	}
	
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.pruneDepth = depth
	pm.logger.Infof("Prune depth changed to %d blocks", depth)
	
	return nil
}

// DisablePruning disables automatic pruning
func (pm *PruneManager) DisablePruning() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.enabled = false
	pm.pruneMode = PruneModeDisabled
	pm.logger.Info("Pruning disabled")
}

// EnablePruning enables automatic pruning
func (pm *PruneManager) EnablePruning() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.enabled = true
	pm.pruneMode = PruneModeAuto
	pm.logger.Info("Pruning enabled")
}

// UpdateBlockFileInfo updates metadata for a block file
func (pm *PruneManager) UpdateBlockFileInfo(fileNumber int, firstBlock, lastBlock uint64, blockCount int) {
	pm.blockFileMu.Lock()
	defer pm.blockFileMu.Unlock()
	
	info, exists := pm.blockFiles[fileNumber]
	if !exists {
		info = &BlockFileInfo{
			FileNumber: fileNumber,
			FilePath:   pm.getBlockFilePath(fileNumber),
			CreatedAt:  time.Now(),
		}
		pm.blockFiles[fileNumber] = info
	}
	
	info.FirstBlock = firstBlock
	info.LastBlock = lastBlock
	info.BlockCount = blockCount
	
	// Check if file is fully prunable
	prunedHeight, _ := pm.db.GetPrunedHeight()
	info.IsPrunable = info.LastBlock <= prunedHeight
	
	// Save to DB
	pm.db.SaveBlockFileInfo(info)
}

// IsBlockPruned checks if a block's data is pruned
func (pm *PruneManager) IsBlockPruned(height uint64) bool {
	return pm.db.IsBlockPruned(height)
}

// GetPrunedHeight returns the highest pruned block height
func (pm *PruneManager) GetPrunedHeight() uint64 {
	height, _ := pm.db.GetPrunedHeight()
	return height
}
