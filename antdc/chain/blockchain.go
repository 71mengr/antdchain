// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package chain

import (
	"context"
	"encoding/binary"
	// "encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	// "strings"
	"time"

	"github.com/antdaza/antdchain/antdc/block"
	"github.com/antdaza/antdchain/antdc/chain/db"
	"github.com/antdaza/antdchain/antdc/checkpoints"
	"github.com/antdaza/antdchain/antdc/monitoring"
	"github.com/antdaza/antdchain/antdc/p2p"
	"github.com/antdaza/antdchain/antdc/pow"
	"github.com/antdaza/antdchain/antdc/reward"
	"github.com/antdaza/antdchain/antdc/rotatingking"
	"github.com/antdaza/antdchain/antdc/staking"
	"github.com/antdaza/antdchain/antdc/state"
	"github.com/antdaza/antdchain/antdc/tx"
	"github.com/antdaza/antdchain/antdc/vm"
	"github.com/antdaza/antdchain/common"
	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/hashicorp/golang-lru"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
)

// Prometheus metrics
var (
	blockHeightGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "antdchain_block_height",
		Help: "Current canonical chain height",
	})
	blockWriteDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "antdchain_block_write_duration_seconds",
		Help:    "Duration of block write operations",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	})
	reorgCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "antdchain_reorg_total",
		Help: "Total number of chain reorganizations",
	})
	reorgDepthHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "antdchain_reorg_depth",
		Help:    "Depth of chain reorganizations",
		Buckets: []float64{1, 2, 3, 5, 10, 20, 50, 100},
	})
	cacheHitCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "antdchain_cache_hits_total",
		Help: "Total cache hits by type",
	}, []string{"type"})
	cacheMissCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "antdchain_cache_misses_total",
		Help: "Total cache misses by type",
	}, []string{"type"})
	stateSnapshotCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "antdchain_state_snapshots_total",
		Help: "Total number of state snapshots created",
	})
	stateRevertCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "antdchain_state_reverts_total",
		Help: "Total number of state reverts",
	})
	pruningCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "antdchain_pruning_operations_total",
		Help: "Total number of pruning operations",
	})
)

// Blockchain represents the main blockchain structure with database-first design
type Blockchain struct {
	mu                   sync.RWMutex
	db                   *db.ChainDB
	latest               atomic.Pointer[block.Block] // Atomic pointer for latest block
	state                *state.State
	stakingManager       *staking.StakingManager
	txPool               *TxPool
	pow                  *pow.PoW
	checkpoints          *checkpoints.Checkpoints
	checkpointManager    *checkpoints.Checkpoints
	statePath            string
	rewardDistributor    *reward.RewardDistributor
	governance           interface{}
	stateMu              sync.Mutex
	minConfirmations     uint64
	monitor              *monitoring.SupplyMonitor
	blockSubmitMu        sync.Mutex
	rotatingKingManager  reward.RotatingKingManager
	syncing              atomic.Bool
	syncTarget           atomic.Uint64
	syncCooldownUntilNs  atomic.Int64
	syncMu               sync.RWMutex
	p2pBroadcaster       rotatingking.P2PBroadcaster
	logger               *logrus.Logger
	blockByNumberCache   *lru.Cache // Cache for number → block (hot path only)
	blockByHashCache     *lru.Cache // Cache for hash → block (hot path only)
	cacheMu              sync.RWMutex
	lastCanonicalHeight  atomic.Uint64 // Cached latest height for fast access
	reorgDepth           atomic.Uint64 // Maximum reorg depth allowed
	ancientStore         *AncientStore // For pruning old blocks
	finalizedHeight      atomic.Uint64 // Height considered finalized (no reorg beyond this)
	pruneManager         *PruneManager // Background block pruning manager
	reorgManager         *ReorgManager // Background chain reorganization manager
	processingReorgBlock atomic.Bool   // Guards internal reorg manager block insertion
	orphanMu             sync.RWMutex
	orphanBlocks         map[common.Hash]*orphanBlock
}

// AncientStore handles storage of ancient (pruned) blocks
type AncientStore struct {
	path string
	mu   sync.RWMutex
}

// NewAncientStore creates a new ancient block store
func NewAncientStore(dataDir string) *AncientStore {
	return &AncientStore{
		path: filepath.Join(dataDir, "ancient"),
	}
}

// StoreBlock stores a block in ancient storage
func (a *AncientStore) StoreBlock(blk *block.Block) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Ensure directory exists
	if err := os.MkdirAll(a.path, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create ancient directory: %w", err)
	}

	// Encode block to RLP
	data, err := rlp.EncodeToBytes(blk)
	if err != nil {
		return fmt.Errorf("failed to encode block: %w", err)
	}

	// Write to file (hash-based naming)
	filename := filepath.Join(a.path, blk.Hash().Hex()+".rlp")
	return os.WriteFile(filename, data, 0644)
}

// ReadBlock reads a block from ancient storage
func (a *AncientStore) ReadBlock(hash common.Hash) (*block.Block, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	filename := filepath.Join(a.path, hash.Hex()+".rlp")
	data, err := os.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read ancient block: %w", err)
	}

	var blk block.Block
	if err := rlp.DecodeBytes(data, &blk); err != nil {
		return nil, fmt.Errorf("failed to decode ancient block: %w", err)
	}

	return &blk, nil
}

const (
	DefaultReorgDepth    = 12   // Allow reorgs up to 12 blocks deep
	MaxReorgDepth        = 64   // Maximum allowed reorg depth
	PruningConfirmations = 8192 // Blocks older than this get pruned (≈3 days at 30s blocks)
	FinalizationDelay    = 100  // Blocks considered finalized after this many confirmations
)

// IsSyncing returns whether the blockchain is in sync mode
func (bc *Blockchain) IsSyncing() bool {
	return bc.syncing.Load()
}

// IsFullySynced returns true when the chain is not syncing, has reached any
// advertised sync target, and is outside the post-sync mining cooldown window.
func (bc *Blockchain) IsFullySynced() bool {
	if bc == nil || bc.IsSyncing() || bc.SyncCooldownRemaining() > 0 {
		return false
	}

	target := bc.GetSyncTarget()
	return target == 0 || bc.GetChainHeight() >= target
}

// StartSync enables sync mode with target height
func (bc *Blockchain) StartSync(target uint64) {
	bc.syncing.Store(true)
	bc.syncTarget.Store(target)
	log.Printf("[blockchain] Sync mode ENABLED → target height = %d", target)
}

// StopSync disables sync mode
func (bc *Blockchain) StopSync() {
	if bc.syncing.Swap(false) {
		log.Printf("[blockchain] Sync mode STOPPED")
	}
	cooldownUntil := time.Now().Add(10 * time.Second)
	bc.syncCooldownUntilNs.Store(cooldownUntil.UnixNano())
	log.Printf("[blockchain] Mining cooldown active until %s", cooldownUntil.Format(time.RFC3339))
	bc.syncTarget.Store(0)
}

// SyncCooldownRemaining returns remaining cooldown duration after sync completes.
func (bc *Blockchain) SyncCooldownRemaining() time.Duration {
	untilNs := bc.syncCooldownUntilNs.Load()
	if untilNs <= 0 {
		return 0
	}
	remaining := time.Until(time.Unix(0, untilNs))
	if remaining < 0 {
		return 0
	}
	return remaining
}

// GetSyncTarget returns the current sync target height
func (bc *Blockchain) GetSyncTarget() uint64 {
	return bc.syncTarget.Load()
}

// GetBlockByNumber returns a block by its number (database-first with cache)
func (bc *Blockchain) GetBlockByNumber(number uint64) (monitoring.BlockProvider, error) {
	blk, err := bc.getBlockByNumber(number)
	if err != nil {
		return nil, err
	}
	if blk == nil {
		return nil, nil
	}
	return &BlockWrapper{blk}, nil
}

// getCanonicalCachedBlock returns a cached height lookup only when the cached
// block still matches the database canonical hash for that height. Hash-based
// lookups may load side-branch blocks, so every number-cache hit is checked
// before it can influence canonical chain decisions.
func (bc *Blockchain) getCanonicalCachedBlock(number uint64) (*block.Block, bool) {
	bc.cacheMu.RLock()
	if bc.blockByNumberCache == nil {
		bc.cacheMu.RUnlock()
		return nil, false
	}

	cached, ok := bc.blockByNumberCache.Get(number)
	if !ok {
		bc.cacheMu.RUnlock()
		return nil, false
	}

	blk, ok := cached.(*block.Block)
	if !ok || blk == nil || blk.Header == nil || blk.Header.Number == nil || blk.Header.Number.Uint64() != number {
		bc.cacheMu.RUnlock()
		bc.cacheMu.Lock()
		bc.blockByNumberCache.Remove(number)
		bc.cacheMu.Unlock()
		return nil, false
	}
	bc.cacheMu.RUnlock()

	canonicalHash, err := bc.db.GetCanonicalHash(number)
	if err != nil || canonicalHash == (common.Hash{}) || blk.Hash() != canonicalHash {
		bc.cacheMu.Lock()
		bc.blockByNumberCache.Remove(number)
		bc.cacheMu.Unlock()
		return nil, false
	}

	cacheHitCounter.WithLabelValues("number").Inc()
	return blk, true
}

// getBlockByNumber internal implementation
func (bc *Blockchain) getBlockByNumber(number uint64) (*block.Block, error) {
	// First try canonical number cache
	if blk, ok := bc.getCanonicalCachedBlock(number); ok {
		return blk, nil
	}

	cacheMissCounter.WithLabelValues("number").Inc()

	// Get canonical hash from database
	hash, err := bc.db.GetCanonicalHash(number)
	if err != nil {
		return nil, fmt.Errorf("failed to get canonical hash for block %d: %w", number, err)
	}
	if hash == (common.Hash{}) {
		// Check ancient storage
		return bc.checkAncientBlock(number)
	}

	// Read block from database
	blk, err := bc.db.ReadBlockByHash(hash)
	if err != nil {
		return nil, fmt.Errorf("failed to read block %d: %w", number, err)
	}
	if blk == nil {
		return bc.checkAncientBlock(number)
	}

	// Cache the block in both caches
	bc.cacheMu.Lock()
	if bc.blockByNumberCache != nil {
		bc.blockByNumberCache.Add(number, blk)
	}
	if bc.blockByHashCache != nil {
		bc.blockByHashCache.Add(hash, blk)
	}
	bc.cacheMu.Unlock()

	return blk, nil
}

// checkAncientBlock checks if a block is in ancient storage
func (bc *Blockchain) checkAncientBlock(number uint64) (*block.Block, error) {
	if bc.ancientStore == nil {
		return nil, nil
	}

	// Get canonical hash from database (headers are always kept)
	hash, err := bc.db.GetCanonicalHash(number)
	if err != nil || hash == (common.Hash{}) {
		return nil, nil
	}

	// Try to read from ancient storage
	return bc.ancientStore.ReadBlock(hash)
}

// GetChainHeight returns the current chain height
func (bc *Blockchain) GetChainHeight() uint64 {
	// Return cached height for fast access
	return bc.lastCanonicalHeight.Load()
}

// GetLatestHeight returns the latest block height (same as GetChainHeight)
func (bc *Blockchain) GetLatestHeight() uint64 {
	return bc.GetChainHeight()
}

// State returns the current state
func (bc *Blockchain) State() *state.State {
	return bc.GetState()
}

func (bc *Blockchain) StakingManager() *staking.StakingManager {
	return bc.stakingManager
}

// GetState returns the state (initializing if necessary)
func (bc *Blockchain) GetState() *state.State {
	if bc.state != nil {
		return bc.state
	}

	bc.stateMu.Lock()
	defer bc.stateMu.Unlock()

	if bc.state == nil {
		s, err := state.NewState(bc.statePath)
		if err != nil {
			return nil
		}
		bc.state = s
	}

	return bc.state
}

// GetStateSafe returns state with error handling
func (bc *Blockchain) GetStateSafe() (*state.State, error) {
	bc.stateMu.Lock()
	defer bc.stateMu.Unlock()

	if bc.state == nil {
		s, err := state.NewState(bc.statePath)
		if err != nil {
			return nil, fmt.Errorf("failed to create state: %w", err)
		}
		bc.state = s
	}

	return bc.state, nil
}

// TruncateTo truncates the chain to a specific height (database operation)
func (bc *Blockchain) TruncateTo(height uint64) error {
	start := time.Now()
	bc.mu.Lock()
	defer bc.mu.Unlock()

	currentHeight := bc.GetChainHeight()
	if height >= currentHeight {
		return fmt.Errorf("cannot truncate to height %d (current height is %d)", height, currentHeight)
	}

	// Delete blocks from database in a batch
	batch := bc.db.DB().NewBatch()

	// Keep track of blocks to delete
	blocksToDelete := make([]common.Hash, 0, currentHeight-height)

	for h := currentHeight; h > height; h-- {
		// Get canonical hash
		hash, err := bc.db.GetCanonicalHash(h)
		if err != nil {
			log.Printf("[blockchain] Warning: failed to get canonical hash for height %d: %v", h, err)
			continue
		}
		if hash == (common.Hash{}) {
			continue
		}

		blocksToDelete = append(blocksToDelete, hash)

		// Delete block entries (keep headers for ancient blocks)
		batch.Delete(db.BlockByHashKey(hash), pebble.NoSync)
		// Don't delete headers - they're needed for ancient block lookup
		batch.Delete(db.CanonicalHashKey(h), pebble.NoSync)
	}

	// Update last canonical height
	if height > 0 {
		heightBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(heightBytes, height)
		batch.Set(lastCanonicalHeightKey(), heightBytes, pebble.Sync)
	} else {
		batch.Delete(lastCanonicalHeightKey(), pebble.Sync)
	}

	// Commit batch
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("failed to commit truncation batch: %w", err)
	}

	// Update cached values
	if height == 0 {
		bc.latest.Store(nil)
		bc.lastCanonicalHeight.Store(0)
	} else {
		hash, err := bc.db.GetCanonicalHash(height)
		if err != nil {
			return fmt.Errorf("failed to get new tip hash: %w", err)
		}
		if hash == (common.Hash{}) {
			return errors.New("new tip block not found")
		}

		blk, err := bc.db.ReadBlockByHash(hash)
		if err != nil {
			return fmt.Errorf("failed to read new tip block: %w", err)
		}
		bc.latest.Store(blk)
		bc.lastCanonicalHeight.Store(height)
	}

	// Clear both caches completely (safer than selective removal)
	bc.cacheMu.Lock()
	if bc.blockByNumberCache != nil {
		bc.blockByNumberCache.Purge()
	}
	if bc.blockByHashCache != nil {
		bc.blockByHashCache.Purge()
	}
	bc.cacheMu.Unlock()

	blockHeightGauge.Set(float64(height))

	log.Printf("[blockchain] Chain truncated to height %d (deleted %d blocks) in %v",
		height, len(blocksToDelete), time.Since(start))
	return nil
}

// Checkpoints returns the checkpoint manager
func (bc *Blockchain) Checkpoints() *checkpoints.Checkpoints {
	return bc.checkpoints
}

// GetParentHash returns the parent hash of a block at given height
func (bc *Blockchain) GetParentHash(height uint64) (common.Hash, error) {
	if height == 0 {
		return common.Hash{}, nil
	}

	// Get block at requested height
	blk, err := bc.getBlockByNumber(height)
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to get block %d: %w", height, err)
	}
	if blk == nil {
		return common.Hash{}, fmt.Errorf("block %d not found", height)
	}

	return blk.Header.ParentHash, nil
}

// Pow returns the PoW/PoS engine
func (bc *Blockchain) Pow() *pow.PoW {
	return bc.pow
}

// GetPoWEngine returns the configured PoW engine.
func (bc *Blockchain) GetPoWEngine() *pow.PoW {
	return bc.pow
}

// TxPool returns the transaction pool
func (bc *Blockchain) TxPool() p2p.TxPool {
	return bc.txPool
}

// HasBlockAtHeight checks if a block exists at given height (database query)
func (bc *Blockchain) HasBlockAtHeight(height uint64) bool {
	hash, err := bc.db.GetCanonicalHash(height)
	if err != nil {
		log.Printf("[blockchain] Failed to check block at height %d: %v", height, err)
		return false
	}
	return hash != (common.Hash{})
}

// Latest returns the latest block
func (bc *Blockchain) Latest() *block.Block {
	return bc.latest.Load()
}

// GetBlock returns a block by number (convenience method)
/*func (bc *Blockchain) GetBlock(number uint64) *block.Block {
    blk, err := bc.getBlockByNumber(number)
    if err != nil || blk == nil {
        return nil
    }
    return blk
}

// GetBlockByHash returns a block by hash (with proper cache)
func (bc *Blockchain) GetBlockByHash(hash common.Hash) (*block.Block, error) {
    // First try hash cache
    bc.cacheMu.RLock()
    if bc.blockByHashCache != nil {
        if cached, ok := bc.blockByHashCache.Get(hash); ok {
            if blk, ok := cached.(*block.Block); ok && blk != nil {
                bc.cacheMu.RUnlock()
                cacheHitCounter.WithLabelValues("hash").Inc()
                return blk, nil
            }
        }
    }
    bc.cacheMu.RUnlock()

    cacheMissCounter.WithLabelValues("hash").Inc()

    // Read from database
    blk, err := bc.db.ReadBlockByHash(hash)
    if err != nil {
        return nil, fmt.Errorf("failed to read block by hash: %w", err)
    }

    // If not in database, check ancient storage
    if blk == nil && bc.ancientStore != nil {
        blk, err = bc.ancientStore.ReadBlock(hash)
        if err != nil {
            return nil, fmt.Errorf("failed to read ancient block: %w", err)
        }
    }

    // Cache the block if found
    if blk != nil {
        bc.cacheMu.Lock()
        if bc.blockByHashCache != nil {
            bc.blockByHashCache.Add(hash, blk)
        }
        if bc.blockByNumberCache != nil {
            bc.blockByNumberCache.Add(blk.Header.Number.Uint64(), blk)
        }
        bc.cacheMu.Unlock()
    }

    return blk, nil
}
*/
// HasBlock checks if a block exists by hash
func (bc *Blockchain) HasBlock(hash common.Hash) bool {
	exists, err := bc.db.HasBlock(hash)
	if err != nil {
		log.Printf("[blockchain] Failed to check block existence: %v", err)
		return false
	}

	// Also check ancient storage
	if !exists && bc.ancientStore != nil {
		blk, _ := bc.ancientStore.ReadBlock(hash)
		exists = blk != nil
	}

	return exists
}

// MinConfirmations returns the minimum confirmations required
func (bc *Blockchain) MinConfirmations() uint64 {
	return bc.minConfirmations
}

// Monitor returns the supply monitor
func (bc *Blockchain) Monitor() *monitoring.SupplyMonitor {
	return bc.monitor
}

// GetRewardDistributor returns the reward distributor
func (bc *Blockchain) GetRewardDistributor() *reward.RewardDistributor {
	return bc.rewardDistributor
}

// GetAccountBalance returns account balance
func (bc *Blockchain) GetAccountBalance(addr common.QuantumAddress) *big.Int {
	return bc.GetState().GetBalance(addr)
}

// MonitoringState returns state for monitoring
func (bc *Blockchain) MonitoringState() monitoring.StateProvider {
	return &StateWrapper{bc.GetState()}
}

// GetRotatingKingManager returns the rotating king manager
func (bc *Blockchain) GetRotatingKingManager() reward.RotatingKingManager {
	if bc == nil {
		return nil
	}
	return bc.rotatingKingManager
}

// GetGovernance returns the governance interface
func (bc *Blockchain) GetGovernance() interface{} {
	return bc.governance
}

// SetRotatingKingManager sets the rotating king manager
func (bc *Blockchain) SetRotatingKingManager(rkm reward.RotatingKingManager) {
	bc.rotatingKingManager = rkm
}

// SetGovernance sets the governance controller
func (bc *Blockchain) SetGovernance(gov interface{}) {
	bc.governance = gov
}

// SetTxPool sets the transaction pool
func (bc *Blockchain) SetTxPool(pool *TxPool) {
	bc.txPool = pool
}

// SetP2PBroadcaster sets the P2P broadcaster
func (bc *Blockchain) SetP2PBroadcaster(broadcaster rotatingking.P2PBroadcaster) {
	bc.p2pBroadcaster = broadcaster
}

// GetCurrentRotatingKing returns current rotating king address
func (bc *Blockchain) GetCurrentRotatingKing() common.QuantumAddress {
	if bc.rotatingKingManager != nil {
		return bc.rotatingKingManager.GetCurrentKing()
	}
	return common.QuantumAddress{}
}

// GetBlocksMinedBy returns number of blocks mined by an address (with scan limit)
func (bc *Blockchain) GetBlocksMinedBy(addr common.QuantumAddress) uint64 {
	return bc.GetBlocksMinedByLimited(addr, 10000) // Default limit of 10,000 blocks
}

// GetBlocksMinedByLimited returns number of blocks mined by an address with scan limit
func (bc *Blockchain) GetBlocksMinedByLimited(addr common.QuantumAddress, maxScan uint64) uint64 {
	currentHeight := bc.GetChainHeight()
	if currentHeight == 0 {
		return 0
	}

	count := uint64(0)
	scanLimit := uint64(0)
	if maxScan > 0 && currentHeight > maxScan {
		scanLimit = currentHeight - maxScan
	}

	// Scan from most recent blocks (most likely to have the miner's blocks)
	for height := currentHeight; height > scanLimit && height > 0; height-- {
		// Get canonical hash
		hash, err := bc.db.GetCanonicalHash(height)
		if err != nil {
			log.Printf("[blockchain] Warning: failed to get hash for height %d: %v", height, err)
			continue
		}
		if hash == (common.Hash{}) {
			continue
		}

		// Read header only (more efficient than full block)
		header, err := bc.db.ReadHeaderByHash(hash)
		if err != nil || header == nil {
			log.Printf("[blockchain] Warning: failed to read header for height %d: %v", height, err)
			continue
		}

		if header.Coinbase == addr {
			count++
		}
	}

	return count
}

// CurrentTarget returns current mining target
func (bc *Blockchain) CurrentTarget() *big.Int {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	if bc.latest.Load() == nil {
		return new(big.Int).Lsh(big.NewInt(1), 256-32) // fallback target
	}
	return bc.pow.GetTarget()
}

// ComputeMixDigest computes mix digest for header
func (bc *Blockchain) ComputeMixDigest(header *block.Header) common.Hash {
	return common.Hash{}
}

// ValidateTransaction validates a transaction
func (bc *Blockchain) ValidateTransaction(t *tx.Tx) error {
	bc.stateMu.Lock()
	defer bc.stateMu.Unlock()

	if valid, err := t.Verify(); err != nil || !valid {
		return fmt.Errorf("invalid signature: %w", err)
	}

	from := common.BytesToQuantumAddress(t.From.Bytes())
	currentNonce := bc.state.GetNonce(from)
	if t.Nonce != currentNonce {
		return fmt.Errorf("invalid nonce: expected %d, got %d", currentNonce, t.Nonce)
	}

	currentHeight := bc.GetChainHeight()
	balance := bc.spendableBalanceAtHeight(from, currentHeight, nil)
	totalCost := new(big.Int).Add(t.Value, new(big.Int).Mul(new(big.Int).SetUint64(t.Gas), t.GasPrice))
	if balance.Cmp(totalCost) < 0 {
		return errors.New("insufficient spendable balance")
	}

	return nil
}

// ValidateTransactionForPoS validates transaction with PoS-specific rules
func (bc *Blockchain) ValidateTransactionForPoS(t *tx.Tx) error {
	// Basic validation
	if valid, err := t.Verify(); err != nil || !valid {
		return fmt.Errorf("invalid signature: %w", err)
	}

	// Standard validation
	currentNonce := bc.State().GetNonce(common.BytesToQuantumAddress(t.From.Bytes()))
	if t.Nonce != currentNonce {
		return fmt.Errorf("invalid nonce: expected %d, got %d", currentNonce, t.Nonce)
	}

	return nil
}

// Close cleans up blockchain resources
func (bc *Blockchain) Close() error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	bc.stateMu.Lock()
	defer bc.stateMu.Unlock()

	if bc.pruneManager != nil {
		bc.pruneManager.Stop()
	}
	if bc.reorgManager != nil {
		bc.reorgManager.Stop()
	}

	// Close rotating king manager
	if bc.rotatingKingManager != nil {
		if closer, ok := bc.rotatingKingManager.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				log.Printf("[blockchain] Failed to close rotating king manager: %v", err)
			}
		}
	}

	// Close other resources
	if bc.pow != nil {
		bc.pow.Release()
	}
	if bc.state != nil {
		bc.state.Close()
	}
	if bc.db != nil {
		if err := bc.db.Close(); err != nil {
			log.Printf("[blockchain] Failed to close chain database: %v", err)
		}
	}

	log.Println("Blockchain shut down")
	return nil
}

// GetPoSStatistics returns PoS statistics
func (bc *Blockchain) GetPoSStatistics() (map[string]interface{}, error) {
	if bc.pow != nil {
		return bc.pow.GetMiningStatistics(), nil
	}
	return nil, errors.New("PoS engine not initialized")
}

// GetStateForMonitoring returns state for monitoring
func (bc *Blockchain) GetStateForMonitoring() monitoring.StateProvider {
	state := bc.GetState()
	return &StateWrapper{state}
}

// InsertChain inserts a chain of blocks, handling reorgs if necessary
func (bc *Blockchain) InsertChain(blocks []*block.Block) (int, error) {
	if len(blocks) == 0 {
		return 0, nil
	}

	bc.mu.Lock()
	defer bc.mu.Unlock()

	start := time.Now()
	log.Printf("[blockchain] InsertChain: attempting to insert %d blocks", len(blocks))

	// Validate the chain segment
	for idx := 0; idx < len(blocks); idx++ {
		blk := blocks[idx]

		// Check block validity
		if err := bc.validateBlock(blk, idx == 0); err != nil {
			log.Printf("[blockchain] InsertChain: block %d invalid: %v", blk.Header.Number.Uint64(), err)
			return 0, fmt.Errorf("invalid block %d: %w", blk.Header.Number.Uint64(), err)
		}

		// Check chain continuity
		if idx > 0 && blocks[idx].Header.ParentHash != blocks[idx-1].Hash() {
			log.Printf("[blockchain] InsertChain: chain discontinuity at block %d", blk.Header.Number.Uint64())
			return 0, fmt.Errorf("chain discontinuity at block %d", blk.Header.Number.Uint64())
		}
	}

	firstBlock := blocks[0]
	lastBlock := blocks[len(blocks)-1]
	firstHeight := firstBlock.Header.Number.Uint64()
	lastHeight := lastBlock.Header.Number.Uint64()

	incomingBlocks := make(map[common.Hash]*block.Block, len(blocks))
	for _, blk := range blocks {
		incomingBlocks[blk.Hash()] = blk
	}

	currentHeight := bc.GetChainHeight()
	currentTip := bc.Latest()

	// Determine if we need to reorg
	if lastHeight <= currentHeight {
		// Check if this is already part of our chain
		existingHash, err := bc.db.GetCanonicalHash(lastHeight)
		if err == nil && existingHash == lastBlock.Hash() {
			log.Printf("[blockchain] InsertChain: blocks already in chain")
			return 0, nil // Already in chain
		}
	}

	// Find common ancestor
	var ancestorHeight uint64
	var reorgRequired bool

	if firstHeight == 0 {
		// Starting from genesis
		ancestorHeight = 0
		reorgRequired = currentHeight > 0 // If we have any blocks, we need to reorg from genesis
	} else {
		// Try to find where this chain connects to ours
		parentHash := firstBlock.Header.ParentHash
		parent, err := bc.GetBlockByHash(parentHash)
		if err != nil || parent == nil {
			log.Printf("[blockchain] InsertChain: parent block %s not found", parentHash.Hex()[:8])
			return 0, fmt.Errorf("parent block not found")
		}

		ancestorHeight = parent.Header.Number.Uint64()

		// Check if parent is on our canonical chain
		canonicalHash, err := bc.db.GetCanonicalHash(ancestorHeight)
		if err != nil || canonicalHash != parent.Hash() {
			// Parent is not canonical - we're on a fork
			reorgRequired = true
		} else {
			// Parent is canonical. This is a simple extension only if the
			// incoming segment does not conflict with an existing canonical
			// block at any covered height. Same-height/same-parent competitors
			// must still pass fork choice before they can replace the tip.
			reorgRequired = false
			for _, candidate := range blocks {
				height := candidate.Header.Number.Uint64()
				if height > currentHeight {
					break
				}

				existingHash, err := bc.db.GetCanonicalHash(height)
				if err != nil {
					return 0, fmt.Errorf("failed to read canonical hash at height %d: %w", height, err)
				}
				if existingHash != (common.Hash{}) && existingHash != candidate.Hash() {
					reorgRequired = true
					break
				}
			}
		}
	}

	// Check reorg depth limit
	if reorgRequired && ancestorHeight+bc.reorgDepth.Load() < currentHeight {
		log.Printf("[blockchain] InsertChain: reorg depth %d exceeds limit %d",
			currentHeight-ancestorHeight, bc.reorgDepth.Load())
		return 0, fmt.Errorf("reorg depth exceeds limit")
	}

	// Apply fork choice rule (strictly heaviest chain)
	if reorgRequired {
		// Calculate total difficulty/weight for both chains
		oldChainWeight, err := bc.calculateChainWeight(currentTip, ancestorHeight)
		if err != nil {
			return 0, fmt.Errorf("failed to calculate old chain weight: %w", err)
		}

		newChainWeight, err := bc.calculateChainWeightWithCandidates(lastBlock, ancestorHeight, incomingBlocks)
		if err != nil {
			return 0, fmt.Errorf("failed to calculate new chain weight: %w", err)
		}

		// Fork choice: prefer a heavier chain. If competing tips have equal
		// accumulated work at the same height, apply the deterministic protocol
		// tie-breaker so peers that receive same-height gossip in different orders
		// still converge on the same canonical block.
		weightCmp := newChainWeight.Cmp(oldChainWeight)
		if weightCmp < 0 || (weightCmp == 0 && (lastHeight != currentHeight || !isBetterBlockCandidate(lastBlock, currentTip))) {
			log.Printf("[blockchain] InsertChain: new chain weight %s does not beat old chain weight %s",
				newChainWeight.String(), oldChainWeight.String())
			return 0, fmt.Errorf("new chain does not win fork choice")
		}

		// Perform reorg
		reorgDepth := currentHeight - ancestorHeight
		reorgCounter.Inc()
		reorgDepthHistogram.Observe(float64(reorgDepth))

		log.Printf("[blockchain] InsertChain: performing reorg depth %d (ancestor: %d, old tip: %d, new tip: %d)",
			reorgDepth, ancestorHeight, currentHeight, lastHeight)

		// Revert state to common ancestor
		if err := bc.revertToHeight(ancestorHeight); err != nil {
			log.Printf("[blockchain] InsertChain: failed to revert state: %v", err)
			return 0, fmt.Errorf("failed to revert state: %w", err)
		}

		// Delete old canonical chain from database
		if err := bc.deleteCanonicalChain(ancestorHeight+1, currentHeight); err != nil {
			log.Printf("[blockchain] InsertChain: failed to delete old chain: %v", err)
			return 0, fmt.Errorf("failed to delete old chain: %w", err)
		}
	}

	// Apply new blocks
	inserted := 0
	for _, blk := range blocks {
		height := blk.Header.Number.Uint64()
		if height <= ancestorHeight {
			continue // Skip blocks before/at ancestor
		}

		// Simplified execution without snapshot (since state doesn't support it)
		// Execute block and get state root
		stateRoot, err := bc.executeBlock(blk)
		if err != nil {
			log.Printf("[blockchain] InsertChain: failed to execute block %d: %v", height, err)
			bc.rollbackInsert(blocks[:inserted], ancestorHeight)
			return inserted, fmt.Errorf("failed to execute block %d: %w", height, err)
		}

		// Verify state root
		if stateRoot != blk.Header.Root {
			log.Printf("[blockchain] InsertChain: state root mismatch at block %d (expected: %s, got: %s)",
				height, blk.Header.Root.Hex()[:8], stateRoot.Hex()[:8])
			bc.rollbackInsert(blocks[:inserted], ancestorHeight)
			return inserted, fmt.Errorf("state root mismatch at block %d", height)
		}

		// Write block (without snapshot support)
		if err := bc.writeBlock(blk); err != nil {
			log.Printf("[blockchain] InsertChain: failed to write block %d: %v", height, err)
			bc.rollbackInsert(blocks[:inserted], ancestorHeight)
			return inserted, fmt.Errorf("failed to write block %d: %w", height, err)
		}

		inserted++

		// Update finalized height (simple finalization rule)
		if height > FinalizationDelay {
			bc.finalizedHeight.Store(height - FinalizationDelay)
		}

		// Check if we should prune old blocks
		if height%PruningConfirmations == 0 {
			bc.pruneAncientBlocks(height - PruningConfirmations)
		}
	}

	log.Printf("[blockchain] InsertChain: successfully inserted %d blocks in %v, new height: %d",
		inserted, time.Since(start), lastHeight)

	return inserted, nil
}

// calculateChainWeight calculates the weight of a chain segment
func (bc *Blockchain) calculateChainWeight(tip *block.Block, fromHeight uint64) (*big.Int, error) {
	return bc.calculateChainWeightWithCandidates(tip, fromHeight, nil)
}

// calculateChainWeightWithCandidates calculates the weight of a chain segment,
// resolving parents from candidate blocks before falling back to persisted data.
func (bc *Blockchain) calculateChainWeightWithCandidates(tip *block.Block, fromHeight uint64, candidates map[common.Hash]*block.Block) (*big.Int, error) {
	weight := big.NewInt(0)
	current := tip

	for current != nil && current.Header.Number.Uint64() > fromHeight {
		if current.Header.Difficulty != nil {
			weight.Add(weight, current.Header.Difficulty)
		}

		// Move to parent
		if current.Header.Number.Uint64() == fromHeight+1 {
			break
		}

		parentHash := current.Header.ParentHash
		if candidates != nil {
			if parent := candidates[parentHash]; parent != nil {
				current = parent
				continue
			}
		}

		parent, err := bc.GetBlockByHash(parentHash)
		if err != nil {
			return nil, fmt.Errorf("failed to get parent block: %w", err)
		}
		current = parent
	}

	return weight, nil
}

// deleteCanonicalChain deletes a segment of the canonical chain
func (bc *Blockchain) deleteCanonicalChain(fromHeight, toHeight uint64) error {
	batch := bc.db.DB().NewBatch()
	defer batch.Close()

	for height := fromHeight; height <= toHeight; height++ {
		hash, err := bc.db.GetCanonicalHash(height)
		if err != nil || hash == (common.Hash{}) {
			continue
		}

		// Delete canonical pointer but keep block data (for possible reorg back)
		batch.Delete(db.CanonicalHashKey(height), pebble.NoSync)
	}

	return batch.Commit(pebble.Sync)
}

// validateBlock validates a single block
func (bc *Blockchain) validateBlock(blk *block.Block, isFirstInChain bool) error {
	// Basic header validation
	if blk.Header == nil {
		return errors.New("nil header")
	}

	// Check block number
	if blk.Header.Number == nil {
		return errors.New("nil block number")
	}

	if err := validateProtocolHeader(blk.Header); err != nil {
		return fmt.Errorf("protocol validation failed: %w", err)
	}

	// Verify PoS/PoW if applicable - simplified check
	if bc.pow != nil {
		// Basic check - just verify the block isn't obviously invalid
		// In production, you'd want proper validation
		if blk.Header.Number.Uint64() == 0 {
			// Genesis block - minimal validation
			return nil
		}

		// Check parent exists
		parent, err := bc.GetBlockByHash(blk.Header.ParentHash)
		if err != nil || parent == nil {
			return fmt.Errorf("parent block not found: %s", blk.Header.ParentHash.Hex())
		}
	}

	// Check transactions
	for _, tx := range blk.Txs {
		if tx == nil {
			continue
		}
		if err := bc.ValidateTransaction(tx); err != nil {
			return fmt.Errorf("invalid transaction: %w", err)
		}
	}

	return nil
}

// revertToHeight reverts chain state to a specific height
func (bc *Blockchain) revertToHeight(height uint64) error {
	currentHeight := bc.GetChainHeight()
	if height > currentHeight {
		return fmt.Errorf("cannot revert to height %d (current: %d)", height, currentHeight)
	}

	// If reverting to current height, nothing to do
	if height == currentHeight {
		return nil
	}

	log.Printf("[blockchain] revertToHeight: reverting from %d to %d", currentHeight, height)

	stateRevertCounter.Inc()

	// Revert state by replaying blocks from genesis.
	// Close the active LevelDB handle before reopening it; on Windows the open
	// handle owns the database lock and opening a second handle fails with
	// "The process cannot access the file because it is being used by another process."
	if bc.state != nil {
		if err := bc.state.Close(); err != nil {
			return fmt.Errorf("failed to close current state: %w", err)
		}
		bc.state = nil
	}

	newState, err := state.NewState(bc.statePath)
	if err != nil {
		return fmt.Errorf("failed to create new state: %w", err)
	}

	if height > 0 {
		for h := uint64(1); h <= height; h++ {
			blk, err := bc.getBlockByNumber(h)
			if err != nil || blk == nil {
				newState.Close()
				return fmt.Errorf("failed to get block %d for state replay: %w", h, err)
			}

			// Execute block on new state
			if _, err := bc.executeBlockOnState(blk, newState); err != nil {
				newState.Close()
				return fmt.Errorf("failed to execute block %d during state replay: %w", h, err)
			}
		}
	}

	bc.state = newState

	// Clear caches
	bc.cacheMu.Lock()
	if bc.blockByNumberCache != nil {
		bc.blockByNumberCache.Purge()
	}
	if bc.blockByHashCache != nil {
		bc.blockByHashCache.Purge()
	}
	bc.cacheMu.Unlock()

	// Update latest block pointer
	if height == 0 {
		bc.latest.Store(nil)
		bc.lastCanonicalHeight.Store(0)
	} else {
		hash, err := bc.db.GetCanonicalHash(height)
		if err != nil {
			return fmt.Errorf("failed to get canonical hash for height %d: %w", height, err)
		}
		if hash == (common.Hash{}) {
			return fmt.Errorf("no canonical block at height %d", height)
		}

		blk, err := bc.db.ReadBlockByHash(hash)
		if err != nil {
			return fmt.Errorf("failed to read block at height %d: %w", height, err)
		}
		bc.latest.Store(blk)
		bc.lastCanonicalHeight.Store(height)
	}

	blockHeightGauge.Set(float64(height))
	return nil
}

// writeBlock writes a block to database (simplified version without snapshot support)
func (bc *Blockchain) writeBlock(blk *block.Block) error {
	start := time.Now()
	defer func() {
		blockWriteDuration.Observe(time.Since(start).Seconds())
	}()

	hash := blk.Hash()
	height := blk.Header.Number.Uint64()

	// Create atomic batch
	batch := bc.db.DB().NewBatch()
	defer batch.Close()

	// Encode full block
	blockRLP, err := rlp.EncodeToBytes(blk)
	if err != nil {
		return fmt.Errorf("failed to encode block: %w", err)
	}

	// Encode header
	headerRLP, err := rlp.EncodeToBytes(blk.Header)
	if err != nil {
		return fmt.Errorf("failed to encode header: %w", err)
	}

	// Write all data in batch
	batch.Set(db.BlockByHashKey(hash), blockRLP, pebble.NoSync)
	batch.Set(db.HeaderByNumberKey(height), headerRLP, pebble.NoSync)
	batch.Set(db.HeaderByHashKey(hash), headerRLP, pebble.NoSync)
	batch.Set(db.CanonicalHashKey(height), hash[:], pebble.NoSync)

	// Update last canonical height
	heightBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(heightBytes, height)
	batch.Set(lastCanonicalHeightKey(), heightBytes, pebble.NoSync)

	// Update head block hash
	batch.Set([]byte("head"), hash[:], pebble.NoSync)

	// Commit batch (sync ensures durability)
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("failed to commit block batch: %w", err)
	}

	// Update in-memory state
	bc.latest.Store(blk)
	bc.lastCanonicalHeight.Store(height)
	blockHeightGauge.Set(float64(height))

	// Cache the new block
	bc.cacheMu.Lock()
	if bc.blockByNumberCache != nil {
		bc.blockByNumberCache.Add(height, blk)
	}
	if bc.blockByHashCache != nil {
		bc.blockByHashCache.Add(hash, blk)
	}
	bc.cacheMu.Unlock()

	log.Printf("[blockchain] Block written: height=%d, hash=%s, stateRoot=%s",
		height, hash.Hex()[:8], blk.Header.Root.Hex()[:8])

	if bc.stakingManager != nil {
		if err := bc.stakingManager.ProcessWithdrawalsAtHeight(height); err != nil {
			log.Printf("[staking] failed to process withdrawals at height %d: %v", height, err)
		}
	}

	return nil
}

// executeBlock executes transactions in a block and returns the resulting state root
func (bc *Blockchain) executeBlock(blk *block.Block) (common.Hash, error) {
	return bc.executeBlockOnState(blk, bc.state)
}

// executeBlockOnState executes transactions on a specific state instance
func (bc *Blockchain) executeBlockOnState(blk *block.Block, st *state.State) (common.Hash, error) {
	ctx := context.Background()

	// Create VM with appropriate gas limit
	v := vm.NewVM(st, blk.Header.GasLimit)

	// Execute transactions
	for _, tx := range blk.Txs {
		if tx == nil {
			continue
		}

		_, _, err := v.Execute(ctx, tx)
		if err != nil {
			return common.Hash{}, fmt.Errorf("transaction execution failed: %w", err)
		}
	}

	// Distribute rewards
	blockTime := blk.Header.Time
	totalFees := big.NewInt(0) // Calculate actual fees from transactions

	_, err := bc.rewardDistributor.DistributeRewards(
		st,
		blk.Header.Coinbase,
		totalFees,
		blk.Header.Number.Uint64(),
		blockTime,
		bc.rotatingKingManager,
		bc.pow,
	)
	if err != nil {
		return common.Hash{}, fmt.Errorf("reward distribution failed: %w", err)
	}

	return st.Root(), nil
}

// rollbackInsert rolls back a failed insert attempt
func (bc *Blockchain) rollbackInsert(insertedBlocks []*block.Block, revertHeight uint64) {
	log.Printf("[blockchain] rollbackInsert: rolling back %d blocks to height %d",
		len(insertedBlocks), revertHeight)

	// Revert state
	if err := bc.revertToHeight(revertHeight); err != nil {
		log.Printf("[blockchain] rollbackInsert: failed to revert state: %v", err)
	}

	// Delete any partially written blocks from database
	batch := bc.db.DB().NewBatch()
	defer batch.Close()

	for _, blk := range insertedBlocks {
		hash := blk.Hash()
		height := blk.Header.Number.Uint64()

		batch.Delete(db.BlockByHashKey(hash), pebble.NoSync)
		batch.Delete(db.HeaderByNumberKey(height), pebble.NoSync)
		batch.Delete(db.HeaderByHashKey(hash), pebble.NoSync)
		batch.Delete(db.CanonicalHashKey(height), pebble.NoSync)
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		log.Printf("[blockchain] rollbackInsert: failed to clean up database: %v", err)
	}
}

// pruneAncientBlocks moves old blocks to ancient storage
func (bc *Blockchain) pruneAncientBlocks(pruneUpToHeight uint64) {
	if bc.ancientStore == nil {
		return
	}

	start := time.Now()
	pruned := 0

	for height := uint64(1); height <= pruneUpToHeight; height++ {
		hash, err := bc.db.GetCanonicalHash(height)
		if err != nil || hash == (common.Hash{}) {
			continue
		}

		// Read block
		blk, err := bc.db.ReadBlockByHash(hash)
		if err != nil || blk == nil {
			continue
		}

		// Move to ancient storage
		if err := bc.ancientStore.StoreBlock(blk); err != nil {
			log.Printf("[blockchain] pruneAncientBlocks: failed to store block %d in ancient: %v", height, err)
			continue
		}

		// Delete from main database (keep header)
		batch := bc.db.DB().NewBatch()
		batch.Delete(db.BlockByHashKey(hash), pebble.NoSync)
		if err := batch.Commit(pebble.Sync); err != nil {
			log.Printf("[blockchain] pruneAncientBlocks: failed to delete block %d: %v", height, err)
			continue
		}

		pruned++
	}

	if pruned > 0 {
		pruningCounter.Add(float64(pruned))
		log.Printf("[blockchain] pruneAncientBlocks: pruned %d blocks (up to height %d) in %v",
			pruned, pruneUpToHeight, time.Since(start))
	}
}

// WriteBlock writes a single block through the same fork-choice path used for chain inserts.
func (bc *Blockchain) WriteBlock(blk *block.Block) error {
	inserted, err := bc.InsertChain([]*block.Block{blk})
	if err != nil {
		return err
	}
	if inserted == 0 {
		return nil
	}
	return nil
}

// SetReorgDepth sets the maximum reorg depth allowed
func (bc *Blockchain) SetReorgDepth(depth uint64) error {
	if depth > MaxReorgDepth {
		return fmt.Errorf("reorg depth %d exceeds maximum %d", depth, MaxReorgDepth)
	}
	bc.reorgDepth.Store(depth)
	log.Printf("[blockchain] Reorg depth set to %d", depth)
	return nil
}

// GetFinalizedHeight returns the finalized block height
func (bc *Blockchain) GetFinalizedHeight() uint64 {
	return bc.finalizedHeight.Load()
}

func (w *BlockWrapper) Hash() common.Hash {
	return w.block.Hash()
}

func (w *BlockWrapper) Number() uint64 {
	return w.block.Header.Number.Uint64()
}

func (w *BlockWrapper) Miner() common.QuantumAddress {
	return w.block.Header.Coinbase
}

func (w *BlockWrapper) Timestamp() uint64 {
	return w.block.Header.Time
}

// lastCanonicalHeightKey returns the key for storing last canonical height
func lastCanonicalHeightKey() []byte {
	return []byte("L") // Single byte key for fast access
}
