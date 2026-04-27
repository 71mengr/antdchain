// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package chain

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antdaza/antdchain/antdc/block"
	"github.com/antdaza/antdchain/antdc/chain/db"
	"github.com/antdaza/antdchain/antdc/checkpoints"
	"github.com/antdaza/antdchain/antdc/monitoring"
	"github.com/antdaza/antdchain/antdc/pow"
	"github.com/antdaza/antdchain/antdc/reward"
	"github.com/antdaza/antdchain/antdc/rotatingking"
	"github.com/antdaza/antdchain/antdc/staking"
	"github.com/antdaza/antdchain/antdc/state"
	"github.com/antdaza/antdchain/antdc/tx"
	"github.com/antdaza/antdchain/antdc/vm"
	"github.com/antdaza/antdchain/common"
	chaincommon "github.com/antdaza/antdchain/common"
	"github.com/cockroachdb/pebble"

	"github.com/hashicorp/golang-lru"
)

// Constants for initialization
const (
	DefaultCacheSize      = 500  // Cache 500 hot blocks
	MaxStateRebuildBlocks = 1000 // Maximum blocks to rebuild state from
	ContinuityCheckDepth  = 10   // Check only last 10 blocks for continuity
)

// NewBlockchain creates a new blockchain instance with database-first design
func NewBlockchain(statePath string, miner common.QuantumAddress) (*Blockchain, error) {
	initStart := time.Now()
	log.Printf("[blockchain] Initializing blockchain from: %s", statePath)
	minerQuantum, err := chaincommon.NewQuantumAddressFromBytes(miner.Bytes())
	if err != nil {
		return nil, fmt.Errorf("invalid quantum-resistant miner address: %w", err)
	}
	log.Printf("[blockchain] Miner address: %s", minerQuantum.String())

	subDirs := []string{
		"chain",        // Pebble chain database
		"blocks",       // Legacy/migration blocks (warned about in logs)
		"rotatingking", // Rotating king manager DB
		"state",

		// "ancient",
		// "checkpoints",
	}
	for _, sub := range subDirs {
		dirPath := filepath.Join(statePath, sub)
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return nil, fmt.Errorf("failed to create required directory %s: %w", sub, err)
		}
		log.Printf("[blockchain] Ensured directory exists: %s", dirPath)
	}

	// ====================
	// INITIALIZE CHAIN DATABASE
	// ====================
	log.Printf("[blockchain] Initializing chain database...")
	chainDbPath := filepath.Join(statePath, "chain")
	if err := os.MkdirAll(chainDbPath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to create chain db directory: %w", err)
	}
	chainDb, err := db.NewChainDB(statePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open chain database: %w", err)
	}

	// ====================
	// LOAD CANONICAL TIP FROM DATABASE
	// ====================
	log.Printf("[blockchain] Loading chain tip from database...")

	var latest *block.Block
	var genesisCreated bool

	// Try to get head block hash
	headHash, err := chainDb.GetHeadBlockHash()
	if err != nil {
		chainDb.Close()
		return nil, fmt.Errorf("failed to get head block hash: %w", err)
	}

	if headHash != (common.Hash{}) {
		// Load tip block from database
		latest, err = chainDb.ReadBlockByHash(headHash)
		if err != nil {
			chainDb.Close()
			return nil, fmt.Errorf("failed to read head block %s: %w", headHash.String(), err)
		}
		if latest == nil {
			log.Printf("[blockchain] Head block not found, will create genesis")
			headHash = common.Hash{}
		} else {
			// Quick continuity check (only parent of tip)
			if err := verifyTipContinuity(chainDb, latest); err != nil {
				log.Printf("[blockchain] Tip continuity error: %v", err)
				// Try to find valid tip
				repairedTip, err := findValidChainTipFast(chainDb)
				if err != nil || repairedTip == nil {
					chainDb.Close()
					return nil, fmt.Errorf("chain tip invalid and no valid alternative found: %w", err)
				}
				latest = repairedTip
				log.Printf("[blockchain] Using alternative tip: height=%d, hash=%s",
					latest.Header.Number.Uint64(), latest.Hash().String())
			}
		}
	}

	// Check if we need to create genesis
	if headHash == (common.Hash{}) {
		log.Printf("[blockchain] No chain found in database, creating genesis...")

		genesis, err := EnsureGenesisBlock(statePath, miner)
		if err != nil {
			chainDb.Close()
			return nil, fmt.Errorf("failed to create genesis block: %w", err)
		}

		// Store genesis atomically
		if err := chainDb.WriteBlock(genesis); err != nil {
			chainDb.Close()
			return nil, fmt.Errorf("failed to write genesis block: %w", err)
		}

		// Mark as canonical and set as head
		if err := chainDb.WriteCanonicalHash(0, genesis.Hash()); err != nil {
			chainDb.Close()
			return nil, fmt.Errorf("failed to write canonical hash: %w", err)
		}

		if err := chainDb.WriteHeadBlockHash(genesis.Hash()); err != nil {
			chainDb.Close()
			return nil, fmt.Errorf("failed to write head block hash: %w", err)
		}

		latest = genesis
		genesisCreated = true

		log.Printf("[blockchain] Genesis block created and stored: %s", genesis.Hash().String())
	} else {
		log.Printf("[blockchain] Existing chain tip loaded: height=%d, hash=%s",
			latest.Header.Number.Uint64(), latest.Hash().String())
	}

	// ====================
	// INITIALIZE STATE DATABASE
	// ====================
	log.Printf("[blockchain] Initializing state database...")
	stateDb, err := state.NewState(statePath)
	if err != nil {
		chainDb.Close()
		return nil, fmt.Errorf("failed to create state: %w", err)
	}

	// ====================
	// CREATE CACHES (FOR HOT BLOCKS ONLY)
	// ====================
	log.Printf("[blockchain] Creating LRU caches...")

	blockByNumberCache, err := lru.New(DefaultCacheSize)
	if err != nil {
		chainDb.Close()
		stateDb.Close()
		return nil, fmt.Errorf("failed to create block number cache: %w", err)
	}

	blockByHashCache, err := lru.New(DefaultCacheSize)
	if err != nil {
		chainDb.Close()
		stateDb.Close()
		return nil, fmt.Errorf("failed to create block hash cache: %w", err)
	}

	// ====================
	// CREATE BLOCKCHAIN INSTANCE
	// ====================
	log.Printf("[blockchain] Creating blockchain instance...")

	bc := &Blockchain{
		db:             chainDb,
		state:          stateDb,
		stakingManager: nil,
		txPool:         nil,
		pow:            nil,
		//		checkpoints:         checkpointMgr,
		statePath:           statePath,
		rewardDistributor:   nil,
		governance:          nil,
		stateMu:             sync.Mutex{},
		minConfirmations:    10,
		monitor:             nil,
		blockSubmitMu:       sync.Mutex{},
		rotatingKingManager: nil,
		syncing:             atomic.Bool{},
		syncTarget:          atomic.Uint64{},
		syncMu:              sync.RWMutex{},
		p2pBroadcaster:      nil,
		blockByNumberCache:  blockByNumberCache,
		blockByHashCache:    blockByHashCache,
		cacheMu:             sync.RWMutex{},
		lastCanonicalHeight: atomic.Uint64{},
		reorgDepth:          atomic.Uint64{},
		ancientStore:        nil,
		finalizedHeight:     atomic.Uint64{},
		orphanBlocks:        make(map[common.Hash]*orphanBlock),
	}

	bc.stakingManager = staking.NewStakingManager(stateDb, pow.MinStakeAmount)
	bc.stakingManager.SetBlockHeightProvider(func() uint64 {
		return bc.GetChainHeight()
	})
	stakingStateKey := []byte("staking:state:v1")
	bc.stakingManager.SetPersistFunc(func(data []byte) error {
		return chainDb.DB().Set(stakingStateKey, data, pebble.Sync)
	})
	if data, closer, err := chainDb.DB().Get(stakingStateKey); err == nil {
		snapshot := append([]byte(nil), data...)
		closer.Close()
		if err := bc.stakingManager.LoadSnapshot(snapshot); err != nil {
			log.Printf("[staking] Warning: Failed to restore staking snapshot: %v", err)
		} else {
			log.Printf("[staking] Restored staking snapshot from database")
		}
	} else if !errors.Is(err, db.ErrNotFound) {
		log.Printf("[staking] Warning: Failed to read staking snapshot: %v", err)
	}

	// Set atomic fields
	if latest != nil {
		bc.latest.Store(latest)
		bc.lastCanonicalHeight.Store(latest.Header.Number.Uint64())

		// Cache the tip block
		bc.cacheMu.Lock()
		bc.blockByNumberCache.Add(latest.Header.Number.Uint64(), latest)
		bc.blockByHashCache.Add(latest.Hash(), latest)
		bc.cacheMu.Unlock()
	}

	// Set default reorg depth
	bc.reorgDepth.Store(DefaultReorgDepth)

	// Check for ancient blocks directory
	ancientPath := filepath.Join(statePath, "ancient")
	if _, err := os.Stat(ancientPath); err == nil {
		bc.ancientStore = NewAncientStore(statePath)
		log.Printf("[blockchain] Ancient block store found at: %s", ancientPath)
	}

	// ====================
	// MIGRATE LEGACY JSON BLOCKS
	// ====================
	migrationStart := time.Now()
	migrated, err := migrateLegacyJSONBlocksToDB(chainDb, statePath)
	if err != nil {
		log.Printf("[blockchain] Warning: Failed to migrate legacy blocks: %v", err)
		// Continue anyway
	} else if migrated > 0 {
		log.Printf("[blockchain] Migrated %d legacy blocks to database in %v",
			migrated, time.Since(migrationStart))

		// Reload latest block after migration
		headHash, err = chainDb.GetHeadBlockHash()
		if err == nil && headHash != (common.Hash{}) {
			newLatest, err := chainDb.ReadBlockByHash(headHash)
			if err == nil && newLatest != nil {
				bc.latest.Store(newLatest)
				bc.lastCanonicalHeight.Store(newLatest.Header.Number.Uint64())
				latest = newLatest

				// Update cache
				bc.cacheMu.Lock()
				bc.blockByNumberCache.Add(newLatest.Header.Number.Uint64(), newLatest)
				bc.blockByHashCache.Add(newLatest.Hash(), newLatest)
				bc.cacheMu.Unlock()
			}
		}
	}

	// ====================
	// INITIALIZE ROTATING KING MANAGER
	// ====================
	log.Printf("[blockchain] Initializing rotating king manager...")

	var provider rotatingking.BlockchainProvider = &blockchainProviderWrapper{bc: bc}
	rotatingKingPath := filepath.Join(statePath, "rotatingking")

	rotatingKingManager, err := rotatingking.NewRotatingKingManager(
		rotatingKingPath,
		provider,
		false,
		nil,
		chainDb.GetRotatingKingDatabase(),
	)
	if err != nil {
		log.Printf("Warning: Rotating king manager init failed: %v", err)
		// Continue without rotating king manager
	} else {
		bc.rotatingKingManager = rotatingKingManager

		// Sync rotating king database in background if needed
		currentHeight := bc.GetChainHeight()
		if currentHeight > 0 {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				if err := rotatingKingManager.SyncBlocks(ctx, currentHeight); err != nil {
					log.Printf("[blockchain] Rotating king sync failed: %v", err)
				} else {
					log.Printf("[blockchain] Rotating king database synced to height %d", currentHeight)
				}
			}()
		}

		// Start periodic sync
		go startPeriodicRotatingKingSync(bc, 30*time.Second)
		log.Printf("[blockchain] Rotating king manager initialized")
	}

	// ====================
	// INITIALIZE PROOF OF STAKE ENGINE
	// ====================
	log.Printf("[blockchain] Initializing Proof of Stake engine...")
	posEngine := pow.NewPoW()
	bc.pow = posEngine
	bc.restoreActiveStakersToPoW()

	// ====================
	// INITIALIZE MAIN KING
	// ====================
	mainKingParsed, err := common.ParseQuantumAddress(GenesisMainKing)
	if err != nil {
		chainDb.Close()
		stateDb.Close()
		return nil, fmt.Errorf("invalid main king address: %w", err)
	}
	mainKingQuantum, err := chaincommon.NewQuantumAddressFromBytes(mainKingParsed.Bytes())
	if err != nil {
		chainDb.Close()
		stateDb.Close()
		return nil, fmt.Errorf("invalid quantum-resistant main king address: %w", err)
	}
	mainKing := common.QuantumAddress(mainKingQuantum)
	log.Printf("[blockchain] Main King loaded: %s", mainKingQuantum.String())
	if miner != (common.QuantumAddress{}) && miner != mainKing {
		log.Printf("[blockchain] Local miner loaded: %s", minerQuantum.String())
	}
	// ====================
	// INITIALIZE REWARD SYSTEM
	// ====================
	log.Printf("[blockchain] Initializing reward system...")
	distributor := reward.NewRewardDistributor(mainKing)
	bc.rewardDistributor = distributor

	// ====================
	// INITIALIZE TRANSACTION POOL
	// ====================
	log.Printf("[blockchain] Initializing transaction pool...")
	txPool := NewTxPool()
	txPool.SetBlockchain(bc)
	bc.txPool = txPool

	if err := bc.txPool.LoadFromDisk(statePath, bc); err != nil {
		log.Printf("[blockchain] Warning: Failed to load saved transactions: %v", err)
		// Continue without saved transactions
	}

	// Start background cleanup
	bc.txPool.StartBackgroundCleanup(bc)

	// ====================
	// INITIALIZE SUPPLY MONITOR
	// ====================
	log.Printf("[blockchain] Initializing supply monitor...")
	cfg := monitoring.MonitorConfig{
		MainKingAddress: mainKing,
		AlertThreshold:  new(big.Int).Mul(big.NewInt(100000), big.NewInt(1e18)),
		MaxSupply:       new(big.Int).Mul(big.NewInt(100000000000), big.NewInt(1e18)),
		CheckInterval:   30 * time.Second,
		EnableRealTime:  true,
		LogFile:         filepath.Join(statePath, "monitor.log"),
	}

	isMainKingNode := miner == mainKing

	// Create monitoring adapter that implements both interfaces
	monitoringAdapter := &blockchainAdapter{
		Blockchain: bc,
	}

	bc.monitor = monitoring.NewSupplyMonitor(monitoringAdapter, cfg, isMainKingNode)

	// Start monitoring in background
	go func() {
		log.Printf("[blockchain] Starting background monitoring...")
		bc.monitor.StartMonitoring()
	}()

	// ====================
	// VERIFY STATE CONSISTENCY (Limited check)
	// ====================
	if !genesisCreated && latest.Header.Number.Uint64() > 0 {
		currentRoot := stateDb.Root()
		if latest.Header.Root != currentRoot {
			log.Printf("[blockchain] Warning: State root mismatch!")
			log.Printf("[blockchain]   Latest block root: %s", latest.Header.Root.String())
			log.Printf("[blockchain]   Current state root: %s", currentRoot.String())
			log.Printf("[blockchain]   Will attempt limited state repair if needed")

			// Don't auto-rebuild - just log warning
			// State will be rebuilt on-demand if needed
		} else {
			log.Printf("[blockchain] State root matches latest block")
		}
	}

	// ====================
	// FINAL INITIALIZATION LOGGING
	// ====================
	initTime := time.Since(initStart)
	log.Printf("[blockchain] Blockchain initialized successfully in %v!", initTime)
	log.Printf("[blockchain]   Chain height:    %d", bc.GetChainHeight())
	log.Printf("[blockchain]   State root:      %s", stateDb.Root().String())
	log.Printf("[blockchain]   Tip hash:        %s", latest.Hash().String())
	log.Printf("[blockchain]   PoS difficulty:  %s", posEngine.GetDifficulty().String())
	log.Printf("[blockchain]   Miner address:   %s", miner.String())
	log.Printf("[blockchain]   Is Main King:    %v", isMainKingNode)
	log.Printf("[blockchain]   Cache size:      %d blocks", DefaultCacheSize)

	if bc.rotatingKingManager != nil {
		syncState, err := rotatingKingManager.GetSyncState()
		if err == nil {
			log.Printf("[blockchain]   Rotating King DB: Height=%d, Progress=%.1f%%",
				syncState.LastSyncedBlock, syncState.SyncProgress*100)
		}
	}

	// Initialize metrics
	blockHeightGauge.Set(float64(bc.GetChainHeight()))

	return bc, nil
}

func (bc *Blockchain) restoreActiveStakersToPoW() {
	if bc == nil || bc.stakingManager == nil || bc.pow == nil {
		return
	}

	restored := 0
	for _, record := range bc.stakingManager.GetStakeRecords() {
		if !record.IsActive || record.Amount == nil || record.Amount.Sign() <= 0 {
			continue
		}
		bc.pow.AutoRegisterIfEligible(record.Address, record.Amount, nil)
		restored++
	}

	if restored > 0 {
		log.Printf("[staking] Restored %d active staker(s) into PoS engine", restored)
	}
}

// verifyTipContinuity checks continuity for tip block only (fast)
func verifyTipContinuity(chainDb *db.ChainDB, tip *block.Block) error {
	if tip == nil {
		return errors.New("tip is nil")
	}

	height := tip.Header.Number.Uint64()
	if height == 0 {
		return nil // Genesis is always valid
	}

	// Check parent exists
	parentHash := tip.Header.ParentHash
	parentHeader, err := chainDb.ReadHeaderByHash(parentHash)
	if err != nil {
		return fmt.Errorf("failed to read parent header: %w", err)
	}
	if parentHeader == nil {
		return errors.New("parent header not found")
	}

	// Verify height continuity
	if parentHeader.Number.Uint64()+1 != height {
		return fmt.Errorf("height discontinuity: parent=%d, tip=%d",
			parentHeader.Number.Uint64(), height)
	}

	// Check if parent is canonical (optional but fast)
	if height <= ContinuityCheckDepth {
		canonParent, _ := chainDb.GetCanonicalHash(parentHeader.Number.Uint64())
		if canonParent != (common.Hash{}) && canonParent != parentHash {
			return fmt.Errorf("parent not in canonical chain")
		}
	}

	return nil
}

// findValidChainTipFast finds a valid chain tip using fast heuristics
func findValidChainTipFast(chainDb *db.ChainDB) (*block.Block, error) {
	log.Printf("[chaindb] Searching for valid chain tip...")

	// Method 1: Try last canonical height from database
	data, closer, err := chainDb.DB().Get([]byte("lastCanonicalHeight"))
	if err == nil && len(data) == 8 {
		defer closer.Close()
		height := binary.BigEndian.Uint64(data)
		hash, _ := chainDb.GetCanonicalHash(height)
		if hash != (common.Hash{}) {
			blk, _ := chainDb.ReadBlockByHash(hash)
			if blk != nil && verifyTipContinuity(chainDb, blk) == nil {
				log.Printf("[chaindb] Found tip via last height key: height=%d", height)
				return blk, nil
			}
		}
	}

	// Method 2: Try genesis
	genesisHash, _ := chainDb.GetCanonicalHash(0)
	if genesisHash != (common.Hash{}) {
		genesis, _ := chainDb.ReadBlockByHash(genesisHash)
		if genesis != nil {
			log.Printf("[chaindb] Using genesis as fallback tip")
			return genesis, nil
		}
	}

	// Method 3: Scan for highest block with valid parent (limited scan)
	maxHeight := uint64(0)
	var bestHash common.Hash

	// Only scan recent heights for performance
	for height := uint64(1000); height > 0; height-- {
		hash, _ := chainDb.GetCanonicalHash(height)
		if hash != (common.Hash{}) {
			blk, _ := chainDb.ReadBlockByHash(hash)
			if blk != nil && verifyTipContinuity(chainDb, blk) == nil {
				maxHeight = height
				bestHash = hash
				break
			}
		}
	}

	if bestHash != (common.Hash{}) {
		blk, _ := chainDb.ReadBlockByHash(bestHash)
		if blk != nil {
			log.Printf("[chaindb] Found valid tip at height %d via limited scan", maxHeight)
			return blk, nil
		}
	}

	return nil, errors.New("no valid chain tip found")
}

// rebuildStateFromHeight rebuilds state from specific height (on-demand, not auto-called)
func (bc *Blockchain) rebuildStateFromHeight(fromHeight, toHeight uint64) error {
	if fromHeight >= toHeight {
		return fmt.Errorf("invalid height range: from=%d, to=%d", fromHeight, toHeight)
	}

	log.Printf("[blockchain] Starting state rebuild from height %d to %d...", fromHeight, toHeight)
	startTime := time.Now()

	// Limit rebuild to prevent excessive replay
	if toHeight-fromHeight > MaxStateRebuildBlocks {
		fromHeight = toHeight - MaxStateRebuildBlocks
		log.Printf("[blockchain] Limiting rebuild to last %d blocks", MaxStateRebuildBlocks)
	}

	bc.stateMu.Lock()
	defer bc.stateMu.Unlock()

	// Create fresh state
	newState, err := state.NewState(bc.statePath)
	if err != nil {
		return fmt.Errorf("failed to create new state: %w", err)
	}

	// Track old state for cleanup
	oldState := bc.state

	// Replay blocks in order
	blocksReplayed := 0
	for height := fromHeight; height <= toHeight; height++ {
		// Get block from database
		hash, err := bc.db.GetCanonicalHash(height)
		if err != nil || hash == (common.Hash{}) {
			return fmt.Errorf("missing block at height %d: %w", height, err)
		}

		blk, err := bc.db.ReadBlockByHash(hash)
		if err != nil || blk == nil {
			return fmt.Errorf("failed to read block at height %d: %w", height, err)
		}

		// Skip genesis (already initialized)
		if height == 0 {
			continue
		}

		// Execute block on new state - convert interface to pointer
		var rkmPtr *rotatingking.RotatingKingManager
		if bc.rotatingKingManager != nil {
			if ptr, ok := bc.rotatingKingManager.(*rotatingking.RotatingKingManager); ok {
				rkmPtr = ptr
			}
		}

		if err := executeBlockOnState(blk, newState, bc.rewardDistributor, rkmPtr, bc.pow); err != nil {
			newState.Close()
			return fmt.Errorf("failed to execute block %d: %w", height, err)
		}

		blocksReplayed++

		// Log progress
		if blocksReplayed%100 == 0 || height == toHeight {
			progress := float64(height-fromHeight) / float64(toHeight-fromHeight) * 100
			log.Printf("[blockchain] State rebuild: %d/%d blocks (%.1f%%)",
				height-fromHeight, toHeight-fromHeight, progress)
		}
	}

	// Update blockchain state
	bc.state = newState
	if oldState != nil {
		oldState.Close()
	}

	rebuildTime := time.Since(startTime)
	log.Printf("[blockchain] State rebuild completed in %v (%d blocks replayed)",
		rebuildTime, blocksReplayed)
	log.Printf("[blockchain] New state root: %s", bc.state.Root().String())

	return nil
}

// executeBlockOnState executes a block on a specific state instance
func executeBlockOnState(blk *block.Block, st *state.State,
	rewardDist *reward.RewardDistributor, rkm *rotatingking.RotatingKingManager, pow *pow.PoW) error {

	ctx := context.Background()

	// Execute transactions if any
	if len(blk.Txs) > 0 {
		v := vm.NewVM(st, blk.Header.GasLimit)

		for _, tx := range blk.Txs {
			if tx == nil {
				continue
			}

			_, _, err := v.Execute(ctx, tx)
			if err != nil {
				return fmt.Errorf("transaction execution failed: %w", err)
			}
		}
	}

	// Distribute rewards
	blockTime := blk.Header.Time
	totalFees := big.NewInt(0) // TODO: Calculate actual fees

	// Convert rotatingking.RotatingKingManager to reward.RotatingKingManager interface
	var rewardRKM reward.RotatingKingManager
	if rkm != nil {
		rewardRKM = rkm
	}

	_, err := rewardDist.DistributeRewards(
		st,
		blk.Header.Coinbase,
		totalFees,
		blk.Header.Number.Uint64(),
		blockTime,
		rewardRKM,
		pow,
	)

	return err
}

// createDefaultCheckpointConfig creates default checkpoint config
func createDefaultCheckpointConfig(configPath string, nodeName string) error {
	if _, err := os.Stat(configPath); err == nil {
		return nil // Config already exists
	}

	config := checkpoints.LocalConfig{
		AuthorityName: nodeName,
		Weight:        100,
		SyncInterval:  1 * time.Hour,
		TrustedRemotes: []checkpoints.RemoteConfig{
			{
				URL:        "https://checkpoints.antdaza.site/mainnet",
				Name:       "Official ANTDChain Checkpoints",
				Priority:   1,
				Enabled:    true,
				Timeout:    30 * time.Second,
				RetryCount: 3,
				ChainID:    "antdchain-mainnet",
			},
		},
	}

	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}

	dir := filepath.Dir(configPath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return err
	}

	return os.WriteFile(configPath, data, 0600)
}

// Helper types and functions

type blockchainProviderWrapper struct {
	bc *Blockchain
}

func (w *blockchainProviderWrapper) GetChainHeight() uint64 {
	return w.bc.GetChainHeight()
}

func (w *blockchainProviderWrapper) GetBlock(height uint64) interface{} {
	blk := w.bc.GetBlock(height)
	if blk != nil {
		return blk
	}
	return nil
}

func (w *blockchainProviderWrapper) State() interface{} {
	return w.bc.State()
}

// migrateLegacyJSONBlocksToDB migrates legacy JSON block files to database
func migrateLegacyJSONBlocksToDB(chainDb *db.ChainDB, statePath string) (int, error) {
	blocksDir := filepath.Join(statePath, "blocks")
	if _, err := os.Stat(blocksDir); os.IsNotExist(err) {
		return 0, nil // No legacy blocks directory
	}

	log.Printf("[migration] Scanning legacy blocks in %s", blocksDir)

	files, err := os.ReadDir(blocksDir)
	if err != nil {
		return 0, fmt.Errorf("failed to read blocks directory: %w", err)
	}

	migrated := 0
	for _, file := range files {
		if !file.IsDir() && len(file.Name()) > 5 && file.Name()[len(file.Name())-5:] == ".json" {
			filePath := filepath.Join(blocksDir, file.Name())
			data, err := os.ReadFile(filePath)
			if err != nil {
				log.Printf("[migration] Warning: Failed to read %s: %v", file.Name(), err)
				continue
			}

			var legacyBlock struct {
				Header *block.Header `json:"header"`
				Txs    []*tx.Tx      `json:"transactions"`
			}

			if err := json.Unmarshal(data, &legacyBlock); err != nil {
				log.Printf("[migration] Warning: Failed to parse %s: %v", file.Name(), err)
				continue
			}

			if legacyBlock.Header == nil {
				log.Printf("[migration] Warning: No header in %s", file.Name())
				continue
			}

			// Create block object
			blk := &block.Block{
				Header: legacyBlock.Header,
				Txs:    legacyBlock.Txs,
			}

			// Write to database
			if err := chainDb.WriteBlock(blk); err != nil {
				log.Printf("[migration] Warning: Failed to write block %s: %v",
					legacyBlock.Header.Hash().String(), err)
				continue
			}

			migrated++

			// Log progress
			if migrated%100 == 0 {
				log.Printf("[migration] Migrated %d blocks...", migrated)
			}
		}
	}

	if migrated > 0 {
		log.Printf("[migration] Completed: Migrated %d legacy blocks to database", migrated)
	}

	return migrated, nil
}

// Helper function for periodic rotating king sync
func startPeriodicRotatingKingSync(bc *Blockchain, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		if bc.rotatingKingManager != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			currentHeight := bc.GetChainHeight()
			if currentHeight > 0 {
				if err := bc.rotatingKingManager.SyncBlocks(ctx, currentHeight); err != nil {
					log.Printf("[blockchain] Periodic rotating king sync failed: %v", err)
				}
			}
			cancel()
		}
	}
}
