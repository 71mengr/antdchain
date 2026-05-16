// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package mining

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
	mathrand "math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	qkeystore "github.com/antdaza/antdchain/antdc/accounts/keystore"
	"github.com/antdaza/antdchain/antdc/block"
	"github.com/antdaza/antdchain/antdc/chain"
	"github.com/antdaza/antdchain/antdc/crypto/quantum"
	"github.com/antdaza/antdchain/antdc/p2p"
	"github.com/antdaza/antdchain/antdc/pow"
	"github.com/antdaza/antdchain/antdc/reward"
	"github.com/antdaza/antdchain/antdc/tx"
	"github.com/antdaza/antdchain/common"
	"github.com/antdaza/antdchain/difficulty"
	"github.com/prometheus/client_golang/prometheus"
)

// ============================================================================
// ANTD-GRADE CONSTANTS
// ============================================================================

const (
	// Mining intervals 
	DefaultMiningInterval          = 10 * time.Second
	MinimumMiningInterval          = 5 * time.Second
	MaxMiningInterval               = 60 * time.Second
	AdaptiveMiningInterval         = true
	
	// Broadcast settings (Bitcoin relay network style)
	DefaultBroadcastMaxRetries     = 10
	DefaultBroadcastInitialBackoff = 50 * time.Millisecond
	DefaultBroadcastMaxBackoff     = 5 * time.Second
	
	// Block resource limits (Bitcoin Core 25.0 defaults)
	DefaultBlockReservedWeight = 4000
	MinimumBlockReservedWeight = 4000
	MaxBlockWeight             = 4000000
	MaxBlockSigOpsCost         = 80000
	MaxBlockBaseSize           = 1000000
	
	// Block assembly limits
	MaxConsecutiveFailures     = 1000
	BlockFullEnoughWeightDelta = 4000
	MaxOrphanTransactions      = 100
	MaxPackageCount            = 25
	MaxAncestorCount           = 25
	MaxDescendantCount         = 25
	
	// Timewarp protection (BIP94)
	MaxTimewarp = 600 // 10 minutes in seconds
	
	// Compact block relay (BIP152)
	CompactBlockVersion        = 2
	CompactBlockReconstruction = true
	
	// SegWit (BIP141)
	WitnessCommitmentHeader = "aa21a9ed"
	
	// Merkle tree limits
	MaxMerkleTreeDepth = 32
	
	// Miner monitoring
	LogEligibilityCheckInterval = 10
	LogSyncStatusInterval       = 10 * time.Second
	PendingBlockTimeout         = 2 * time.Second
	
	// Multi-miner settings
	DefaultMaxConcurrentMiners = 16
	DefaultMinerThreads        = 4
	DefaultMiningCacheSize     = 10000
	
	// Stratum protocol (Bitcoin mining pool support)
	StratumPort                 = 3333
	StratumProtocolVersion      = "2.0"
)

// ============================================================================
// PROMETHEUS METRICS
// ============================================================================

var (
	miningBlocksTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "antdchain_mining_blocks_total",
			Help: "Total blocks successfully mined by each miner",
		},
		[]string{"miner_address"},
	)
	miningEligibilityChecks = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "antdchain_mining_eligibility_checks_total",
			Help: "Eligibility checks by result",
		},
		[]string{"result"},
	)
	miningBroadcastSuccess = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "antdchain_mining_broadcast_success_total",
			Help: "Successful block broadcasts",
		},
	)
	miningBroadcastFailures = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "antdchain_mining_broadcast_failures_total",
			Help: "Failed block broadcasts",
		},
	)
	miningSessionDuration = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "antdchain_mining_session_duration_seconds",
			Help: "Current mining session duration in seconds",
		},
	)
	miningUptime = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "antdchain_mining_uptime_seconds",
			Help: "Total mining uptime in seconds",
		},
	)
	miningRewardsTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "antdchain_mining_rewards_total_antd",
			Help: "Total mining rewards earned in ANTD",
		},
		[]string{"miner_address"},
	)
	miningHashrate = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "antdchain_mining_hashrate_hps",
			Help: "Mining hashrate in hashes per second",
		},
		[]string{"miner_address"},
	)
	miningStaleBlocks = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "antdchain_mining_stale_blocks_total",
			Help: "Stale blocks mined",
		},
		[]string{"miner_address"},
	)
)

func init() {
	prometheus.MustRegister(miningBlocksTotal)
	prometheus.MustRegister(miningEligibilityChecks)
	prometheus.MustRegister(miningBroadcastSuccess)
	prometheus.MustRegister(miningBroadcastFailures)
	prometheus.MustRegister(miningSessionDuration)
	prometheus.MustRegister(miningUptime)
	prometheus.MustRegister(miningRewardsTotal)
	prometheus.MustRegister(miningHashrate)
	prometheus.MustRegister(miningStaleBlocks)
}

// ============================================================================
// MINER STRUCTS (Bitcoin-style multi-miner support)
// ============================================================================

// TxEntry represents a mempool transaction with Bitcoin-style metrics
type TxEntry struct {
	Tx          *tx.Tx
	Fee         *big.Int
	ModifiedFee *big.Int
	AncestorCount int
	DescendantCount int
	Weight      uint64
	SigOpsCost  uint64
	TxSize      uint64
	Height      uint64
	Time        uint64
	Score       *big.Int // For fee-based sorting
}

// IsFinal checks if transaction is final (BIP125 locktime verification)
func (txe *TxEntry) IsFinal(blockHeight uint64, blockTime uint64) bool {
	if txe.Tx == nil {
		return false
	}
	return true
}

// GetPriority calculates Bitcoin-style priority (coin age)
func (txe *TxEntry) GetPriority(blockHeight uint64) float64 {
	if txe.TxSize == 0 {
		return 0
	}
	// Sum input ages (simplified)
	age := blockHeight - txe.Height
	if age > 0 {
		return float64(age) / float64(txe.TxSize)
	}
	return 0
}

// BlockTemplate represents a candidate block 
type BlockTemplate struct {
	Block           *block.Block
	TxFees          []*big.Int
	TxSigOpsCosts   []uint64
	PackageFeerates []uint64
	TotalFees       *big.Int
	TotalWeight     uint64
	TotalSigOpsCost uint64
	CoinbaseValue   *big.Int
	CreatedAt       time.Time
}

// BlockAssemblerOptions (Bitcoin Core 25.0 style)
type BlockAssemblerOptions struct {
	BlockMaxWeight                    uint64
	BlockMaxSize                      uint64
	BlockMinFeeRate                   uint64
	BlockReservedWeight               *uint64
	CoinbaseOutputMaxAdditionalSigops uint64
	PrintModifiedFee                  bool
	TestBlockValidity                 bool
	IncludeDummyExtranonce            bool
	UseMempool                        bool
	IgnoreMempoolLimits               bool
	UseSegWit                         bool
	UseCompactBlocks                  bool
}

// DefaultBlockAssemblerOptions returns Bitcoin-like defaults
func DefaultBlockAssemblerOptions() BlockAssemblerOptions {
	reservedWeight := uint64(DefaultBlockReservedWeight)
	return BlockAssemblerOptions{
		BlockMaxWeight:                    MaxBlockWeight,
		BlockMaxSize:                      MaxBlockBaseSize,
		BlockMinFeeRate:                   1000, // 1 sat/byte equivalent
		BlockReservedWeight:               &reservedWeight,
		CoinbaseOutputMaxAdditionalSigops: 0,
		PrintModifiedFee:                  false,
		TestBlockValidity:                 true,
		IncludeDummyExtranonce:            true,
		UseMempool:                        true,
		IgnoreMempoolLimits:               false,
		UseSegWit:                         true,
		UseCompactBlocks:                  true,
	}
}

// MinerInstance represents an individual miner (multi-miner support)
type MinerInstance struct {
	ID               string
	Address          common.QuantumAddress
	PrivateKey       []byte
	Threads          int
	Hashrate         uint64
	BlocksMined      uint64
	StaleBlocks      uint64
	TotalRewards     *big.Int
	LastMinedHeight  uint64
	LastMinedTime    time.Time
	IsActive         bool
	mu               sync.RWMutex
}

// NewMinerInstance creates a new miner instance
func NewMinerInstance(id string, addr common.QuantumAddress, threads int) *MinerInstance {
	return &MinerInstance{
		ID:           id,
		Address:      addr,
		Threads:      threads,
		TotalRewards: big.NewInt(0),
		IsActive:     false,
	}
}

// PosMiningState (Bitcoin-style with multi-miner support)
type PosMiningState struct {
	mining        bool
	enabled       bool
	powEngine     *pow.PoW
	
	// Multi-miner management
	miners        map[string]*MinerInstance
	activeMiners  map[string]*MinerInstance
	defaultMiner  string
	minerMu       sync.RWMutex
	
	// Global settings
	miningInterval          time.Duration
	adaptiveInterval        bool
	broadcastMaxRetries     int
	broadcastInitialBackoff time.Duration
	broadcastMaxBackoff     time.Duration
	pendingBlockTimeout     time.Duration
	
	// Block assembly
	assembler     *BlockAssembler
	assemblerOpts BlockAssemblerOptions
	
	// Orphan block management 
	orphanBlocks     map[common.Hash]*orphanBlockEntry
	orphanMu         sync.RWMutex
	
	// Pending heights (for block propagation)
	pendingHeights map[uint64]time.Time
	pendingMu      sync.RWMutex
	
	// Work queue for miners
	workQueue    chan *MiningWork
	workResult   chan *MiningResult
	
	// Statistics
	globalBlocksMined  uint64
	globalTotalRewards *big.Int
	
	// Performance
	currentHashrate   uint64
	hashrateMu        sync.RWMutex
	
	// Stale block tracking
	staleBlocks       map[uint64]common.Hash
	staleMu           sync.RWMutex
	
	// Context
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	
	// Callbacks
	onSyncChange    func(isSyncing bool)
	
	// Current work template
	currentTemplate *BlockTemplate
	templateMu      sync.RWMutex
}

// MiningWork represents work for a miner
type MiningWork struct {
	Block      *block.Block
	Header     *block.Header
	NonceStart uint64
	NonceRange uint64
	MinerID    string
	Height     uint64
	Difficulty *big.Int
	Timestamp  time.Time
}

// MiningResult represents a mining result
type MiningResult struct {
	WorkerID   string
	Nonce      [8]byte
	MixDigest  common.Hash
	Block      *block.Block
	Success    bool
	Duration   time.Duration
	HashRate   uint64
	MinerID    string
}

// Orphan block entry
type orphanBlockEntry struct {
	Block     *block.Block
	ExpiresAt time.Time
	Reason    string
}

// ============================================================================
// BLOCK ASSEMBLER (Bitcoin Core style)
// ============================================================================

// BlockAssembler handles block template creation (Bitcoin Core algorithm)
type BlockAssembler struct {
	chainstate      *chain.Blockchain
	options         BlockAssemblerOptions
	blockWeight     uint64
	blockSigOpsCost uint64
	blockTx         uint32
	blockFees       *big.Int
	height          uint64
	lockTimeCutoff  uint64
	template        *BlockTemplate
	timeStart       time.Time
	timeBuild       time.Duration
	feeEstimator    *FeeEstimator
}

// FeeEstimator for dynamic fee estimation (Bitcoin Core style)
type FeeEstimator struct {
	historicalFees []uint64
	mu             sync.RWMutex
}

func NewFeeEstimator() *FeeEstimator {
	return &FeeEstimator{
		historicalFees: make([]uint64, 0, 1008), // 1 week of blocks
	}
}

func (fe *FeeEstimator) AddBlockFee(feePerKB uint64) {
	fe.mu.Lock()
	defer fe.mu.Unlock()
	fe.historicalFees = append(fe.historicalFees, feePerKB)
	if len(fe.historicalFees) > 1008 {
		fe.historicalFees = fe.historicalFees[1:]
	}
}

func (fe *FeeEstimator) EstimateFee(blocksTarget int) uint64 {
	fe.mu.RLock()
	defer fe.mu.RUnlock()
	
	if len(fe.historicalFees) == 0 {
		return 1000 // Default min fee
	}
	
	// Calculate median fee for target blocks
	windowSize := blocksTarget * 6 // Convert to ~10-min blocks
	if windowSize > len(fe.historicalFees) {
		windowSize = len(fe.historicalFees)
	}
	
	window := make([]uint64, windowSize)
	copy(window, fe.historicalFees[len(fe.historicalFees)-windowSize:])
	sort.Slice(window, func(i, j int) bool { return window[i] < window[j] })
	
	return window[len(window)/2]
}

// NewBlockAssembler creates a new Bitcoin-style block assembler
func NewBlockAssembler(chainstate *chain.Blockchain, opts BlockAssemblerOptions) *BlockAssembler {
	return &BlockAssembler{
		chainstate:   chainstate,
		options:      clampOptions(opts),
		blockFees:    big.NewInt(0),
		feeEstimator: NewFeeEstimator(),
	}
}

func clampOptions(opts BlockAssemblerOptions) BlockAssemblerOptions {
	reservedWeight := uint64(DefaultBlockReservedWeight)
	if opts.BlockReservedWeight != nil {
		reservedWeight = *opts.BlockReservedWeight
	}
	if reservedWeight < MinimumBlockReservedWeight {
		reservedWeight = MinimumBlockReservedWeight
	}
	if reservedWeight > MaxBlockWeight {
		reservedWeight = MaxBlockWeight
	}
	opts.BlockReservedWeight = &reservedWeight
	
	if opts.CoinbaseOutputMaxAdditionalSigops > MaxBlockSigOpsCost {
		opts.CoinbaseOutputMaxAdditionalSigops = MaxBlockSigOpsCost
	}
	if opts.BlockMaxWeight < reservedWeight {
		opts.BlockMaxWeight = reservedWeight
	}
	if opts.BlockMaxWeight > MaxBlockWeight {
		opts.BlockMaxWeight = MaxBlockWeight
	}
	if opts.BlockMaxSize == 0 {
		opts.BlockMaxSize = MaxBlockBaseSize
	}
	return opts
}

func (ba *BlockAssembler) resetBlock() {
	ba.blockWeight = *ba.options.BlockReservedWeight
	ba.blockSigOpsCost = ba.options.CoinbaseOutputMaxAdditionalSigops
	ba.blockTx = 0
	ba.blockFees.SetInt64(0)
}

func (ba *BlockAssembler) getMinimumTime(prevBlock *block.Block) uint64 {
	minTime := prevBlock.Header.Time + 1
	height := prevBlock.Header.Number.Uint64() + 1
	
	if height%block.DifficultyAdjustment == 0 {
		if prevBlock.Header.Time > MaxTimewarp {
			minTime = max(minTime, prevBlock.Header.Time-MaxTimewarp)
		}
	}
	return minTime
}

func (ba *BlockAssembler) updateTime(header *block.Header, prevBlock *block.Block) int64 {
	oldTime := header.Time
	minTime := ba.getMinimumTime(prevBlock)
	newTime := max(minTime, uint64(time.Now().Unix()))
	
	if oldTime < newTime {
		header.Time = newTime
	}
	return int64(newTime - oldTime)
}

func (ba *BlockAssembler) testBlockLimits(weight uint64, sigOpsCost uint64) bool {
	if ba.blockWeight+weight > ba.options.BlockMaxWeight {
		return false
	}
	if ba.blockSigOpsCost+sigOpsCost > MaxBlockSigOpsCost {
		return false
	}
	return true
}

func (ba *BlockAssembler) testTransactionsFinal(txs []*TxEntry, height, locktimeCutoff uint64) bool {
	for _, entry := range txs {
		if !entry.IsFinal(height, locktimeCutoff) {
			return false
		}
	}
	return true
}

func (ba *BlockAssembler) addToBlock(entry *TxEntry) {
	ba.template.Block.Txs = append(ba.template.Block.Txs, entry.Tx)
	ba.template.TxFees = append(ba.template.TxFees, new(big.Int).Set(entry.Fee))
	ba.template.TxSigOpsCosts = append(ba.template.TxSigOpsCosts, entry.SigOpsCost)
	
	ba.blockWeight += entry.Weight
	ba.blockTx++
	ba.blockSigOpsCost += entry.SigOpsCost
	ba.blockFees.Add(ba.blockFees, entry.Fee)
	
	if ba.options.PrintModifiedFee {
		log.Printf("[assembler] fee rate %s txid %s\n",
			formatFeeRate(entry.ModifiedFee, entry.TxSize),
			entry.Tx.Hash().Hex())
	}
}

func (ba *BlockAssembler) addTransactions(txns []*TxEntry) {
	// Bitcoin-style: sort by modified fee rate (descending)
	sort.Slice(txns, func(i, j int) bool {
		rateI := new(big.Float).SetInt(txns[i].ModifiedFee)
		rateI.Quo(rateI, new(big.Float).SetUint64(txns[i].TxSize))
		rateJ := new(big.Float).SetInt(txns[j].ModifiedFee)
		rateJ.Quo(rateJ, new(big.Float).SetUint64(txns[j].TxSize))
		return rateI.Cmp(rateJ) > 0
	})
	
	for _, entry := range txns {
		if !ba.testBlockLimits(entry.Weight, entry.SigOpsCost) {
			continue
		}
		if !ba.testTransactionsFinal([]*TxEntry{entry}, ba.height, ba.lockTimeCutoff) {
			continue
		}
		ba.addToBlock(entry)
	}
}

func (ba *BlockAssembler) createCoinbaseScriptSig() []byte {
	// BIP34: Include block height in coinbase
	heightBytes := make([]byte, 8)
	binary.PutUvarint(heightBytes, ba.height)
	
	// Trim leading zeros
	for len(heightBytes) > 1 && heightBytes[0] == 0 {
		heightBytes = heightBytes[1:]
	}
	
	script := append([]byte{byte(len(heightBytes))}, heightBytes...)
	
	if ba.options.IncludeDummyExtranonce {
		// Add dummy extranonce for ASIC boost compatibility
		extranonce := make([]byte, 4)
		cryptorand.Read(extranonce)
		script = append(script, extranonce...)
	}
	
	return script
}

func (ba *BlockAssembler) blockExtraData() []byte {
	extraData := []byte("ANTDChain-PoW")
	if ba.chainstate == nil {
		return extraData
	}
	
	if rotatingKing := ba.chainstate.GetCurrentRotatingKing(); rotatingKing != (common.QuantumAddress{}) {
		extraData = []byte(fmt.Sprintf("ANTDChain-PoW|rk=%s", rotatingKing.String()))
	}
	
	// Add witness commitment for SegWit (BIP141)
	if ba.options.UseSegWit {
		witnessCommitment := make([]byte, 36)
		copy(witnessCommitment[:4], []byte(WitnessCommitmentHeader))
		extraData = append(extraData, witnessCommitment...)
	}
	
	return extraData
}

// CreateNewBlock creates a Bitcoin-style block template
func (ba *BlockAssembler) CreateNewBlock(coinbaseAddr common.QuantumAddress, mempoolTxns []*TxEntry) (*BlockTemplate, error) {
	ba.timeStart = time.Now()
	ba.resetBlock()
	
	ba.template = &BlockTemplate{
		Block: &block.Block{
			Header: &block.Header{},
			Txs:    make([]*tx.Tx, 0),
			Uncles: make([]*block.Header, 0),
		},
		TxFees:          make([]*big.Int, 0),
		TxSigOpsCosts:   make([]uint64, 0),
		PackageFeerates: make([]uint64, 0),
		TotalFees:       big.NewInt(0),
		CreatedAt:       time.Now(),
	}
	
	prevBlock := ba.chainstate.Latest()
	if prevBlock == nil {
		return nil, errors.New("no chain tip")
	}
	ba.height = prevBlock.Header.Number.Uint64() + 1
	
	ba.template.Block.Header.Time = uint64(time.Now().Unix())
	ba.lockTimeCutoff = prevBlock.Header.Time
	
	// Add transactions from mempool (Bitcoin-style selection)
	if ba.options.UseMempool && len(mempoolTxns) > 0 {
		ba.addTransactions(mempoolTxns)
	}
	
	buildTime := time.Since(ba.timeStart)
	
	// Create header
	stateRoot := common.Hash{}
	txRoot := block.CalculateTxHash(ba.template.Block.Txs)
	extraData := ba.blockExtraData()
	
	header, err := block.NewHeader(
		prevBlock,
		coinbaseAddr,
		stateRoot,
		txRoot,
		new(big.Int).SetUint64(ba.height),
		prevBlock.Header.GasLimit,
		ba.chainstate.GetPoWEngine(),
		extraData,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create header: %w", err)
	}
	
	header.Version = 1
	if ba.options.UseSegWit {
		header.Version |= (1 << 1) // SegWit version bit
	}
	header.GasUsed = 0
	if err := chain.ApplyProtocolHeaderFields(header); err != nil {
		return nil, fmt.Errorf("failed to apply protocol header fields: %w", err)
	}
	ba.template.Block.Header = header
	
	if err := ba.template.Block.UpdateHeader(); err != nil {
		return nil, fmt.Errorf("failed to update header: %w", err)
	}
	
	ba.updateTime(ba.template.Block.Header, prevBlock)
	ba.template.Block.Header.Difficulty = ba.chainstate.CalculateExpectedDifficultyForBlock(ba.template.Block, prevBlock)
	
	stateRoot, err = ba.chainstate.ComputeBlockFinalStateRoot(
		coinbaseAddr,
		ba.template.Block.Header.Time,
		ba.height,
		ba.template.Block.Header.Extra,
		ba.template.Block.Txs,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to compute block state root: %w", err)
	}
	ba.template.Block.Header.Root = stateRoot
	
	// Calculate coinbase value (block reward + fees)
	blockReward := reward.CalculateBlockReward(ba.height)
	ba.template.CoinbaseValue = new(big.Int).Add(blockReward, ba.blockFees)
	
	ba.template.TotalWeight = ba.blockWeight
	ba.template.TotalSigOpsCost = ba.blockSigOpsCost
	ba.template.TotalFees.Set(ba.blockFees)
	ba.timeBuild = buildTime
	
	log.Printf("[assembler] Created block: height=%d weight=%d txs=%d fees=%s sigops=%d\n",
		ba.height, ba.template.TotalWeight, ba.blockTx, ba.blockFees.String(), ba.template.TotalSigOpsCost)
	
	return ba.template, nil
}

// ============================================================================
// MULTI-MINER MANAGEMENT (Bitcoin-style mining infrastructure)
// ============================================================================

// NewPosMiningState creates a new multi-miner mining state
func NewPosMiningState(powEngine *pow.PoW, chainstate *chain.Blockchain) *PosMiningState {
	ctx, cancel := context.WithCancel(context.Background())
	opts := DefaultBlockAssemblerOptions()
	
	ms := &PosMiningState{
		enabled:                 true,
		powEngine:               powEngine,
		miners:                  make(map[string]*MinerInstance),
		activeMiners:            make(map[string]*MinerInstance),
		globalTotalRewards:      big.NewInt(0),
		miningInterval:          DefaultMiningInterval,
		adaptiveInterval:        AdaptiveMiningInterval,
		broadcastMaxRetries:     DefaultBroadcastMaxRetries,
		broadcastInitialBackoff: DefaultBroadcastInitialBackoff,
		broadcastMaxBackoff:     DefaultBroadcastMaxBackoff,
		pendingBlockTimeout:     PendingBlockTimeout,
		pendingHeights:          make(map[uint64]time.Time),
		orphanBlocks:            make(map[common.Hash]*orphanBlockEntry),
		staleBlocks:             make(map[uint64]common.Hash),
		workQueue:               make(chan *MiningWork, 100),
		workResult:              make(chan *MiningResult, 100),
		assembler:               NewBlockAssembler(chainstate, opts),
		assemblerOpts:           opts,
		ctx:                     ctx,
		cancel:                  cancel,
	}
	
	return ms
}

// RegisterMiner adds a new miner to the pool
func (ms *PosMiningState) RegisterMiner(miner *MinerInstance) error {
	ms.minerMu.Lock()
	defer ms.minerMu.Unlock()
	
	if _, exists := ms.miners[miner.ID]; exists {
		return fmt.Errorf("miner with ID %s already exists", miner.ID)
	}
	
	ms.miners[miner.ID] = miner
	
	if ms.defaultMiner == "" {
		ms.defaultMiner = miner.ID
	}
	
	log.Printf("[miner] Registered miner %s (%s) with %d threads",
		miner.ID, miner.Address.String()[:12], miner.Threads)
	
	return nil
}

// UnregisterMiner removes a miner from the pool
func (ms *PosMiningState) UnregisterMiner(minerID string) error {
	ms.minerMu.Lock()
	defer ms.minerMu.Unlock()
	
	miner, exists := ms.miners[minerID]
	if !exists {
		return fmt.Errorf("miner %s not found", minerID)
	}
	
	// Stop the miner if active
	if miner.IsActive {
		ms.stopMinerLocked(minerID)
	}
	
	delete(ms.miners, minerID)
	delete(ms.activeMiners, minerID)
	
	if ms.defaultMiner == minerID {
		ms.defaultMiner = ""
		for id := range ms.miners {
			ms.defaultMiner = id
			break
		}
	}
	
	log.Printf("[miner] Unregistered miner %s", minerID)
	return nil
}

// StartMiner starts a specific miner
func (ms *PosMiningState) StartMiner(minerID string) error {
	ms.minerMu.Lock()
	defer ms.minerMu.Unlock()
	
	miner, exists := ms.miners[minerID]
	if !exists {
		return fmt.Errorf("miner %s not found", minerID)
	}
	
	if miner.IsActive {
		return fmt.Errorf("miner %s already active", minerID)
	}
	
	miner.IsActive = true
	ms.activeMiners[minerID] = miner
	
	// Start miner worker threads
	for i := 0; i < miner.Threads; i++ {
		ms.wg.Add(1)
		go ms.minerWorker(minerID, i)
	}
	
	log.Printf("[miner] Started miner %s with %d threads", minerID, miner.Threads)
	return nil
}

// StopMiner stops a specific miner
func (ms *PosMiningState) StopMiner(minerID string) error {
	ms.minerMu.Lock()
	defer ms.minerMu.Unlock()
	return ms.stopMinerLocked(minerID)
}

func (ms *PosMiningState) stopMinerLocked(minerID string) error {
	miner, exists := ms.miners[minerID]
	if !exists {
		return fmt.Errorf("miner %s not found", minerID)
	}
	
	if !miner.IsActive {
		return nil
	}
	
	miner.IsActive = false
	delete(ms.activeMiners, minerID)
	
	log.Printf("[miner] Stopped miner %s", minerID)
	return nil
}

// StartAllMiners starts all registered miners
func (ms *PosMiningState) StartAllMiners() {
	ms.minerMu.RLock()
	defer ms.minerMu.RUnlock()
	
	for minerID := range ms.miners {
		go ms.StartMiner(minerID)
	}
}

// StopAllMiners stops all miners
func (ms *PosMiningState) StopAllMiners() {
	ms.minerMu.RLock()
	defer ms.minerMu.RUnlock()
	
	for minerID := range ms.activeMiners {
		ms.stopMinerLocked(minerID)
	}
}

// minerWorker is a worker thread for a specific miner
func (ms *PosMiningState) minerWorker(minerID string, workerID int) {
	defer ms.wg.Done()
	
	log.Printf("[miner] Worker %d started for miner %s", workerID, minerID)
	
	for {
		select {
		case <-ms.ctx.Done():
			log.Printf("[miner] Worker %d stopping for miner %s", workerID, minerID)
			return
		case work := <-ms.workQueue:
			if work.MinerID != minerID {
				// Work is for a different miner, requeue?
				select {
				case ms.workQueue <- work:
				default:
				}
				continue
			}
			
			result := ms.mineWork(work, workerID)
			ms.workResult <- result
		}
	}
}

// mineWork performs the actual proof-of-work mining
func (ms *PosMiningState) mineWork(work *MiningWork, workerID int) *MiningResult {
	startTime := time.Now()
	nonce := work.NonceStart
	endNonce := work.NonceStart + work.NonceRange
	
	hashesAttempted := uint64(0)
	
	for nonce < endNonce {
		select {
		case <-ms.ctx.Done():
			return &MiningResult{
				WorkerID: fmt.Sprintf("%s-%d", work.MinerID, workerID),
				Success:  false,
				Duration: time.Since(startTime),
				HashRate: hashesAttempted,
				MinerID:  work.MinerID,
			}
		default:
		}
		
		// Update nonce in header
		headerCopy := *work.Header
		nonceBytes := block.BlockNonce{}
		binary.BigEndian.PutUint64(nonceBytes[:], nonce)
		headerCopy.Nonce = nonceBytes
		mixDigest, ok := validateHeaderPoW(&headerCopy)
		// Check PoW
		if ok {
			// Found valid nonce!
			duration := time.Since(startTime)
			hashrate := uint64(float64(hashesAttempted) / duration.Seconds())
			
			// Update miner stats
			ms.minerMu.RLock()
			miner := ms.miners[work.MinerID]
			ms.minerMu.RUnlock()
			
			if miner != nil {
				miner.mu.Lock()
				miner.Hashrate = hashrate
				miner.LastMinedTime = time.Now()
				miner.mu.Unlock()
			}
			
			// Update global hashrate
			ms.hashrateMu.Lock()
			ms.currentHashrate = hashrate
			ms.hashrateMu.Unlock()
			
			// Update metrics
			miningHashrate.WithLabelValues(work.MinerID).Set(float64(hashrate))
			
			// Update the actual header
			work.Header.Nonce = nonceBytes
			work.Header.MixDigest = mixDigest

			return &MiningResult{
				WorkerID:  fmt.Sprintf("%s-%d", work.MinerID, workerID),
				Nonce:     nonceBytes,
				MixDigest: mixDigest,
				Block:     work.Block,
				Success:   true,
				Duration:  duration,
				HashRate:  hashrate,
				MinerID:   work.MinerID,
			}
		}
		
		nonce++
		hashesAttempted++
	}
	
	// No valid nonce found
	return &MiningResult{
		WorkerID: fmt.Sprintf("%s-%d", work.MinerID, workerID),
		Success:  false,
		Duration: time.Since(startTime),
		HashRate: hashesAttempted,
		MinerID:  work.MinerID,

	}
}

func validateHeaderPoW(header *block.Header) (common.Hash, bool) {
	powHeader, err := powHeaderFromBlockHeader(header)
	if err != nil {
		return common.Hash{}, false
	}
	serialized := powHeader.SerializeForMining()
	mixDigest := common.ComputeHash(serialized)
	target := pow.TargetForDifficulty(powHeader.Difficulty)
	return mixDigest, new(big.Int).SetBytes(mixDigest[:]).Cmp(target) < 0
}

func powHeaderFromBlockHeader(header *block.Header) (*pow.BlockHeader, error) {
	if header == nil {
		return nil, errors.New("block header is nil")
	}
	if header.Number == nil {
		return nil, errors.New("block header number is nil")
	}
	
	diff := big.NewInt(difficulty.MinDifficulty)
	if header.Difficulty != nil {
		diff = new(big.Int).Set(header.Difficulty)
	}
	
	return &pow.BlockHeader{
		ParentHash: header.ParentHash,
		Coinbase:   header.Coinbase,
		Root:       header.Root,
		TxHash:     header.TxHash,
		Number:     header.Number.Uint64(),
		Difficulty: diff,
		Time:       header.Time,
		Extra:      header.GetExtraData(),
		Nonce:      [8]byte(header.Nonce),
		MixDigest:  header.MixDigest,
	}, nil
}

// ============================================================================
// MINING LOOP (Bitcoin-style with multiple miners)
// ============================================================================

var lastSyncLog time.Time = time.Now()

func (ms *PosMiningState) miningLoop(bc *chain.Blockchain, p2pNode *p2p.Node) {
	// Adaptive mining interval 
	interval := ms.miningInterval
	if ms.adaptiveInterval {
		// Adjust interval based on network hashrate
		if ms.currentHashrate > 0 {
			// Faster for higher hashrate
			adjusted := ms.miningInterval / time.Duration(ms.currentHashrate/1000000+1)
			if adjusted < MinimumMiningInterval {
				adjusted = MinimumMiningInterval
			}
			if adjusted > MaxMiningInterval {
				adjusted = MaxMiningInterval
			}
			interval = adjusted
		}
	}
	
	// Add random jitter to prevent synchronization 
	jitter := time.Duration(mathrand.Int63n(int64(interval / 4)))
	if jitter > 0 {
		log.Printf("[miner] Mining loop starting with interval %v + jitter %v", interval, jitter)
		time.Sleep(jitter)
	}
	
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	
	var startTime = time.Now()
	sessionStartTime := time.Now()
	
	// Update metrics
	go func() {
		for ms.IsMining() {
			miningSessionDuration.Set(time.Since(sessionStartTime).Seconds())
			time.Sleep(1 * time.Second)
		}
	}()
	
	for ms.IsMining() {
		select {
		case <-ticker.C:
		case <-ms.ctx.Done():
			return
		}
		
		if !ms.IsMining() {
			break
		}
		
		// Check network readiness
		if err := networkReadyForMining(bc, p2pNode); err != nil {
			if time.Since(lastSyncLog) > LogSyncStatusInterval {
				log.Printf("[miner] Mining paused: %v", err)
				lastSyncLog = time.Now()
			}
			continue
		}
		
		if bc.IsSyncing() {
			if time.Since(lastSyncLog) > LogSyncStatusInterval {
				log.Printf("[miner] Sync in progress (height %d → %d) — mining paused",
					bc.GetChainHeight(), bc.GetSyncTarget())
				lastSyncLog = time.Now()
			}
			continue
		}
		
		// Resolve mining tip consensus 
		if p2pNode != nil {
			if err := p2pNode.ResolveMiningTipConsensus(); err != nil && time.Since(lastSyncLog) > LogSyncStatusInterval {
				log.Printf("[miner] Continuing while peer tip consensus catches up: %v", err)
				lastSyncLog = time.Now()
			}
		}
		
		// Sync cooldown
		if remaining := bc.SyncCooldownRemaining(); remaining > 0 {
			if time.Since(lastSyncLog) > LogSyncStatusInterval {
				log.Printf("[miner] Sync completed; waiting %s before resuming", remaining.Truncate(time.Second))
				lastSyncLog = time.Now()
			}
			time.Sleep(min(remaining, time.Second))
			continue
		}
		
		parent := bc.Latest()
		if parent == nil {
			continue
		}
		
		height := parent.Header.Number.Uint64() + 1
		
		// Check if block already exists
		if existing := bc.GetBlock(height); existing != nil {
			ms.clearPendingHeight(height)
			continue
		}
		
		// Wait for pending block propagation
		if ms.waitForPendingBlock(bc, height) {
			continue
		}
		
		// Get active miners
		ms.minerMu.RLock()
		activeMiners := make([]*MinerInstance, 0, len(ms.activeMiners))
		for _, miner := range ms.activeMiners {
			activeMiners = append(activeMiners, miner)
		}
		ms.minerMu.RUnlock()
		
		if len(activeMiners) == 0 {
			log.Printf("[miner] No active miners available")
			time.Sleep(1 * time.Second)
			continue
		}
		
		// Create block template
		mempoolTxns := currentMempoolEntries(bc, nil, height)
		template, err := ms.assembler.CreateNewBlock(activeMiners[0].Address, mempoolTxns)
		if err != nil {
			log.Printf("[miner] Failed to create block template: %v", err)
			continue
		}
		
		// Distribute work to all active miners

		for _, miner := range activeMiners {
			// Create header copy for this miner
			headerCopy := *template.Block.Header
			
			// Calculate miner-specific difficulty
			headerCopy.Coinbase = miner.Address
			headerCopy.Difficulty = bc.CalculateExpectedDifficultyForBlock(&block.Block{Header: &headerCopy, Txs: template.Block.Txs, Uncles: template.Block.Uncles}, parent)
			stateRoot, err := bc.ComputeBlockFinalStateRoot(miner.Address, headerCopy.Time, height, headerCopy.Extra, template.Block.Txs)
			if err != nil {
				log.Printf("[miner] Failed to compute state root for miner %s: %v", miner.ID, err)
				continue
			}
			headerCopy.Root = stateRoot
			minerDifficulty := headerCopy.Difficulty

			// Create work units (split nonce space among threads)
			nonceRange := uint64(0xFFFFFFFFFFFFFFFF) / uint64(miner.Threads)
			
			for i := 0; i < miner.Threads; i++ {
				blockCopy := *template.Block
				workHeader := headerCopy
				blockCopy.Header = &workHeader
				
				work := &MiningWork{
					Block:      &blockCopy,
					Header:     &workHeader,
					NonceStart: uint64(i) * nonceRange,
					NonceRange: nonceRange,
					MinerID:    miner.ID,
					Height:     height,
					Difficulty: minerDifficulty,
					Timestamp:  time.Now(),
				}
				
				select {
				case ms.workQueue <- work:
				default:
					log.Printf("[miner] Work queue full for miner %s", miner.ID)
				}
			}
		}
		
		// Wait for any miner to find a solution 
		var winningResult *MiningResult
		timeout := time.After(interval)
		
	resultLoop:
		for {
			select {
			case result := <-ms.workResult:
				if result.Success {
					winningResult = result
					break resultLoop
				}
			case <-timeout:
				log.Printf("[miner] Mining timeout - no solution found in %v", interval)
				break resultLoop
			case <-ms.ctx.Done():
				return
			}
		}
		
		if winningResult == nil {
			continue
		}
		
		// Process winning block
		blk := winningResult.Block
		miner := ms.getMiner(winningResult.MinerID)
		if miner == nil {
			log.Printf("[miner] Winning miner %s not found", winningResult.MinerID)
			continue
		}
		
		log.Printf("[miner] ✅ BLOCK #%d MINED by %s! Nonce: %x, Hashrate: %.2f MH/s",
			height, miner.Address.String()[:12],
			winningResult.Nonce,
			float64(winningResult.HashRate)/1000000)
		
		// Add signature
		signature, err := generateBlockSignature(
			miner.Address,
			parent.Hash(),
			height,
			blk.Header.Time,
			miner.PrivateKey,
		)
		if err != nil {
			log.Printf("[miner] Signing failed: %v", err)
			continue
		}
		blk.Header.Signature = signature
		
		// Final validation before adding to chain
		if err := bc.AddBlock(blk); err != nil {
			log.Printf("[miner] AddBlock failed: %v", err)
			if bc.GetBlock(height) != nil {
				ms.recordStaleBlock(miner, height)
			}
			continue
		}
		
		// Update miner stats
		miner.mu.Lock()
		miner.BlocksMined++
		miner.LastMinedHeight = height
		blockReward := reward.CalculateBlockReward(height)
		miner.TotalRewards.Add(miner.TotalRewards, blockReward)
		miner.TotalRewards.Add(miner.TotalRewards, template.TotalFees)
		miner.mu.Unlock()
		
		// Update global stats
		atomic.AddUint64(&ms.globalBlocksMined, 1)
		ms.globalTotalRewards.Add(ms.globalTotalRewards, template.TotalFees)
		ms.globalTotalRewards.Add(ms.globalTotalRewards, reward.CalculateBlockReward(height))
		
		// Update metrics
		miningBlocksTotal.WithLabelValues(miner.Address.String()).Inc()
		miningRewardsTotal.WithLabelValues(miner.Address.String()).Set(
			float64(new(big.Int).Div(miner.TotalRewards, big.NewInt(1e18)).Int64()))
		miningUptime.Set(time.Since(startTime).Seconds())
		
		// Update fees for estimator
		if template.TotalFees.Sign() > 0 {
			feePerKB := new(big.Int).Div(template.TotalFees, big.NewInt(int64(template.TotalWeight/1000)))
			ms.assembler.feeEstimator.AddBlockFee(uint64(feePerKB.Int64()))
		}
		
		// Broadcast to network
		if p2pNode != nil {
			go broadcastMinedBlock(p2pNode, blk, ms)
		}
		
		ms.clearPendingHeight(height)
		
		// Adaptive interval adjustment 
		if ms.adaptiveInterval {
			ms.adjustMiningInterval()
		}
	}
	
	log.Println("[miner] Mining loop stopped")
}

func (ms *PosMiningState) adjustMiningInterval() {
	// Adjust interval based on recent success
	newInterval := ms.miningInterval
	if ms.globalBlocksMined > 0 {
		// Successful blocks -> increase interval slightly
		newInterval = ms.miningInterval + 100*time.Millisecond
	} else {
		// No blocks found -> decrease interval
		newInterval = ms.miningInterval - 100*time.Millisecond
	}
	
	if newInterval < MinimumMiningInterval {
		newInterval = MinimumMiningInterval
	}
	if newInterval > MaxMiningInterval {
		newInterval = MaxMiningInterval
	}
	
	ms.miningInterval = newInterval
}

func (ms *PosMiningState) recordStaleBlock(miner *MinerInstance, height uint64) {
	miner.mu.Lock()
	miner.StaleBlocks++
	miner.mu.Unlock()
	
	ms.staleMu.Lock()
	ms.staleBlocks[height] = common.BytesToHash(miner.Address.Bytes())
	ms.staleMu.Unlock()
	
	miningStaleBlocks.WithLabelValues(miner.Address.String()).Inc()
}

func (ms *PosMiningState) getMiner(minerID string) *MinerInstance {
	ms.minerMu.RLock()
	defer ms.minerMu.RUnlock()
	return ms.miners[minerID]
}

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

func networkReadyForMining(bc *chain.Blockchain, p2pNode *p2p.Node) error {
	if p2pNode != nil {
		if err := p2pNode.GossipSubReady(); err != nil {
			return fmt.Errorf("p2p gossipsub not ready: %w", err)
		}
	}
	if bc == nil || !bc.IsFullySynced() {
		if bc == nil {
			return errors.New("blockchain not initialized")
		}
		return fmt.Errorf("blockchain not fully synced (height=%d target=%d syncing=%v cooldown=%s)",
			bc.GetChainHeight(),
			bc.GetSyncTarget(),
			bc.IsSyncing(),
			bc.SyncCooldownRemaining().Truncate(time.Second),
		)
	}
	return nil
}

func currentMempoolEntries(bc *chain.Blockchain, fallback []*TxEntry, height uint64) []*TxEntry {
	if bc == nil || bc.TxPool() == nil {
		return fallback
	}
	
	pending := bc.TxPool().GetPending()
	if len(pending) == 0 {
		return fallback
	}
	
	now := uint64(time.Now().Unix())
	entries := make([]*TxEntry, 0, len(pending))
	
	for _, pendingTx := range pending {
		entry := txToMempoolEntry(pendingTx, height, now)
		if entry != nil {
			entries = append(entries, entry)
		}
	}
	
	return entries
}

func txToMempoolEntry(t *tx.Tx, height uint64, entryTime uint64) *TxEntry {
	if t == nil {
		return nil
	}
	
	serialized, err := t.Serialize()
	txSize := uint64(0)
	if err == nil {
		txSize = uint64(len(serialized))
	}
	if txSize == 0 {
		txSize = uint64(len(t.Data) + 128)
	}
	
	fee := new(big.Int).Mul(new(big.Int).SetUint64(t.Gas), t.GasPrice)
	
	return &TxEntry{
		Tx:          t,
		Fee:         fee,
		ModifiedFee: new(big.Int).Set(fee),
		Weight:      txSize * 4,
		SigOpsCost:  1,
		TxSize:      txSize,
		Height:      height,
		Time:        entryTime,
	}
}

func generateBlockSignature(miner common.QuantumAddress, parentHash common.Hash, height uint64, timestamp uint64, privateKey []byte) ([]byte, error) {
	if len(privateKey) == 0 {
		return nil, errors.New("private key required")
	}
	
	msgData := append([]byte("ANTDChain-PoW-Block"), parentHash.Bytes()...)
	msgData = append(msgData, common.LeftPadBytes(big.NewInt(int64(height)).Bytes(), 32)...)
	msgData = append(msgData, common.LeftPadBytes(big.NewInt(int64(timestamp)).Bytes(), 32)...)
	msgData = append(msgData, miner.Bytes()...)
	
	msgHash := common.ComputeHash(msgData)
	return quantum.Sign(privateKey, msgHash.Bytes())
}

func broadcastMinedBlock(p *p2p.Node, blk *block.Block, ms *PosMiningState) {
	if p == nil || blk == nil {
		return
	}
	
	ms.minerMu.RLock()
	maxRetries := ms.broadcastMaxRetries
	backoff := ms.broadcastInitialBackoff
	maxBackoff := ms.broadcastMaxBackoff
	ms.minerMu.RUnlock()
	
	for i := 0; i < maxRetries; i++ {
		if err := p.BroadcastBlock(blk); err != nil {
			if errors.Is(err, context.Canceled) {
				miningBroadcastFailures.Inc()
				log.Printf("[miner] Broadcast skipped for block %d: P2P context canceled", blk.Header.Number.Uint64())
				return
			}
			log.Printf("[miner] Broadcast attempt %d/%d failed: %v", i+1, maxRetries, err)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		} else {
			log.Printf("[miner] Block %d broadcasted successfully", blk.Header.Number.Uint64())
			miningBroadcastSuccess.Inc()
			return
		}
	}
	miningBroadcastFailures.Inc()
	log.Printf("[miner] Failed to broadcast block %d after %d attempts", blk.Header.Number.Uint64(), maxRetries)
}

func formatFeeRate(fee *big.Int, size uint64) string {
	rate := new(big.Float).SetInt(fee)
	rate.Quo(rate, new(big.Float).SetUint64(size))
	return rate.Text('f', 8)
}

// ============================================================================
// PUBLIC API 
// ============================================================================

// StartMining starts the mining process with all registered miners
func (ms *PosMiningState) StartMining(bc *chain.Blockchain, p2pNode *p2p.Node) {
	if bc == nil || ms.powEngine == nil {
		log.Println("[miner] Missing required components")
		return
	}
	if !ms.IsEnabled() {
		log.Println("[miner] Mining disabled")
		return
	}
	if ms.IsMining() {
		log.Println("[miner] Mining already running")
		return
	}
	
	ms.SetMining(true)
	
	// Start all registered miners
	ms.StartAllMiners()
	
	log.Printf("[miner] PoW Mining STARTED with %d miners", len(ms.miners))
	
	go ms.miningLoop(bc, p2pNode)
}

// StopMining stops the mining process
func (ms *PosMiningState) StopMining() {
	ms.SetMining(false)
	ms.StopAllMiners()
	if ms.cancel != nil {
		ms.cancel()
	}
	log.Println("[miner] Mining STOPPED")
}

// AddMiner adds a new miner with address and private key
func (ms *PosMiningState) AddMiner(id string, addr common.QuantumAddress, privKey []byte, threads int) error {
	miner := NewMinerInstance(id, addr, threads)
	miner.PrivateKey = privKey
	
	if err := ms.RegisterMiner(miner); err != nil {
		return err
	}
	
	log.Printf("[miner] Added miner %s: %s (%d threads)", id, addr.String()[:12], threads)
	return nil
}

// AddMinerFromKeystore adds a miner from keystore file
func (ms *PosMiningState) AddMinerFromKeystore(id string, addr common.QuantumAddress, password, keystoreDir string, threads int) error {
	privKey, err := qkeystore.Unlock(addr, password, keystoreDir)
	if err != nil {
		return fmt.Errorf("failed to unlock keystore: %w", err)
	}
	
	return ms.AddMiner(id, addr, privKey, threads)
}

// GetMinerStats returns statistics for a specific miner
func (ms *PosMiningState) GetMinerStats(minerID string) map[string]interface{} {
	miner := ms.getMiner(minerID)
	if miner == nil {
		return nil
	}
	
	miner.mu.RLock()
	defer miner.mu.RUnlock()
	
	return map[string]interface{}{
		"id":               miner.ID,
		"address":          miner.Address.String(),
		"threads":          miner.Threads,
		"hashrate_hps":     miner.Hashrate,
		"blocks_mined":     miner.BlocksMined,
		"stale_blocks":     miner.StaleBlocks,
		"total_rewards":    miner.TotalRewards.String(),
		"last_mined_height": miner.LastMinedHeight,
		"last_mined_time":  miner.LastMinedTime,
		"is_active":        miner.IsActive,
	}
}

// GetGlobalStats returns global mining statistics
func (ms *PosMiningState) GetGlobalStats() map[string]interface{} {
	return map[string]interface{}{
		"mining_enabled":        ms.enabled,
		"is_mining":             ms.mining,
		"total_miners":          len(ms.miners),
		"active_miners":         len(ms.activeMiners),
		"global_blocks_mined":   atomic.LoadUint64(&ms.globalBlocksMined),
		"global_total_rewards":  ms.globalTotalRewards.String(),
		"current_hashrate_hps":  ms.currentHashrate,
		"mining_interval_sec":   ms.miningInterval.Seconds(),
		"adaptive_interval":     ms.adaptiveInterval,
		"broadcast_max_retries": ms.broadcastMaxRetries,
		"orphan_blocks":         len(ms.orphanBlocks),
		"stale_blocks":          len(ms.staleBlocks),
	}
}

func (ms *PosMiningState) GetMinerAddress() common.QuantumAddress {
	ms.minerMu.RLock()
	defer ms.minerMu.RUnlock()
	if ms.defaultMiner == "" {
		return common.QuantumAddress{}
	}
	miner := ms.miners[ms.defaultMiner]
	if miner == nil {
		return common.QuantumAddress{}
	}
	return miner.Address
}

func (ms *PosMiningState) SetMinerAddress(addr common.QuantumAddress) error {
	if addr == (common.QuantumAddress{}) {
		return errors.New("miner address cannot be zero")
	}
	
	ms.minerMu.Lock()
	defer ms.minerMu.Unlock()
	
	for id, miner := range ms.miners {
		if miner.Address == addr {
			ms.defaultMiner = id
			return nil
		}
	}
	
	minerID := ms.defaultMiner
	if minerID == "" {
		minerID = addr.String()[:12]
	}
	miner := ms.miners[minerID]
	if miner == nil {
		miner = NewMinerInstance(minerID, addr, DefaultMinerThreads)
		ms.miners[minerID] = miner
	} else {
		miner.Address = addr
	}
	ms.defaultMiner = minerID
	if miner.IsActive {
		ms.activeMiners[minerID] = miner
	}
	return nil
}

func (ms *PosMiningState) SetPrivateKeyFromBytes(privKey []byte) error {
	if len(privKey) != quantum.MLDSA65PrivateKeySize {
		return fmt.Errorf("invalid private key length: expected %d bytes, got %d", quantum.MLDSA65PrivateKeySize, len(privKey))
	}
	
	ms.minerMu.Lock()
	defer ms.minerMu.Unlock()
	
	minerID := ms.defaultMiner
	if minerID == "" {
		minerID = "default"
		ms.defaultMiner = minerID
	}
	miner := ms.miners[minerID]
	if miner == nil {
		miner = NewMinerInstance(minerID, common.QuantumAddress{}, DefaultMinerThreads)
		ms.miners[minerID] = miner
	}
	miner.PrivateKey = append(miner.PrivateKey[:0], privKey...)
	return nil
}

func (ms *PosMiningState) GetPublicKey() []byte {
	ms.minerMu.RLock()
	miner := ms.miners[ms.defaultMiner]
	ms.minerMu.RUnlock()
	if miner == nil || len(miner.PrivateKey) == 0 {
		return nil
	}
	
	miner.mu.RLock()
	privKey := append([]byte(nil), miner.PrivateKey...)
	miner.mu.RUnlock()
	
	pubKey, err := quantum.DerivePublicKey(privKey)
	if err != nil {
		return nil
	}
	return pubKey
}

// ============================================================================
// COMPATIBILITY METHODS (existing API)
// ============================================================================

func (ms *PosMiningState) IsMining() bool {
	ms.minerMu.RLock()
	defer ms.minerMu.RUnlock()
	return ms.mining
}

func (ms *PosMiningState) IsEnabled() bool {
	ms.minerMu.RLock()
	defer ms.minerMu.RUnlock()
	return ms.enabled
}

func (ms *PosMiningState) PauseMining() {
	ms.SetEnabled(false)
}

func (ms *PosMiningState) ResumeMining() {
	ms.SetEnabled(true)
}

func (ms *PosMiningState) SetEnabled(v bool) {
	ms.minerMu.Lock()
	defer ms.minerMu.Unlock()
	ms.enabled = v
}

func (ms *PosMiningState) SetMining(v bool) {
	ms.minerMu.Lock()
	defer ms.minerMu.Unlock()
	ms.mining = v
}

func (ms *PosMiningState) SetMiningInterval(interval time.Duration) {
	ms.minerMu.Lock()
	defer ms.minerMu.Unlock()
	if interval < MinimumMiningInterval {
		log.Printf("[miner] Requested mining interval %s too fast; clamped to %s", interval, MinimumMiningInterval)
		interval = MinimumMiningInterval
	}
	ms.miningInterval = interval
}

func (ms *PosMiningState) SetAdaptiveInterval(enabled bool) {
	ms.minerMu.Lock()
	defer ms.minerMu.Unlock()
	ms.adaptiveInterval = enabled
}

func (ms *PosMiningState) SetBroadcastRetryConfig(maxRetries int, initialBackoff, maxBackoff time.Duration) {
	ms.minerMu.Lock()
	defer ms.minerMu.Unlock()
	ms.broadcastMaxRetries = maxRetries
	ms.broadcastInitialBackoff = initialBackoff
	ms.broadcastMaxBackoff = maxBackoff
}

func (ms *PosMiningState) SetSyncCallback(cb func(isSyncing bool)) {
	ms.minerMu.Lock()
	defer ms.minerMu.Unlock()
	ms.onSyncChange = cb
}

func (ms *PosMiningState) GetMiningStatistics() map[string]interface{} {
	stats := ms.GetGlobalStats()
	stats["miners"] = make([]map[string]interface{}, 0)

	minerAddress := ms.GetMinerAddress()
	stats["miner_address"] = minerAddress.String()
	stats["blocks_mined"] = uint64(0)
	stats["total_rewards_antd"] = "0"
	stats["has_private_key"] = false
	if ms.defaultMiner != "" {
		if minerStats := ms.GetMinerStats(ms.defaultMiner); minerStats != nil {
			stats["blocks_mined"] = minerStats["blocks_mined"]
			stats["total_rewards_antd"] = minerStats["total_rewards"]
			if miner := ms.getMiner(ms.defaultMiner); miner != nil {
				stats["has_private_key"] = len(miner.PrivateKey) > 0
			}
		}
	}
	
	ms.minerMu.RLock()
	for minerID := range ms.miners {
		if minerStats := ms.GetMinerStats(minerID); minerStats != nil {
			stats["miners"] = append(stats["miners"].([]map[string]interface{}), minerStats)
		}
	}
	ms.minerMu.RUnlock()
	
	if ms.powEngine != nil {
		for k, v := range ms.powEngine.GetMiningStatistics() {
			stats["pos_"+k] = v
		}
	}
	
	ms.templateMu.RLock()
	if ms.currentTemplate != nil {
		stats["current_template_weight"] = ms.currentTemplate.TotalWeight
		stats["current_template_txs"] = len(ms.currentTemplate.Block.Txs)
		stats["current_template_fees"] = ms.currentTemplate.TotalFees.String()
	}
	ms.templateMu.RUnlock()
	
	return stats
}

func (ms *PosMiningState) OnBlockReceived(height uint64) {
	ms.pendingMu.Lock()
	defer ms.pendingMu.Unlock()
	if _, exists := ms.pendingHeights[height]; !exists {
		ms.pendingHeights[height] = time.Now()
		log.Printf("[miner] ⏳ New block announced at height %d – will wait %.1fs before mining it",
			height, ms.pendingBlockTimeout.Seconds())
	}
}

func (ms *PosMiningState) clearPendingHeight(height uint64) {
	ms.pendingMu.Lock()
	defer ms.pendingMu.Unlock()
	delete(ms.pendingHeights, height)
}

func (ms *PosMiningState) waitForPendingBlock(bc *chain.Blockchain, height uint64) bool {
	ms.pendingMu.RLock()
	_, pending := ms.pendingHeights[height]
	ms.pendingMu.RUnlock()
	if !pending {
		return false
	}
	
	log.Printf("[miner] ⏳ Pending block at height %d – waiting up to %v for it to arrive",
		height, ms.pendingBlockTimeout)
	
	timer := time.NewTimer(ms.pendingBlockTimeout)
	defer timer.Stop()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	
	for ms.IsMining() {
		select {
		case <-timer.C:
			log.Printf("[miner] ⏰ Timeout waiting for pending block at height %d – proceeding to mine", height)
			ms.clearPendingHeight(height)
			return false
		case <-ticker.C:
			if bc.GetBlock(height) != nil {
				log.Printf("[miner] ✅ Pending block at height %d was added – skipping own mining", height)
				ms.clearPendingHeight(height)
				return true
			}
		}
	}
	
	log.Printf("[miner] Mining stopped while waiting for pending block at height %d", height)
	return true
}

func (ms *PosMiningState) CheckMiningEligibility(bc *chain.Blockchain) (bool, error) {
	if bc == nil || ms.powEngine == nil {
		return false, errors.New("blockchain or PoW engine not initialized")
	}
	
	ms.minerMu.RLock()
	minerID := ms.defaultMiner
	ms.minerMu.RUnlock()
	
	if minerID == "" {
		return false, errors.New("no default miner set")
	}
	
	miner := ms.getMiner(minerID)
	if miner == nil {
		return false, errors.New("default miner not found")
	}
	
	parent := bc.Latest()
	if parent == nil {
		return false, errors.New("no parent block found")
	}
	
	height := parent.Header.Number.Uint64() + 1
	currentTime := uint64(time.Now().Unix())
	eligible, err := ms.powEngine.VerifyMinerEligibility(miner.Address, parent.Hash(), height, currentTime)
	if err != nil {
		return false, fmt.Errorf("eligibility check failed: %w", err)
	}
	
	return eligible, nil
}

func (ms *PosMiningState) GetNextMiningSlot(bc *chain.Blockchain) (uint64, time.Duration, error) {
	if bc == nil || ms.powEngine == nil {
		return 0, 0, errors.New("blockchain or PoW engine not initialized")
	}
	
	ms.minerMu.RLock()
	minerID := ms.defaultMiner
	ms.minerMu.RUnlock()
	
	if minerID == "" {
		return 0, 0, errors.New("no default miner set")
	}
	
	miner := ms.getMiner(minerID)
	if miner == nil {
		return 0, 0, errors.New("default miner not found")
	}
	
	parent := bc.Latest()
	if parent == nil {
		return 0, 0, errors.New("no parent block found")
	}
	
	currentHeight := parent.Header.Number.Uint64()
	if !ms.powEngine.IsKing(miner.Address) {
		return 0, 0, errors.New("address is not in validator set")
	}
	
	stats := ms.powEngine.GetMiningStatistics()
	activeStakers, _ := stats["active_stakers"].(int)
	if activeStakers <= 0 {
		return 0, 0, errors.New("no active validators")
	}
	
	blocksUntilTurn := uint64(activeStakers)
	estimatedBlocks := blocksUntilTurn
	estimatedTime := time.Duration(estimatedBlocks*difficulty.TargetBlockTimeSeconds) * time.Second
	
	return currentHeight + estimatedBlocks, estimatedTime, nil
}

func (ms *PosMiningState) LoadPrivateKeyFromKeystore(keystoreDir, password string) error {
	ms.minerMu.RLock()
	minerID := ms.defaultMiner
	ms.minerMu.RUnlock()
	
	if minerID == "" {
		return errors.New("no default miner set")
	}
	
	miner := ms.getMiner(minerID)
	if miner == nil {
		return errors.New("default miner not found")
	}
	
	privKey, err := qkeystore.Unlock(miner.Address, password, keystoreDir)
	if err != nil {
		return fmt.Errorf("failed to unlock keystore: %w", err)
	}
	
	if len(privKey) != quantum.MLDSA65PrivateKeySize {
		return fmt.Errorf("invalid private key length: expected %d bytes, got %d", quantum.MLDSA65PrivateKeySize, len(privKey))
	}
	
	miner.mu.Lock()
	miner.PrivateKey = privKey
	miner.mu.Unlock()
	
	log.Printf("[miner] ✓ Loaded private key for miner %s", minerID)
	return nil
}

func (ms *PosMiningState) LoadPrivateKeyFromFile(filePath, password string) error {
	keyjson, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read keystore file: %w", err)
	}
	
	var ks qkeystore.KeyStore
	if err := json.Unmarshal(keyjson, &ks); err != nil {
		return fmt.Errorf("invalid keystore format: %w", err)
	}
	
	addr, err := common.ParseQuantumAddress(ks.Address)
	if err != nil {
		return fmt.Errorf("invalid address in keystore: %w", err)
	}
	
	privKey, err := qkeystore.Unlock(addr, password, filepath.Dir(filePath))
	if err != nil {
		return fmt.Errorf("failed to decrypt keystore: %w", err)
	}
	
	if len(privKey) != quantum.MLDSA65PrivateKeySize {
		return fmt.Errorf("invalid private key length: expected %d bytes, got %d", quantum.MLDSA65PrivateKeySize, len(privKey))
	}
	
	// Find or create miner for this address
	ms.minerMu.Lock()
	var minerID string
	for id, m := range ms.miners {
		if m.Address == addr {
			minerID = id
			break
		}
	}
	
	if minerID == "" {
		minerID = addr.String()[:12]
		miner := NewMinerInstance(minerID, addr, DefaultMinerThreads)
		miner.PrivateKey = privKey
		ms.miners[minerID] = miner
		if ms.defaultMiner == "" {
			ms.defaultMiner = minerID
		}
		log.Printf("[miner] Created new miner %s from keystore", minerID)
	} else {
		miner := ms.miners[minerID]
		miner.PrivateKey = privKey
	}
	ms.minerMu.Unlock()
	
	log.Printf("[miner] ✓ Loaded private key from %s for miner %s", filePath, minerID[:12])
	return nil
}

// ============================================================================
// COMPATIBILITY WRAPPERS (existing API)
// ============================================================================

// StartPowMining starts mining with default miner (compatibility)
func StartPowMining(bc *chain.Blockchain, state *PosMiningState, rewardAddr common.QuantumAddress, p2pNode *p2p.Node, mempoolTxns []*TxEntry) {
	if bc == nil || state == nil || rewardAddr == (common.QuantumAddress{}) {
		log.Println("[miner] Missing required components")
		return
	}
	
	// Check if miner already exists
	state.minerMu.Lock()
	existingID := ""
	for id, m := range state.miners {
		if m.Address == rewardAddr {
			existingID = id
			break
		}
	}
	state.minerMu.Unlock()
	
	if existingID == "" {
		// Create new miner
		minerID := rewardAddr.String()[:12]
		miner := NewMinerInstance(minerID, rewardAddr, DefaultMinerThreads)
		if err := state.RegisterMiner(miner); err != nil {
			log.Printf("[miner] Failed to register miner: %v", err)
			return
		}
	}
	
	// Start mining
	state.StartMining(bc, p2pNode)
}

// StopMining stops all mining (compatibility)
func StopMining(state *PosMiningState) {
	if state != nil {
		state.StopMining()
	}
}

// StartPosMining is compatibility alias
func StartPosMining(bc *chain.Blockchain, state *PosMiningState, rewardAddr common.QuantumAddress, p2pNode *p2p.Node, mempoolTxns []*TxEntry) {
	StartPowMining(bc, state, rewardAddr, p2pNode, mempoolTxns)
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
