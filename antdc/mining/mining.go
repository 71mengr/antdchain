// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package mining

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
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
	"github.com/antdaza/antdchain/common/hexutil"
	"github.com/prometheus/client_golang/prometheus"
)

// Configuration constants following Bitcoin's defaults
const (
	DefaultMiningInterval          = 12 * time.Second
	MinimumMiningInterval          = 10 * time.Second
	DefaultBroadcastMaxRetries     = 5
	DefaultBroadcastInitialBackoff = 100 * time.Millisecond
	DefaultBroadcastMaxBackoff     = 2 * time.Second

	// Block resource limits (Bitcoin equivalents)
	DefaultBlockReservedWeight = 4000
	MinimumBlockReservedWeight = 4000
	MaxBlockWeight             = 4000000
	MaxBlockSigOpsCost         = 80000

	// Chunk selection constants
	MaxConsecutiveFailures     = 1000
	BlockFullEnoughWeightDelta = 4000

	// Timewarp protection (BIP94)
	MaxTimewarp = 600 // 10 minutes in seconds

	LogEligibilityCheckInterval = 10
	LogSyncStatusInterval       = 10 * time.Second
	PendingBlockTimeout         = 2 * time.Second
)

var (
	miningBlocksTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "antdchain_mining_blocks_total",
			Help: "Total blocks successfully mined by this node",
		},
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
	miningRewardsTotal = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "antdchain_mining_rewards_total_antd",
			Help: "Total mining rewards earned in ANTD",
		},
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
}

// TxEntry represents a mempool transaction with metadata
type TxEntry struct {
	Tx          *tx.Tx
	Fee         *big.Int
	ModifiedFee *big.Int
	Weight      uint64
	SigOpsCost  uint64
	TxSize      uint64
	Height      uint64
	Time        uint64
}

// IsFinal checks if transaction is final (locktime verification)
func (txe *TxEntry) IsFinal(blockHeight uint64, blockTime uint64) bool {
	_ = blockHeight
	if txe.Tx == nil {
		return false
	}
	if txe.Tx.Timestamp == 0 {
		return true
	}
	return txe.Tx.Timestamp <= blockTime
}

// BlockTemplate represents a candidate block being assembled
type BlockTemplate struct {
	Block           *block.Block
	TxFees          []*big.Int
	TxSigOpsCosts   []uint64
	PackageFeerates []uint64
	TotalFees       *big.Int
	TotalWeight     uint64
	TotalSigOpsCost uint64
}

// BlockAssemblerOptions configures block assembly
type BlockAssemblerOptions struct {
	BlockMaxWeight                    uint64
	BlockMinFeeRate                   uint64
	BlockReservedWeight               *uint64
	CoinbaseOutputMaxAdditionalSigops uint64
	PrintModifiedFee                  bool
	TestBlockValidity                 bool
	IncludeDummyExtranonce            bool
	UseMempool                        bool
}

// DefaultBlockAssemblerOptions returns default options
func DefaultBlockAssemblerOptions() BlockAssemblerOptions {
	reservedWeight := uint64(DefaultBlockReservedWeight)
	return BlockAssemblerOptions{
		BlockMaxWeight:                    MaxBlockWeight,
		BlockMinFeeRate:                   0,
		BlockReservedWeight:               &reservedWeight,
		CoinbaseOutputMaxAdditionalSigops: 0,
		PrintModifiedFee:                  false,
		TestBlockValidity:                 true,
		IncludeDummyExtranonce:            true,
		UseMempool:                        true,
	}
}

// clampOptions applies bounds to options
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

	return opts
}

// BlockAssembler handles block template creation
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
}

// NewBlockAssembler creates a new block assembler
func NewBlockAssembler(chainstate *chain.Blockchain, opts BlockAssemblerOptions) *BlockAssembler {
	return &BlockAssembler{
		chainstate: chainstate,
		options:    clampOptions(opts),
		blockFees:  big.NewInt(0),
	}
}

// resetBlock resets block building state
func (ba *BlockAssembler) resetBlock() {
	ba.blockWeight = *ba.options.BlockReservedWeight
	ba.blockSigOpsCost = ba.options.CoinbaseOutputMaxAdditionalSigops
	ba.blockTx = 0
	ba.blockFees.SetInt64(0)
}

// getMinimumTime calculates minimum allowed timestamp
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

// updateTime updates block timestamp
func (ba *BlockAssembler) updateTime(header *block.Header, prevBlock *block.Block) int64 {
	oldTime := header.Time
	minTime := ba.getMinimumTime(prevBlock)
	newTime := max(minTime, uint64(time.Now().Unix()))

	if oldTime < newTime {
		header.Time = newTime
	}

	return int64(newTime - oldTime)
}

// testChunkBlockLimits checks if a chunk fits
func (ba *BlockAssembler) testChunkBlockLimits(chunkWeight uint64, chunkSigOpsCost uint64) bool {
	if ba.blockWeight+chunkWeight > ba.options.BlockMaxWeight {
		return false
	}
	if ba.blockSigOpsCost+chunkSigOpsCost > MaxBlockSigOpsCost {
		return false
	}
	return true
}

// testChunkTransactions checks transaction finality
func (ba *BlockAssembler) testChunkTransactions(txs []*TxEntry) bool {
	for _, entry := range txs {
		if !entry.IsFinal(ba.height, ba.lockTimeCutoff) {
			return false
		}
	}
	return true
}

// addToBlock adds a transaction to the block
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

// addTransactions adds transactions from mempool
func (ba *BlockAssembler) addTransactions(mempoolTxns []*TxEntry) {
	for _, entry := range mempoolTxns {
		if !ba.testChunkBlockLimits(entry.Weight, entry.SigOpsCost) {
			continue
		}
		if !ba.testChunkTransactions([]*TxEntry{entry}) {
			continue
		}
		ba.addToBlock(entry)
	}
}

// createCoinbaseScriptSig creates the coinbase scriptSig with BIP34 height
func (ba *BlockAssembler) createCoinbaseScriptSig() []byte {
	heightBytes := make([]byte, 8)
	binary.PutUvarint(heightBytes, ba.height)

	for len(heightBytes) > 1 && heightBytes[0] == 0 {
		heightBytes = heightBytes[1:]
	}

	script := append([]byte{byte(len(heightBytes))}, heightBytes...)

	if ba.options.IncludeDummyExtranonce {
		script = append(script, 0x00)
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

	return extraData
}

// CreateNewBlock creates a new block template
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
	}

	prevBlock := ba.chainstate.Latest()
	if prevBlock == nil {
		return nil, errors.New("no chain tip")
	}
	ba.height = prevBlock.Header.Number.Uint64() + 1

	ba.template.Block.Header.Time = uint64(time.Now().Unix())
	ba.lockTimeCutoff = prevBlock.Header.Time

	if ba.options.UseMempool && len(mempoolTxns) > 0 {
		ba.addTransactions(mempoolTxns)
	}

	buildTime := time.Since(ba.timeStart)

	// Create header using NewHeader
	stateRoot := common.Hash{}
	txRoot := block.CalculateTxHash(ba.template.Block.Txs)

	header, err := block.NewHeader(
		prevBlock,
		coinbaseAddr,
		stateRoot,
		txRoot,
		new(big.Int).SetUint64(ba.height),
		prevBlock.Header.GasLimit,
		ba.chainstate.GetPoWEngine(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create header: %w", err)
	}

	header.Version = 1
	header.GasUsed = 0
	header.Extra = ba.blockExtraData()
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
	ba.template.TotalWeight = ba.blockWeight
	ba.template.TotalSigOpsCost = ba.blockSigOpsCost
	ba.template.TotalFees.Set(ba.blockFees)
	ba.timeBuild = buildTime

	log.Printf("[assembler] Created new block: weight=%d txs=%d fees=%s sigops=%d\n",
		ba.template.TotalWeight, ba.blockTx, ba.blockFees.String(), ba.template.TotalSigOpsCost)

	return ba.template, nil
}

// PosMiningState manages mining state
type PosMiningState struct {
	mining        bool
	enabled       bool
	minerAddress  common.QuantumAddress
	powEngine     *pow.PoW
	privateKey    []byte
	hasPrivateKey bool

	blocksMined  uint64
	totalRewards *big.Int

	miningInterval          time.Duration
	broadcastMaxRetries     int
	broadcastInitialBackoff time.Duration
	broadcastMaxBackoff     time.Duration
	pendingBlockTimeout     time.Duration

	assembler     *BlockAssembler
	assemblerOpts BlockAssemblerOptions

	pendingHeights map[uint64]time.Time
	pendingMu      sync.RWMutex

	currentTemplate *BlockTemplate
	templateMu      sync.RWMutex

	mu           sync.RWMutex
	onSyncChange func(isSyncing bool)
}

// NewPosMiningState creates a new mining state
func NewPosMiningState(powEngine *pow.PoW, chainstate *chain.Blockchain) *PosMiningState {
	opts := DefaultBlockAssemblerOptions()
	return &PosMiningState{
		enabled:                 true,
		powEngine:               powEngine,
		totalRewards:            big.NewInt(0),
		miningInterval:          DefaultMiningInterval,
		broadcastMaxRetries:     DefaultBroadcastMaxRetries,
		broadcastInitialBackoff: DefaultBroadcastInitialBackoff,
		broadcastMaxBackoff:     DefaultBroadcastMaxBackoff,
		pendingBlockTimeout:     PendingBlockTimeout,
		pendingHeights:          make(map[uint64]time.Time),
		assembler:               NewBlockAssembler(chainstate, opts),
		assemblerOpts:           opts,
	}
}

// OnBlockReceived marks a height as pending
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

	log.Printf("[miner] �� Pending block at height %d – waiting up to %v for it to arrive",
		height, ms.pendingBlockTimeout)

	deadline := time.Now().Add(ms.pendingBlockTimeout)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		<-ticker.C
		if bc.GetBlock(height) != nil {
			log.Printf("[miner] ✅ Pending block at height %d was added – skipping own mining", height)
			ms.clearPendingHeight(height)
			return true
		}
	}

	log.Printf("[miner] ⏰ Timeout waiting for pending block at height %d – proceeding to mine", height)
	ms.clearPendingHeight(height)
	return false
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

func mineBlockProofOfWork(ctx context.Context, engine *pow.PoW, header *block.Header) error {
	if engine == nil {
		return errors.New("PoW engine not initialized")
	}
	if header == nil {
		return errors.New("block header is nil")
	}

	difficulty := big.NewInt(pow.MinDifficulty)
	if header.Difficulty != nil {
		difficulty = new(big.Int).Set(header.Difficulty)
	}

	powHeader := &pow.BlockHeader{
		ParentHash: header.ParentHash,
		Coinbase:   header.Coinbase,
		Root:       header.Root,
		TxHash:     header.TxHash,
		Number:     header.Number.Uint64(),
		Difficulty: difficulty,
		Time:       header.Time,
		Extra:      append([]byte(nil), header.Extra...),
		Nonce:      [8]byte(header.Nonce),
		MixDigest:  header.MixDigest,
	}

	if err := engine.MineBlock(ctx, powHeader); err != nil {
		return err
	}

	header.Nonce = block.BlockNonce(powHeader.Nonce)
	header.MixDigest = powHeader.MixDigest
	return nil
}

func broadcastMinedBlock(p *p2p.Node, blk *block.Block, ms *PosMiningState) {
	if p == nil || blk == nil {
		return
	}

	ms.mu.RLock()
	maxRetries := ms.broadcastMaxRetries
	backoff := ms.broadcastInitialBackoff
	maxBackoff := ms.broadcastMaxBackoff
	ms.mu.RUnlock()

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

func networkReadyForMining(bc *chain.Blockchain, p2pNode *p2p.Node) error {
	if p2pNode != nil {
		if err := p2pNode.GossipSubReady(); err != nil {
			return fmt.Errorf("p2p gossipsub is not ready: %w", err)
		}
	}
	if bc == nil || !bc.IsFullySynced() {
		if bc == nil {
			return errors.New("blockchain is not initialized")
		}
		return fmt.Errorf("blockchain is not fully synced (height=%d target=%d syncing=%v cooldown=%s)",
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

func miningLoop(bc *chain.Blockchain, ms *PosMiningState, p2pNode *p2p.Node, mempoolTxns []*TxEntry) {
	ms.mu.RLock()
	miningInterval := ms.miningInterval
	ms.mu.RUnlock()

	jitter := time.Duration(rand.Int63n(int64(miningInterval / 2)))
	time.Sleep(jitter)
	log.Printf("[miner] Mining loop started – base interval %v + jitter %v", miningInterval, jitter)

	ticker := time.NewTicker(miningInterval)
	defer ticker.Stop()

	var (
		consecutiveMisses int
		startTime         = time.Now()
		sessionStartTime  = time.Now()
	)

	go func() {
		for ms.mining {
			miningSessionDuration.Set(time.Since(sessionStartTime).Seconds())
			time.Sleep(1 * time.Second)
		}
	}()

	for ms.mining {
		<-ticker.C

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

		if p2pNode != nil {
			if err := p2pNode.ResolveMiningTipConsensus(); err != nil {
				if time.Since(lastSyncLog) > LogSyncStatusInterval {
					log.Printf("[miner] Mining paused while connected peers resolve tip: %v", err)
					lastSyncLog = time.Now()
				}
				continue
			}
		}

		if remaining := bc.SyncCooldownRemaining(); remaining > 0 {
			if time.Since(lastSyncLog) > LogSyncStatusInterval {
				log.Printf("[miner] Sync completed; waiting %s before resuming mining", remaining.Truncate(time.Second))
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

		if existing := bc.GetBlock(height); existing != nil {
			ms.clearPendingHeight(height)
			continue
		}

		if ms.waitForPendingBlock(bc, height) {
			continue
		}

		currentTxns := currentMempoolEntries(bc, mempoolTxns, height)

		template, err := ms.assembler.CreateNewBlock(ms.minerAddress, currentTxns)
		if err != nil {
			log.Printf("[miner] Failed to create block template: %v", err)
			continue
		}

		ms.mu.RLock()
		minerAddr := ms.minerAddress
		hasPrivKey := ms.hasPrivateKey
		privKey := ms.privateKey
		ms.mu.RUnlock()

		if !hasPrivKey || minerAddr == (common.QuantumAddress{}) {
			consecutiveMisses++
			if consecutiveMisses == 1 || consecutiveMisses%LogEligibilityCheckInterval == 0 {
				log.Printf("[miner] Waiting — no eligible wallet key loaded")
			}
			miningEligibilityChecks.WithLabelValues("not_eligible").Inc()
			continue
		}

		consecutiveMisses = 0
		miningEligibilityChecks.WithLabelValues("eligible").Inc()

		currentTime := uint64(time.Now().Unix())
		eligible, err := ms.powEngine.VerifyMinerEligibility(minerAddr, parent.Hash(), height, currentTime)
		if err != nil || !eligible {
			log.Printf("[miner] Eligibility failed: %v", err)
			ms.powEngine.RecordMissedBlock(minerAddr)
			continue
		}

		log.Printf("[miner] ✅ Wallet eligible. Mining block %d as %s", height, minerAddr.String()[:12])

		currentTip := bc.Latest()
		if currentTip == nil || currentTip.Hash() != parent.Hash() {
			log.Printf("[miner] Tip changed while preparing block %d; regenerating template", height)
			continue
		}

		blk := template.Block
		signature, err := generateBlockSignature(minerAddr, parent.Hash(), height, blk.Header.Time, privKey)
		if err != nil {
			log.Printf("[miner] Signing failed: %v", err)
			continue
		}

		if len(signature) > 0 {
			sigMarker := []byte("|SIG|")
			blk.Header.Extra = append(blk.Header.Extra, sigMarker...)
			blk.Header.Extra = append(blk.Header.Extra, signature...)
		}

		blk.Header.Difficulty = bc.CalculateExpectedDifficultyForBlock(blk, parent)
		stateRoot, err := bc.ComputeBlockFinalStateRoot(
			minerAddr,
			blk.Header.Time,
			height,
			blk.Header.Extra,
			blk.Txs,
		)
		if err != nil {
			log.Printf("[miner] Failed to recompute state root for block %d: %v", height, err)
			continue
		}
		blk.Header.Root = stateRoot

		if err := mineBlockProofOfWork(context.Background(), ms.powEngine, blk.Header); err != nil {
			log.Printf("[miner] Proof-of-work failed for block %d: %v", height, err)
			continue
		}

		currentTip = bc.Latest()
		if currentTip == nil || currentTip.Hash() != parent.Hash() {
			log.Printf("[miner] Tip changed while mining block %d; discarding candidate", height)
			continue
		}

		if err := bc.AddBlock(blk); err != nil {
			log.Printf("[miner] AddBlock failed: %v", err)
			if bc.GetBlock(height) != nil {
				ms.powEngine.RecordMissedBlock(minerAddr)
			}
			continue
		}

		ms.blocksMined++
		ms.mu.Lock()
		ms.totalRewards.Add(ms.totalRewards, template.TotalFees)
		blockReward := reward.CalculateBlockReward(height)
		ms.totalRewards.Add(ms.totalRewards, blockReward)
		ms.mu.Unlock()

		miningBlocksTotal.Inc()
		miningRewardsTotal.Set(float64(new(big.Int).Div(ms.totalRewards, big.NewInt(1e18)).Int64()))
		miningUptime.Set(time.Since(startTime).Seconds())
		ms.powEngine.RecordBlockMined(minerAddr, height)

		log.Printf("[miner] �� BLOCK #%d MINED by %s! Reward: %s ANTD",
			height, minerAddr.String()[:12],
			new(big.Int).Div(new(big.Int).Add(template.TotalFees, blockReward), big.NewInt(1e18)).String())

		ms.clearPendingHeight(height)

		if p2pNode != nil {
			go broadcastMinedBlock(p2pNode, blk, ms)
		}

		ms.templateMu.Lock()
		ms.currentTemplate = nil
		ms.templateMu.Unlock()
	}

	log.Println("[miner] Mining stopped")
}

// StartPowMining starts the mining loop
func StartPowMining(bc *chain.Blockchain, state *PosMiningState, rewardAddr common.QuantumAddress, p2pNode *p2p.Node, mempoolTxns []*TxEntry) {
	if bc == nil || rewardAddr == (common.QuantumAddress{}) || state.powEngine == nil {
		log.Println("[miner] Missing required components")
		return
	}
	if !state.enabled {
		log.Println("[miner] Mining disabled")
		return
	}
	if err := networkReadyForMining(bc, p2pNode); err != nil {
		log.Printf("[miner] Mining not started: %v", err)
		return
	}
	if state.mining {
		state.mining = false
		time.Sleep(200 * time.Millisecond)
	}
	if err := state.SetMinerAddress(rewardAddr); err != nil {
		log.Printf("[miner] Cannot set miner address: %v", err)
		return
	}

	log.Printf("[miner] Starting PoW mining checks for %s", rewardAddr.String()[:12])
	state.mining = true
	log.Printf("[miner] PoW Mining STARTED → %s", rewardAddr.String())

	go miningLoop(bc, state, p2pNode, mempoolTxns)
}

// StopMining stops mining
func StopMining(state *PosMiningState) {
	if state != nil {
		state.mining = false
		log.Println("[miner] Mining STOPPED")
	}
}

func formatFeeRate(fee *big.Int, size uint64) string {
	rate := new(big.Float).SetInt(fee)
	rate.Quo(rate, new(big.Float).SetUint64(size))
	return rate.Text('f', 8)
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

// Existing methods for compatibility
func (ms *PosMiningState) IsMining() bool    { return ms.mining }
func (ms *PosMiningState) IsEnabled() bool   { return ms.enabled }
func (ms *PosMiningState) SetEnabled(v bool) { ms.enabled = v }
func (ms *PosMiningState) SetMining(v bool)  { ms.mining = v }

func (ms *PosMiningState) SetMiningInterval(interval time.Duration) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if interval < MinimumMiningInterval {
		log.Printf("[miner] Requested mining interval %s too fast; clamped to %s", interval, MinimumMiningInterval)
		interval = MinimumMiningInterval
	}
	ms.miningInterval = interval
}

func (ms *PosMiningState) SetBroadcastRetryConfig(maxRetries int, initialBackoff, maxBackoff time.Duration) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.broadcastMaxRetries = maxRetries
	ms.broadcastInitialBackoff = initialBackoff
	ms.broadcastMaxBackoff = maxBackoff
}

func (ms *PosMiningState) SetMinerAddress(addr common.QuantumAddress) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.powEngine == nil {
		return errors.New("PoW engine not initialized")
	}
	ms.minerAddress = addr
	log.Printf("[miner] PoW miner address set → %s", addr.String()[:12])
	return nil
}

func (ms *PosMiningState) SetPrivateKeyFromBytes(keyBytes []byte) error {
	if len(keyBytes) == 0 {
		return errors.New("private key bytes are empty")
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.privateKey = append([]byte(nil), keyBytes...)
	ms.hasPrivateKey = true
	return nil
}

func (ms *PosMiningState) SetPrivateKeyFromHex(hexKey string) error {
	keyBytes, err := hexutil.Decode(hexKey)
	if err != nil {
		return fmt.Errorf("invalid hex key: %w", err)
	}
	return ms.SetPrivateKeyFromBytes(keyBytes)
}

func (ms *PosMiningState) GetMinerAddress() common.QuantumAddress {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.minerAddress
}

func (ms *PosMiningState) GetPublicKey() []byte {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if !ms.hasPrivateKey || len(ms.privateKey) == 0 {
		return nil
	}
	pubKey, err := quantum.DerivePublicKey(ms.privateKey)
	if err != nil {
		return nil
	}
	return pubKey
}

func (ms *PosMiningState) SetSyncCallback(cb func(isSyncing bool)) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.onSyncChange = cb
}

func (ms *PosMiningState) PauseMining() {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.mining {
		ms.mining = false
		log.Println("[miner] Mining PAUSED due to sync")
		if ms.onSyncChange != nil {
			ms.onSyncChange(true)
		}
	}
}

func (ms *PosMiningState) ResumeMining() {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if !ms.mining {
		ms.mining = true
		log.Println("[miner] Mining RESUMED")
		if ms.onSyncChange != nil {
			ms.onSyncChange(false)
		}
	}
}

func (ms *PosMiningState) CheckMiningEligibility(bc *chain.Blockchain) (bool, error) {
	if bc == nil || ms.powEngine == nil {
		return false, errors.New("blockchain or PoW engine not initialized")
	}
	ms.mu.RLock()
	minerAddr := ms.minerAddress
	ms.mu.RUnlock()
	if minerAddr == (common.QuantumAddress{}) {
		return false, errors.New("miner address not set")
	}
	parent := bc.Latest()
	if parent == nil {
		return false, errors.New("no parent block found")
	}
	height := parent.Header.Number.Uint64() + 1
	currentTime := uint64(time.Now().Unix())
	eligible, err := ms.powEngine.VerifyMinerEligibility(minerAddr, parent.Hash(), height, currentTime)
	if err != nil {
		return false, fmt.Errorf("eligibility check failed: %w", err)
	}
	return eligible, nil
}

func (ms *PosMiningState) GetNextMiningSlot(bc *chain.Blockchain) (uint64, time.Duration, error) {
	if bc == nil || ms.powEngine == nil {
		return 0, 0, errors.New("blockchain or PoW engine not initialized")
	}
	ms.mu.RLock()
	minerAddr := ms.minerAddress
	ms.mu.RUnlock()
	if minerAddr == (common.QuantumAddress{}) {
		return 0, 0, errors.New("miner address not set")
	}
	parent := bc.Latest()
	if parent == nil {
		return 0, 0, errors.New("no parent block found")
	}
	currentHeight := parent.Header.Number.Uint64()
	if !ms.powEngine.IsKing(minerAddr) {
		return 0, 0, errors.New("address is not in validator set")
	}
	stats := ms.powEngine.GetMiningStatistics()
	activeStakers, _ := stats["active_stakers"].(int)
	if activeStakers <= 0 {
		return 0, 0, errors.New("no active validators")
	}
	blocksUntilTurn := uint64(activeStakers)
	estimatedBlocks := blocksUntilTurn
	estimatedTime := time.Duration(estimatedBlocks*pow.TargetBlockTimeSeconds) * time.Second
	return currentHeight + estimatedBlocks, estimatedTime, nil
}

func (ms *PosMiningState) GetMiningStatistics() map[string]interface{} {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	stats := map[string]interface{}{
		"mining_enabled":          ms.enabled,
		"is_mining":               ms.mining,
		"miner_address":           ms.minerAddress.String(),
		"blocks_mined":            ms.blocksMined,
		"total_rewards_antd":      formatWei(ms.totalRewards),
		"total_rewards_wei":       ms.totalRewards.String(),
		"has_private_key":         ms.hasPrivateKey,
		"mining_interval_seconds": ms.miningInterval.Seconds(),
		"broadcast_max_retries":   ms.broadcastMaxRetries,
	}
	if ms.powEngine != nil {
		for k, v := range ms.powEngine.GetMiningStatistics() {
			stats["pos_"+k] = v
		}
	}
	if ms.currentTemplate != nil {
		stats["current_template_weight"] = ms.currentTemplate.TotalWeight
		stats["current_template_txs"] = len(ms.currentTemplate.Block.Txs)
		stats["current_template_fees"] = ms.currentTemplate.TotalFees.String()
	}
	return stats
}

func (ms *PosMiningState) LoadPrivateKeyFromKeystore(keystoreDir, password string) error {
	ms.mu.RLock()
	minerAddress := ms.minerAddress
	ms.mu.RUnlock()
	if minerAddress == (common.QuantumAddress{}) {
		return errors.New("miner address not set")
	}
	privKey, err := qkeystore.Unlock(minerAddress, password, keystoreDir)
	if err != nil {
		return fmt.Errorf("failed to unlock keystore: %w", err)
	}
	if len(privKey) != quantum.MLDSA65PrivateKeySize {
		return fmt.Errorf("invalid private key length: expected %d bytes, got %d", quantum.MLDSA65PrivateKeySize, len(privKey))
	}
	return ms.SetPrivateKeyFromBytes(privKey)
}

func (ms *PosMiningState) LoadPrivateKeyFromFile(filePath, password string) error {
	ms.mu.RLock()
	minerAddress := ms.minerAddress
	ms.mu.RUnlock()
	if minerAddress == (common.QuantumAddress{}) {
		return errors.New("miner address not set")
	}
	keyjson, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read keystore file: %w", err)
	}
	var ks qkeystore.KeyStore
	if err := json.Unmarshal(keyjson, &ks); err != nil {
		return fmt.Errorf("invalid keystore format: %w", err)
	}
	if ks.Address != minerAddress.String() {
		return fmt.Errorf("key address mismatch: expected %s, got %s", minerAddress.String(), ks.Address)
	}
	privKey, err := qkeystore.Unlock(minerAddress, password, filepath.Dir(filePath))
	if err != nil {
		return fmt.Errorf("failed to decrypt keystore: %w", err)
	}
	if len(privKey) != quantum.MLDSA65PrivateKeySize {
		return fmt.Errorf("invalid private key length: expected %d bytes, got %d", quantum.MLDSA65PrivateKeySize, len(privKey))
	}
	if err := ms.SetPrivateKeyFromBytes(privKey); err != nil {
		return err
	}
	log.Printf("[miner] ✓ Loaded private key from %s", filePath)
	return nil
}

func formatWei(wei *big.Int) string {
	if wei == nil {
		return "0"
	}
	antd := new(big.Float).SetInt(wei)
	antd.Quo(antd, big.NewFloat(1e18))
	return antd.Text('f', 6)
}

// StartPosMining is compatibility alias
func StartPosMining(bc *chain.Blockchain, state *PosMiningState, rewardAddr common.QuantumAddress, p2pNode *p2p.Node, mempoolTxns []*TxEntry) {
	StartPowMining(bc, state, rewardAddr, p2pNode, mempoolTxns)
}

var lastSyncLog time.Time = time.Now()
