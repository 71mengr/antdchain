// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package mining

import (
	"encoding/json"
	"errors"
	"fmt"
	qkeystore "github.com/antdaza/antdchain/antdc/accounts/keystore"
	"github.com/antdaza/antdchain/antdc/block"
	"github.com/antdaza/antdchain/antdc/chain"
	"github.com/antdaza/antdchain/antdc/crypto/quantum"
	"github.com/antdaza/antdchain/antdc/p2p"
	"github.com/antdaza/antdchain/antdc/pow"
	"github.com/antdaza/antdchain/common"
	"github.com/antdaza/antdchain/common/hexutil"
	"github.com/prometheus/client_golang/prometheus"
	"log"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Configuration constants
const (
	DefaultMiningInterval         = 12 * time.Second
	MinimumMiningInterval         = 10 * time.Second
	DefaultBroadcastMaxRetries    = 5
	DefaultBroadcastInitialBackoff = 100 * time.Millisecond
	DefaultBroadcastMaxBackoff     = 2 * time.Second
	LogEligibilityCheckInterval    = 10
	LogSyncStatusInterval          = 10 * time.Second

	// First‑seen rule: how long to wait for a pending block at same height
	DefaultPendingBlockTimeout = 2 * time.Second
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

var lastSyncLog time.Time = time.Now()

// PosMiningState manages Proof-of-Work mining
type PosMiningState struct {
	mining       bool
	enabled      bool
	minerAddress common.QuantumAddress
	powEngine    *pow.PoW
	privateKey   []byte
	hasPrivateKey bool

	blocksMined  uint64
	totalRewards *big.Int

	// Configuration
	miningInterval           time.Duration
	broadcastMaxRetries      int
	broadcastInitialBackoff  time.Duration
	broadcastMaxBackoff      time.Duration
	pendingBlockTimeout      time.Duration

	// Tracking first‑seen blocks
	pendingHeights map[uint64]time.Time
	pendingMu      sync.RWMutex

	mu sync.RWMutex
	onSyncChange func(isSyncing bool)
}

func NewPosMiningState(powEngine *pow.PoW) *PosMiningState {
	return &PosMiningState{
		enabled:                true,
		powEngine:              powEngine,
		totalRewards:           big.NewInt(0),
		miningInterval:         DefaultMiningInterval,
		broadcastMaxRetries:    DefaultBroadcastMaxRetries,
		broadcastInitialBackoff: DefaultBroadcastInitialBackoff,
		broadcastMaxBackoff:     DefaultBroadcastMaxBackoff,
		pendingBlockTimeout:     DefaultPendingBlockTimeout,
		pendingHeights:          make(map[uint64]time.Time),
	}
}

// OnBlockReceived must be called by the p2p layer whenever a new block is announced or received.
// It marks the height as “pending” so the miner will wait before trying to mine the same height.
func (ms *PosMiningState) OnBlockReceived(height uint64) {
	ms.pendingMu.Lock()
	defer ms.pendingMu.Unlock()
	if _, exists := ms.pendingHeights[height]; !exists {
		ms.pendingHeights[height] = time.Now()
		log.Printf("[miner] ⏳ New block announced at height %d – will wait %.1fs before mining it",
			height, ms.pendingBlockTimeout.Seconds())
	}
}

// clearPendingHeight removes a height from the pending set.
func (ms *PosMiningState) clearPendingHeight(height uint64) {
	ms.pendingMu.Lock()
	defer ms.pendingMu.Unlock()
	delete(ms.pendingHeights, height)
}

// waitForPendingBlock waits up to pendingBlockTimeout for a block at the given height
// to appear in the chain. Returns true if a block was added, false after timeout.
func (ms *PosMiningState) waitForPendingBlock(bc *chain.Blockchain, height uint64) bool {
	// Quick check: is this height even pending?
	ms.pendingMu.RLock()
	_, pending := ms.pendingHeights[height]
	ms.pendingMu.RUnlock()
	if !pending {
		return false
	}

	log.Printf("[miner] 🕒 Pending block at height %d – waiting up to %v for it to arrive",
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

// IsMining, IsEnabled, SetEnabled, SetMining (unchanged, but included for completeness)
func (ms *PosMiningState) IsMining() bool    { return ms.mining }
func (ms *PosMiningState) IsEnabled() bool   { return ms.enabled }
func (ms *PosMiningState) SetEnabled(v bool) { ms.enabled = v }
func (ms *PosMiningState) SetMining(v bool)  { ms.mining = v }

// SetMiningInterval sets the mining check interval
func (ms *PosMiningState) SetMiningInterval(interval time.Duration) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if interval < MinimumMiningInterval {
		log.Printf("[miner] Requested mining interval %s too fast; clamped to %s", interval, MinimumMiningInterval)
		interval = MinimumMiningInterval
	}
	ms.miningInterval = interval
}

// SetBroadcastRetryConfig sets broadcast retry configuration
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

// StartPowMining starts the mining loop
func StartPowMining(bc *chain.Blockchain, state *PosMiningState, rewardAddr common.QuantumAddress, p2pNode *p2p.Node) {
	if bc == nil || rewardAddr == (common.QuantumAddress{}) || state.powEngine == nil {
		log.Println("[miner] Missing required components")
		return
	}
	if !state.enabled {
		log.Println("[miner] Mining disabled")
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
	go posMiningLoop(bc, state, p2pNode)
}

func StopMining(state *PosMiningState) {
	if state != nil {
		state.mining = false
		log.Println("[miner] Mining STOPPED")
	}
}

// posMiningLoop is the main mining goroutine
func posMiningLoop(bc *chain.Blockchain, ms *PosMiningState, p2pNode *p2p.Node) {
	ms.mu.RLock()
	miningInterval := ms.miningInterval
	ms.mu.RUnlock()

	// Add random jitter to reduce simultaneous mining attempts
	jitter := time.Duration(rand.Int63n(int64(miningInterval / 2)))
	time.Sleep(jitter)
	log.Printf("[miner] Mining loop started – base interval %v + jitter %v", miningInterval, jitter)

	ticker := time.NewTicker(miningInterval)
	defer ticker.Stop()

	var (
		consecutiveMisses int
		totalMined        uint64
		startTime         = time.Now()
		sessionStartTime  = time.Now()
		eligibilityChecks int
	)

	// Update session duration metric
	go func() {
		for ms.mining {
			miningSessionDuration.Set(time.Since(sessionStartTime).Seconds())
			time.Sleep(1 * time.Second)
		}
	}()

	for ms.mining {
		<-ticker.C

		if bc.IsSyncing() {
			if time.Since(lastSyncLog) > LogSyncStatusInterval {
				log.Printf("[miner] Sync in progress (height %d → %d) — mining paused",
					bc.GetChainHeight(), bc.GetSyncTarget())
				lastSyncLog = time.Now()
			}
			time.Sleep(1 * time.Second)
			continue
		}

		parent := bc.Latest()
		if parent == nil {
			continue
		}

		height := parent.Header.Number.Uint64() + 1
		if height <= bc.GetChainHeight() {
			continue
		}
		if existing := bc.GetBlock(height); existing != nil {
			ms.clearPendingHeight(height)
			continue
		}

		// ----- CRITICAL: Wait for a pending block (first‑seen rule) -----
		if ms.waitForPendingBlock(bc, height) {
			continue // block arrived, skip mining
		}
		// -------------------------------------------------------------

		if ms.powEngine == nil {
			continue
		}

		// Dynamic wallet mining: get eligible key and address
		var (
			eligiblePrivKey   []byte
			configuredMiner   common.QuantumAddress
			loadedKeyAddress  common.QuantumAddress
		)
		ms.mu.RLock()
		configuredMiner = ms.minerAddress
		if ms.hasPrivateKey && len(ms.privateKey) > 0 {
			pubKey, err := quantum.DerivePublicKey(ms.privateKey)
			if err == nil {
				parsedAddr, addrErr := common.ParseQuantumAddress(quantum.PubKeyToAddress(pubKey))
				if addrErr == nil {
					loadedKeyAddress = parsedAddr
					eligiblePrivKey = append([]byte(nil), ms.privateKey...)
				}
			}
		}
		ms.mu.RUnlock()

		minerToUse := loadedKeyAddress
		if minerToUse == (common.QuantumAddress{}) {
			minerToUse = configuredMiner
		}

		eligibilityChecks++
		if len(eligiblePrivKey) == 0 || minerToUse == (common.QuantumAddress{}) {
			consecutiveMisses++
			if consecutiveMisses == 1 || consecutiveMisses%LogEligibilityCheckInterval == 0 {
				log.Printf("[miner] Waiting — no eligible wallet key loaded (configured: %s, key: %s)",
					configuredMiner.String()[:12], loadedKeyAddress.String()[:12])
			}
			miningEligibilityChecks.WithLabelValues("not_eligible").Inc()
			continue
		}

		consecutiveMisses = 0
		miningEligibilityChecks.WithLabelValues("eligible").Inc()
		log.Printf("[miner] ✅ Wallet eligible. Mining block %d as %s", height, minerToUse.String()[:12])

		currentTime := uint64(time.Now().Unix())
		eligible, err := ms.powEngine.VerifyMinerEligibility(minerToUse, parent.Hash(), height, currentTime)
		if err != nil || !eligible {
			log.Printf("[miner] Eligibility failed: %v", err)
			ms.powEngine.RecordMissedBlock(minerToUse)
			continue
		}

		// Create block
		newBlock, _, err := bc.CreateMiningBlock(minerToUse)
		if err != nil || newBlock == nil {
			log.Printf("[miner] Block creation failed: %v", err)
			continue
		}

		// Check tip hasn't changed
		currentTip := bc.Latest()
		if currentTip == nil || currentTip.Hash() != parent.Hash() || currentTip.Header.Number.Uint64()+1 != height {
			log.Printf("[miner] Tip changed while building block %d; resyncing", height)
			continue
		}
		if bc.IsSyncing() {
			log.Printf("[miner] Sync resumed while preparing block %d; skipping", height)
			continue
		}

		// Sign
		signature, err := generateBlockSignature(minerToUse, parent.Hash(), height, newBlock.Header.Time, eligiblePrivKey)
		if err != nil {
			log.Printf("[miner] Signing failed: %v", err)
			continue
		}
		if len(signature) > 0 {
			sigMarker := []byte("|SIG|")
			extra := append(newBlock.Header.Extra, sigMarker...)
			extra = append(extra, signature...)
			newBlock.Header.Extra = extra
		}

		// Submit
		if err := bc.AddBlock(newBlock); err != nil {
			log.Printf("[miner] AddBlock failed: %v", err)
			if bc.GetBlock(height) != nil {
				ms.powEngine.RecordMissedBlock(minerToUse)
			}
			continue
		}

		// Success
		totalMined++
		ms.blocksMined++
		blockReward := calculateBlockReward(height, bc)
		ms.mu.Lock()
		ms.totalRewards.Add(ms.totalRewards, blockReward)
		ms.mu.Unlock()

		miningBlocksTotal.Inc()
		miningRewardsTotal.Set(float64(new(big.Int).Div(ms.totalRewards, big.NewInt(1e18)).Int64()))
		miningUptime.Set(time.Since(startTime).Seconds())
		ms.powEngine.RecordBlockMined(minerToUse, height)

		log.Printf("[miner] 🎉 BLOCK #%d MINED by %s! Reward: %s ANTD",
			height, minerToUse.String()[:12],
			new(big.Int).Div(blockReward, big.NewInt(1e18)).String())
		log.Printf("[miner]   Total mined this session: %d", totalMined)

		ms.clearPendingHeight(height)

		if p2pNode != nil {
			go broadcastMinedBlock(p2pNode, newBlock, ms)
		}

		if totalMined%5 == 0 {
			avg := time.Since(startTime).Seconds() / float64(totalMined)
			log.Printf("[miner] 📊 Mined %d blocks (avg %.1fs/block)", totalMined, avg)
		}
	}
	log.Println("[miner] Mining stopped")
}

// generateBlockSignature creates a quantum signature for a block
func generateBlockSignature(miner common.QuantumAddress, parentHash common.Hash, height uint64, timestamp uint64, privateKey []byte) ([]byte, error) {
	if len(privateKey) == 0 {
		return nil, errors.New("private key required")
	}
	msg := common.ComputeHash(
		append(
			[]byte("ANTDChain-PoW-Block"),
			append(
				parentHash.Bytes(),
				append(
					common.LeftPadBytes(big.NewInt(int64(height)).Bytes(), 32),
					append(
						common.LeftPadBytes(big.NewInt(int64(timestamp)).Bytes(), 32),
						miner.Bytes()...,
					)...,
				)...,
			)...,
		),
	).Bytes()
	return quantum.Sign(privateKey, msg)
}

// verifyBlockSignature verifies a block signature (kept for completeness)
func verifyBlockSignature(miner common.QuantumAddress, parentHash common.Hash, height uint64, timestamp uint64, signature []byte, expectedPublicKey []byte) (bool, error) {
	if len(signature) == 0 {
		return true, nil
	}
	if len(expectedPublicKey) == 0 {
		return false, errors.New("expected public key required")
	}
	msg := common.ComputeHash(
		append(
			[]byte("ANTDChain-PoW-Block"),
			append(
				parentHash.Bytes(),
				append(
					common.LeftPadBytes(big.NewInt(int64(height)).Bytes(), 32),
					append(
						common.LeftPadBytes(big.NewInt(int64(timestamp)).Bytes(), 32),
						miner.Bytes()...,
					)...,
				)...,
			)...,
		),
	).Bytes()
	return quantum.Verify(expectedPublicKey, msg, signature), nil
}

// broadcastMinedBlock with exponential backoff
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

// extractSignatureFromBlock extracts the signature from a block's Extra field
func extractSignatureFromBlock(blk *block.Block) []byte {
	if blk == nil || blk.Header == nil {
		return nil
	}
	extra := blk.Header.Extra
	sigMarker := []byte("|SIG|")
	for i := 0; i <= len(extra)-len(sigMarker); i++ {
		if string(extra[i:i+len(sigMarker)]) == string(sigMarker) {
			return extra[i+len(sigMarker):]
		}
	}
	return nil
}

// VerifyBlockSignature verifies the signature of a mined block
func (ms *PosMiningState) VerifyBlockSignature(blk *block.Block, expectedPublicKey []byte) (bool, error) {
	if blk == nil || blk.Header == nil {
		return false, errors.New("nil block or header")
	}
	signature := extractSignatureFromBlock(blk)
	if len(signature) == 0 {
		return false, errors.New("no signature found in block")
	}
	return verifyBlockSignature(
		blk.Header.Coinbase,
		blk.Header.ParentHash,
		blk.Header.Number.Uint64(),
		blk.Header.Time,
		signature,
		expectedPublicKey,
	)
}

// GetMiningStatistics returns current mining stats
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
	return stats
}

// LoadPrivateKeyFromKeystore loads and decrypts the private key from keystore
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

// LoadPrivateKeyFromFile loads a private key from a keystore file
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

// CheckMiningEligibility checks if the current miner address is eligible to mine
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

// GetNextMiningSlot estimates when this miner will get to mine next
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

func calculateBlockReward(height uint64, bc *chain.Blockchain) *big.Int {
	baseReward := new(big.Int).Mul(big.NewInt(200), big.NewInt(1e18))
	if height > 0 && height%1000000 == 0 {
		baseReward.Div(baseReward, big.NewInt(2))
	}
	return baseReward
}

// UpdateTotalRewards updates the total rewards with additional reward
func (ms *PosMiningState) UpdateTotalRewards(additionalReward *big.Int) {
	if additionalReward == nil || additionalReward.Sign() == 0 {
		return
	}
	ms.mu.Lock()
	ms.totalRewards.Add(ms.totalRewards, additionalReward)
	ms.mu.Unlock()
	miningRewardsTotal.Set(float64(new(big.Int).Div(ms.totalRewards, big.NewInt(1e18)).Int64()))
}

// StartPosMining is kept as a compatibility alias
func StartPosMining(bc *chain.Blockchain, state *PosMiningState, rewardAddr common.QuantumAddress, p2pNode *p2p.Node) {
	StartPowMining(bc, state, rewardAddr, p2pNode)
}