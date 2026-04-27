// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package pow

import (
	"errors"
	"fmt"
	"log"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/antdaza/antdchain/antdc/crypto/quantum"
	"github.com/antdaza/antdchain/common"
)

const (
	BlocksPerMiner         = 5
	TargetBlockTimeSeconds = 12
	MaxFutureBlockTime     = 30
	MaxSkipBlocks          = 10
	MaxStakersInRotation   = 100
	EpochLength            = 100 // Blocks per epoch for difficulty recalculation
	BaseDifficulty         = 1
	MaxDifficulty          = 1000000
	// Additional selection weight awarded to the rotating king when it is also a staker.
	RotatingKingHashrateBoostPercent = 50
)

var (
	ErrInsufficientStake   = errors.New("insufficient stake for mining")
	ErrNotEligibleMiner    = errors.New("address not eligible to mine")
	ErrBlockTooFarInFuture = errors.New("block timestamp too far in future")
	ErrMinerSkippedTooMany = errors.New("miner skipped too many blocks")
	ErrInvalidSignature    = errors.New("invalid PoS signature")
	ErrDoubleSigning       = errors.New("detected double signing/equivocation")
	ErrNotUnbonding        = errors.New("validator not in unbonding state")
)

// Minimum stake: 1,000,000 ANTD = 1e6 * 1e18 wei
var MinStakeAmount = new(big.Int).Mul(big.NewInt(1_000_000), big.NewInt(1e18))

// Prometheus metrics
var (
	validatorSlashingCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "antdchain_validator_slashing_total",
			Help: "Total slashing events by reason",
		},
		[]string{"reason"},
	)

	activeStakersGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "antdchain_active_stakers",
			Help: "Current number of active stakers",
		},
	)

	totalStakedGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "antdchain_total_staked_antd",
			Help: "Total ANTD staked across all validators",
		},
	)

	missedBlocksCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "antdchain_missed_blocks_total",
			Help: "Total missed blocks by all validators",
		},
	)

	difficultyGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "antdchain_difficulty",
			Help: "Current network difficulty",
		},
	)

	blockTimeGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "antdchain_block_time_seconds",
			Help: "Average block time in seconds",
		},
	)
)

func init() {
	prometheus.MustRegister(validatorSlashingCounter)
	prometheus.MustRegister(activeStakersGauge)
	prometheus.MustRegister(totalStakedGauge)
	prometheus.MustRegister(missedBlocksCounter)
	prometheus.MustRegister(difficultyGauge)
	prometheus.MustRegister(blockTimeGauge)
}

// StakerInfo holds data about a validator
type StakerInfo struct {
	Address         common.QuantumAddress
	PublicKey       []byte // ML‑DSA‑65 public key (1952 bytes)
	StakeAmount     *big.Int
	TotalBlocks     uint64
	LastBlockHeight uint64
	MissedBlocks    uint64
	PenaltyScore    uint64
	IsActive        bool
	JoinTime        time.Time
	UnbondingEnd    *time.Time // When unbonding period ends (nil if not unbonding)
	LastSignedBlock uint64     // Last block height this validator signed
}

// PoW is the Proof-of-Stake consensus engine (kept name for compatibility)
type PoW struct {
	mu sync.RWMutex

	stakers     map[common.QuantumAddress]*StakerInfo
	stakerList  []common.QuantumAddress // ordered list for rotation and display
	totalStaked *big.Int

	currentMiner       common.QuantumAddress
	currentMinerBlocks uint64
	priorityMiner      common.QuantumAddress
	priorityBoostPct   uint64

	blockHistory []common.QuantumAddress // recent miners
	blockTimes   []uint64                // recent block times for difficulty calculation

	totalBlocks  atomic.Uint64
	rotations    atomic.Uint64
	missedBlocks atomic.Uint64

	// Difficulty-related fields
	currentDifficulty        *big.Int
	lastDifficultyAdjustment uint64
	averageBlockTime         float64
}

// NewPoW creates a new PoS engine
func NewPoW() *PoW {
	p := &PoW{
		stakers:                  make(map[common.QuantumAddress]*StakerInfo),
		stakerList:               make([]common.QuantumAddress, 0),
		totalStaked:              big.NewInt(0),
		blockHistory:             make([]common.QuantumAddress, 0, 100),
		blockTimes:               make([]uint64, 0, 100),
		currentDifficulty:        big.NewInt(BaseDifficulty),
		lastDifficultyAdjustment: 0,
		averageBlockTime:         float64(TargetBlockTimeSeconds),
	}

	log.Printf("[pos] ANTDChain PoS Engine initialized — Auto-registration enabled")
	log.Printf("[pos] Minimum stake: 1,000,000 ANTD")
	log.Printf("[pos] Blocks per rotation: %d", BlocksPerMiner)
	log.Printf("[pos] Target block time: %d seconds", TargetBlockTimeSeconds)
	log.Printf("[pos] Epoch length: %d blocks", EpochLength)

	return p
}

// === Difficulty Calculation Methods ===

// CalculateExpectedDifficulty calculates the expected difficulty for a block
// based on network conditions, stake distribution, and recent performance
func (p *PoW) CalculateExpectedDifficulty(height uint64, parentTime, currentTime uint64) *big.Int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Keep bootstrap blocks at the fixed network baseline difficulty.
	// This avoids transient validator-set effects on the first post-genesis blocks.
	if height <= 2 {
		return big.NewInt(1000000)
	}

	// Calculate time difference
	timeDiff := currentTime - parentTime
	if timeDiff == 0 {
		timeDiff = 1 // Avoid division by zero
	}

	// Calculate base difficulty based on active stakers
	activeStakers := 0
	totalEffectiveStake := big.NewInt(0)
	for _, staker := range p.stakers {
		if staker.IsActive && staker.UnbondingEnd == nil {
			activeStakers++
			totalEffectiveStake.Add(totalEffectiveStake, staker.StakeAmount)
		}
	}

	if activeStakers == 0 {
		return big.NewInt(BaseDifficulty)
	}

	// Base difficulty is inversely proportional to active stakers
	// More stakers = lower difficulty (easier to reach consensus)
	baseDifficulty := big.NewInt(int64(MaxDifficulty / (activeStakers + 1)))
	if baseDifficulty.Cmp(big.NewInt(BaseDifficulty)) < 0 {
		baseDifficulty = big.NewInt(BaseDifficulty)
	}

	// Adjust based on block time (aim for TargetBlockTimeSeconds)
	timeAdjustment := big.NewInt(int64(TargetBlockTimeSeconds * 100 / int(timeDiff)))

	// Adjust based on stake concentration (Gini coefficient-like measure)
	stakeConcentration := p.calculateStakeConcentration()
	concentrationFactor := big.NewInt(int64(stakeConcentration * 100))

	// Calculate final difficulty
	// NOTE: Do not include local runtime counters (e.g. missed blocks) here.
	// They can legitimately differ between nodes and lead to non-deterministic
	// expected difficulty during block validation.
	//
	// Difficulty = base * timeAdjustment * concentrationFactor
	difficulty := new(big.Int).Mul(baseDifficulty, timeAdjustment)
	difficulty.Mul(difficulty, concentrationFactor)

	// Apply bounds
	if difficulty.Cmp(big.NewInt(BaseDifficulty)) < 0 {
		difficulty = big.NewInt(BaseDifficulty)
	}
	if difficulty.Cmp(big.NewInt(MaxDifficulty)) > 0 {
		difficulty = big.NewInt(MaxDifficulty)
	}

	// Store for metrics
	p.currentDifficulty = new(big.Int).Set(difficulty)
	difficultyGauge.Set(float64(difficulty.Int64()))

	return difficulty
}

// calculateStakeConcentration calculates how concentrated stakes are (0-1)
// Lower value = more decentralized, Higher value = more concentrated
func (p *PoW) calculateStakeConcentration() float64 {
	if len(p.stakers) == 0 || p.totalStaked.Sign() == 0 {
		return 0.5 // Neutral value
	}

	var totalSquare big.Int
	for _, staker := range p.stakers {
		if staker.IsActive && staker.UnbondingEnd == nil {
			stake := new(big.Int).Set(staker.StakeAmount)
			stake.Mul(stake, stake)
			totalSquare.Add(&totalSquare, stake)
		}
	}

	// Calculate Herfindahl-Hirschman Index (HHI)
	// HHI = sum(s_i^2) where s_i is market share of validator i
	totalStakeSquared := new(big.Int).Mul(p.totalStaked, p.totalStaked)
	if totalStakeSquared.Sign() == 0 {
		return 0.5
	}

	// Convert to float for HHI calculation
	hhi := new(big.Rat).SetFrac(&totalSquare, totalStakeSquared)
	hhiFloat, _ := hhi.Float64()

	return hhiFloat
}

// AdjustDifficulty adjusts the difficulty based on recent block times and stake changes
func (p *PoW) AdjustDifficulty(height uint64, parentTime, currentTime uint64) *big.Int {
	// Record block time for moving average
	p.recordBlockTime(parentTime, currentTime)

	// Only adjust difficulty at epoch boundaries
	if height%uint64(EpochLength) != 0 {
		return p.currentDifficulty
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Calculate new difficulty based on recent performance
	newDifficulty := p.calculateNewDifficulty(height)
	p.currentDifficulty = new(big.Int).Set(newDifficulty)
	p.lastDifficultyAdjustment = height

	// Update metrics
	difficultyGauge.Set(float64(newDifficulty.Int64()))
	blockTimeGauge.Set(p.averageBlockTime)

	log.Printf("[pos] Difficulty adjusted at epoch %d: %d (avg block time: %.2fs)",
		height/EpochLength, newDifficulty.Int64(), p.averageBlockTime)

	return newDifficulty
}

// calculateNewDifficulty calculates new difficulty based on recent performance
func (p *PoW) calculateNewDifficulty(height uint64) *big.Int {
	if len(p.blockTimes) == 0 {
		return p.currentDifficulty
	}

	// Calculate average block time from recent blocks
	var totalTime uint64
	for _, t := range p.blockTimes {
		totalTime += t
	}
	avgTime := float64(totalTime) / float64(len(p.blockTimes))

	// Store for metrics
	p.averageBlockTime = avgTime

	// Adjust difficulty based on deviation from target
	ratio := avgTime / float64(TargetBlockTimeSeconds)
	adjustment := new(big.Float).SetFloat64(ratio)

	currentDiff := new(big.Float).SetInt(p.currentDifficulty)
	currentDiff.Mul(currentDiff, adjustment)

	newDiff := new(big.Int)
	currentDiff.Int(newDiff)

	// Apply bounds
	if newDiff.Cmp(big.NewInt(BaseDifficulty)) < 0 {
		newDiff = big.NewInt(BaseDifficulty)
	}
	if newDiff.Cmp(big.NewInt(MaxDifficulty)) > 0 {
		newDiff = big.NewInt(MaxDifficulty)
	}

	return newDiff
}

// recordBlockTime records block time for moving average
func (p *PoW) recordBlockTime(parentTime, currentTime uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	timeDiff := currentTime - parentTime
	if timeDiff == 0 {
		timeDiff = 1
	}

	p.blockTimes = append(p.blockTimes, timeDiff)
	if len(p.blockTimes) > 100 {
		p.blockTimes = p.blockTimes[1:]
	}
}

func (p *PoW) GetDifficulty() *big.Int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return new(big.Int).Set(p.currentDifficulty)
}

func (p *PoW) GetTarget() *big.Int {
	return new(big.Int).Exp(big.NewInt(2), big.NewInt(256), nil)
}

func (p *PoW) SetDifficulty(diff *big.Int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.currentDifficulty = new(big.Int).Set(diff)
	difficultyGauge.Set(float64(diff.Int64()))
}

func (p *PoW) Release() {}

// === AUTOMATIC STAKER REGISTRATION ===
// AutoRegisterIfEligible checks balance and registers if ≥ 1M ANTD
func (p *PoW) AutoRegisterIfEligible(addr common.QuantumAddress, balance *big.Int, pubKey []byte) {
	if balance.Cmp(MinStakeAmount) < 0 {
		p.mu.Lock()
		if _, exists := p.stakers[addr]; exists {
			p.removeStakerLocked(addr)
		}
		p.mu.Unlock()
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	info, exists := p.stakers[addr]
	if exists {
		// Allow a valid key to repair legacy/incomplete registrations even when stake is unchanged.
		if len(pubKey) == quantum.MLDSA65PublicKeySize && len(info.PublicKey) != quantum.MLDSA65PublicKeySize {
			info.PublicKey = append([]byte(nil), pubKey...)
			log.Printf("[pos] Auto-updated staker %s public key", addr.String()[:12])
		}

		if info.IsActive && info.StakeAmount.Cmp(balance) == 0 {
			return // already correctly registered
		}
		p.totalStaked.Sub(p.totalStaked, info.StakeAmount)
		info.StakeAmount = new(big.Int).Set(balance)
		// Do not overwrite a previously valid key with an empty/invalid update.
		// Many call sites auto-refresh stake without a key payload.
		if len(pubKey) == quantum.MLDSA65PublicKeySize {
			info.PublicKey = append([]byte(nil), pubKey...)
		}
		info.IsActive = true
		info.UnbondingEnd = nil
		p.totalStaked.Add(p.totalStaked, balance)

		totalStakedGauge.Set(float64(new(big.Int).Div(p.totalStaked, big.NewInt(1e18)).Int64()))

		log.Printf("[pos] Auto-updated staker %s → %s ANTD",
			addr.String()[:12],
			new(big.Int).Div(balance, big.NewInt(1e18)))
		return
	}

	info = &StakerInfo{
		Address:     addr,
		PublicKey:   append([]byte(nil), pubKey...),
		StakeAmount: new(big.Int).Set(balance),
		IsActive:    true,
		JoinTime:    time.Now(),
	}
	p.stakers[addr] = info
	p.stakerList = append(p.stakerList, addr)
	p.totalStaked.Add(p.totalStaked, balance)

	activeStakersGauge.Inc()
	totalStakedGauge.Set(float64(new(big.Int).Div(p.totalStaked, big.NewInt(1e18)).Int64()))

	if len(pubKey) != quantum.MLDSA65PublicKeySize {
		log.Printf("[pos] Auto-registered staker %s with %s ANTD (no public key provided yet)",
			addr.String()[:12],
			new(big.Int).Div(balance, big.NewInt(1e18)))
		return
	}

	log.Printf("[pos] �� Auto-registered new staker: %s with %s ANTD",
		addr.String()[:12],
		new(big.Int).Div(balance, big.NewInt(1e18)))
}

func (p *PoW) removeStakerLocked(addr common.QuantumAddress) {
	info := p.stakers[addr]
	if info == nil {
		return
	}

	p.totalStaked.Sub(p.totalStaked, info.StakeAmount)
	delete(p.stakers, addr)
	for i, a := range p.stakerList {
		if a == addr {
			p.stakerList = append(p.stakerList[:i], p.stakerList[i+1:]...)
			break
		}
	}

	if info.IsActive && info.UnbondingEnd == nil {
		activeStakersGauge.Dec()
	}
	totalStakedGauge.Set(float64(new(big.Int).Div(p.totalStaked, big.NewInt(1e18)).Int64()))
}

// GetNextMiner selects the next miner based on stake-weighted VRF selection
func (p *PoW) GetNextMiner(parentHash common.Hash, height uint64) (common.QuantumAddress, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.stakerList) == 0 {
		return common.QuantumAddress{}, errors.New("no active stakers")
	}

	next := p.selectNextMinerLocked(parentHash, height)
	if next == (common.QuantumAddress{}) {
		return common.QuantumAddress{}, errors.New("failed to select miner")
	}
	if next != p.currentMiner {
		p.currentMiner = next
		p.currentMinerBlocks = 0
		p.rotations.Add(1)
		log.Printf("[pos] Rotation → current miner: %s (block %d)", next.String()[:12], height)
	}

	return p.currentMiner, nil
}

func (p *PoW) selectNextMinerLocked(parentHash common.Hash, height uint64) common.QuantumAddress {
	n := len(p.stakerList)
	if n == 0 {
		return common.QuantumAddress{}
	}

	type candidate struct {
		addr   common.QuantumAddress
		weight uint64
	}
	var candidates []candidate
	var totalWeight uint64

	for _, addr := range p.stakerList {
		s := p.stakers[addr]
		if !s.IsActive || s.UnbondingEnd != nil || s.StakeAmount.Cmp(MinStakeAmount) < 0 {
			continue
		}
		base := new(big.Int).Div(s.StakeAmount, big.NewInt(1e18)).Uint64()
		if base == 0 {
			base = 1
		}
		// NOTE: consensus miner selection must be deterministic across nodes.
		// Do not include local runtime counters/flags (e.g. missed blocks or
		// locally configured priority miner bonus) in this calculation.
		// Those values can differ transiently between peers and cause valid
		// blocks to be rejected with "address not eligible to mine".
		candidates = append(candidates, candidate{addr: addr, weight: base})
		totalWeight += base
	}

	if len(candidates) == 0 {
		return common.QuantumAddress{}
	}

	// Quantum‑safe VRF‑like selection using SHA3‑256 instead of Keccak256
	seed := common.ComputeHash(append(parentHash.Bytes(),
		common.LeftPadBytes(big.NewInt(int64(height)).Bytes(), 32)...))
	randVal := new(big.Int).SetBytes(seed.Bytes()).Uint64() % totalWeight

	var cum uint64
	for _, c := range candidates {
		cum += c.weight
		if randVal < cum {
			return c.addr
		}
	}
	return candidates[0].addr
}

// MAIN CONSENSUS RULE
func (p *PoW) VerifyMinerEligibility(
	miner common.QuantumAddress,
	parentHash common.Hash,
	height uint64,
	timestamp uint64,
) (bool, error) {
	if timestamp > uint64(time.Now().Unix()+MaxFutureBlockTime) {
		return false, ErrBlockTooFarInFuture
	}

	expected, err := p.GetNextMiner(parentHash, height)
	if err != nil || expected != miner {
		return false, ErrNotEligibleMiner
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	s, ok := p.stakers[miner]
	if !ok || !s.IsActive || s.UnbondingEnd != nil || s.StakeAmount.Cmp(MinStakeAmount) < 0 {
		return false, ErrInsufficientStake
	}

	return true, nil
}

// RecordBlockMined updates stats after successful mining
func (p *PoW) RecordBlockMined(miner common.QuantumAddress, height uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.blockHistory = append(p.blockHistory, miner)
	if len(p.blockHistory) > 100 {
		p.blockHistory = p.blockHistory[1:]
	}

	if s, ok := p.stakers[miner]; ok {
		s.TotalBlocks++
		s.LastBlockHeight = height
		s.LastSignedBlock = height
		s.MissedBlocks = 0
		s.PenaltyScore = 0
	}

	if miner == p.currentMiner {
		p.currentMinerBlocks++
	}

	p.totalBlocks.Add(1)
}

// RecordMissedBlock applies penalties
func (p *PoW) RecordMissedBlock(miner common.QuantumAddress) {
	p.mu.Lock()
	defer p.mu.Unlock()

	s, ok := p.stakers[miner]
	if !ok {
		return
	}

	s.MissedBlocks++
	s.PenaltyScore += 10
	p.missedBlocks.Add(1)
	missedBlocksCounter.Inc()

	if s.MissedBlocks > MaxSkipBlocks {
		slash := new(big.Int).Div(s.StakeAmount, big.NewInt(20)) // 5%
		s.StakeAmount.Sub(s.StakeAmount, slash)
		p.totalStaked.Sub(p.totalStaked, slash)

		validatorSlashingCounter.WithLabelValues("missed_blocks").Add(float64(slash.Uint64()) / 1e18)
		totalStakedGauge.Set(float64(new(big.Int).Div(p.totalStaked, big.NewInt(1e18)).Int64()))

		if s.StakeAmount.Cmp(MinStakeAmount) < 0 {
			s.IsActive = false
			activeStakersGauge.Dec()
			log.Printf("[pos] Staker %s deactivated after slashing", miner.String()[:12])
		}
		log.Printf("[pos] Slashed 5%% from %s for missing blocks", miner.String()[:12])
	}
}

// RecordDoubleSign applies heavy penalty for equivocation
func (p *PoW) RecordDoubleSign(miner common.QuantumAddress, blockHeight uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	s, ok := p.stakers[miner]
	if !ok {
		return errors.New("validator not found")
	}

	if s.LastSignedBlock != blockHeight {
		return nil
	}

	slash := new(big.Int).Div(s.StakeAmount, big.NewInt(5))
	s.StakeAmount.Sub(s.StakeAmount, slash)
	p.totalStaked.Sub(p.totalStaked, slash)

	validatorSlashingCounter.WithLabelValues("double_sign").Add(float64(slash.Uint64()) / 1e18)
	totalStakedGauge.Set(float64(new(big.Int).Div(p.totalStaked, big.NewInt(1e18)).Int64()))

	s.IsActive = false
	activeStakersGauge.Dec()

	log.Printf("[pos] ⚠️ SEVERE: Validator %s slashed 20%% for double-signing at height %d",
		miner.String()[:12], blockHeight)

	return nil
}

// UnregisterValidator starts the unbonding process
func (p *PoW) UnregisterValidator(addr common.QuantumAddress) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	info, exists := p.stakers[addr]
	if !exists {
		return errors.New("validator not found")
	}

	if info.UnbondingEnd != nil {
		return errors.New("validator already unbonding")
	}

	unbondingEnd := time.Now().Add(7 * 24 * time.Hour)
	info.UnbondingEnd = &unbondingEnd
	info.IsActive = false
	activeStakersGauge.Dec()

	log.Printf("[pos] Validator %s started unbonding period (ends: %v)",
		addr.String()[:12], unbondingEnd)

	return nil
}

// CompleteUnbonding completes the unbonding process and removes validator
func (p *PoW) CompleteUnbonding(addr common.QuantumAddress) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	info, exists := p.stakers[addr]
	if !exists {
		return false, errors.New("validator not found")
	}

	if info.UnbondingEnd == nil {
		return false, ErrNotUnbonding
	}

	if time.Now().Before(*info.UnbondingEnd) {
		return false, nil
	}

	p.removeStakerLocked(addr)
	log.Printf("[pos] Validator %s completed unbonding and removed",
		addr.String()[:12])

	return true, nil
}

// GenerateBlockSignature generates an ML‑DSA‑65 signature for a PoS block.
func (p *PoW) GenerateBlockSignature(miner common.QuantumAddress, parentHash common.Hash, height, timestamp uint64, privKey []byte) ([]byte, error) {
	if len(privKey) == 0 {
		return nil, errors.New("private key required")
	}

	msg := common.ComputeHash(
		append(
			[]byte("ANTDChain-PoS-Block"),
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

	return quantum.Sign(privKey, msg)
}

// VerifyBlockSignature verifies an ML‑DSA‑65 block signature.
// The public key is retrieved from the staker set.
func (p *PoW) VerifyBlockSignature(miner common.QuantumAddress, parentHash common.Hash, height, timestamp uint64, sig []byte) (bool, error) {
	if len(sig) != quantum.MLDSA65SignatureSize {
		return false, fmt.Errorf("invalid signature length: expected %d, got %d", quantum.MLDSA65SignatureSize, len(sig))
	}

	p.mu.RLock()
	stakerInfo, exists := p.stakers[miner]
	p.mu.RUnlock()
	if !exists {
		return false, fmt.Errorf("miner %s not found in staker set", miner.String())
	}
	if len(stakerInfo.PublicKey) != quantum.MLDSA65PublicKeySize {
		return false, errors.New("invalid public key stored for miner")
	}

	msg := common.ComputeHash(
		append(
			[]byte("ANTDChain-PoS-Block"),
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

	if !quantum.Verify(stakerInfo.PublicKey, msg, sig) {
		return false, errors.New("quantum signature verification failed")
	}

	return true, nil
}

// GetStatistics returns detailed statistics
func (p *PoW) GetMiningStatistics() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	active := 0
	for _, s := range p.stakers {
		if s.IsActive && s.UnbondingEnd == nil {
			active++
		}
	}

	return map[string]interface{}{
		"active_stakers":       active,
		"total_stakers":        len(p.stakers),
		"total_staked_antd":    new(big.Int).Div(p.totalStaked, big.NewInt(1e18)).String(),
		"total_blocks":         p.totalBlocks.Load(),
		"current_miner":        p.currentMiner.String(),
		"blocks_this_turn":     p.currentMinerBlocks,
		"blocks_per_turn":      BlocksPerMiner,
		"rotations":            p.rotations.Load(),
		"missed_blocks":        p.missedBlocks.Load(),
		"current_difficulty":   p.currentDifficulty.String(),
		"average_block_time":   p.averageBlockTime,
		"target_block_time":    TargetBlockTimeSeconds,
		"epoch_length":         EpochLength,
		"unbonding_validators": p.countUnbondingValidators(),
	}
}

func (p *PoW) countUnbondingValidators() int {
	count := 0
	for _, s := range p.stakers {
		if s.UnbondingEnd != nil {
			count++
		}
	}
	return count
}

// GetKingAddresses returns list of active validator addresses
func (p *PoW) GetKingAddresses() []common.QuantumAddress {
	p.mu.RLock()
	defer p.mu.RUnlock()
	list := make([]common.QuantumAddress, 0, len(p.stakerList))
	for _, addr := range p.stakerList {
		if s, ok := p.stakers[addr]; ok && s.IsActive && s.UnbondingEnd == nil {
			list = append(list, addr)
		}
	}
	return list
}

// IsKing checks if address is an active validator
func (p *PoW) IsKing(addr common.QuantumAddress) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	s, ok := p.stakers[addr]
	return ok && s.IsActive && s.UnbondingEnd == nil
}

// SetPriorityMiner configures an optional mining weight bonus for one staker.
func (p *PoW) SetPriorityMiner(addr common.QuantumAddress, boostPercent uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.priorityMiner = addr
	p.priorityBoostPct = boostPercent
}
