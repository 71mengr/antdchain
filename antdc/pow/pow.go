// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package pow

import (
	"context"
	"encoding/binary"
	"errors"
	"log"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antdaza/antdchain/common"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	BlockTimeTarget                  = 3 * 60 // seconds per block
	TargetBlockTimeSeconds           = BlockTimeTarget
	DifficultyAdjustment             = 100       // blocks between retargets
	MaxDifficulty                    = 1_000_000 // arbitrary cap
	MinDifficulty                    = 1
	MaxFutureBlockTime               = 30 // seconds of clock drift allowed
	BaseDifficulty                   = 1000
	RotatingKingHashrateBoostPercent = 50
)

var (
	ErrInvalidNonce        = errors.New("invalid nonce")
	ErrBlockTooFarInFuture = errors.New("block timestamp too far in future")
)

// ─── Prometheus metrics ─────────────────────────────────────────────────────
var (
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
	blocksMinedCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "antdchain_blocks_mined_total",
			Help: "Total blocks mined by this node",
		},
	)
)

func init() {
	prometheus.MustRegister(difficultyGauge, blockTimeGauge, blocksMinedCounter)
}

// PoW is a quantum‑resistant Proof‑of‑Work engine using SHA3‑256.
type PoW struct {
	mu sync.RWMutex

	difficulty       *big.Int
	blockTimes       []uint64 // last N block times for moving average
	averageBlockTime float64
	lastAdjustment   uint64
	priorityMiner    common.QuantumAddress
	priorityBoost    uint64

	totalBlocks atomic.Uint64
}

// NewPoW creates a new PoW engine with default difficulty.
func NewPoW() *PoW {
	p := &PoW{
		difficulty:       big.NewInt(BaseDifficulty), // initial difficulty
		blockTimes:       make([]uint64, 0, DifficultyAdjustment),
		averageBlockTime: float64(BlockTimeTarget),
	}
	log.Printf("[pow] Quantum‑resistant PoW engine initialized (SHA3‑256)")
	log.Printf("[pow] Initial difficulty: %s", p.difficulty.String())
	return p
}

// ── Difficulty & Target ────────────────────────────────────────────────────

func (p *PoW) GetDifficulty() *big.Int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return new(big.Int).Set(p.difficulty)
}

func (p *PoW) SetDifficulty(diff *big.Int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.difficulty = new(big.Int).Set(diff)
	difficultyGauge.Set(float64(diff.Int64()))
}

// GetTarget returns the current mining target = (2^256 - 1) / difficulty.
func (p *PoW) GetTarget() *big.Int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return targetForDifficulty(p.difficulty)
}

func targetForDifficulty(difficulty *big.Int) *big.Int {
	if difficulty == nil || difficulty.Sign() <= 0 {
		difficulty = big.NewInt(MinDifficulty)
	}
	maxTarget := new(big.Int).Exp(big.NewInt(2), big.NewInt(256), nil)
	return new(big.Int).Div(maxTarget, difficulty)
}

// CalculateExpectedDifficulty predicts the next difficulty without mutating engine state.
func (p *PoW) CalculateExpectedDifficulty(height uint64, parentTime, currentTime uint64) *big.Int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	window := append([]uint64(nil), p.blockTimes...)
	delta := currentTime - parentTime
	if delta == 0 {
		delta = 1
	}
	window = append(window, delta)
	if len(window) > DifficultyAdjustment {
		window = window[1:]
	}
	return CalculateDifficultyFromWindow(new(big.Int).Set(p.difficulty), height, window)
}

// CalculateDifficultyFromWindow calculates difficulty from a base difficulty and observed block times.
func CalculateDifficultyFromWindow(baseDifficulty *big.Int, height uint64, blockTimes []uint64) *big.Int {
	if baseDifficulty == nil {
		baseDifficulty = big.NewInt(MinDifficulty)
	}
	return computeAdjustedDifficulty(new(big.Int).Set(baseDifficulty), height, blockTimes)
}

// AdjustDifficulty recalculates difficulty every DifficultyAdjustment blocks.
func (p *PoW) AdjustDifficulty(height uint64, parentTime, currentTime uint64) *big.Int {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Record block time for moving average
	delta := currentTime - parentTime
	if delta == 0 {
		delta = 1
	}
	p.blockTimes = append(p.blockTimes, delta)
	if len(p.blockTimes) > DifficultyAdjustment {
		p.blockTimes = p.blockTimes[1:]
	}

	adjusted := CalculateDifficultyFromWindow(new(big.Int).Set(p.difficulty), height, p.blockTimes)
	if adjusted.Cmp(p.difficulty) == 0 {
		return p.difficulty
	}

	var total uint64
	for _, t := range p.blockTimes {
		total += t
	}
	avg := float64(total) / float64(len(p.blockTimes))
	p.averageBlockTime = avg
	p.difficulty = adjusted
	p.lastAdjustment = height
	difficultyGauge.Set(float64(adjusted.Int64()))
	blockTimeGauge.Set(p.averageBlockTime)

	log.Printf("[pow] Difficulty adjusted at height %d → %s (avg block time %.1fs)",
		height, adjusted.String(), avg)

	return adjusted
}

func computeAdjustedDifficulty(current *big.Int, height uint64, blockTimes []uint64) *big.Int {
	if height%uint64(DifficultyAdjustment) != 0 || len(blockTimes) == 0 {
		return current
	}

	var total uint64
	for _, t := range blockTimes {
		total += t
	}
	avg := float64(total) / float64(len(blockTimes))

	ratio := float64(BlockTimeTarget) / avg
	if ratio > 4.0 {
		ratio = 4.0
	} else if ratio < 0.25 {
		ratio = 0.25
	}

	newDiff := new(big.Float).SetInt(current)
	newDiff.Mul(newDiff, big.NewFloat(ratio))
	adjusted := new(big.Int)
	newDiff.Int(adjusted)

	if adjusted.Cmp(big.NewInt(MinDifficulty)) < 0 {
		return big.NewInt(MinDifficulty)
	}
	if adjusted.Cmp(big.NewInt(MaxDifficulty)) > 0 {
		return big.NewInt(MaxDifficulty)
	}
	return adjusted
}

// ── Mining & Verification ──────────────────────────────────────────────────

// MineBlock performs Proof‑of‑Work on the given header.
// It modifies the header's Nonce and MixDigest upon success.
// The context allows cancellation when a new block arrives.
func (p *PoW) MineBlock(ctx context.Context, header *BlockHeader) error {
	if header == nil {
		return errors.New("block header is nil")
	}
	target := targetForDifficulty(header.Difficulty)
	serialized := header.SerializeForMining()
	noncePos := len(serialized) - 8

	var nonce uint64
	for nonce = 0; nonce < ^uint64(0); nonce++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		binary.BigEndian.PutUint64(serialized[noncePos:], nonce)
		hash := common.ComputeHash(serialized)
		if hashToBig(&hash).Cmp(target) < 0 {
			header.Nonce = encodeNonce(nonce)
			header.MixDigest = hash
			return nil
		}
	}
	return ErrInvalidNonce
}

// Verify checks whether the block's Proof‑of‑Work is valid.
func (p *PoW) Verify(header *BlockHeader) bool {
	if header == nil {
		return false
	}
	target := targetForDifficulty(header.Difficulty)
	serialized := header.SerializeForMining()
	hash := common.ComputeHash(serialized)
	if hash != header.MixDigest {
		return false
	}
	return hashToBig(&hash).Cmp(target) < 0
}

// ── Additional required methods (stubs for compatibility if any caller expects them) ──

// VerifyMinerEligibility is not used in PoW; always true.
func (p *PoW) VerifyMinerEligibility(miner common.QuantumAddress, parentHash common.Hash, height, timestamp uint64) (bool, error) {
	if timestamp > uint64(time.Now().Unix()+MaxFutureBlockTime) {
		return false, ErrBlockTooFarInFuture
	}
	return true, nil
}

// RecordBlockMined increments the total blocks counter.
func (p *PoW) RecordBlockMined(miner common.QuantumAddress, height uint64) {
	p.totalBlocks.Add(1)
	blocksMinedCounter.Inc()
}

func (p *PoW) Release() {}

// Compatibility no-op for legacy callers.
func (p *PoW) AutoRegisterIfEligible(common.QuantumAddress, *big.Int, []byte) {}

// SetPriorityMiner sets a preferred miner for informational compatibility only.
func (p *PoW) SetPriorityMiner(miner common.QuantumAddress, boostPercent uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.priorityMiner = miner
	p.priorityBoost = boostPercent
}

// IsKing reports whether the provided address matches the configured priority miner.
func (p *PoW) IsKing(addr common.QuantumAddress) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.priorityMiner == addr
}

// VerifyBlockSignature is unused in PoW mode; blocks are validated by hash target.
func (p *PoW) VerifyBlockSignature(common.QuantumAddress, common.Hash, uint64, uint64, []byte) (bool, error) {
	return true, nil
}

// GetNextMiner returns the preferred miner when configured, otherwise the provided miner.
func (p *PoW) GetNextMiner(parentHash common.Hash, height uint64, timestamp ...uint64) (common.QuantumAddress, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.priorityMiner, nil
}

// RecordMissedBlock is a no-op compatibility hook in PoW mode.
func (p *PoW) RecordMissedBlock(common.QuantumAddress, ...uint64) {}

// GetMiningStatistics provides a minimal compatibility payload.

// GetKingAddresses returns the configured compatibility priority miner list.
func (p *PoW) GetKingAddresses() []common.QuantumAddress {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.priorityMiner == (common.QuantumAddress{}) {
		return nil
	}
	return []common.QuantumAddress{p.priorityMiner}
}

func (p *PoW) GetMiningStatistics() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return map[string]interface{}{
		"difficulty":         p.difficulty.String(),
		"average_block_time": p.averageBlockTime,
		"total_blocks":       p.totalBlocks.Load(),
		"priority_miner":     p.priorityMiner.String(),
		"priority_boost":     p.priorityBoost,
	}
}

// ── Helpers ─────────────────────────────────────────────────────────────────

// BlockHeader matches your existing Header struct.
type BlockHeader struct {
	ParentHash common.Hash
	Coinbase   common.QuantumAddress
	Root       common.Hash
	TxHash     common.Hash
	Number     uint64
	Difficulty *big.Int
	Time       uint64
	Extra      []byte
	Nonce      [8]byte
	MixDigest  common.Hash
}

// SerializeForMining returns the exact serialization used for the PoW hash.
func (h *BlockHeader) SerializeForMining() []byte {
	buf := make([]byte, 0, 256)

	buf = append(buf, h.ParentHash[:]...)
	buf = append(buf, h.Coinbase.Bytes()...)
	buf = append(buf, h.Root[:]...)
	buf = append(buf, h.TxHash[:]...)

	num := make([]byte, 8)
	binary.BigEndian.PutUint64(num, h.Number)
	buf = append(buf, num...)

	difficulty := h.Difficulty
	if difficulty == nil || difficulty.Sign() <= 0 {
		difficulty = big.NewInt(MinDifficulty)
	}
	diffBytes := difficulty.Bytes()
	padDiff := make([]byte, 32)
	copy(padDiff[32-len(diffBytes):], diffBytes)
	buf = append(buf, padDiff...)

	timeBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timeBytes, h.Time)
	buf = append(buf, timeBytes...)

	extraLen := make([]byte, 4)
	binary.BigEndian.PutUint32(extraLen, uint32(len(h.Extra)))
	buf = append(buf, extraLen...)
	buf = append(buf, h.Extra...)

	// Nonce placeholder (last 8 bytes)
	buf = append(buf, h.Nonce[:]...)
	return buf
}

func hashToBig(h *common.Hash) *big.Int {
	return new(big.Int).SetBytes(h[:])
}

func encodeNonce(nonce uint64) [8]byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], nonce)
	return b
}
