// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package difficulty

import (
	"math/big"
	"sort"

	"github.com/antdaza/antdchain/common"
)

// ============================================================================
// DASH/LITECOIN STYLE DIFFICULTY ADJUSTMENT
// ============================================================================
// Features:
// - Dark Gravity Wave (DGW) v3 for rapid difficulty adjustment
// - LWMA (Linear Weighted Moving Average) for smooth adjustments
// - DigiShield protection against time warp attacks
// - Kimoto Gravity Well protection for rapid hashrate changes
// ============================================================================

const (
	// Target block time: 2.5 minutes (Dash)
	BlockTimeTarget        = 150 // seconds (2.5 minutes)
	TargetBlockTimeSeconds = BlockTimeTarget
	
	// DGW Constants
	DGWWindow          = 24   // Dark Gravity Wave window size (Dash uses 24)
	LWMAWindow         = 60   // LWMA window for smooth adjustments
	AdjustmentWindow   = 100  // Legacy window (kept for compatibility)
	
	// Difficulty bounds
	MaxDifficulty          = new(big.Int).Exp(big.NewInt(2), big.NewInt(256), nil) // Max 2^256
	MinDifficulty          = 1
	BaseDifficulty         = 131072 // Litecoin's initial difficulty- lol
	
	// DigiShield bounds
	MaxAdjustmentFactor    = 4      // Max 4x difficulty change per block
	MinAdjustmentFactor    = 0.25   // Min 0.25x difficulty change
	
	// Protection against timestamp manipulation
	MaxFutureBlockTime     = 15 * 60   // 15 minutes max future
	MaxPastBlockTime       = 15 * 60   // 15 minutes max past
	
	// Miner offset bits (kept from original)
	MinerOffsetBits = common.QuantumAddressLength * 8
)

var minerDomain = new(big.Int).Add(
	new(big.Int).Lsh(big.NewInt(1), MinerOffsetBits),
	big.NewInt(1),
)

// ============================================================================
// MAIN DIFFICULTY CALCULATION FUNCTIONS
// ============================================================================

// CalculateDifficulty - Main entry point for DGW/LWMA difficulty
// Mimics Dash's DGW v3 algorithm for rapid, accurate adjustments
func CalculateDifficulty(prevDifficulty *big.Int, height uint64, timestamps []uint64) *big.Int {
	if height == 0 {
		return big.NewInt(BaseDifficulty)
	}
	
	// Use DGW for rapid adjustment (Dash style)
	if height >= DGWWindow {
		return CalculateDarkGravityWave(prevDifficulty, height, timestamps)
	}
	
	// Fallback to LWMA for smoother adjustments during initial chain
	return CalculateLWMA(prevDifficulty, timestamps)
}

// CalculateDarkGravityWave - Dash's DGW v3 implementation
// Rapidly adjusts to hashrate changes while preventing time warp attacks
func CalculateDarkGravityWave(prevDifficulty *big.Int, height uint64, timestamps []uint64) *big.Int {
	if len(timestamps) < DGWWindow {
		return Clamp(prevDifficulty)
	}
	
	// Get the last DGWWindow blocks
	window := timestamps
	if len(window) > DGWWindow {
		window = window[len(window)-DGWWindow:]
	}
	
	// Calculate average block time over the window
	var totalTime uint64
	for i := 1; i < len(window); i++ {
		diff := window[i] - window[i-1]
		// Clamp to reasonable bounds
		if diff > MaxFutureBlockTime {
			diff = MaxFutureBlockTime
		}
		if diff < 1 {
			diff = 1
		}
		totalTime += diff
	}
	
	avgTime := float64(totalTime) / float64(len(window)-1)
	
	// Calculate target time sum (expected total time)
	targetTotal := float64(BlockTimeTarget) * float64(len(window)-1)
	
	// Calculate ratio with bounds (DigiShield style)
	ratio := targetTotal / avgTime
	if ratio > MaxAdjustmentFactor {
		ratio = MaxAdjustmentFactor
	}
	if ratio < MinAdjustmentFactor {
		ratio = MinAdjustmentFactor
	}
	
	// Calculate new difficulty
	currentDiff := Normalize(prevDifficulty)
	newDiff := new(big.Float).SetInt(currentDiff)
	newDiff.Mul(newDiff, big.NewFloat(ratio))
	
	result := new(big.Int)
	newDiff.Int(result)
	
	// Apply additional smoothing
	result = applyDigiShield(result, currentDiff, ratio)
	result = Clamp(result)
	
	// Ensure difficulty changes (prevents stuck difficulty)
	return ensureDifficultyChanges(currentDiff, result, avgTime)
}

// CalculateLWMA - Linear Weighted Moving Average (like newer Dash)
// Gives more weight to recent blocks for faster response
func CalculateLWMA(prevDifficulty *big.Int, timestamps []uint64) *big.Int {
	if len(timestamps) < LWMAWindow {
		// Not enough blocks, use simple average
		return CalculateSimpleAverage(prevDifficulty, timestamps)
	}
	
	// Use last LWMAWindow blocks
	window := timestamps
	if len(window) > LWMAWindow {
		window = window[len(window)-LWMAWindow:]
	}
	
	var weightedSum float64
	var weightSum float64
	totalWeight := 0
	
	// Calculate weighted average (more weight to recent blocks)
	for i := 1; i < len(window); i++ {
		blockTime := window[i] - window[i-1]
		if blockTime > MaxFutureBlockTime {
			blockTime = MaxFutureBlockTime
		}
		if blockTime < 1 {
			blockTime = 1
		}
		
		// Weight increases linearly (most recent gets highest weight)
		weight := i
		weightedSum += float64(blockTime) * float64(weight)
		weightSum += float64(weight)
		totalWeight += weight
	}
	
	avgTime := weightedSum / weightSum
	
	// Calculate ratio with bounds
	ratio := float64(BlockTimeTarget) / avgTime
	if ratio > MaxAdjustmentFactor {
		ratio = MaxAdjustmentFactor
	}
	if ratio < MinAdjustmentFactor {
		ratio = MinAdjustmentFactor
	}
	
	// Apply ratio to difficulty
	currentDiff := Normalize(prevDifficulty)
	newDiff := new(big.Float).SetInt(currentDiff)
	newDiff.Mul(newDiff, big.NewFloat(ratio))
	
	result := new(big.Int)
	newDiff.Int(result)
	
	return Clamp(result)
}

// CalculateSimpleAverage - Simple average for early chain
func CalculateSimpleAverage(prevDifficulty *big.Int, timestamps []uint64) *big.Int {
	if len(timestamps) < 2 {
		return Clamp(prevDifficulty)
	}
	
	var totalTime uint64
	for i := 1; i < len(timestamps); i++ {
		diff := timestamps[i] - timestamps[i-1]
		if diff > MaxFutureBlockTime {
			diff = MaxFutureBlockTime
		}
		if diff < 1 {
			diff = 1
		}
		totalTime += diff
	}
	
	avgTime := float64(totalTime) / float64(len(timestamps)-1)
	ratio := float64(BlockTimeTarget) / avgTime
	
	if ratio > MaxAdjustmentFactor {
		ratio = MaxAdjustmentFactor
	}
	if ratio < MinAdjustmentFactor {
		ratio = MinAdjustmentFactor
	}
	
	currentDiff := Normalize(prevDifficulty)
	newDiff := new(big.Float).SetInt(currentDiff)
	newDiff.Mul(newDiff, big.NewFloat(ratio))
	
	result := new(big.Int)
	newDiff.Int(result)
	
	return Clamp(result)
}

// ============================================================================
// PROTECTIONS AGAINST ATTACKS
// ============================================================================

// applyDigiShield - Protects against time warp attacks
// Used by DigiByte, Dash, and other modern coins
func applyDigiShield(newDiff, oldDiff *big.Int, ratio float64) *big.Int {
	// Prevent extreme jumps in either direction
	if ratio > MaxAdjustmentFactor {
		// Cap at max adjustment
		maxDiff := new(big.Int).Mul(oldDiff, big.NewInt(int64(MaxAdjustmentFactor)))
		if newDiff.Cmp(maxDiff) > 0 {
			return maxDiff
		}
	}
	
	if ratio < MinAdjustmentFactor {
		// Cap at min adjustment
		minDiff := new(big.Int).Div(oldDiff, big.NewInt(int64(1/MinAdjustmentFactor)))
		if newDiff.Cmp(minDiff) < 0 {
			return minDiff
		}
	}
	
	return newDiff
}

// KimotoGravityWell - Alternative protection for rapid changes
// Used by some coins as additional protection
func KimotoGravityWell(timestamps []uint64) float64 {
	if len(timestamps) < 12 {
		return 1.0
	}
	
	// Take last 12 blocks for quick response
	window := timestamps[len(timestamps)-12:]
	
	var totalDeviation float64
	for i := 1; i < len(window); i++ {
		blockTime := float64(window[i] - window[i-1])
		if blockTime < 1 {
			blockTime = 1
		}
		if blockTime > float64(MaxFutureBlockTime) {
			blockTime = float64(MaxFutureBlockTime)
		}
		
		// Calculate deviation from target
		deviation := (blockTime - float64(BlockTimeTarget)) / float64(BlockTimeTarget)
		totalDeviation += deviation * deviation
	}
	
	// Higher deviation = more aggressive adjustment
	avgDeviation := totalDeviation / float64(len(window)-1)
	return 1.0 + avgDeviation
}

// ============================================================================
// UTILITY FUNCTIONS (preserved and enhanced from original)
// ============================================================================

// Target returns the mining target for either a legacy base difficulty or a
// miner-specific full difficulty.
func Target(value *big.Int) *big.Int {
	value = Normalize(value)
	maxTarget := new(big.Int).Exp(big.NewInt(2), big.NewInt(256), nil)
	return new(big.Int).Div(maxTarget, value)
}

// ForMiner appends a deterministic miner-specific suffix to a normalized base
// difficulty.
func ForMiner(baseDifficulty *big.Int, miner common.QuantumAddress) *big.Int {
	base := Normalize(baseDifficulty)
	minerOffset := new(big.Int).SetBytes(miner.Bytes())
	minerOffset.Add(minerOffset, big.NewInt(1))
	
	full := new(big.Int).Mul(base, minerDomain)
	full.Add(full, minerOffset)
	return full
}

// Normalize strips any miner-specific suffix and clamps the result
func Normalize(value *big.Int) *big.Int {
	if value == nil || value.Sign() <= 0 {
		return big.NewInt(MinDifficulty)
	}
	
	normalized := new(big.Int).Set(value)
	if normalized.Cmp(minerDomain) >= 0 {
		normalized.Div(normalized, minerDomain)
	}
	return Clamp(normalized)
}

// Display returns the exact header difficulty for user-facing output
func Display(value *big.Int) string {
	if value == nil || value.Sign() <= 0 {
		return "0"
	}
	return value.String()
}

// FromWindow calculates difficulty using DGW/LWMA
// Legacy function kept for compatibility
func FromWindow(baseDifficulty *big.Int, height uint64, blockTimes []uint64) *big.Int {
	if height == 0 {
		return big.NewInt(BaseDifficulty)
	}
	
	// Convert block times to timestamps format
	timestamps := make([]uint64, len(blockTimes))
	copy(timestamps, blockTimes)
	
	return CalculateDifficulty(baseDifficulty, height, timestamps)
}

// ElapsedBlockTime returns the positive observed time between parent and current
// block timestamps.
func ElapsedBlockTime(parentTime, currentTime uint64) uint64 {
	if currentTime <= parentTime {
		return 1
	}
	return currentTime - parentTime
}

// Clamp limits difficulty to consensus bounds
func Clamp(value *big.Int) *big.Int {
	if value == nil {
		return big.NewInt(MinDifficulty)
	}
	
	if value.Cmp(big.NewInt(MinDifficulty)) < 0 {
		return big.NewInt(MinDifficulty)
	}
	
	// Check max difficulty (protect against overflow)
	if MaxDifficulty != nil && value.Cmp(MaxDifficulty) > 0 {
		return new(big.Int).Set(MaxDifficulty)
	}
	
	return new(big.Int).Set(value)
}

// ensureDifficultyChanges prevents difficulty from getting stuck
func ensureDifficultyChanges(current, adjusted *big.Int, avgTime float64) *big.Int {
	if adjusted.Cmp(current) != 0 {
		return adjusted
	}
	
	// If no change but blocks are too fast, increase difficulty
	if avgTime <= float64(BlockTimeTarget) && current.Cmp(big.NewInt(MinDifficulty)) > 0 {
		if current.Cmp(big.NewInt(MaxDifficulty.Int64())) < 0 {
			return new(big.Int).Add(current, big.NewInt(1))
		}
		return new(big.Int).Sub(current, big.NewInt(1))
	}
	
	// If no change but blocks are too slow, decrease difficulty
	if avgTime > float64(BlockTimeTarget) && current.Cmp(big.NewInt(MinDifficulty)) > 0 {
		return new(big.Int).Sub(current, big.NewInt(1))
	}
	
	return adjusted
}

// ============================================================================
// ADVANCED DIFFICULTY ALGORITHMS
// ============================================================================

// CalculateASERT - Asynchronous Stake-based Elastic Retargeting
// Newer algorithm used by some coins, very smooth adjustments
func CalculateASERT(prevDifficulty *big.Int, height uint64, timestamps []uint64, targetInterval float64) *big.Int {
	if len(timestamps) < 2 {
		return Clamp(prevDifficulty)
	}
	
	// Use the full timestamp range
	startTime := timestamps[0]
	endTime := timestamps[len(timestamps)-1]
	
	// Calculate actual time elapsed
	timeElapsed := float64(endTime - startTime)
	if timeElapsed < 1 {
		timeElapsed = 1
	}
	
	// Expected time elapsed
	expectedElapsed := float64(len(timestamps)-1) * targetInterval
	
	// Calculate ratio with half-life of 24 hours
	halfLife := 24.0 * 3600.0 // 24 hours in seconds
	ratio := timeElapsed / expectedElapsed
	
	// ASERT formula: new_diff = old_diff * 2^(time_ratio / half_life)
	exponent := (timeElapsed - expectedElapsed) / halfLife
	multiplier := new(big.Float).SetFloat64(1.0)
	
	if exponent > 0 {
		multiplier.SetFloat64(1.0 + (exponent / 10.0))
	} else if exponent < 0 {
		multiplier.SetFloat64(1.0 - (-exponent / 10.0))
	}
	
	currentDiff := Normalize(prevDifficulty)
	newDiff := new(big.Float).SetInt(currentDiff)
	newDiff.Mul(newDiff, multiplier)
	
	result := new(big.Int)
	newDiff.Int(result)
	
	// Apply bounds
	if ratio > MaxAdjustmentFactor {
		ratio = MaxAdjustmentFactor
	}
	if ratio < MinAdjustmentFactor {
		ratio = MinAdjustmentFactor
	}
	
	return Clamp(result)
}

// GetDifficultyAlgorithm returns the current algorithm name for debugging
func GetDifficultyAlgorithm(height uint64) string {
	if height >= DGWWindow {
		return "Dark Gravity Wave v3"
	}
	return "LWMA (Linear Weighted Moving Average)"
}

// GetTargetBlockTime returns the target block time in seconds
func GetTargetBlockTime() uint64 {
	return BlockTimeTarget
}

// GetAdjustmentWindow returns the current adjustment window size
func GetAdjustmentWindow() int {
	return DGWWindow
}
