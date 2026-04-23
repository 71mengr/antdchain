// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package chain

import (
	"errors"
	"fmt"
	"log"
	"math/big"
	"time"

	"github.com/antdaza/antdchain/antdc/block"
	"github.com/prometheus/client_golang/prometheus"
)

// Prometheus metrics for validation
var (
	validationFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "antdchain_block_validation_failures_total",
			Help: "Block validation failures by reason",
		},
		[]string{"reason"},
	)

	validationSuccesses = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "antdchain_block_validation_successes_total",
			Help: "Total successful block validations",
		},
	)

	rotatingKingUpdateFailures = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "antdchain_rotating_king_update_failures_total",
			Help: "Total rotating king update failures during block validation",
		},
	)
)

func init() {
	// Register Prometheus metrics
	prometheus.MustRegister(validationFailures)
	prometheus.MustRegister(validationSuccesses)
	prometheus.MustRegister(rotatingKingUpdateFailures)
}

// validateAndExecuteBlock validates and executes a new block
func (bc *Blockchain) validateAndExecuteBlock(b *block.Block, parent *block.Block) error {
	// BASIC VALIDATION
	if err := bc.validateBasicBlockIntegrity(b, parent); err != nil {
		validationFailures.WithLabelValues("basic_integrity").Inc()
		return fmt.Errorf("basic validation failed: %w", err)
	}

	// BLOCK HASH VERIFICATION
	if err := bc.validateBlockHash(b); err != nil {
		validationFailures.WithLabelValues("block_hash").Inc()
		return fmt.Errorf("block hash validation failed: %w", err)
	}

	// PROOF-OF-STAKE VALIDATION
	if err := bc.validatePoSEligibility(b, parent); err != nil {
		validationFailures.WithLabelValues("pos_eligibility").Inc()
		return fmt.Errorf("PoS validation failed: %w", err)
	}

	// DIFFICULTY VALIDATION
	if err := bc.validateDifficulty(b, parent); err != nil {
		validationFailures.WithLabelValues("difficulty").Inc()
		return fmt.Errorf("difficulty validation failed: %w", err)
	}

	// TRANSACTION ROOT VERIFICATION
	if err := bc.validateTransactionRoot(b); err != nil {
		validationFailures.WithLabelValues("transaction_root").Inc()
		return fmt.Errorf("transaction root mismatch: %w", err)
	}

	// BLOCK SIGNATURE VERIFICATION
	if err := bc.validateBlockSignature(b); err != nil {
		validationFailures.WithLabelValues("block_signature").Inc()
		return fmt.Errorf("block signature invalid: %w", err)
	}

	// STATE EXECUTION (TRANSACTION PROCESSING)
	var totalFees *big.Int
	var gasUsed uint64
	var execErr error

	if len(b.Txs) == 0 {
		// Empty block fast path (do not mutate header fields; hash must remain stable)
		totalFees = big.NewInt(0)
		gasUsed = 0

		if b.Header.GasUsed != 0 {
			validationFailures.WithLabelValues("gas_used_mismatch").Inc()
			return fmt.Errorf("gas used mismatch for empty block: header=%d executed=%d",
				b.Header.GasUsed, gasUsed)
		}
	} else {
		// Execute transactions
		totalFees, gasUsed, execErr = bc.executeBlockTransactions(b)
		if execErr != nil {
			validationFailures.WithLabelValues("transaction_execution").Inc()
			return fmt.Errorf("transaction execution failed: %w", execErr)
		}

		// Validate execution results against committed header fields
		if b.Header.GasUsed != gasUsed {
			validationFailures.WithLabelValues("gas_used_mismatch").Inc()
			return fmt.Errorf("gas used mismatch: header=%d executed=%d", b.Header.GasUsed, gasUsed)
		}

		// Verify gas usage doesn't exceed limit
		if gasUsed > b.Header.GasLimit {
			validationFailures.WithLabelValues("gas_limit").Inc()
			return fmt.Errorf("gas limit exceeded: %d > %d", gasUsed, b.Header.GasLimit)
		}
	}

	// REWARD DISTRIBUTION
	distribution, err := bc.distributeBlockRewards(b, totalFees)
	if err != nil {
		validationFailures.WithLabelValues("reward_distribution").Inc()
		return fmt.Errorf("reward distribution failed: %w", err)
	}

	// ROTATING KING UPDATES
	if err := bc.processRotatingKingForBlock(b, distribution); err != nil {
		rotatingKingUpdateFailures.Inc()
		log.Printf("[blockchain] Warning: rotating king update failed: %v", err)
		// Continue block validation even if rotating king update fails
		// This is a non-critical failure that shouldn't reject the block
	}

	finalRoot := bc.state.Root()
	if b.Header.Root != finalRoot {
		validationFailures.WithLabelValues("state_root_mismatch").Inc()
		return fmt.Errorf("state root mismatch: header=%s final=%s",
			b.Header.Root.Hex(), finalRoot.Hex())
	}

	// DIFFICULTY ADJUSTMENT (update engine state)
	newDifficulty := bc.pow.AdjustDifficulty(
		b.Header.Number.Uint64(),
		parent.Header.Time,
		b.Header.Time,
	)
	bc.pow.SetDifficulty(newDifficulty)

	// LOGGING AND METRICS
	bc.logBlockValidationSuccess(b, distribution, gasUsed, totalFees)
	validationSuccesses.Inc()

	return nil
}

// validateBasicBlockIntegrity performs basic structural validation
func (bc *Blockchain) validateBasicBlockIntegrity(b *block.Block, parent *block.Block) error {
	if b == nil || b.Header == nil {
		return errors.New("block or header is nil")
	}

	if parent == nil || parent.Header == nil {
		return errors.New("parent block or header is nil")
	}

	blockHeight := b.Header.Number.Uint64()
	parentHeight := parent.Header.Number.Uint64()

	if blockHeight == 0 {
		return errors.New("genesis block cannot be validated")
	}

	if blockHeight != parentHeight+1 {
		return fmt.Errorf("invalid block height: expected %d, got %d",
			parentHeight+1, blockHeight)
	}

	if b.Header.ParentHash != parent.Hash() {
		return fmt.Errorf("invalid parent hash: got %s, want %s",
			b.Header.ParentHash.Hex(), parent.Hash().Hex())
	}

	if b.Header.Time <= parent.Header.Time {
		return fmt.Errorf("block timestamp %d not greater than parent %d",
			b.Header.Time, parent.Header.Time)
	}

	// Reject blocks with timestamps too far in the future
	maxFutureTime := uint64(30) // 30 seconds
	currentTime := uint64(time.Now().Unix())
	if b.Header.Time > currentTime+maxFutureTime {
		return fmt.Errorf("block timestamp %d too far in future (current: %d)",
			b.Header.Time, currentTime)
	}

	// Minimum block time for PoS (e.g., 2 seconds)
	minBlockTime := uint64(2)
	if b.Header.Time < parent.Header.Time+minBlockTime {
		return fmt.Errorf("block too fast: %d < %d + %d",
			b.Header.Time, parent.Header.Time, minBlockTime)
	}

	return nil
}

// validateBlockHash verifies the block hash matches the header hash
func (bc *Blockchain) validateBlockHash(b *block.Block) error {
	computedHash := b.Hash()
	headerHash := b.Header.Hash()

	if computedHash != headerHash {
		return fmt.Errorf("invalid block hash: computed %s, header %s",
			computedHash.Hex(), headerHash.Hex())
	}

	return nil
}

// validatePoSEligibility checks miner eligibility for Proof-of-Stake
func (bc *Blockchain) validatePoSEligibility(b *block.Block, parent *block.Block) error {
	if bc.pow == nil {
		return errors.New("PoS engine not initialized")
	}

	// Keep the validator set synchronized with on-chain balances.
	// This allows any address with >= 1,000,000 ANTD to be considered for mining.
	if bc.state != nil {
		bc.pow.AutoRegisterIfEligible(b.Header.Coinbase, bc.state.GetBalance(b.Header.Coinbase), nil)
	}

	eligible, err := bc.pow.VerifyMinerEligibility(
		b.Header.Coinbase,
		b.Header.ParentHash,
		b.Header.Number.Uint64(),
		b.Header.Time,
	)
	if err != nil {
		return fmt.Errorf("miner eligibility verification failed: %w", err)
	}

	if !eligible {
		return errors.New("miner not eligible for this block")
	}

	return nil
}

// validateDifficulty verifies the block difficulty is correct
func (bc *Blockchain) validateDifficulty(b *block.Block, parent *block.Block) error {
	if bc.pow == nil {
		return errors.New("PoS engine not initialized")
	}

	expectedDifficulty := bc.pow.CalculateExpectedDifficulty(
		b.Header.Number.Uint64(),
		parent.Header.Time,
		b.Header.Time,
	)

	if b.Header.Difficulty.Cmp(expectedDifficulty) != 0 {
		return fmt.Errorf("invalid difficulty: got %s, expected %s",
			b.Header.Difficulty.String(), expectedDifficulty.String())
	}

	return nil
}

// validateTransactionRoot verifies the transaction Merkle root
func (bc *Blockchain) validateTransactionRoot(b *block.Block) error {
	computedTxRoot := CalcTxRoot(b.Txs)
	if computedTxRoot != b.Header.TxHash {
		// Log detailed mismatch information
		log.Printf("[error] Block %d: Transaction root validation failed", b.Header.Number.Uint64())
		log.Printf("[error]   Block hash:      %s", b.Hash().Hex())
		log.Printf("[error]   Header TxHash:   %s", b.Header.TxHash.Hex())
		log.Printf("[error]   Calculated Root: %s", computedTxRoot.Hex())
		log.Printf("[error]   Transaction count: %d", len(b.Txs))

		return fmt.Errorf("transaction root mismatch: header=%s, calculated=%s",
			b.Header.TxHash.Hex(), computedTxRoot.Hex())
	}

	return nil
}

// validateBlockSignature verifies the PoS block signature
func (bc *Blockchain) validateBlockSignature(b *block.Block) error {
	sig := extractSignatureFromBlock(b)
	if len(sig) == 0 {
		return errors.New("missing PoS signature")
	}

	ok, err := bc.pow.VerifyBlockSignature(
		b.Header.Coinbase,
		b.Header.ParentHash,
		b.Header.Number.Uint64(),
		b.Header.Time,
		sig,
	)
	if err != nil {
		return fmt.Errorf("signature verification error: %w", err)
	}

	if !ok {
		return errors.New("invalid PoS signature")
	}

	return nil
}

// validateBlockForSync validates a block during sync
func (bc *Blockchain) validateBlockForSync(b *block.Block, parent *block.Block) error {
	if b == nil || b.Header == nil {
		validationFailures.WithLabelValues("sync_nil_block").Inc()
		return errors.New("nil block or header")
	}

	// Basic validation during sync
	if parent != nil && b.Header.ParentHash != parent.Hash() {
		validationFailures.WithLabelValues("sync_parent_hash").Inc()
		return fmt.Errorf("invalid parent hash during sync: got %s, want %s",
			b.Header.ParentHash.Hex(), parent.Hash().Hex())
	}

	// Verify block hash
	if err := bc.validateBlockHash(b); err != nil {
		validationFailures.WithLabelValues("sync_block_hash").Inc()
		return fmt.Errorf("invalid block hash during sync: %w", err)
	}

	return nil
}
