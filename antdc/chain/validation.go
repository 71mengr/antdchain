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
	"github.com/antdaza/antdchain/antdc/pow"
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

	// PROOF-OF-WORK VALIDATION
	if err := bc.validateProofOfWork(b); err != nil {
		validationFailures.WithLabelValues("proof_of_work").Inc()
		return fmt.Errorf("proof-of-work validation failed: %w", err)
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

	// STATE ROOT PRECHECK
	// Compute the post-block root on an isolated state clone before mutating the
	// canonical state. If a peer sends a block with a bad root, rejecting it here
	// prevents the failed validation path from poisoning local balances/nonces and
	// causing later valid blocks to fail with cascading root mismatches.
	expectedRoot, err := bc.computeBlockFinalStateRoot(headerMinerBlockView{
		miner:     b.Header.Coinbase,
		blockTime: b.Header.Time,
		blockNum:  b.Header.Number.Uint64(),
		extra:     b.Header.Extra,
	}, b.Txs)
	if err != nil {
		validationFailures.WithLabelValues("state_root_precheck").Inc()
		return fmt.Errorf("state root precheck failed: %w", err)
	}
	if b.Header.Root != expectedRoot {
		validationFailures.WithLabelValues("state_root_mismatch").Inc()
		return fmt.Errorf("state root mismatch: header=%s final=%s",
			b.Header.Root.Hex(), expectedRoot.Hex())
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

	finalRoot := bc.state.Root()
	if b.Header.Root != finalRoot {
		validationFailures.WithLabelValues("state_root_mismatch").Inc()
		return fmt.Errorf("state root mismatch: header=%s final=%s",
			b.Header.Root.Hex(), finalRoot.Hex())
	}

	// ROTATING KING UPDATES
	// These updates are consensus-adjacent metadata and may persist manager state.
	// Verify the committed account state before running them so metadata writes
	// cannot make an otherwise valid block fail its state-root check.
	if err := bc.processRotatingKingForBlock(b, distribution); err != nil {
		rotatingKingUpdateFailures.Inc()
		log.Printf("[blockchain] Warning: rotating king update failed: %v", err)
		// Continue block validation even if rotating king update fails
		// This is a non-critical failure that shouldn't reject the block
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
	maxFutureTime := uint64(pow.MaxFutureBlockTime)
	currentTime := uint64(time.Now().Unix())
	if b.Header.Time > currentTime+maxFutureTime {
		return fmt.Errorf("block timestamp %d too far in future (current: %d)",
			b.Header.Time, currentTime)
	}

	// Enforce the network target block time for PoS.
	minBlockTime := uint64(pow.TargetBlockTimeSeconds)
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

	// Keep the local PoS validator set synchronized before checking eligibility.
	//
	// During sync, peers may deliver blocks mined by addresses that are valid
	// stakers but not yet present in this node's in-memory PoS set (e.g. after
	// restart or before local staking snapshot restoration completes). In that
	// case, fall back to account balance so imported blocks are validated against
	// deterministic on-chain data instead of transient local runtime state.
	if bc.stakingManager != nil {
		if stakeAmt, err := bc.stakingManager.GetStake(b.Header.Coinbase); err == nil && stakeAmt != nil {
			bc.pow.AutoRegisterIfEligible(b.Header.Coinbase, stakeAmt, nil)
		} else if st := bc.GetState(); st != nil {
			bc.pow.AutoRegisterIfEligible(b.Header.Coinbase, st.GetBalance(b.Header.Coinbase), nil)
		}
	} else if st := bc.GetState(); st != nil {
		bc.pow.AutoRegisterIfEligible(b.Header.Coinbase, st.GetBalance(b.Header.Coinbase), nil)
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

	expectedDifficulty := bc.calculateExpectedDifficultyFromChainState(b, parent)

	if b.Header.Difficulty.Cmp(expectedDifficulty) != 0 {
		return fmt.Errorf("invalid difficulty: got %s, expected %s",
			b.Header.Difficulty.String(), expectedDifficulty.String())
	}

	return nil
}

// CalculateExpectedDifficultyForBlock returns the deterministic chain-derived difficulty
// expected for b on top of parent without mutating the PoW engine state.
func (bc *Blockchain) CalculateExpectedDifficultyForBlock(b *block.Block, parent *block.Block) *big.Int {
	return bc.calculateExpectedDifficultyFromChainState(b, parent)
}

func (bc *Blockchain) calculateExpectedDifficultyFromChainState(b *block.Block, parent *block.Block) *big.Int {
	height := b.Header.Number.Uint64()
	if parent == nil || parent.Header == nil || parent.Header.Difficulty == nil {
		return bc.pow.CalculateExpectedDifficulty(height, 0, b.Header.Time)
	}
	if height == 0 {
		return bc.pow.CalculateExpectedDifficulty(height, parent.Header.Time, b.Header.Time)
	}

	window := make([]uint64, 0, pow.DifficultyAdjustment)
	curr := parent
	for len(window) < pow.DifficultyAdjustment-1 && curr != nil && curr.Header != nil && curr.Header.Number.Uint64() > 0 {
		ancestor, err := bc.GetBlockByHash(curr.Header.ParentHash)
		if err != nil || ancestor == nil || ancestor.Header == nil {
			break
		}

		delta := curr.Header.Time - ancestor.Header.Time
		if delta == 0 {
			delta = 1
		}
		window = append(window, delta)
		curr = ancestor
	}

	delta := b.Header.Time - parent.Header.Time
	if delta == 0 {
		delta = 1
	}
	window = append(window, delta)

	return pow.CalculateDifficultyFromWindow(new(big.Int).Set(parent.Header.Difficulty), height, window)
}

// validateProofOfWork verifies the block nonce/mix digest satisfy the header difficulty.
func (bc *Blockchain) validateProofOfWork(b *block.Block) error {
	if bc.pow == nil {
		return errors.New("PoW engine not initialized")
	}
	if b == nil || b.Header == nil {
		return errors.New("block or header is nil")
	}

	powHeader, err := powHeaderFromBlockHeader(b.Header)
	if err != nil {
		return err
	}
	if !bc.pow.Verify(powHeader) {
		return fmt.Errorf("invalid proof-of-work: nonce=0x%s mixdigest=%s difficulty=%s",
			b.Header.Nonce.String(),
			b.Header.MixDigest.Hex(),
			powHeader.Difficulty.String(),
		)
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

// ValidateMinedBlockProposal performs non-mutating consensus checks for a mined
// block received by P2P before it is offered to AddBlock. State-transition checks
// still happen in AddBlock; this preflight keeps invalid mining output from
// entering fork-choice comparison.
func (bc *Blockchain) ValidateMinedBlockProposal(b *block.Block) error {
	if b == nil || b.Header == nil {
		return errors.New("nil block or header")
	}
	if b.Header.Number == nil {
		return errors.New("nil block number")
	}
	if b.Header.Number.Sign() == 0 {
		return validateProtocolHeader(b.Header)
	}

	parent, err := bc.GetBlockByHash(b.Header.ParentHash)
	if err != nil || parent == nil {
		return fmt.Errorf("parent block not found for mined proposal: %s", b.Header.ParentHash.Hex())
	}

	if err := validateProtocolHeader(b.Header); err != nil {
		return fmt.Errorf("protocol header validation failed: %w", err)
	}
	if err := bc.validateBasicBlockIntegrity(b, parent); err != nil {
		return err
	}
	if err := bc.validateBlockHash(b); err != nil {
		return err
	}
	if err := bc.validateDifficulty(b, parent); err != nil {
		return err
	}
	if err := bc.validateProofOfWork(b); err != nil {
		return err
	}
	if err := bc.validateTransactionRoot(b); err != nil {
		return err
	}
	if err := bc.validateBlockSignature(b); err != nil {
		return err
	}

	return nil
}
