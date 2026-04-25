// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package chain

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math/big"
	"os"
	"time"

	"github.com/antdaza/antdchain/antdc/block"
	"github.com/antdaza/antdchain/antdc/state"
	"github.com/antdaza/antdchain/antdc/tx"
	"github.com/antdaza/antdchain/antdc/vm"
	"github.com/antdaza/antdchain/common"
)

// BlockTemplate represents a mining template
type BlockTemplate struct {
	Height         uint64                   `json:"height"`
	PrevHash       string                   `json:"previousblockhash"`
	CoinbaseValue  string                   `json:"coinbasevalue"`
	Target         string                   `json:"target"`
	CurTime        uint64                   `json:"curtime"`
	Transactions   []map[string]interface{} `json:"transactions"`
	Version        uint32                   `json:"version"`
	Bits           string                   `json:"bits"`
	Mintime        uint64                   `json:"mintime"`
	Mutable        []string                 `json:"mutable"`
	NonceRange     string                   `json:"noncerange"`
	SigOpLimit     int                      `json:"sigoplimit"`
	SizeLimit      int                      `json:"sizelimit"`
	WeightLimit    int                      `json:"weightlimit"`
	LongPollID     string                   `json:"longpollid"`
	DefaultWitness string                   `json:"default_witness_commitment"`
	Capabilities   []string                 `json:"capabilities"`
	Rules          []string                 `json:"rules"`
	VBAvailable    map[string]int           `json:"vbavailable"`
	VBRequired     int                      `json:"vbrequired"`
	CoinbaseAux    map[string]string        `json:"coinbaseaux"`
}

// GenerateBlockTemplate generates a block template for mining
func (bc *Blockchain) GenerateBlockTemplate(rewardAddr common.QuantumAddress) (*BlockTemplate, error) {
	if bc == nil {
		return nil, errors.New("blockchain is nil")
	}

	parent := bc.Latest()
	if parent == nil {
		return nil, errors.New("no parent block available")
	}

	// Get confirmed transactions from pool
	confirmedTxs := bc.txPool.GetConfirmedTxs(bc, bc.MinConfirmations())

	// Calculate total fees and build transactions list
	totalFees := big.NewInt(0)
	transactions := make([]map[string]interface{}, 0, len(confirmedTxs))

	for _, t := range confirmedTxs {
		fee := new(big.Int).Mul(new(big.Int).SetUint64(t.Gas), t.GasPrice)
		totalFees.Add(totalFees, fee)

		data, err := t.Serialize()
		if err != nil {
			continue // Skip invalid transactions
		}

		txEntry := map[string]interface{}{
			"data":    hex.EncodeToString(data),
			"txid":    t.Hash().String(),
			"hash":    t.Hash().String(),
			"depends": []int{},
			"fee":     fee.Int64(),
			"sigops":  1,
			"weight":  len(data) * 4,
		}
		transactions = append(transactions, txEntry)
	}

	// Calculate total reward (200 ANTD block reward + fees)
	blockReward, ok := new(big.Int).SetString("200000000000000000000", 10)
	if !ok {
		// Handle error - maybe use a default value
		blockReward = new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil) // 10^18 = 1 ANTD
		blockReward.Mul(blockReward, big.NewInt(200))                       // 200 ANTD
	}
	coinbaseValue := new(big.Int).Add(blockReward, totalFees)

	// Get current PoW target
	target := bc.pow.GetTarget()
	targetHex := fmt.Sprintf("%064x", target)

	// Convert difficulty to compact format (Bitcoin-style bits)
	compact := diffToCompact(bc.Pow().GetDifficulty())

	now := uint64(time.Now().Unix())
	parentHeight := parent.Header.Number.Uint64()

	return &BlockTemplate{
		Height:         parentHeight + 1,
		PrevHash:       parent.Hash().String(),
		CoinbaseValue:  coinbaseValue.String(),
		Target:         targetHex,
		CurTime:        now,
		Transactions:   transactions,
		Version:        536870912, // Bitcoin-compatible version
		Bits:           fmt.Sprintf("%08x", compact),
		Mintime:        now - 7200, // 2 hours ago
		Mutable:        []string{"time", "transactions", "prevblock"},
		NonceRange:     "00000000ffffffff",
		SigOpLimit:     80000,
		SizeLimit:      4000000,
		WeightLimit:    4000000,
		LongPollID:     parent.Hash().String() + fmt.Sprintf("%d", len(transactions)),
		DefaultWitness: "",
		Capabilities:   []string{"proposal"},
		Rules:          []string{},
		VBAvailable:    map[string]int{},
		VBRequired:     0,
		CoinbaseAux:    map[string]string{},
	}, nil
}

// CreateMiningBlock creates a mining block
func (bc *Blockchain) CreateMiningBlock(rewardAddr common.QuantumAddress) (*block.Block, []*tx.Tx, error) {
	return bc.CreatePoSBlock(rewardAddr)
}

// CreatePoSBlock creates a block for Proof-of-Stake consensus
func (bc *Blockchain) CreatePoSBlock(miner common.QuantumAddress) (*block.Block, []*tx.Tx, error) {
	if miner == (common.QuantumAddress{}) {
		return nil, nil, errors.New("miner address is empty")
	}

	parent := bc.Latest()
	if parent == nil {
		return nil, nil, errors.New("no parent block")
	}

	// Enforce 12-second block time
	currentTime := uint64(time.Now().Unix())
	if currentTime-parent.Header.Time < 12 {
		wait := 12 - (currentTime - parent.Header.Time)
		log.Printf("[miner] Waiting %d seconds for proper block timing...", wait)
		time.Sleep(time.Duration(wait) * time.Second)
		currentTime = uint64(time.Now().Unix())
	}

	// Check miner eligibility
	if bc.pow != nil {
		if bc.state != nil {
			bc.pow.AutoRegisterIfEligible(miner, bc.state.GetBalance(miner), nil)
		}

		eligible, err := bc.pow.VerifyMinerEligibility(
			miner,
			parent.Hash(),
			parent.Header.Number.Uint64()+1,
			currentTime,
		)
		if err != nil || !eligible {
			return nil, nil, fmt.Errorf("miner %s not eligible: %w", miner.String(), err)
		}
	}

	// SAFE TRANSACTION INCLUSION
	var includedTxs []*tx.Tx
	if pool := bc.txPool; pool != nil {
		candidates := pool.GetPending()

		// Only include transactions older than 5 seconds → ensures propagation
		propagationDelay := uint64(5)
		cutoff := currentTime - propagationDelay

		includedCount := 0
		skippedRecent := 0

		for _, tx := range candidates {
			if tx == nil {
				continue
			}

			// Skip transactions created too recently
			if tx.Timestamp >= cutoff {
				skippedRecent++
				continue
			}

			// Basic validation
			if valid, err := tx.Verify(); err != nil || !valid {
				continue
			}

			// Check correct nonce
			expectedNonce := bc.State().GetNonce(common.BytesToQuantumAddress(tx.From.Bytes()))
			if tx.Nonce != expectedNonce {
				continue
			}

			includedTxs = append(includedTxs, tx)
			includedCount++
		}

		// Limit block size
		if len(includedTxs) > 100 {
			includedTxs = includedTxs[:100]
		}

		if skippedRecent > 0 {
			log.Printf("[miner] Skipped %d recent transaction(s) for propagation safety", skippedRecent)
		}
		log.Printf("[blockchain] Including %d well-propagated transaction(s) in block", includedCount)
	}

	extraData := []byte("ANTDChain-PoS")
	if bc.rotatingKingManager != nil {
		if rotatingKing := bc.rotatingKingManager.GetCurrentKing(); rotatingKing != (common.QuantumAddress{}) {
			extraData = []byte(fmt.Sprintf("ANTDChain-PoS|rk=%s", rotatingKing.String()))
		}
	}

	// Calculate transaction root
	txRoot := CalcTxRoot(includedTxs)

	// Header root must reflect final post-block state (transactions + rewards),
	// otherwise peers will reject with committed-state-root mismatches.
	stateRoot, err := bc.computeBlockFinalStateRoot(headerMinerBlockView{
		miner:     miner,
		blockTime: currentTime,
		blockNum:  new(big.Int).Add(parent.Header.Number, big.NewInt(1)).Uint64(),
		extra:     extraData,
	}, includedTxs)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to compute block state root: %w", err)
	}

	// Create block header
	header := &block.Header{
		ParentHash: parent.Hash(),
		Coinbase:   miner,
		Root:       stateRoot,
		TxHash:     txRoot,
		Number:     new(big.Int).Add(parent.Header.Number, big.NewInt(1)),
		GasLimit:   10_000_000,
		Time:       currentTime,
		Difficulty: bc.pow.CalculateExpectedDifficulty(
			parent.Header.Number.Uint64()+1,
			parent.Header.Time,
			currentTime,
		),
		Extra: extraData,
	}
	if err := applyProtocolHeaderFields(header); err != nil {
		return nil, nil, fmt.Errorf("failed to apply protocol header fields: %w", err)
	}

	// Create the block
	newBlock, err := block.NewBlock(header, includedTxs, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create block: %w", err)
	}

	log.Printf("[blockchain] Created PoS block %d with %d transaction(s)",
		header.Number.Uint64(), len(includedTxs))

	return newBlock, includedTxs, nil
}

// computeBlockFinalStateRoot computes the expected state root after executing
// txs and applying block rewards on top of canonical state (without mutating it).
type headerMinerBlockView struct {
	miner     common.QuantumAddress
	blockTime uint64
	blockNum  uint64
	extra     []byte
}

func (bc *Blockchain) computeBlockFinalStateRoot(view headerMinerBlockView, txs []*tx.Tx) (common.Hash, error) {
	currentState := bc.State()
	if currentState == nil {
		return common.Hash{}, errors.New("state is nil")
	}

	snapshotDir, err := os.MkdirTemp("", "antdchain-state-snapshot-*")
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to create temp state dir: %w", err)
	}
	defer os.RemoveAll(snapshotDir)

	snapshotState, err := currentState.Clone(snapshotDir)
	if err != nil {
		return common.Hash{}, fmt.Errorf("failed to clone state: %w", err)
	}
	defer snapshotState.Close()

	if _, err := executeTransactionsOnState(snapshotState, txs, 10_000_000); err != nil {
		return common.Hash{}, err
	}

	if bc.rewardDistributor == nil {
		return common.Hash{}, errors.New("reward distributor not initialized")
	}

	simBlock := &block.Block{
		Header: &block.Header{
			Coinbase: view.miner,
			Number:   new(big.Int).SetUint64(view.blockNum),
			Time:     view.blockTime,
			Extra:    view.extra,
		},
		Txs: txs,
	}

	totalFees := big.NewInt(0)
	for _, tx := range txs {
		if tx == nil {
			continue
		}
		txFee := new(big.Int).Mul(tx.GasPrice, big.NewInt(int64(tx.Gas)))
		totalFees.Add(totalFees, txFee)
	}

	rkManager := bc.resolveRotatingKingManagerForBlock(simBlock)
	if _, err := bc.rewardDistributor.DistributeRewards(
		snapshotState,
		view.miner,
		totalFees,
		view.blockNum,
		view.blockTime,
		rkManager,
		bc.pow,
	); err != nil {
		return common.Hash{}, fmt.Errorf("failed to simulate reward distribution: %w", err)
	}

	return snapshotState.Root(), nil
}

func executeTransactionsOnState(st *state.State, txs []*tx.Tx, gasLimit uint64) (common.Hash, error) {
	v := vm.NewVM(st, gasLimit)
	ctx := context.Background()

	for i, transaction := range txs {
		if transaction == nil {
			return common.Hash{}, fmt.Errorf("transaction %d is nil", i)
		}

		if valid, err := transaction.Verify(); err != nil || !valid {
			return common.Hash{}, fmt.Errorf("transaction %d invalid signature", i)
		}

		if _, _, execErr := v.Execute(ctx, transaction); execErr != nil {
			return common.Hash{}, fmt.Errorf("transaction %d execution error: %w", i, execErr)
		}
	}

	return st.Root(), nil
}
