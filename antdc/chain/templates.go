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
	"sort"
	"time"

	"github.com/antdaza/antdchain/antdc/block"
	"github.com/antdaza/antdchain/antdc/pow"
	"github.com/antdaza/antdchain/antdc/state"
	"github.com/antdaza/antdchain/antdc/tx"
	"github.com/antdaza/antdchain/antdc/vm"
	"github.com/antdaza/antdchain/common"
)

// BlockTemplate represents a mining template
type BlockTemplate struct {
	Height           uint64                   `json:"height"`
	PrevHash         string                   `json:"previousblockhash"`
	CoinbaseValue    string                   `json:"coinbasevalue"`
	Target           string                   `json:"target"`
	CurTime          uint64                   `json:"curtime"`
	Transactions     []map[string]interface{} `json:"transactions"`
	Version          uint32                   `json:"version"`
	Bits             string                   `json:"bits"`
	Mintime          uint64                   `json:"mintime"`
	Mutable          []string                 `json:"mutable"`
	NonceRange       string                   `json:"noncerange"`
	SigOpLimit       int                      `json:"sigoplimit"`
	SizeLimit        int                      `json:"sizelimit"`
	WeightLimit      int                      `json:"weightlimit"`
	LongPollID       string                   `json:"longpollid"`
	DefaultWitness   string                   `json:"default_witness_commitment"`
	Capabilities     []string                 `json:"capabilities"`
	Rules            []string                 `json:"rules"`
	VBAvailable      map[string]int           `json:"vbavailable"`
	VBRequired       int                      `json:"vbrequired"`
	CoinbaseAux      map[string]string        `json:"coinbaseaux"`
	Miner            string                   `json:"miner"`
	StateRoot        string                   `json:"stateroot"`
	TransactionsRoot string                   `json:"transactionsroot"`
	Difficulty       string                   `json:"difficulty"`
	GasLimit         uint64                   `json:"gaslimit"`
	GasUsed          uint64                   `json:"gasused"`
	ExtraData        string                   `json:"extradata"`
	Nonce            string                   `json:"nonce"`
	MixDigest        string                   `json:"mixdigest"`
	MixHash          string                   `json:"mixHash"`
}

func powHeaderFromBlockHeader(header *block.Header) (*pow.BlockHeader, error) {
	if header == nil {
		return nil, errors.New("block header is nil")
	}
	if header.Number == nil {
		return nil, errors.New("block header number is nil")
	}

	difficulty := big.NewInt(pow.MinDifficulty)
	if header.Difficulty != nil {
		difficulty = new(big.Int).Set(header.Difficulty)
	}

	return &pow.BlockHeader{
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
	}, nil
}

func targetHexForDifficulty(difficulty *big.Int) string {
	target := pow.TargetForDifficulty(difficulty)
	return fmt.Sprintf("%064x", target)
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

	// Get pending transactions from pool so newly submitted transactions can be mined.
	confirmedTxs := bc.txPool.GetPending()

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

	now := uint64(time.Now().Unix())
	parentHeight := parent.Header.Number.Uint64()
	height := parentHeight + 1
	txRoot := CalcTxRoot(confirmedTxs)
	extraData := []byte("ANTDChain-PoW")
	if bc.rotatingKingManager != nil {
		if rotatingKing := bc.rotatingKingManager.GetCurrentKing(); rotatingKing != (common.QuantumAddress{}) {
			extraData = []byte(fmt.Sprintf("ANTDChain-PoW|rk=%s", rotatingKing.String()))
		}
	}

	header := &block.Header{
		ParentHash: parent.Hash(),
		Coinbase:   rewardAddr,
		Root:       common.Hash{},
		TxHash:     txRoot,
		Number:     new(big.Int).SetUint64(height),
		GasLimit:   parent.Header.GasLimit,
		GasUsed:    0,
		Time:       now,
		Extra:      extraData,
	}
	header.Difficulty = bc.calculateExpectedDifficultyFromChainState(&block.Block{Header: header}, parent)
	if header.GasLimit == 0 {
		header.GasLimit = 10_000_000
	}
	if root, err := bc.ComputeBlockFinalStateRoot(rewardAddr, now, height, extraData, confirmedTxs); err == nil {
		header.Root = root
	} else {
		log.Printf("[template] failed to compute template state root: %v", err)
	}
	if err := applyProtocolHeaderFields(header); err != nil {
		return nil, fmt.Errorf("failed to apply protocol header fields: %w", err)
	}

	// Convert difficulty to compact format (Bitcoin-style bits)
	compact := diffToCompact(header.Difficulty)

	return &BlockTemplate{
		Height:           height,
		PrevHash:         parent.Hash().String(),
		CoinbaseValue:    coinbaseValue.String(),
		Target:           targetHexForDifficulty(header.Difficulty),
		CurTime:          now,
		Transactions:     transactions,
		Version:          536870912, // Bitcoin-compatible version
		Bits:             fmt.Sprintf("%08x", compact),
		Mintime:          now - 7200, // 2 hours ago
		Mutable:          []string{"time", "transactions", "prevblock", "nonce"},
		NonceRange:       "0000000000000000ffffffffffffffff",
		SigOpLimit:       80000,
		SizeLimit:        4000000,
		WeightLimit:      4000000,
		LongPollID:       parent.Hash().String() + fmt.Sprintf("%d", len(transactions)),
		DefaultWitness:   "",
		Capabilities:     []string{"proposal", "pow", "mixdigest"},
		Rules:            []string{},
		VBAvailable:      map[string]int{},
		VBRequired:       0,
		CoinbaseAux:      map[string]string{},
		Miner:            rewardAddr.String(),
		StateRoot:        header.Root.Hex(),
		TransactionsRoot: header.TxHash.Hex(),
		Difficulty:       header.Difficulty.String(),
		GasLimit:         header.GasLimit,
		GasUsed:          header.GasUsed,
		ExtraData:        hex.EncodeToString(header.Extra),
		Nonce:            "0x" + header.Nonce.String(),
		MixDigest:        header.MixDigest.Hex(),
		MixHash:          header.MixDigest.Hex(),
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

	// Enforce the network target block time.
	currentTime := uint64(time.Now().Unix())
	if currentTime-parent.Header.Time < pow.TargetBlockTimeSeconds {
		wait := pow.TargetBlockTimeSeconds - (currentTime - parent.Header.Time)
		log.Printf("[miner] Waiting %d seconds for proper block timing...", wait)
		time.Sleep(time.Duration(wait) * time.Second)
		currentTime = uint64(time.Now().Unix())
	}

	// Check miner eligibility
	if bc.pow != nil {
		if bc.stakingManager != nil {
			if stakeAmt, err := bc.stakingManager.GetStake(miner); err == nil && stakeAmt != nil {
				bc.pow.AutoRegisterIfEligible(miner, stakeAmt, nil)
			}
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

		// Deterministic ordering across miners prevents divergent state roots
		// when multiple eligible miners draw from similar mempools.
		sort.SliceStable(candidates, func(i, j int) bool {
			if candidates[i] == nil {
				return false
			}
			if candidates[j] == nil {
				return true
			}

			hashI := candidates[i].Hash().Hex()
			hashJ := candidates[j].Hash().Hex()
			if hashI != hashJ {
				return hashI < hashJ
			}

			if candidates[i].Nonce != candidates[j].Nonce {
				return candidates[i].Nonce < candidates[j].Nonce
			}

			return candidates[i].Timestamp < candidates[j].Timestamp
		})

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
		Extra:      extraData,
		Stakers:    bc.blockHeaderStakerRegistrations(),
	}
	header.Difficulty = bc.calculateExpectedDifficultyFromChainState(
		&block.Block{Header: header},
		parent,
	)
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

// ComputeBlockFinalStateRoot computes the committed state root for a mined block
// using the same transaction execution and reward distribution path as validation.
func (bc *Blockchain) ComputeBlockFinalStateRoot(miner common.QuantumAddress, blockTime uint64, blockNum uint64, extra []byte, txs []*tx.Tx) (common.Hash, error) {
	return bc.computeBlockFinalStateRoot(headerMinerBlockView{
		miner:     miner,
		blockTime: blockTime,
		blockNum:  blockNum,
		extra:     extra,
	}, txs)
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
		txFee := new(big.Int).Mul(tx.GasPrice, big.NewInt(int64(transactionCommittedGas(tx))))
		totalFees.Add(totalFees, txFee)
	}

	rkManager := bc.resolveRotatingKingManagerForBlock(simBlock)
	if rkManager != nil {
		rkManager = &simulatedRotatingKingManager{RotatingKingManager: rkManager}
	}
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

func (bc *Blockchain) blockHeaderStakerRegistrations() []block.StakerRegistration {
	if bc == nil || bc.stakingManager == nil {
		return nil
	}

	records := bc.stakingManager.GetStakeRecords()
	if len(records) == 0 {
		return nil
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].Address.String() < records[j].Address.String()
	})

	registrations := make([]block.StakerRegistration, 0, len(records))
	for _, record := range records {
		registrations = append(registrations, block.StakerRegistration{
			Address:           record.Address,
			Amount:            new(big.Int).Set(record.Amount),
			RegisteredHeight:  record.RegisteredHeight,
			UnlockStakeHeight: record.UnlockBlock,
			IsActive:          record.IsActive,
		})
	}

	return registrations
}
