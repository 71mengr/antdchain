package staking

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
	"sync"
	"time"

	"github.com/antdaza/antdchain/antdc/crypto/quantum"
	"github.com/antdaza/antdchain/antdc/state"
	"github.com/antdaza/antdchain/common"
	// "github.com/antdaza/antdchain/antdc/types"
)

var (
	ErrInsufficientStake = errors.New("insufficient stake")
	ErrAlreadyStaked     = errors.New("already staked")
	ErrNotStaked         = errors.New("not staked")
	ErrStakeLocked       = errors.New("stake is locked")
	ErrWithdrawalTooSoon = errors.New("withdrawal requested too soon")
	ErrInvalidStakeValue = errors.New("invalid stake amount")
)

const unstakeUnlockDelayBlocks uint64 = 20

type StakingManager struct {
	mu sync.RWMutex

	// Staking storage
	stakes      map[common.QuantumAddress]*StakeInfo
	totalStaked *big.Int

	// Withdrawal queue
	withdrawals map[common.QuantumAddress]*WithdrawalRequest

	// Configuration
	minStakeAmount  *big.Int
	lockDuration    time.Duration
	slashPercentage *big.Int // 0-100%
	blockHeightFn   func() uint64

	// State reference
	statedb *state.State

	// Events
	stakeEvents chan StakeEvent

	persistFn func([]byte) error
}

type StakeInfo struct {
	Address       common.QuantumAddress
	Amount        *big.Int
	StartTime     time.Time
	LockUntil     time.Time
	BlocksMined   uint64
	RewardsEarned *big.Int
	IsActive      bool
	SlashCount    uint64
	LastActivity  uint64 // Block height
}

type WithdrawalRequest struct {
	Address      common.QuantumAddress
	Amount       *big.Int
	RequestTime  time.Time
	RequestBlock uint64
	UnlockBlock  uint64
	Status       WithdrawalStatus
}

type WithdrawalStatus int

const (
	Pending WithdrawalStatus = iota
	Processing
	Completed
	Cancelled
)

type StakeEvent struct {
	Type      string
	Address   common.QuantumAddress
	Amount    *big.Int
	Timestamp time.Time
	Block     uint64
}

type StakeRecord struct {
	Address      common.QuantumAddress `json:"address"`
	Amount       *big.Int              `json:"amount"`
	IsActive     bool                  `json:"is_active"`
	LastActivity uint64                `json:"last_activity"`
	UnlockBlock  *uint64               `json:"unlock_block,omitempty"`
}

type stakingSnapshot struct {
	Stakes      []stakeSnapshotEntry      `json:"stakes"`
	Withdrawals []withdrawalSnapshotEntry `json:"withdrawals"`
}

type stakeSnapshotEntry struct {
	Address       common.QuantumAddress `json:"address"`
	Amount        string                `json:"amount"`
	StartUnix     int64                 `json:"start_unix"`
	LockUntilUnix int64                 `json:"lock_until_unix"`
	BlocksMined   uint64                `json:"blocks_mined"`
	RewardsEarned string                `json:"rewards_earned"`
	IsActive      bool                  `json:"is_active"`
	SlashCount    uint64                `json:"slash_count"`
	LastActivity  uint64                `json:"last_activity"`
}

type withdrawalSnapshotEntry struct {
	Address      common.QuantumAddress `json:"address"`
	Amount       string                `json:"amount"`
	RequestUnix  int64                 `json:"request_unix"`
	RequestBlock uint64                `json:"request_block"`
	UnlockBlock  uint64                `json:"unlock_block"`
	Status       WithdrawalStatus      `json:"status"`
}

type MinerInfo struct {
	Address     common.QuantumAddress
	StakeAmount *big.Int
	IsActive    bool
	BlocksMined uint64
}

func NewStakingManager(statedb *state.State, minStake *big.Int) *StakingManager {
	if minStake == nil {
		minStake = new(big.Int).Mul(big.NewInt(1_000_000), big.NewInt(1e18))
	}

	return &StakingManager{
		stakes:          make(map[common.QuantumAddress]*StakeInfo),
		withdrawals:     make(map[common.QuantumAddress]*WithdrawalRequest),
		totalStaked:     big.NewInt(0),
		minStakeAmount:  minStake,
		lockDuration:    7 * 24 * time.Hour, // 7 days
		slashPercentage: big.NewInt(5),      // 5% slash for misbehavior
		statedb:         statedb,
		stakeEvents:     make(chan StakeEvent, 100),
	}
}

func (sm *StakingManager) SetBlockHeightProvider(fn func() uint64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.blockHeightFn = fn
}

func (sm *StakingManager) SetPersistFunc(fn func([]byte) error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.persistFn = fn
}

// Stake allows an address to stake tokens for mining eligibility
func (sm *StakingManager) Stake(address common.QuantumAddress, amount *big.Int, privKey []byte) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check minimum stake
	if amount.Cmp(sm.minStakeAmount) < 0 {
		return fmt.Errorf("%w: %s < %s", ErrInsufficientStake,
			amount.String(), sm.minStakeAmount.String())
	}
	if amount.Cmp(sm.minStakeAmount) != 0 {
		return fmt.Errorf("%w: required exact amount %s", ErrInvalidStakeValue, sm.minStakeAmount.String())
	}

	// Check if already staked or waiting for unlock completion.
	if existing, exists := sm.stakes[address]; exists {
		withdrawal, hasWithdrawal := sm.withdrawals[address]
		if existing.IsActive || (hasWithdrawal && withdrawal.Status == Pending) {
			return ErrAlreadyStaked
		}
	}

	// Verify balance
	balance := sm.statedb.GetBalance(address)
	if balance.Cmp(amount) < 0 {
		return errors.New("insufficient balance")
	}

	// Sign stake commitment
	commitment := stakeCommitmentHash("STAKE", address, amount, uint64(time.Now().Unix()))

	signature, err := quantum.Sign(privKey, commitment.Bytes())
	if err != nil {
		return fmt.Errorf("failed to sign stake: %v", err)
	}
	_ = signature

	// Deduct stake from balance
	if err := sm.statedb.AddBalance(address, new(big.Int).Neg(amount)); err != nil {
		return fmt.Errorf("failed to lock stake amount: %w", err)
	}

	// Create stake record
	sm.stakes[address] = &StakeInfo{
		Address:      address,
		Amount:       new(big.Int).Set(amount),
		StartTime:    time.Now(),
		LockUntil:    time.Now().Add(sm.lockDuration),
		IsActive:     true,
		LastActivity: sm.currentBlockHeight(),
	}

	sm.totalStaked.Add(sm.totalStaked, amount)

	// Emit event
	sm.emitEvent(StakeEvent{
		Type:      "Stake",
		Address:   address,
		Amount:    amount,
		Timestamp: time.Now(),
		Block:     sm.currentBlockHeight(),
	})

	log.Printf("[staking] Address %s staked %s ANTD",
		address.Hex()[:12], new(big.Int).Div(amount, big.NewInt(1e18)).String())

	if err := sm.persistLocked(); err != nil {
		return err
	}

	return nil
}

// GetStake returns the stake amount for an address
func (sm *StakingManager) GetStake(address common.QuantumAddress) (*big.Int, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	stake, exists := sm.stakes[address]
	if !exists {
		return big.NewInt(0), nil
	}

	if !stake.IsActive {
		return big.NewInt(0), nil
	}

	return new(big.Int).Set(stake.Amount), nil
}

// GetEligibleMiners returns all addresses with sufficient stake
func (sm *StakingManager) GetEligibleMiners(minStake *big.Int) []common.QuantumAddress {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	var eligible []common.QuantumAddress
	for addr, stake := range sm.stakes {
		if stake.IsActive && stake.Amount.Cmp(minStake) >= 0 {
			eligible = append(eligible, addr)
		}
	}

	return eligible
}

// Unstake initiates withdrawal of staked tokens
func (sm *StakingManager) Unstake(address common.QuantumAddress, privKey []byte) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	stake, exists := sm.stakes[address]
	if !exists {
		return ErrNotStaked
	}

	if !stake.IsActive {
		return ErrNotStaked
	}

	// Check lock period
	if time.Now().Before(stake.LockUntil) {
		return ErrStakeLocked
	}

	// Sign unstake request
	commitment := stakeCommitmentHash("UNSTAKE", address, stake.Amount, uint64(time.Now().Unix()))

	signature, err := quantum.Sign(privKey, commitment.Bytes())
	if err != nil {
		return fmt.Errorf("failed to sign unstake: %v", err)
	}
	_ = signature

	// Mark stake as inactive
	stake.IsActive = false
	sm.totalStaked.Sub(sm.totalStaked, stake.Amount)

	currentHeight := sm.currentBlockHeight()
	// Create withdrawal request
	sm.withdrawals[address] = &WithdrawalRequest{
		Address:      address,
		Amount:       new(big.Int).Set(stake.Amount),
		RequestTime:  time.Now(),
		RequestBlock: currentHeight,
		UnlockBlock:  currentHeight + unstakeUnlockDelayBlocks,
		Status:       Pending,
	}

	sm.emitEvent(StakeEvent{
		Type:      "Unstake",
		Address:   address,
		Amount:    stake.Amount,
		Timestamp: time.Now(),
		Block:     sm.currentBlockHeight(),
	})

	log.Printf("[staking] Address %s requested unstake of %s ANTD",
		address.Hex()[:12], new(big.Int).Div(stake.Amount, big.NewInt(1e18)).String())

	if err := sm.persistLocked(); err != nil {
		return err
	}

	return nil
}

// ProcessWithdrawals processes pending withdrawals after unlock block delay
func (sm *StakingManager) ProcessWithdrawals() error {
	return sm.ProcessWithdrawalsAtHeight(sm.currentBlockHeight())
}

// ProcessWithdrawalsAtHeight processes pending withdrawals using an explicit block height.
func (sm *StakingManager) ProcessWithdrawalsAtHeight(currentHeight uint64) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	processed := 0

	for addr, withdrawal := range sm.withdrawals {
		if withdrawal.Status == Pending && currentHeight >= withdrawal.UnlockBlock {
			// Return staked tokens
			if err := sm.statedb.AddBalance(addr, withdrawal.Amount); err != nil {
				return fmt.Errorf("failed to process withdrawal: %w", err)
			}
			withdrawal.Status = Completed

			sm.emitEvent(StakeEvent{
				Type:      "Withdrawal",
				Address:   addr,
				Amount:    withdrawal.Amount,
				Timestamp: time.Now(),
				Block:     currentHeight,
			})

			log.Printf("[staking] Processed withdrawal for %s: %s ANTD",
				addr.Hex()[:12], new(big.Int).Div(withdrawal.Amount, big.NewInt(1e18)).String())

			processed++
		}
	}

	if processed > 0 {
		if err := sm.persistLocked(); err != nil {
			return err
		}
	}

	return nil
}

// Slash penalizes a miner for misbehavior
func (sm *StakingManager) Slash(address common.QuantumAddress, reason string, reporter common.QuantumAddress) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	stake, exists := sm.stakes[address]
	if !exists || !stake.IsActive {
		return ErrNotStaked
	}

	// Calculate slash amount
	slashAmount := new(big.Int).Mul(stake.Amount, sm.slashPercentage)
	slashAmount.Div(slashAmount, big.NewInt(100))

	// Reduce stake
	stake.Amount.Sub(stake.Amount, slashAmount)
	sm.totalStaked.Sub(sm.totalStaked, slashAmount)

	// Burn slashed tokens or send to treasury
	if err := sm.statedb.AddBalance(common.QuantumAddress{}, slashAmount); err != nil {
		return fmt.Errorf("failed to transfer slashed funds: %w", err)
	}

	stake.SlashCount++

	// If stake falls below minimum, deactivate
	if stake.Amount.Cmp(sm.minStakeAmount) < 0 {
		stake.IsActive = false
		log.Printf("[staking] Miner %s deactivated due to insufficient stake after slash",
			address.Hex()[:12])
	}

	sm.emitEvent(StakeEvent{
		Type:      "Slash",
		Address:   address,
		Amount:    slashAmount,
		Timestamp: time.Now(),
		Block:     sm.currentBlockHeight(),
	})

	log.Printf("[staking] Slashed %s from %s for: %s",
		new(big.Int).Div(slashAmount, big.NewInt(1e18)).String(),
		address.Hex()[:12], reason)

	if err := sm.persistLocked(); err != nil {
		return err
	}

	return nil
}

// GetParentHash is a helper for miner selection
func (sm *StakingManager) GetParentHash(height uint64) common.Hash {
	// This would typically get the parent hash from blockchain
	// For now, return a dummy hash
	// TODO: correct this
	return common.ComputeHash([]byte(fmt.Sprintf("parent-%d", height)))
}

func stakeCommitmentHash(action string, address common.QuantumAddress, amount *big.Int, unixTime uint64) common.Hash {
	buf := new(bytes.Buffer)
	buf.WriteString(action)
	buf.Write(address.Bytes())
	buf.Write(amount.Bytes())

	timeBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timeBytes, unixTime)
	buf.Write(timeBytes)

	return common.ComputeHash(buf.Bytes())
}

func (sm *StakingManager) emitEvent(event StakeEvent) {
	select {
	case sm.stakeEvents <- event:
	default:
		// Channel full, drop event
		log.Println("[staking] Stake event channel full, dropping event")
	}
}

func (sm *StakingManager) currentBlockHeight() uint64 {
	if sm.blockHeightFn != nil {
		return sm.blockHeightFn()
	}
	return 0
}

func (sm *StakingManager) LoadSnapshot(data []byte) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if len(bytes.TrimSpace(data)) == 0 {
		return nil
	}

	var snap stakingSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("failed to decode staking snapshot: %w", err)
	}

	sm.stakes = make(map[common.QuantumAddress]*StakeInfo, len(snap.Stakes))
	sm.withdrawals = make(map[common.QuantumAddress]*WithdrawalRequest, len(snap.Withdrawals))
	sm.totalStaked = big.NewInt(0)

	for _, entry := range snap.Stakes {
		amount := new(big.Int)
		if _, ok := amount.SetString(entry.Amount, 10); !ok {
			return fmt.Errorf("invalid stake amount for %s", entry.Address.String())
		}
		rewards := new(big.Int)
		if entry.RewardsEarned != "" {
			if _, ok := rewards.SetString(entry.RewardsEarned, 10); !ok {
				return fmt.Errorf("invalid rewards amount for %s", entry.Address.String())
			}
		}
		sm.stakes[entry.Address] = &StakeInfo{
			Address:       entry.Address,
			Amount:        amount,
			StartTime:     time.Unix(entry.StartUnix, 0),
			LockUntil:     time.Unix(entry.LockUntilUnix, 0),
			BlocksMined:   entry.BlocksMined,
			RewardsEarned: rewards,
			IsActive:      entry.IsActive,
			SlashCount:    entry.SlashCount,
			LastActivity:  entry.LastActivity,
		}
		if entry.IsActive {
			sm.totalStaked.Add(sm.totalStaked, amount)
		}
	}

	for _, entry := range snap.Withdrawals {
		amount := new(big.Int)
		if _, ok := amount.SetString(entry.Amount, 10); !ok {
			return fmt.Errorf("invalid withdrawal amount for %s", entry.Address.String())
		}
		sm.withdrawals[entry.Address] = &WithdrawalRequest{
			Address:      entry.Address,
			Amount:       amount,
			RequestTime:  time.Unix(entry.RequestUnix, 0),
			RequestBlock: entry.RequestBlock,
			UnlockBlock:  entry.UnlockBlock,
			Status:       entry.Status,
		}
	}

	return nil
}

func (sm *StakingManager) Snapshot() ([]byte, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.snapshotBytesLocked()
}

func (sm *StakingManager) GetStakeRecords() []StakeRecord {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	records := make([]StakeRecord, 0, len(sm.stakes))
	for addr, stake := range sm.stakes {
		record := StakeRecord{
			Address:      addr,
			Amount:       new(big.Int).Set(stake.Amount),
			IsActive:     stake.IsActive,
			LastActivity: stake.LastActivity,
		}
		if w, ok := sm.withdrawals[addr]; ok && w != nil {
			unlockBlock := w.UnlockBlock
			record.UnlockBlock = &unlockBlock
		}
		records = append(records, record)
	}

	return records
}

func (sm *StakingManager) snapshotBytesLocked() ([]byte, error) {
	snap := stakingSnapshot{
		Stakes:      make([]stakeSnapshotEntry, 0, len(sm.stakes)),
		Withdrawals: make([]withdrawalSnapshotEntry, 0, len(sm.withdrawals)),
	}

	for _, stake := range sm.stakes {
		snap.Stakes = append(snap.Stakes, stakeSnapshotEntry{
			Address:       stake.Address,
			Amount:        stake.Amount.String(),
			StartUnix:     stake.StartTime.Unix(),
			LockUntilUnix: stake.LockUntil.Unix(),
			BlocksMined:   stake.BlocksMined,
			RewardsEarned: stake.RewardsEarned.String(),
			IsActive:      stake.IsActive,
			SlashCount:    stake.SlashCount,
			LastActivity:  stake.LastActivity,
		})
	}

	for _, withdrawal := range sm.withdrawals {
		snap.Withdrawals = append(snap.Withdrawals, withdrawalSnapshotEntry{
			Address:      withdrawal.Address,
			Amount:       withdrawal.Amount.String(),
			RequestUnix:  withdrawal.RequestTime.Unix(),
			RequestBlock: withdrawal.RequestBlock,
			UnlockBlock:  withdrawal.UnlockBlock,
			Status:       withdrawal.Status,
		})
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil, fmt.Errorf("failed to encode staking snapshot: %w", err)
	}

	return data, nil
}

func (sm *StakingManager) persistLocked() error {
	if sm.persistFn == nil {
		return nil
	}
	data, err := sm.snapshotBytesLocked()
	if err != nil {
		return err
	}
	if err := sm.persistFn(data); err != nil {
		return fmt.Errorf("failed to persist staking state: %w", err)
	}
	return nil
}

// GetStakingStatistics returns current staking stats
func (sm *StakingManager) GetStakingStatistics() map[string]interface{} {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	activeMiners := 0
	for _, stake := range sm.stakes {
		if stake.IsActive {
			activeMiners++
		}
	}

	return map[string]interface{}{
		"total_staked":        sm.totalStaked.String(),
		"active_miners":       activeMiners,
		"total_stakers":       len(sm.stakes),
		"pending_withdrawals": len(sm.withdrawals),
		"min_stake_amount":    sm.minStakeAmount.String(),
		"lock_duration_hours": sm.lockDuration.Hours(),
	}
}
