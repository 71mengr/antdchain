package staking

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"log"
	"math/big"
	"sync"
	"time"

	"github.com/antdaza/antdchain/antdc/state"
	"github.com/antdaza/antdchain/common"
	"github.com/ethereum/go-ethereum/crypto"
	// "github.com/antdaza/antdchain/antdc/types"
)

var (
	ErrInsufficientStake = errors.New("insufficient stake")
	ErrAlreadyStaked     = errors.New("already staked")
	ErrNotStaked         = errors.New("not staked")
	ErrStakeLocked       = errors.New("stake is locked")
	ErrWithdrawalTooSoon = errors.New("withdrawal requested too soon")
)

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
	withdrawalDelay time.Duration
	slashPercentage *big.Int // 0-100%

	// State reference
	statedb *state.State

	// Events
	stakeEvents chan StakeEvent
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
	Address     common.QuantumAddress
	Amount      *big.Int
	RequestTime time.Time
	ProcessTime time.Time
	Status      WithdrawalStatus
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
		withdrawalDelay: 2 * 24 * time.Hour, // 2 days
		slashPercentage: big.NewInt(5),      // 5% slash for misbehavior
		statedb:         statedb,
		stakeEvents:     make(chan StakeEvent, 100),
	}
}

// Stake allows an address to stake tokens for mining eligibility
func (sm *StakingManager) Stake(address common.QuantumAddress, amount *big.Int, privKey *ecdsa.PrivateKey) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check minimum stake
	if amount.Cmp(sm.minStakeAmount) < 0 {
		return fmt.Errorf("%w: %s < %s", ErrInsufficientStake,
			amount.String(), sm.minStakeAmount.String())
	}

	// Check if already staked
	if _, exists := sm.stakes[address]; exists {
		return ErrAlreadyStaked
	}

	// Verify balance
	balance := sm.statedb.GetBalance(address)
	if balance.Cmp(amount) < 0 {
		return errors.New("insufficient balance")
	}

	// Sign stake commitment
	commitment := crypto.Keccak256Hash(
		[]byte("STAKE"),
		address.Bytes(),
		amount.Bytes(),
		[]byte(fmt.Sprintf("%d", time.Now().Unix())),
	)

	signature, err := crypto.Sign(commitment.Bytes(), privKey)
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
func (sm *StakingManager) Unstake(address common.QuantumAddress, privKey *ecdsa.PrivateKey) error {
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
	commitment := crypto.Keccak256Hash(
		[]byte("UNSTAKE"),
		address.Bytes(),
		stake.Amount.Bytes(),
		[]byte(fmt.Sprintf("%d", time.Now().Unix())),
	)

	signature, err := crypto.Sign(commitment.Bytes(), privKey)
	if err != nil {
		return fmt.Errorf("failed to sign unstake: %v", err)
	}
	_ = signature

	// Mark stake as inactive
	stake.IsActive = false
	sm.totalStaked.Sub(sm.totalStaked, stake.Amount)

	// Create withdrawal request
	sm.withdrawals[address] = &WithdrawalRequest{
		Address:     address,
		Amount:      new(big.Int).Set(stake.Amount),
		RequestTime: time.Now(),
		ProcessTime: time.Now().Add(sm.withdrawalDelay),
		Status:      Pending,
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

	return nil
}

// ProcessWithdrawals processes pending withdrawals after delay
func (sm *StakingManager) ProcessWithdrawals() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	now := time.Now()
	processed := 0

	for addr, withdrawal := range sm.withdrawals {
		if withdrawal.Status == Pending && now.After(withdrawal.ProcessTime) {
			// Return staked tokens
			if err := sm.statedb.AddBalance(addr, withdrawal.Amount); err != nil {
				return fmt.Errorf("failed to process withdrawal: %w", err)
			}
			withdrawal.Status = Completed

			// Remove stake record
			delete(sm.stakes, addr)

			sm.emitEvent(StakeEvent{
				Type:      "Withdrawal",
				Address:   addr,
				Amount:    withdrawal.Amount,
				Timestamp: now,
				Block:     sm.currentBlockHeight(),
			})

			log.Printf("[staking] Processed withdrawal for %s: %s ANTD",
				addr.Hex()[:12], new(big.Int).Div(withdrawal.Amount, big.NewInt(1e18)).String())

			processed++
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

	return nil
}

// GetParentHash is a helper for miner selection
func (sm *StakingManager) GetParentHash(height uint64) common.Hash {
	// This would typically get the parent hash from blockchain
	// For now, return a dummy hash
	// TODO: correct this
	return common.BytesToHash(crypto.Keccak256([]byte(fmt.Sprintf("parent-%d", height))))
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
	return 0
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
