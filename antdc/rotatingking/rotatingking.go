// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package rotatingking
import (
        "fmt"
        "math/big"
        "strings"
        "sync"
        "time"
        "errors"
        "context"
        "github.com/antdaza/antdchain/common"
        "go.uber.org/zap"
//        "github.com/antdaza/antdchain/antdc/chain/db"
)



type RotationProposal struct {
    ProposalID       string           `json:"proposalId"`
    BlockHeight      uint64           `json:"blockHeight"`
    ProposerNodeID   string           `json:"proposerNodeId"`
    ProposerAddress  string           `json:"proposerAddress"`
    CurrentKing      common.QuantumAddress   `json:"currentKing"`
    NextKing         common.QuantumAddress   `json:"nextKing"`
    RotationHeight   uint64           `json:"rotationHeight"`
    NextRotationAt   uint64           `json:"nextRotationAt"`
    ProposerState    string           `json:"proposerState"`
    Timestamp        time.Time        `json:"timestamp"`
    Expiration       time.Time        `json:"expiration"`
    Signature        []byte           `json:"signature,omitempty"`
}

type RotationVote struct {
    ProposalID    string         `json:"proposalId"`
    VoterNodeID   string         `json:"voterNodeId"`
    VoterAddress  string         `json:"voterAddress"`
    Approved      bool           `json:"approved"`
    Reason        string         `json:"reason,omitempty"`
    VoterState    string         `json:"voterState"`
    Timestamp     time.Time      `json:"timestamp"`
    Signature     []byte         `json:"signature,omitempty"`
}

type ConsensusResult struct {
    ProposalID     string    `json:"proposalId"`
    BlockHeight    uint64    `json:"blockHeight"`
    Approved       bool      `json:"approved"`
    ApprovalCount  int       `json:"approvalCount"`
    TotalPeers     int       `json:"totalPeers"`
    Timestamp      time.Time `json:"timestamp"`
    FinalStateHash string    `json:"finalStateHash"`
    Signature      []byte    `json:"signature,omitempty"`
}

type KingStateBroadcast struct {
    BlockHeight        uint64           `json:"blockHeight"`
    CurrentKingIndex   int              `json:"currentKingIndex"`
    KingAddresses      []common.QuantumAddress `json:"kingAddresses"`
    RotationHeight     uint64           `json:"rotationHeight"`
    NextRotationAt     uint64           `json:"nextRotationAt"`
    RotationCount      uint64           `json:"rotationCount"`
    LastUpdated        time.Time        `json:"lastUpdated"`
    BroadcastTimestamp time.Time        `json:"broadcastTimestamp"`
}

type P2PBroadcaster interface {
    BroadcastKingListUpdate(event *KingListUpdateEvent) error
    BroadcastKingState(event *KingStateBroadcast) error
    BroadcastRotation(event *KingRotationBroadcast) error
    BroadcastDatabaseSync(request *DatabaseSyncRequest) error
    BroadcastRotationProposal(proposal *RotationProposal) error
    BroadcastRotationVote(vote *RotationVote) error
    BroadcastConsensusResult(result *ConsensusResult) error
    GetPeers() []string
    GetPeerCount() int
}

type KingRotationBroadcast struct {
    BlockHeight  uint64         `json:"blockHeight"`
    BlockHash    common.Hash    `json:"blockHash"`
    PreviousKing common.QuantumAddress `json:"previousKing"`
    NewKing      common.QuantumAddress `json:"newKing"`
    Timestamp    time.Time      `json:"timestamp"`
}

type DatabaseSyncRequest struct {
    NodeID          string    `json:"nodeId"`
    LastSyncedBlock uint64    `json:"lastSyncedBlock"`
    SyncTimestamp   time.Time `json:"syncTimestamp"`
    RequestType     string    `json:"requestType"` // full_sync, incremental, state_only
}

type KingListUpdateEvent struct {
        BlockHeight uint64             `json:"height"`
        NewList     []common.QuantumAddress   `json:"newList"`
        Added       common.QuantumAddress     `json:"added,omitempty"`
        Removed     common.QuantumAddress     `json:"removed,omitempty"`
        Timestamp   time.Time          `json:"ts"`
        Reason      string             `json:"reason,omitempty"`
}

// 100,000 ANTD = 100000 * 10^18
var EligibilityThreshold = new(big.Int).Mul(big.NewInt(100000), big.NewInt(1e18))

// BlockchainProvider interface for blockchain interactions
type BlockchainProvider interface {
        GetChainHeight() uint64
        GetBlock(uint64) interface{} // Return the appropriate block type
        State() interface{}          // Return the appropriate state type
}
/*
type DatabaseManager interface {
        SaveState(state *RotatingKingState) error
        LoadState() (*RotatingKingState, error)
        SaveConfig(config *RotatingKingConfig) error
        LoadConfig() (*RotatingKingConfig, error)
        SaveRotationEvent(rotation *KingRotation) error
        SaveBlockSync(record *BlockSyncRecord) error
        GetLastSyncedBlock() (*BlockSyncRecord, error)
        SaveSyncState(state *SyncState) error
        LoadSyncState() (*SyncState, error)
        GetRotationEvents(fromBlock, toBlock uint64) ([]KingRotation, error)
        Close() error
        SyncToBlock(ctx context.Context, blockHeight uint64, state *RotatingKingState, config *RotatingKingConfig) error
        GetMetrics() *DBMetrics
        Backup(backupPath string) error
}*/

// handles king rotation logic with database integration
type RotatingKingManager struct {
        mu          sync.RWMutex
        config      RotatingKingConfig
        state       RotatingKingState
        statePath   string
        bc          BlockchainProvider
        isMainKing  bool
        broadcaster P2PBroadcaster
        database    RotatingKingDatabase
        logger      *zap.Logger
}

//  returns the default rotation configuration
func DefaultRotatingKingConfig() RotatingKingConfig {
        return RotatingKingConfig{
                RotationInterval: 100,
                RotationOffset:   0,
                KingAddresses: []common.QuantumAddress{

                },
                ActivationDelay:  2,
                MinStakeRequired: new(big.Int).Set(EligibilityThreshold),
        }
}

// NewRotatingKingManager creates a new rotation manager with database
func NewRotatingKingManager(statePath string, bc BlockchainProvider, isMainKing bool, broadcaster P2PBroadcaster, database RotatingKingDatabase) (*RotatingKingManager, error) {
    logger, err := zap.NewProduction()
    if err != nil {
        return nil, fmt.Errorf("failed to create logger: %w", err)
    }

    // Try to load existing state and config from database
    config := DefaultRotatingKingConfig()
    state := RotatingKingState{
        CurrentKingIndex:         0,
        RotationHeight:           0,
        NextRotationAt:           config.RotationInterval,
        LastUpdated:              time.Now(),
        RotationCount:            0,
        KingsHistory:             make([]KingRotation, 0),
        TotalRewardsDistributed:  big.NewInt(0),
        KingRewards:              make(map[common.QuantumAddress]*big.Int),
    }

    // Load from database if available
    if loadedConfig, err := database.ReadRotatingKingConfig(); err == nil && loadedConfig != nil {
        config = *loadedConfig
        logger.Info("loaded rotating king config from database")
    }

    if loadedState, err := database.ReadRotatingKingState(); err == nil && loadedState != nil {
        state = *loadedState
        logger.Info("loaded rotating king state from database")
    }

    m := &RotatingKingManager{
        config:      config,
        state:       state,
        statePath:   statePath,
        bc:          bc,
        isMainKing:  isMainKing,
        broadcaster: broadcaster,
        database:    database,
        logger:      logger,
    }

    // Save initial state to database
    if err := m.saveState(); err != nil {
        logger.Warn("failed to save initial state", zap.Error(err))
    }

    if err := m.saveConfig(); err != nil {
        logger.Warn("failed to save initial config", zap.Error(err))
    }

    logger.Info("rotating king manager initialized with database")
    return m, nil
}


// PUBLIC METHODS
func (m *RotatingKingManager) GetCurrentKing() common.QuantumAddress {
        m.mu.RLock()
        defer m.mu.RUnlock()
        if len(m.config.KingAddresses) == 0 {
                return common.QuantumAddress{}
        }
        return m.config.KingAddresses[m.state.CurrentKingIndex]
}

func (m *RotatingKingManager) GetNextKing() common.QuantumAddress {
        m.mu.RLock()
        defer m.mu.RUnlock()
        if len(m.config.KingAddresses) == 0 {
                return common.QuantumAddress{}
        }
        nextIndex := (m.state.CurrentKingIndex + 1) % len(m.config.KingAddresses)
        return m.config.KingAddresses[nextIndex]
}

func (m *RotatingKingManager) GetRotationInfo(height uint64) map[string]interface{} {
        m.mu.RLock()
        defer m.mu.RUnlock()

        info := make(map[string]interface{})
        if len(m.config.KingAddresses) == 0 {
                return info
        }

        currentKing := m.config.KingAddresses[m.state.CurrentKingIndex]
        nextKing := m.config.KingAddresses[(m.state.CurrentKingIndex+1)%len(m.config.KingAddresses)]

        blocksUntilRotation := uint64(0)
        if m.state.NextRotationAt > height {
                blocksUntilRotation = m.state.NextRotationAt - height
        }

        info["currentKing"] = currentKing.String()
        info["nextKing"] = nextKing.String()
        info["blocksUntilRotation"] = blocksUntilRotation
        info["rotationHeight"] = m.state.RotationHeight
        info["nextRotationAt"] = m.state.NextRotationAt
        info["rotationInterval"] = m.config.RotationInterval
        info["kingCount"] = len(m.config.KingAddresses)
        info["rotationCount"] = m.state.RotationCount
        info["isActive"] = height >= m.state.RotationHeight+m.config.ActivationDelay

        if m.state.TotalRewardsDistributed != nil {
                info["totalRewardsDistributed"] = m.state.TotalRewardsDistributed.String()
        }

        if m.state.KingRewards[currentKing] != nil {
                info["currentKingRewards"] = m.state.KingRewards[currentKing].String()
        }

        if m.bc != nil && blocksUntilRotation > 0 {
                avgBlockTime := m.calculateAverageBlockTime()
                estTime := time.Duration(blocksUntilRotation) * avgBlockTime
                info["estimatedTimeUntilRotation"] = estTime.String()
        }

        return info
}

func (m *RotatingKingManager) ShouldRotate(blockHeight uint64) bool {
        m.mu.RLock()
        defer m.mu.RUnlock()
        return blockHeight >= m.state.NextRotationAt
}

func (m *RotatingKingManager) RecordRewardDistribution(king common.QuantumAddress, reward *big.Int, blockHeight uint64) {
        m.mu.Lock()
        defer m.mu.Unlock()

        if reward == nil {
                reward = big.NewInt(0)
        }

        m.state.TotalRewardsDistributed.Add(m.state.TotalRewardsDistributed, reward)

        if m.state.KingRewards == nil {
                m.state.KingRewards = make(map[common.QuantumAddress]*big.Int)
        }
        if m.state.KingRewards[king] == nil {
                m.state.KingRewards[king] = big.NewInt(0)
        }
        m.state.KingRewards[king].Add(m.state.KingRewards[king], reward)

        for i := len(m.state.KingsHistory) - 1; i >= 0; i-- {
                entry := &m.state.KingsHistory[i]
                if entry.NewKing == king && entry.Reward.Sign() == 0 {
                        entry.Reward = new(big.Int).Set(reward)
                        entry.WasEligible = true
                        break
                }
        }

        go func() {
                if err := m.saveState(); err != nil {
                        m.logger.Error("failed to persist reward state", zap.Error(err))
                }
        }()

        m.logger.Info("king received reward",
                zap.String("king", king.String()[:10]),
                zap.String("reward", formatBalance(reward)),
                zap.Uint64("block", blockHeight))
}

/*func (m *RotatingKingManager) RotateToNextKing(blockHeight uint64, blockHash common.Hash) error {
        m.mu.Lock()
        defer m.mu.Unlock()

    if len(m.config.KingAddresses) == 0 {
        return fmt.Errorf("no king addresses configured")
    }

    if blockHeight < m.state.NextRotationAt {
        return fmt.Errorf("rotation not due yet (next at %d)", m.state.NextRotationAt)
    }

    // Check if other nodes agree on rotation
    if m.broadcaster != nil {
        consensus, err := m.checkRotationConsensus(blockHeight)
        if err != nil {
            return fmt.Errorf("consensus check failed: %w", err)
        }

        if !consensus {
            m.logger.Warn("Rotation consensus not reached, waiting...")
            return fmt.Errorf("rotation consensus not reached")
        }
    }

    // First, cleanup ineligible kings before rotation
    if m.bc != nil {
        stateProvider, ok := m.bc.State().(interface {
            GetBalance(common.QuantumAddress) *big.Int
        })
        if ok {
            // Check if next king is eligible
            nextIndex := (m.state.CurrentKingIndex + 1) % len(m.config.KingAddresses)
            nextKing := m.config.KingAddresses[nextIndex]
            balance := stateProvider.GetBalance(nextKing)

            if balance.Cmp(m.config.MinStakeRequired) < 0 {
                // Skip to next eligible king
                m.logger.Warn("Next king ineligible, searching for eligible king",
                    zap.String("address", nextKing.String()[:10]),
                    zap.String("balance", formatBalance(balance)))

                // Find next eligible king
                for i := 1; i < len(m.config.KingAddresses); i++ {
                    candidateIndex := (m.state.CurrentKingIndex + i) % len(m.config.KingAddresses)
                    candidate := m.config.KingAddresses[candidateIndex]
                    candidateBalance := stateProvider.GetBalance(candidate)

                    if candidateBalance.Cmp(m.config.MinStakeRequired) >= 0 {
                        nextIndex = candidateIndex
                        nextKing = candidate
                        m.logger.Info("Found eligible king",
                            zap.String("address", nextKing.String()[:10]),
                            zap.String("balance", formatBalance(candidateBalance)))
                        break
                    }
                }
            }
        }
    }
        if len(m.config.KingAddresses) == 0 {
                return fmt.Errorf("no king addresses configured")
        }

        if blockHeight < m.state.NextRotationAt {
                return fmt.Errorf("rotation not due yet (next at %d)", m.state.NextRotationAt)
        }

        previousKing := m.config.KingAddresses[m.state.CurrentKingIndex]
        newIndex := (m.state.CurrentKingIndex + 1) % len(m.config.KingAddresses)
        newKing := m.config.KingAddresses[newIndex]

        isEligible := false
        eligibilityBalance := big.NewInt(0)
        if m.bc != nil {
                stateProvider, ok := m.bc.State().(interface {
                        GetBalance(common.QuantumAddress) *big.Int
                })
                if ok && stateProvider != nil {
                        eligibilityBalance = stateProvider.GetBalance(newKing)
                        if eligibilityBalance.Cmp(m.config.MinStakeRequired) >= 0 {
                                isEligible = true
                        }
                }
        }

        rotation := KingRotation{
                BlockHeight:  blockHeight,
                PreviousKing: previousKing,
                NewKing:      newKing,
                Timestamp:    time.Now(),
                Reward:       big.NewInt(0),
                WasEligible:  isEligible,
        }

        m.state.KingsHistory = append(m.state.KingsHistory, rotation)
        if len(m.state.KingsHistory) > 100 {
                m.state.KingsHistory = m.state.KingsHistory[1:]
        }

        m.state.CurrentKingIndex = newIndex
        m.state.RotationHeight = blockHeight
        m.state.NextRotationAt = blockHeight + m.config.RotationInterval
        m.state.RotationCount++
        m.state.LastUpdated = time.Now()

        // Save rotation event to database
        if err := m.database.SaveRotationEvent(&rotation); err != nil {
                m.logger.Warn("failed to save rotation event", zap.Error(err))
        }

        // Save state to database
        if err := m.saveState(); err != nil {
                m.logger.Error("failed to save state", zap.Error(err))
                return err
        }

        // Save block sync record
        record := &BlockSyncRecord{
                BlockHeight:    blockHeight,
                BlockHash:      blockHash,
                Timestamp:      time.Now(),
                RotationEvents: []string{fmt.Sprintf("rotation:%s->%s", previousKing.String(), newKing.String())},
                SyncDuration:   0,
        }
        if err := m.database.SaveBlockSync(record); err != nil {
                m.logger.Warn("failed to save block sync", zap.Error(err))
        }

        if isEligible {
                m.logger.Info("king rotation - eligible",
                        zap.String("newKing", newKing.String()),
                        zap.String("balance", formatBalance(eligibilityBalance)))
        } else {
                m.logger.Warn("king rotation - ineligible",
                        zap.String("newKing", newKing.String()),
                        zap.String("balance", formatBalance(eligibilityBalance)),
                        zap.String("required", formatBalance(m.config.MinStakeRequired)))
        }

    // Broadcast the rotation to all peers
    if m.broadcaster != nil {
        rotationEvent := &KingRotationBroadcast{
            BlockHeight:  blockHeight,
            BlockHash:    blockHash,
            PreviousKing: previousKing,
            NewKing:      newKing,
            Timestamp:    time.Now(),
        }

        if err := m.broadcaster.BroadcastRotation(rotationEvent); err != nil {
            m.logger.Warn("Failed to broadcast rotation", zap.Error(err))
        }
    }

        return nil
}
*/
func (m *RotatingKingManager) GetKingRewards(king common.QuantumAddress) *big.Int {
        m.mu.RLock()
        defer m.mu.RUnlock()
        if rewards, exists := m.state.KingRewards[king]; exists {
                return new(big.Int).Set(rewards)
        }
        return big.NewInt(0)
}

func (m *RotatingKingManager) GetTotalRewardsDistributed() *big.Int {
        m.mu.RLock()
        defer m.mu.RUnlock()
        return new(big.Int).Set(m.state.TotalRewardsDistributed)
}

func (m *RotatingKingManager) GetKingStats(king common.QuantumAddress) map[string]interface{} {
        m.mu.RLock()
        defer m.mu.RUnlock()

        stats := make(map[string]interface{})
        kingIndex := -1
        for i, addr := range m.config.KingAddresses {
                if addr == king {
                        kingIndex = i
                        break
                }
        }

        if kingIndex == -1 {
                stats["inRotation"] = false
                return stats
        }

        stats["inRotation"] = true
        stats["position"] = kingIndex + 1
        stats["totalPositions"] = len(m.config.KingAddresses)

        if m.state.CurrentKingIndex == kingIndex {
                stats["isCurrentKing"] = true
                stats["becameKingAtBlock"] = m.state.RotationHeight
                stats["nextRotationAtBlock"] = m.state.NextRotationAt
        } else {
                stats["isCurrentKing"] = false
                diff := (kingIndex - m.state.CurrentKingIndex + len(m.config.KingAddresses)) % len(m.config.KingAddresses)
                stats["rotationsUntilKing"] = diff
                stats["estimatedBlocksUntilKing"] = uint64(diff) * m.config.RotationInterval
        }

        if rewards, exists := m.state.KingRewards[king]; exists {
                stats["totalRewards"] = rewards.String()
                stats["totalRewardsFormatted"] = formatBalance(rewards)
        } else {
                stats["totalRewards"] = "0"
                stats["totalRewardsFormatted"] = "0"
        }

        return stats
}

func (m *RotatingKingManager) GetRotationInterval() uint64 {
        m.mu.RLock()
        defer m.mu.RUnlock()
        return m.config.RotationInterval
}

func (m *RotatingKingManager) IsCurrentKing(address common.QuantumAddress) bool {
        m.mu.RLock()
        defer m.mu.RUnlock()
        if len(m.config.KingAddresses) == 0 {
                return false
        }
        return m.config.KingAddresses[m.state.CurrentKingIndex] == address
}

func (m *RotatingKingManager) GetCurrentKingIndex() int {
        m.mu.RLock()
        defer m.mu.RUnlock()
        return m.state.CurrentKingIndex
}

func (m *RotatingKingManager) GetKingAddresses() []common.QuantumAddress {
        m.mu.RLock()
        defer m.mu.RUnlock()
        addresses := make([]common.QuantumAddress, len(m.config.KingAddresses))
        copy(addresses, m.config.KingAddresses)
        return addresses
}

//DATABASE-ENHANCED METHODS

// Synchronizes state with blockchain up to target height
func (m *RotatingKingManager) SyncBlocks(ctx context.Context, targetHeight uint64) error {

    m.logger.Info("starting block synchronization with ChainDB",
        zap.Uint64("targetHeight", targetHeight),
        zap.Uint64("currentHeight", m.bc.GetChainHeight()))

    // Sync rotations up to target height
    if err := m.syncRotationsToHeight(ctx, targetHeight); err != nil {
        return fmt.Errorf("failed to sync rotations: %w", err)
    }

    // Update sync state
    syncState := &SyncState{
        LastSyncedBlock: targetHeight,
        LastSyncTime:    time.Now(),
        SyncProgress:    1.0,
        TotalBlocks:     targetHeight,
    }
    if err := m.SaveSyncState(syncState); err != nil {
        m.logger.Warn("failed to save sync state", zap.Error(err))
    }

    m.logger.Info("block synchronization completed with ChainDB",
        zap.Uint64("targetHeight", targetHeight))

    return nil
}


// GetSyncState returns synchronization status
func (m *RotatingKingManager) GetSyncState() (*SyncState, error) {
    return m.database.ReadSyncState()
}

// GetRotationHistoryFromDB retrieves rotation history from database
func (m *RotatingKingManager) GetRotationHistoryFromDB(fromBlock, toBlock uint64) ([]KingRotation, error) {
    return m.database.GetRotationEvents(fromBlock, toBlock)
}

func (m *RotatingKingManager) GetDBMetrics() *DBMetrics {
    return m.database.GetMetrics()
}

// BackupDatabase creates a database backup
func (m *RotatingKingManager) BackupDatabase(backupPath string) error {
    // ChainDB handles its own backup through pebble
    // Or I will implement backup through blockchain's ChainDB, let me test and see
    m.logger.Info("backup handled by ChainDB", zap.String("path", backupPath))
    return nil
}

// ========== PRIVATE METHODS ==========
func (m *RotatingKingManager) calculateAverageBlockTime() time.Duration {
        if m.bc == nil {
                return 30 * time.Second
        }

        var totalTime uint64
        var blockCount uint64
        height := m.bc.GetChainHeight()

        for i := height; i > 0 && i > height-10; i-- {
                block := m.bc.GetBlock(i)
                if block == nil {
                        continue
                }

                var blockTime uint64
                switch b := block.(type) {
                case interface{ GetTime() uint64 }:
                        blockTime = b.GetTime()
                case interface{ Header() interface{ GetTime() uint64 } }:
                        if header := b.Header(); header != nil {
                                blockTime = header.GetTime()
                        }
                default:
                        continue
                }

                if i > 0 {
                        parent := m.bc.GetBlock(i - 1)
                        if parent != nil {
                                var parentTime uint64
                                switch p := parent.(type) {
                                case interface{ GetTime() uint64 }:
                                        parentTime = p.GetTime()
                                case interface{ Header() interface{ GetTime() uint64 } }:
                                        if header := p.Header(); header != nil {
                                                parentTime = header.GetTime()
                                        }
                                default:
                                        continue
                                }

                                timeDiff := blockTime - parentTime
                                if timeDiff > 0 && timeDiff < 300 {
                                        totalTime += timeDiff
                                        blockCount++
                                }
                        }
                }
        }

        if blockCount == 0 {
                return 30 * time.Second
        }
        avgSeconds := totalTime / blockCount
        return time.Duration(avgSeconds) * time.Second
}

func (m *RotatingKingManager) saveState() error {
    return m.database.WriteRotatingKingState(&m.state)
}

func (m *RotatingKingManager) saveConfig() error {
    return m.database.WriteRotatingKingConfig(&m.config)
}

func (m *RotatingKingManager) Close() error {
    // Save final state
    m.mu.RLock()
    state := m.state
    config := m.config
    m.mu.RUnlock()

    if err := m.database.WriteRotatingKingState(&state); err != nil {
        m.logger.Warn("failed to save final state", zap.Error(err))
    }

    if err := m.database.WriteRotatingKingConfig(&config); err != nil {
        m.logger.Warn("failed to save final config", zap.Error(err))
    }

    // Close database
    if err := m.database.Close(); err != nil {
        return fmt.Errorf("failed to close database: %w", err)
    }

    m.logger.Info("rotating king manager closed")
    return nil
}

func (m *RotatingKingManager) GetRotationHistory(limit int) []KingRotation {
        m.mu.RLock()
        defer m.mu.RUnlock()

        if len(m.state.KingsHistory) == 0 {
                return []KingRotation{}
        }

        start := 0
        if len(m.state.KingsHistory) > limit {
                start = len(m.state.KingsHistory) - limit
        }

        history := make([]KingRotation, len(m.state.KingsHistory)-start)
        copy(history, m.state.KingsHistory[start:])

        return history
}

func (m *RotatingKingManager) GetKingRewardMultiplier() *big.Float {
        m.mu.RLock()
        defer m.mu.RUnlock()
        return big.NewFloat(1.05)
}


func (m *RotatingKingManager) ForceRotate(index int, reason string) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    if len(m.config.KingAddresses) == 0 {
        return fmt.Errorf("no king addresses configured")
    }

    if index < 0 || index >= len(m.config.KingAddresses) {
        return fmt.Errorf("invalid index: %d (valid: 0-%d)", index, len(m.config.KingAddresses)-1)
    }

    currentHeight := uint64(0)
    if m.bc != nil {
        currentHeight = m.bc.GetChainHeight()
    }

    previousKing := m.config.KingAddresses[m.state.CurrentKingIndex]
    newKing := m.config.KingAddresses[index]

    isEligible := false
    var eligibilityBalance *big.Int
    if m.bc != nil {
        stateProvider, ok := m.bc.State().(interface {
            GetBalance(common.QuantumAddress) *big.Int
        })
        if ok && stateProvider != nil {
            eligibilityBalance = stateProvider.GetBalance(newKing)
            if eligibilityBalance.Cmp(m.config.MinStakeRequired) >= 0 {
                isEligible = true
            }
        }
    }

    rotation := KingRotation{
        BlockHeight:  currentHeight,
        PreviousKing: previousKing,
        NewKing:      newKing,
        Timestamp:    time.Now(),
        Reward:       big.NewInt(0),
        WasEligible:  isEligible,
        Reason:       reason,
    }

    m.state.KingsHistory = append(m.state.KingsHistory, rotation)
    if len(m.state.KingsHistory) > 100 {
        m.state.KingsHistory = m.state.KingsHistory[1:]
    }

    m.state.CurrentKingIndex = index
    m.state.RotationHeight = currentHeight
    m.state.NextRotationAt = currentHeight + m.config.RotationInterval
    m.state.RotationCount++
    m.state.LastUpdated = time.Now()

    // Save to database
    if err := m.database.WriteRotationEvent(&rotation); err != nil {
        m.logger.Warn("failed to save rotation event", zap.Error(err))
    }

    if err := m.saveState(); err != nil {
        m.logger.Error("failed to save state", zap.Error(err))
        return err
    }

    m.logger.Info("force rotation executed",
        zap.String("from", previousKing.String()[:8]),
        zap.String("to", newKing.String()[:8]),
        zap.Uint64("height", currentHeight),
        zap.String("reason", reason))

    return nil
}

func (m *RotatingKingManager) IsKing(address common.QuantumAddress) bool {
        m.mu.RLock()
        defer m.mu.RUnlock()

        for _, addr := range m.config.KingAddresses {
                if addr == address {
                        return true
                }
        }
        return false
}

func (m *RotatingKingManager) UpdateKingAddresses(newAddresses []common.QuantumAddress) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    if len(newAddresses) == 0 {
        return fmt.Errorf("cannot set empty king address list")
    }

    m.config.KingAddresses = make([]common.QuantumAddress, len(newAddresses))
    copy(m.config.KingAddresses, newAddresses)

    // Save configuration
    if err := m.saveConfig(); err != nil {
        return fmt.Errorf("failed to save config: %w", err)
    }

    // Broadcast update event
    if m.broadcaster != nil {
        event := &KingListUpdateEvent{
            BlockHeight: m.bc.GetChainHeight(),
            NewList:     newAddresses,
            Added:       common.QuantumAddress{},
            Removed:     common.QuantumAddress{},
            Timestamp:   time.Now(),
            Reason:      "manual_update",
        }
        if err := m.broadcaster.BroadcastKingListUpdate(event); err != nil {
            m.logger.Warn("failed to broadcast list update", zap.Error(err))
        }

        // NEW: Also broadcast configuration
        m.broadcastKingConfig()
    }

    if m.state.CurrentKingIndex >= len(m.config.KingAddresses) {
        m.state.CurrentKingIndex = 0
    }

    // Save state
    if err := m.saveState(); err != nil {
        return fmt.Errorf("failed to save state: %w", err)
    }

    m.logger.Info("king list updated",
        zap.Int("count", len(newAddresses)))
    return nil
}

// Add this method to broadcast configuration
func (m *RotatingKingManager) broadcastKingConfig() {
    if m.broadcaster == nil {
        return
    }

    // We need to add this method to P2P broadcaster interface
    // For now, use existing broadcast mechanism
}


func formatBalance(amount *big.Int) string {
        if amount == nil || amount.Sign() == 0 {
                return "0"
        }
        oneANTD := big.NewInt(1e18)
        whole := new(big.Int).Div(amount, oneANTD)
        remainder := new(big.Int).Mod(amount, oneANTD)
        if remainder.Sign() == 0 {
                return whole.String()
        }
        f := new(big.Float).SetInt(remainder)
        f.Quo(f, new(big.Float).SetInt(oneANTD))
        fracStr := f.Text('f', 6)
        if strings.HasPrefix(fracStr, "0.") {
                fracStr = fracStr[2:]
        }
        fracStr = strings.TrimRight(fracStr, "0")
        if fracStr == "" {
                return whole.String()
        }
        return whole.String() + "." + fracStr
}

func (m *RotatingKingManager) IsEligible(height uint64) bool {
        m.mu.RLock()
        defer m.mu.RUnlock()

        king := m.GetCurrentKing()
        if king == (common.QuantumAddress{}) {
                return false
        }

        if m.bc == nil {
                return false
        }

        stateProvider, ok := m.bc.State().(interface {
                GetBalance(common.QuantumAddress) *big.Int
        })
        if !ok {
                m.logger.Warn("state provider does not support GetBalance")
                return false
        }

        balance := stateProvider.GetBalance(king)
        return balance.Cmp(EligibilityThreshold) >= 0
}

func (m *RotatingKingManager) ForceRotateToAddress(newKing common.QuantumAddress, reason string) error {
        m.mu.Lock()
        defer m.mu.Unlock()

        // Find the index of the new king
        index := -1
        for i, addr := range m.config.KingAddresses {
                if addr == newKing {
                        index = i
                        break
                }
        }
        if index == -1 {
                return fmt.Errorf("invalid king address: %s", newKing.String())
        }

        currentHeight := m.bc.GetChainHeight()

        // Update state
        m.state.CurrentKingIndex = index
        m.state.RotationHeight = currentHeight
        m.state.NextRotationAt = currentHeight + m.config.RotationInterval
        m.state.RotationCount++
        m.state.LastUpdated = time.Now()

        // Save to database
        if err := m.saveState(); err != nil {
                m.logger.Error("failed to save state after force rotate", zap.Error(err))
                return err
        }

        m.logger.Info("forced rotation to address",
                zap.String("king", newKing.String()[:8]),
                zap.Uint64("height", currentHeight),
                zap.String("reason", reason))

        return nil
}

// CleanupIneligibleKings removes kings below minimum stake
func (m *RotatingKingManager) CleanupIneligibleKings() ([]common.QuantumAddress, error) {
    m.mu.Lock()
    defer m.mu.Unlock()

    if m.bc == nil {
        return nil, fmt.Errorf("blockchain provider not available")
    }

    // Get state provider
    stateProvider, ok := m.bc.State().(interface {
        GetBalance(common.QuantumAddress) *big.Int
    })
    if !ok {
        return nil, fmt.Errorf("state provider does not support GetBalance")
    }

    var removed []common.QuantumAddress
    var newAddresses []common.QuantumAddress

    for _, addr := range m.config.KingAddresses {
        balance := stateProvider.GetBalance(addr)

        // Check if balance is below minimum stake
        if balance.Cmp(m.config.MinStakeRequired) >= 0 {
            newAddresses = append(newAddresses, addr)
        } else {
            removed = append(removed, addr)
            m.logger.Warn("Removing ineligible king",
                zap.String("address", addr.String()[:10]),
                zap.String("balance", formatBalance(balance)),
                zap.String("required", formatBalance(m.config.MinStakeRequired)))
        }
    }

    // If nothing changed, return
    if len(removed) == 0 {
        return nil, nil
    }

    // Update the list
    m.config.KingAddresses = newAddresses

    // Adjust current king index if needed
    if len(m.config.KingAddresses) > 0 {
        if m.state.CurrentKingIndex >= len(m.config.KingAddresses) {
            m.state.CurrentKingIndex = 0
        }
    } else {
        // If all kings removed, reset to defaults
        m.logger.Warn("All kings ineligible, resetting to default")
        defaultConfig := DefaultRotatingKingConfig()
        m.config.KingAddresses = defaultConfig.KingAddresses
        m.state.CurrentKingIndex = 0
    }

    // Save changes
    if err := m.saveConfig(); err != nil {
        return nil, fmt.Errorf("failed to save config: %w", err)
    }
    if err := m.saveState(); err != nil {
        return nil, fmt.Errorf("failed to save state: %w", err)
    }

    // Broadcast the update
    if m.broadcaster != nil && len(removed) > 0 {
        event := &KingListUpdateEvent{
            BlockHeight: m.bc.GetChainHeight(),
            NewList:     m.config.KingAddresses,
            Removed:     common.QuantumAddress{}, // Can't specify multiple
            Timestamp:   time.Now(),
            Reason:      "ineligible_cleanup",
        }
        if err := m.broadcaster.BroadcastKingListUpdate(event); err != nil {
            m.logger.Warn("failed to broadcast cleanup", zap.Error(err))
        }
    }

    m.logger.Info("cleaned up ineligible kings",
        zap.Int("removed", len(removed)),
        zap.Int("remaining", len(m.config.KingAddresses)))

    return removed, nil
}

// SyncDatabaseWithPeers synchronizes rotating king database with peers
func (m *RotatingKingManager) SyncDatabaseWithPeers(ctx context.Context) error {
    if m.broadcaster == nil {
        return errors.New("no P2P broadcaster available")
    }

    m.logger.Info("Starting database sync with peers")

    // Get local sync state
    localState, err := m.GetSyncState()
    if err != nil {
        return fmt.Errorf("failed to get local sync state: %w", err)
    }

    // Create sync request
    syncRequest := &DatabaseSyncRequest{
        NodeID:           "local-node", // Should be actual node ID
        LastSyncedBlock:  localState.LastSyncedBlock,
        SyncTimestamp:    time.Now(),
        RequestType:      "full_sync",
    }

    // Broadcast sync request
    if err := m.broadcaster.BroadcastDatabaseSync(syncRequest); err != nil {
        m.logger.Warn("Failed to broadcast sync request", zap.Error(err))
    }

    // Also broadcast our current state
    if err := m.broadcastKingState(); err != nil {
        m.logger.Warn("Failed to broadcast king state", zap.Error(err))
    }

    return nil
}

// broadcastKingState broadcasts current rotating king state
func (m *RotatingKingManager) broadcastKingState() error {
    if m.broadcaster == nil {
        return errors.New("no P2P broadcaster available")
    }

    m.mu.RLock()
    defer m.mu.RUnlock()

    stateEvent := &KingStateBroadcast{
        BlockHeight:          m.bc.GetChainHeight(),
        CurrentKingIndex:     m.state.CurrentKingIndex,
        KingAddresses:        m.config.KingAddresses,
        RotationHeight:       m.state.RotationHeight,
        NextRotationAt:       m.state.NextRotationAt,
        RotationCount:        m.state.RotationCount,
        LastUpdated:          m.state.LastUpdated,
        BroadcastTimestamp:   time.Now(),
    }

    return m.broadcaster.BroadcastKingState(stateEvent)
}

func (m *RotatingKingManager) StartPeriodicSync(interval time.Duration) {
    ticker := time.NewTicker(interval)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            // Sync with blockchain
            if m.bc != nil {
                currentHeight := m.bc.GetChainHeight()
                ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
                if err := m.SyncBlocks(ctx, currentHeight); err != nil {
                    m.logger.Warn("Periodic sync failed", zap.Error(err))
                }
                cancel()
            }

            // Sync with peers
            ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
            if err := m.SyncDatabaseWithPeers(ctx); err != nil {
                m.logger.Warn("Peer sync failed", zap.Error(err))
            }
            cancel()
        }
    }
}

func (m *RotatingKingManager) checkRotationConsensus(blockHeight uint64) (bool, error) {
    // TODO: For simplicity, require at least 2/3 of peers to agree
    return true, nil // Temporary - implement proper consensus
}

func (m *RotatingKingManager) SaveKingListToDB(addresses []common.QuantumAddress, height uint64) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    // Update configuration
    m.config.KingAddresses = make([]common.QuantumAddress, len(addresses))
    copy(m.config.KingAddresses, addresses)

    // Save to database
    if err := m.saveConfig(); err != nil {
        return fmt.Errorf("failed to save config: %w", err)
    }

    // Update state if needed
    if m.state.CurrentKingIndex >= len(m.config.KingAddresses) {
        m.state.CurrentKingIndex = 0
    }

    if err := m.saveState(); err != nil {
        return fmt.Errorf("failed to save state: %w", err)
    }

    // Log the update
    m.logger.Info("king list saved to database",
        zap.Int("count", len(addresses)),
        zap.Uint64("height", height))

    return nil
}

func (m *RotatingKingManager) SaveBlockSync(record *BlockSyncRecord) error {
    return m.database.WriteBlockSyncRecord(record)
}

func (m *RotatingKingManager) GetLastSyncedBlock() (*BlockSyncRecord, error) {
    return m.database.GetLastBlockSyncRecord()
}

func (m *RotatingKingManager) SaveSyncState(state *SyncState) error {
    return m.database.WriteSyncState(state)
}

func (m *RotatingKingManager) LoadSyncState() (*SyncState, error) {
    return m.database.ReadSyncState()
}

func (m *RotatingKingManager) GetRotationEvents(fromBlock, toBlock uint64) ([]KingRotation, error) {
    return m.database.GetRotationEvents(fromBlock, toBlock)
}

func (m *RotatingKingManager) syncRotationsToHeight(ctx context.Context, targetHeight uint64) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    currentHeight := m.bc.GetChainHeight()
    if targetHeight > currentHeight {
        targetHeight = currentHeight
    }

    // Start from current rotation height
    startHeight := m.state.RotationHeight
    if startHeight == 0 {
        startHeight = 1 // Start from genesis
    }

    for height := startHeight; height <= targetHeight; height++ {
        // Check context cancellation
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
        }

        // Check if rotation should happen at this height
        if height >= m.state.NextRotationAt {
            // Perform rotation
            block := m.bc.GetBlock(height)
            if block == nil {
                continue
            }

            // Get block hash
            var blockHash common.Hash
            switch b := block.(type) {
            case interface{ Hash() common.Hash }:
                blockHash = b.Hash()
            default:
                continue
            }

            // Perform rotation
            if err := m.rotateToNextKingInternal(height, blockHash); err != nil {
                m.logger.Warn("failed to rotate at height", zap.Uint64("height", height), zap.Error(err))
            }
        }
    }

    return nil
}


func (m *RotatingKingManager) RotateToNextKing(blockHeight uint64, blockHash common.Hash) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    if len(m.config.KingAddresses) == 0 {
        return fmt.Errorf("no king addresses configured")
    }

    if blockHeight < m.state.NextRotationAt {
        return fmt.Errorf("rotation not due yet (next at %d)", m.state.NextRotationAt)
    }

    // Check if other nodes agree on rotation
    if m.broadcaster != nil {
        consensus, err := m.checkRotationConsensus(blockHeight)
        if err != nil {
            return fmt.Errorf("consensus check failed: %w", err)
        }

        if !consensus {
            m.logger.Warn("Rotation consensus not reached, waiting...")
            return fmt.Errorf("rotation consensus not reached")
        }
    }

    // First, cleanup ineligible kings before rotation
    if m.bc != nil {
        stateProvider, ok := m.bc.State().(interface {
            GetBalance(common.QuantumAddress) *big.Int
        })
        if ok {
            // Check if next king is eligible
            nextIndex := (m.state.CurrentKingIndex + 1) % len(m.config.KingAddresses)
            nextKing := m.config.KingAddresses[nextIndex]
            balance := stateProvider.GetBalance(nextKing)

            if balance.Cmp(m.config.MinStakeRequired) < 0 {
                // Skip to next eligible king
                m.logger.Warn("Next king ineligible, searching for eligible king",
                    zap.String("address", nextKing.String()[:10]),
                    zap.String("balance", formatBalance(balance)))

                // Find next eligible king
                for i := 1; i < len(m.config.KingAddresses); i++ {
                    candidateIndex := (m.state.CurrentKingIndex + i) % len(m.config.KingAddresses)
                    candidate := m.config.KingAddresses[candidateIndex]
                    candidateBalance := stateProvider.GetBalance(candidate)

                    if candidateBalance.Cmp(m.config.MinStakeRequired) >= 0 {
                        nextIndex = candidateIndex
                        nextKing = candidate
                        m.logger.Info("Found eligible king",
                            zap.String("address", nextKing.String()[:10]),
                            zap.String("balance", formatBalance(candidateBalance)))
                        break
                    }
                }
            }
        }
    }

    previousKing := m.config.KingAddresses[m.state.CurrentKingIndex]
    newIndex := (m.state.CurrentKingIndex + 1) % len(m.config.KingAddresses)
    newKing := m.config.KingAddresses[newIndex]

    isEligible := false
    eligibilityBalance := big.NewInt(0)
    if m.bc != nil {
        stateProvider, ok := m.bc.State().(interface {
            GetBalance(common.QuantumAddress) *big.Int
        })
        if ok && stateProvider != nil {
            eligibilityBalance = stateProvider.GetBalance(newKing)
            if eligibilityBalance.Cmp(m.config.MinStakeRequired) >= 0 {
                isEligible = true
            }
        }
    }

    rotation := KingRotation{
        BlockHeight:  blockHeight,
        PreviousKing: previousKing,
        NewKing:      newKing,
        Timestamp:    time.Now(),
        Reward:       big.NewInt(0),
        WasEligible:  isEligible,
    }

    m.state.KingsHistory = append(m.state.KingsHistory, rotation)
    if len(m.state.KingsHistory) > 100 {
        m.state.KingsHistory = m.state.KingsHistory[1:]
    }

    m.state.CurrentKingIndex = newIndex
    m.state.RotationHeight = blockHeight
    m.state.NextRotationAt = blockHeight + m.config.RotationInterval
    m.state.RotationCount++
    m.state.LastUpdated = time.Now()

    // Save rotation event to database
    if err := m.database.WriteRotationEvent(&rotation); err != nil {
        m.logger.Warn("failed to save rotation event", zap.Error(err))
    }

    // Save state to database
    if err := m.saveState(); err != nil {
        m.logger.Error("failed to save state", zap.Error(err))
        return err
    }

    // Save block sync record
    record := &BlockSyncRecord{
        BlockHeight:    blockHeight,
        BlockHash:      blockHash,
        Timestamp:      time.Now(),
        RotationEvents: []string{fmt.Sprintf("rotation:%s->%s", previousKing.String(), newKing.String())},
        SyncDuration:   0,
    }
    
    if err := m.database.WriteBlockSyncRecord(record); err != nil {
        m.logger.Warn("failed to save block sync", zap.Error(err))
    }

    if isEligible {
        m.logger.Info("king rotation - eligible",
            zap.String("newKing", newKing.String()),
            zap.String("balance", formatBalance(eligibilityBalance)))
    } else {
        m.logger.Warn("king rotation - ineligible",
            zap.String("newKing", newKing.String()),
            zap.String("balance", formatBalance(eligibilityBalance)),
            zap.String("required", formatBalance(m.config.MinStakeRequired)))
    }

    // Broadcast the rotation to all peers
    if m.broadcaster != nil {
        rotationEvent := &KingRotationBroadcast{
            BlockHeight:  blockHeight,
            BlockHash:    blockHash,
            PreviousKing: previousKing,
            NewKing:      newKing,
            Timestamp:    time.Now(),
        }

        if err := m.broadcaster.BroadcastRotation(rotationEvent); err != nil {
            m.logger.Warn("Failed to broadcast rotation", zap.Error(err))
        }
    }

    return nil
}
/*
func NewRotatingKingManagerWithDB(statePath string, bc BlockchainProvider, isMainKing bool, broadcaster P2PBroadcaster, chainDb interface{}) (*RotatingKingManager, error) {
    logger, err := zap.NewProduction()
    if err != nil {
        return nil, fmt.Errorf("failed to create logger: %w", err)
    }

    // Try to load existing state and config from ChainDB
    config := DefaultRotatingKingConfig()
    state := RotatingKingState{
        CurrentKingIndex:         0,
        RotationHeight:           0,
        NextRotationAt:           config.RotationInterval,
        LastUpdated:              time.Now(),
        RotationCount:            0,
        KingsHistory:             make([]KingRotation, 0),
        TotalRewardsDistributed:  big.NewInt(0),
        KingRewards:              make(map[common.QuantumAddress]*big.Int),
    }

    // Load from ChainDB if available
    if loadedConfig, err := chainDb.ReadRotatingKingConfig(); err == nil && loadedConfig != nil {
        config = *loadedConfig
        logger.Info("loaded rotating king config from ChainDB")
    }

    if loadedState, err := chainDb.ReadRotatingKingState(); err == nil && loadedState != nil {
        state = *loadedState
        logger.Info("loaded rotating king state from ChainDB")
    }

    m := &RotatingKingManager{
        config:      config,
        state:       state,
        statePath:   statePath,
        bc:          bc,
        isMainKing:  isMainKing,
        broadcaster: broadcaster,
        chainDb:     chainDb,
        logger:      logger,
    }

    // Save initial state to ChainDB
    if err := m.saveState(); err != nil {
        logger.Warn("failed to save initial state to ChainDB", zap.Error(err))
    }

    if err := m.saveConfig(); err != nil {
        logger.Warn("failed to save initial config to ChainDB", zap.Error(err))
    }

    logger.Info("rotating king manager initialized with shared ChainDB")
    return m, nil
}*/

func (m *RotatingKingManager) rotateToNextKingInternal(blockHeight uint64, blockHash common.Hash) error {
    if len(m.config.KingAddresses) == 0 {
        return fmt.Errorf("no king addresses configured")
    }

    previousKing := m.config.KingAddresses[m.state.CurrentKingIndex]
    newIndex := (m.state.CurrentKingIndex + 1) % len(m.config.KingAddresses)
    newKing := m.config.KingAddresses[newIndex]

    // Check eligibility
    isEligible := false
    if m.bc != nil {
        stateProvider, ok := m.bc.State().(interface {
            GetBalance(common.QuantumAddress) *big.Int
        })
        if ok && stateProvider != nil {
            balance := stateProvider.GetBalance(newKing)
            if balance.Cmp(m.config.MinStakeRequired) >= 0 {
                isEligible = true
            }
        }
    }

    rotation := KingRotation{
        BlockHeight:  blockHeight,
        PreviousKing: previousKing,
        NewKing:      newKing,
        Timestamp:    time.Now(),
        Reward:       big.NewInt(0),
        WasEligible:  isEligible,
    }

    // FIXED: Use m.database instead of m.chainDb
    // Save to database
    if err := m.database.WriteRotationEvent(&rotation); err != nil {
        m.logger.Warn("failed to save rotation event to database", zap.Error(err))
    }

    m.state.KingsHistory = append(m.state.KingsHistory, rotation)
    if len(m.state.KingsHistory) > 100 {
        m.state.KingsHistory = m.state.KingsHistory[1:]
    }

    m.state.CurrentKingIndex = newIndex
    m.state.RotationHeight = blockHeight
    m.state.NextRotationAt = blockHeight + m.config.RotationInterval
    m.state.RotationCount++
    m.state.LastUpdated = time.Now()

    // Save state to database
    if err := m.saveState(); err != nil {
        return fmt.Errorf("failed to save state to database: %w", err)
    }

    // Save block sync record
    record := &BlockSyncRecord{
        BlockHeight:    blockHeight,
        BlockHash:      blockHash,
        Timestamp:      time.Now(),
        RotationEvents: []string{fmt.Sprintf("rotation:%s->%s", previousKing.String(), newKing.String())},
        SyncDuration:   0,
    }
    
    if err := m.database.WriteBlockSyncRecord(record); err != nil {
        m.logger.Warn("failed to save block sync to database", zap.Error(err))
    }

    return nil
}

func (m *RotatingKingManager) SaveRotationEvent(rotation *KingRotation) error {
    return m.database.WriteRotationEvent(rotation)
}
