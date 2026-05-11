// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package p2p
import (
	"encoding/json"
    "strings"
	"fmt"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/sirupsen/logrus"
	"github.com/antdaza/antdchain/antdc/block"
        "github.com/antdaza/antdchain/common"
	"github.com/antdaza/antdchain/antdc/checkpoints"
	"github.com/antdaza/antdchain/antdc/rotatingking"
	"github.com/antdaza/antdchain/antdc/tx"
)

// BanManager handles detection and banning of misbehaving peers with checkpoint verification
type BanManager struct {
	node          *Node
	logger        *logrus.Logger
	checkpoints   *checkpoints.Checkpoints // Added checkpoint system
	mu            sync.RWMutex
	
	// Banned peers
	bannedPeers      map[peer.ID]BanRecord
	bannedAddresses  map[string]BanRecord
	bannedHashes     map[common.Hash]time.Time
	
	// Violation tracking
	violations       map[peer.ID][]Violation
	violationCounter map[peer.ID]int
	
	// Rate limiting
	lastMessages     map[peer.ID][]time.Time
	blockTimestamps  map[peer.ID][]time.Time
	
	// Checkpoint violation tracking
	checkpointViolations map[peer.ID][]CheckpointViolation
	
	// Config
	config           BanConfig
}

// BanConfig configuration for the ban manager
type BanConfig struct {
	MaxViolations          int           // Max violations before ban
	BanDuration            time.Duration // Duration of ban
	RateLimitMessages      int           // Max messages per second
	RateLimitBlocks        int           // Max blocks per minute
	CheckInterval          time.Duration // How often to check for violations
	EnableAutoBan          bool          // Enable automatic banning
	BanMalformedBlocks     bool          // Ban peers sending malformed blocks
	BanInvalidTxs          bool          // Ban peers sending invalid transactions
	BanSpam                bool          // Ban spamming peers
	BanDuplicateBroadcast  bool          // Ban duplicate broadcasters
	BanOutOfOrderBlocks    bool          // Ban peers sending blocks out of order
	BanCheckpointViolators bool          // Ban peers violating checkpoints
	BanHistoricalRewrite   bool          // Ban peers trying to rewrite history
	CheckpointWeight       int           // Weight for checkpoint violations (higher = more severe)
	MaxCheckpointViolations int          // Max checkpoint violations before ban
}

// DefaultBanConfig returns default ban configuration
func DefaultBanConfig() BanConfig {
	return BanConfig{
		MaxViolations:          5,
		BanDuration:            1 * time.Hour,
		RateLimitMessages:      50,    // 50 messages/sec
		RateLimitBlocks:        10,    // 10 blocks/min
		CheckInterval:          30 * time.Second,
		EnableAutoBan:          true,
		BanMalformedBlocks:     true,
		BanInvalidTxs:          true,
		BanSpam:                true,
		BanDuplicateBroadcast:  true,
		BanOutOfOrderBlocks:    true,
		BanCheckpointViolators: true,  // Enable checkpoint checking
		BanHistoricalRewrite:   true,  // Ban attempts to rewrite history
		CheckpointWeight:       10,    // Checkpoint violations are severe
		MaxCheckpointViolations: 1,    // Zero tolerance for checkpoint violations
	}
}

// BanRecord contains information about a banned peer
type BanRecord struct {
	PeerID      peer.ID
	Address     string
	Reason      string
	BannedAt    time.Time
	ExpiresAt   time.Time
	Violations  []Violation
	CheckpointViolations []CheckpointViolation
	BanCount    int
}

// Violation represents a rule violation by a peer
type Violation struct {
	Type      string
	Severity  int // 1-10, 10 being most severe
	Timestamp time.Time
	Details   string
	Data      interface{}
}

// CheckpointViolation represents a checkpoint-specific violation
type CheckpointViolation struct {
	Type       string
	Height     uint64
	Expected   common.Hash
	Received   common.Hash
	Checkpoint *checkpoints.Checkpoint
	Timestamp  time.Time
	Severity   int // Always high for checkpoint violations
}

// NewBanManager creates a new ban manager with checkpoint support
func NewBanManager(node *Node, config BanConfig, cp *checkpoints.Checkpoints) *BanManager {
	return &BanManager{
		node:                 node,
		logger:               node.logger,
		checkpoints:          cp,
		bannedPeers:          make(map[peer.ID]BanRecord),
		bannedAddresses:      make(map[string]BanRecord),
		bannedHashes:         make(map[common.Hash]time.Time),
		violations:           make(map[peer.ID][]Violation),
		violationCounter:     make(map[peer.ID]int),
		lastMessages:         make(map[peer.ID][]time.Time),
		blockTimestamps:      make(map[peer.ID][]time.Time),
		checkpointViolations: make(map[peer.ID][]CheckpointViolation),
		config:               config,
	}
}

// CheckMessageWithCheckpoints checks a message for violations including checkpoint verification
func (bm *BanManager) CheckMessageWithCheckpoints(msg *pubsub.Message, msgType byte) (bool, *Violation, *CheckpointViolation) {
	if msg == nil {
		return false, nil, nil
	}
	
	peerID := msg.GetFrom()
	
	// Check if peer is already banned
	if bm.IsBanned(peerID) {
		return false, nil, nil // Already banned, ignore
	}
	
	// Check rate limiting
	if violation := bm.checkRateLimit(peerID); violation != nil {
		return true, violation, nil
	}
	
	// Check message content based on type with checkpoint verification
	switch msgType {
	case msgTypeBlock:
		shouldBan, violation, cpViolation := bm.checkBlockMessageWithCheckpoints(peerID, msg.Data)
		return shouldBan, violation, cpViolation
	case msgTypeTx:
		shouldBan, violation := bm.checkTxMessage(peerID, msg.Data)
		return shouldBan, violation, nil
	case msgTypeKingRotation:
		shouldBan, violation := bm.checkKingMessage(peerID, msg.Data)
		return shouldBan, violation, nil
	case msgTypeKingListUpdate:
		shouldBan, violation := bm.checkKingListMessage(peerID, msg.Data)
		return shouldBan, violation, nil
	}
	
	return false, nil, nil
}

// checkBlockMessageWithCheckpoints checks block messages with checkpoint verification
func (bm *BanManager) checkBlockMessageWithCheckpoints(peerID peer.ID, data []byte) (bool, *Violation, *CheckpointViolation) {
	if len(data) < 2 {
		return true, &Violation{
			Type:      "MALFORMED_BLOCK",
			Severity:  8,
			Timestamp: time.Now(),
			Details:   "Block message too short",
			Data:      len(data),
		}, nil
	}
	
	// Parse block
	var blk block.Block
	if err := json.Unmarshal(data[1:], &blk); err != nil {
		return true, &Violation{
			Type:      "MALFORMED_BLOCK",
			Severity:  9,
			Timestamp: time.Now(),
			Details:   "Failed to parse block: " + err.Error(),
			Data:      err.Error(),
		}, nil
	}
	
	// Check block validity
	if blk.Header == nil {
		return true, &Violation{
			Type:      "INVALID_BLOCK",
			Severity:  10,
			Timestamp: time.Now(),
			Details:   "Block has nil header",
		}, nil
	}
	
	height := blk.Header.Number.Uint64()
	blockHash := blk.Hash()
	
	// ========== CHECKPOINT VERIFICATION ==========
	if bm.config.BanCheckpointViolators && bm.checkpoints != nil {
		// Check against known checkpoints
		if checkpointViolation := bm.verifyBlockAgainstCheckpoints(height, blockHash, &blk); checkpointViolation != nil {
			return true, nil, checkpointViolation
		}
	}
	
	// ========== HISTORICAL REWRITE DETECTION ==========
	if bm.config.BanHistoricalRewrite {
		// Check if peer is trying to rewrite old blocks
		currentHeight := bm.node.currentHeight()
		if height < currentHeight-100 {
			// Peer is sending very old blocks - potential rewrite attempt
			return true, &Violation{
				Type:      "HISTORICAL_REWRITE_ATTEMPT",
				Severity:  9,
				Timestamp: time.Now(),
				Details:   fmt.Sprintf("Attempting to rewrite history: height %d (current: %d)", height, currentHeight),
				Data: map[string]interface{}{
					"blockHeight": height,
					"currentHeight": currentHeight,
					"difference": currentHeight - height,
				},
			}, nil
		}
	}
	
	// Check block height
	currentHeight := bm.node.currentHeight()
	if height > currentHeight+100 {
		// Peer is sending blocks far in the future
		return true, &Violation{
			Type:      "FUTURE_BLOCK",
			Severity:  6,
			Timestamp: time.Now(),
			Details:   fmt.Sprintf("Block height %d is too far in future (current: %d)", 
				height, currentHeight),
			Data: map[string]interface{}{
				"blockHeight": height,
				"currentHeight": currentHeight,
			},
		}, nil
	}
	
	// Check duplicate block hash
	if _, exists := bm.bannedHashes[blockHash]; exists {
		return true, &Violation{
			Type:      "DUPLICATE_BLOCK",
			Severity:  5,
			Timestamp: time.Now(),
			Details:   "Duplicate block broadcast",
			Data:      blockHash.Hex(),
		}, nil
	}
	
	// Track block timestamp for rate limiting
	bm.mu.Lock()
	timestamps, exists := bm.blockTimestamps[peerID]
	if !exists {
		timestamps = make([]time.Time, 0)
	}
	
	now := time.Now()
	// Clean timestamps older than 1 minute
	cleanTimestamps := make([]time.Time, 0)
	for _, ts := range timestamps {
		if now.Sub(ts) <= time.Minute {
			cleanTimestamps = append(cleanTimestamps, ts)
		}
	}
	
	cleanTimestamps = append(cleanTimestamps, now)
	bm.blockTimestamps[peerID] = cleanTimestamps
	
	// Check block rate limit
	if len(cleanTimestamps) > bm.config.RateLimitBlocks {
		bm.mu.Unlock()
		return true, &Violation{
			Type:      "BLOCK_SPAM",
			Severity:  8,
			Timestamp: now,
			Details:   fmt.Sprintf("Exceeded block rate limit: %d blocks/min", len(cleanTimestamps)),
			Data:      len(cleanTimestamps),
		}, nil
	}
	bm.mu.Unlock()
	
	return false, nil, nil
}

// verifyBlockAgainstCheckpoints verifies a block against known checkpoints
func (bm *BanManager) verifyBlockAgainstCheckpoints(height uint64, blockHash common.Hash, blk *block.Block) *CheckpointViolation {
	if bm.checkpoints == nil {
		return nil
	}
	
	// Check if we have a checkpoint for this height
	cp, exists := bm.checkpoints.GetCheckpoint(height)
	if !exists {
		// No checkpoint for this height, can't verify
		return nil
	}
	
	// Verify block hash against checkpoint
	if err := bm.checkpoints.ValidateBlock(height, blockHash); err != nil {
		// Checkpoint violation!
		violation := &CheckpointViolation{
			Type:       "CHECKPOINT_HASH_MISMATCH",
			Height:     height,
			Expected:   cp.Hash,
			Received:   blockHash,
			Checkpoint: cp,
			Timestamp:  time.Now(),
			Severity:   bm.config.CheckpointWeight, // Use configurable weight
		}
		
		bm.logger.WithFields(logrus.Fields{
			"peer":       bm.node.host.ID().String()[:8],
			"height":     height,
			"expected":   cp.Hash.Hex()[:12],
			"received":   blockHash.Hex()[:12],
			"checkpoint": cp.Source,
			"signatures": len(cp.Signatures),
		}).Error("🚨 CHECKPOINT VIOLATION DETECTED!")
		
		return violation
	}
	
	// Verify additional context if available in checkpoint
	if blk != nil && blk.Header != nil {
		// Try to extract miner and rotating king from block
		var miner, rotatingKing common.QuantumAddress
		
		if len(blk.Txs) > 0 {
			// Could extract from coinbase transaction
		}
		
		// Validate context
		if err := bm.checkpoints.ValidateBlockWithContext(
			height, blockHash, blk.Header.ParentHash,
			miner, rotatingKing,
		); err != nil {
			bm.logger.WithFields(logrus.Fields{
				"height": height,
				"error":  err.Error(),
			}).Warn("Checkpoint context validation warning")
		}
	}
	
	return nil
}

// checkRateLimit checks if peer is sending messages too fast
func (bm *BanManager) checkRateLimit(peerID peer.ID) *Violation {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	
	now := time.Now()
	
	// Track message timestamps
	timestamps, exists := bm.lastMessages[peerID]
	if !exists {
		timestamps = make([]time.Time, 0)
	}
	
	// Clean old timestamps (older than 1 second)
	cleanTimestamps := make([]time.Time, 0)
	for _, ts := range timestamps {
		if now.Sub(ts) <= time.Second {
			cleanTimestamps = append(cleanTimestamps, ts)
		}
	}
	
	// Add current timestamp
	cleanTimestamps = append(cleanTimestamps, now)
	bm.lastMessages[peerID] = cleanTimestamps
	
	// Check rate limit
	if len(cleanTimestamps) > bm.config.RateLimitMessages {
		return &Violation{
			Type:      "RATE_LIMIT_EXCEEDED",
			Severity:  7,
			Timestamp: now,
			Details:   fmt.Sprintf("Exceeded rate limit: %d messages/sec", len(cleanTimestamps)),
			Data:      len(cleanTimestamps),
		}
	}
	
	return nil
}

// checkTxMessage checks transaction messages for violations
func (bm *BanManager) checkTxMessage(peerID peer.ID, data []byte) (bool, *Violation) {
	if len(data) < 2 {
		return true, &Violation{
			Type:      "MALFORMED_TX",
			Severity:  8,
			Timestamp: time.Now(),
			Details:   "Transaction message too short",
			Data:      len(data),
		}
	}
	
	// Parse transaction
	var txObj tx.Tx
	if err := json.Unmarshal(data[1:], &txObj); err != nil {
		return true, &Violation{
			Type:      "MALFORMED_TX",
			Severity:  9,
			Timestamp: time.Now(),
			Details:   "Failed to parse transaction: " + err.Error(),
			Data:      err.Error(),
		}
	}
	
	// Validate transaction
	if err := txObj.Validate(); err != nil {
		return true, &Violation{
			Type:      "INVALID_TX",
			Severity:  9,
			Timestamp: time.Now(),
			Details:   "Invalid transaction: " + err.Error(),
			Data:      err.Error(),
		}
	}
	
	return false, nil
}

// checkKingMessage checks king rotation messages
func (bm *BanManager) checkKingMessage(peerID peer.ID, data []byte) (bool, *Violation) {
	if len(data) < 2 {
		return true, &Violation{
			Type:      "MALFORMED_KING_MSG",
			Severity:  7,
			Timestamp: time.Now(),
			Details:   "King message too short",
		}
	}
	
	// Check if peer is authorized to send king messages
	currentHeight := bm.node.currentHeight()
	
	var msgData map[string]interface{}
	if err := json.Unmarshal(data[1:], &msgData); err != nil {
		return true, &Violation{
			Type:      "MALFORMED_KING_MSG",
			Severity:  8,
			Timestamp: time.Now(),
			Details:   "Failed to parse king message",
		}
	}
	
	// Check if message height is reasonable
	if height, ok := msgData["blockHeight"].(float64); ok {
		if uint64(height) > currentHeight+10 {
			return true, &Violation{
				Type:      "FUTURE_KING_ROTATION",
				Severity:  6,
				Timestamp: time.Now(),
				Details:   fmt.Sprintf("King rotation too far in future: %d", uint64(height)),
			}
		}
	}
	
	return false, nil
}

// checkKingListMessage checks king list update messages
func (bm *BanManager) checkKingListMessage(peerID peer.ID, data []byte) (bool, *Violation) {
	if len(data) < 2 {
		return true, &Violation{
			Type:      "MALFORMED_KING_LIST",
			Severity:  8,
			Timestamp: time.Now(),
			Details:   "King list message too short",
		}
	}
	
	var event rotatingking.KingListUpdateEvent
	if err := json.Unmarshal(data[1:], &event); err != nil {
		return true, &Violation{
			Type:      "MALFORMED_KING_LIST",
			Severity:  9,
			Timestamp: time.Now(),
			Details:   "Failed to parse king list",
		}
	}
	
	// Check if king list is reasonable
	if len(event.NewList) == 0 {
		return true, &Violation{
			Type:      "EMPTY_KING_LIST",
			Severity:  8,
			Timestamp: time.Now(),
			Details:   "Empty king list received",
		}
	}
	
	if len(event.NewList) > 100 {
		return true, &Violation{
			Type:      "EXCESSIVE_KING_LIST",
			Severity:  7,
			Timestamp: time.Now(),
			Details:   fmt.Sprintf("King list too large: %d addresses", len(event.NewList)),
		}
	}
	
	return false, nil
}

// ========== ENHANCED BAN MANAGEMENT WITH CHECKPOINTS ==========

// RecordViolation records a violation by a peer
func (bm *BanManager) RecordViolation(peerID peer.ID, violation *Violation, cpViolation *CheckpointViolation) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	
	if violation != nil {
		// Add to violations list
		bm.violations[peerID] = append(bm.violations[peerID], *violation)
		bm.violationCounter[peerID]++
		
		bm.logger.Warnf("🚨 VIOLATION DETECTED: Peer %s - %s (Severity: %d)",
			peerID.String()[:8], violation.Type, violation.Severity)
	}
	
	if cpViolation != nil {
		// Record checkpoint violation
		bm.checkpointViolations[peerID] = append(bm.checkpointViolations[peerID], *cpViolation)
		
		bm.logger.Errorf("🚨🚨 CHECKPOINT VIOLATION: Peer %s - %s at height %d (Expected: %s, Got: %s)",
			peerID.String()[:8], cpViolation.Type, cpViolation.Height,
			cpViolation.Expected.Hex()[:12], cpViolation.Received.Hex()[:12])
		
		// Checkpoint violations are severe - consider immediate ban
		if bm.config.BanCheckpointViolators {
			// Zero tolerance for checkpoint violations
			if len(bm.checkpointViolations[peerID]) >= bm.config.MaxCheckpointViolations {
				bm.BanPeer(peerID, cpViolation.Type, 
					fmt.Sprintf("Checkpoint violation at height %d: expected %s, got %s",
						cpViolation.Height, cpViolation.Expected.Hex()[:12], cpViolation.Received.Hex()[:12]))
				return
			}
		}
	}
	
	// Check if should ban based on regular violations
	if bm.shouldBanPeer(peerID) {
		reason := "multiple_violations"
		if violation != nil {
			reason = violation.Type
		}
		bm.BanPeer(peerID, reason, "")
	}
}

// shouldBanPeer determines if a peer should be banned
func (bm *BanManager) shouldBanPeer(peerID peer.ID) bool {
	if !bm.config.EnableAutoBan {
		return false
	}
	
	// Check for checkpoint violations first (most severe)
	if len(bm.checkpointViolations[peerID]) > 0 {
		return true
	}
	
	// Check violation count
	if bm.violationCounter[peerID] >= bm.config.MaxViolations {
		return true
	}
	
	// Check for high-severity violations
	violations, exists := bm.violations[peerID]
	if !exists {
		return false
	}
	
	// Ban immediately for critical violations
	for _, v := range violations {
		if v.Severity >= 9 {
			return true
		}
	}
	
	return false
}

// BanPeer bans a peer from the network
func (bm *BanManager) BanPeer(peerID peer.ID, reason, details string) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	
	// Check if already banned
	if record, exists := bm.bannedPeers[peerID]; exists {
		// Update existing ban
		record.BanCount++
		record.ExpiresAt = time.Now().Add(bm.config.BanDuration * time.Duration(record.BanCount))
		record.Reason = reason
		bm.bannedPeers[peerID] = record
		
		bm.logger.Warnf("🔄 Extended ban for peer %s (count: %d, reason: %s)",
			peerID.String()[:8], record.BanCount, reason)
		return
	}
	
	// Create new ban record
	record := BanRecord{
		PeerID:     peerID,
		Reason:     reason,
		BannedAt:   time.Now(),
		ExpiresAt:  time.Now().Add(bm.config.BanDuration),
		Violations: bm.violations[peerID],
		CheckpointViolations: bm.checkpointViolations[peerID],
		BanCount:   1,
	}
	
	bm.bannedPeers[peerID] = record
	bm.violations[peerID] = nil
	bm.violationCounter[peerID] = 0
	bm.checkpointViolations[peerID] = nil
	
	// Disconnect from the peer
	if bm.node != nil && bm.node.host != nil {
		go func() {
			bm.node.host.Network().ClosePeer(peerID)
			bm.logger.Warnf("🔌 Disconnected from banned peer %s", peerID.String()[:8])
		}()
	}
	
	// Log the ban
	bm.logger.Warnf("🚫 BANNED PEER: %s | Reason: %s | Details: %s | Expires: %s",
		peerID.String()[:8], reason, details, record.ExpiresAt.Format("15:04:05"))
	
	// Broadcast ban to other peers (optional)
	go bm.broadcastBanNotification(peerID, reason)
	
	// If this is a checkpoint violation, also report to checkpoints system
	if strings.Contains(reason, "CHECKPOINT") && bm.checkpoints != nil {
		bm.reportCheckpointViolation(peerID, reason, details)
	}
}

// reportCheckpointViolation reports a checkpoint violation for network-wide awareness
func (bm *BanManager) reportCheckpointViolation(peerID peer.ID, reason, details string) {
	bm.logger.WithFields(logrus.Fields{
		"peer":    peerID.String(),
		"reason":  reason,
		"details": details,
		"node":    bm.node.host.ID().String()[:8],
	}).Error("CHECKPOINT VIOLATION REPORTED")
}

// ValidateChainHistory validates a peer's chain against checkpoints
func (bm *BanManager) ValidateChainHistory(peerID peer.ID, heights []uint64, hashes []common.Hash) (bool, *CheckpointViolation) {
	if bm.checkpoints == nil || len(heights) != len(hashes) {
		return true, nil
	}
	
	// Check each height against checkpoints
	for i, height := range heights {
		hash := hashes[i]
		
		// Check if we have a checkpoint for this height
		cp, exists := bm.checkpoints.GetCheckpoint(height)
		if !exists {
			continue
		}
		
		// Verify hash matches checkpoint
		if cp.Hash != hash {
			violation := &CheckpointViolation{
				Type:       "CHAIN_HISTORY_MISMATCH",
				Height:     height,
				Expected:   cp.Hash,
				Received:   hash,
				Checkpoint: cp,
				Timestamp:  time.Now(),
				Severity:   bm.config.CheckpointWeight,
			}
			
			return false, violation
		}
	}
	
	return true, nil
}

// GetCheckpointProtectedHeights returns heights protected by checkpoints
func (bm *BanManager) GetCheckpointProtectedHeights() []uint64 {
	if bm.checkpoints == nil {
		return []uint64{}
	}
	
	// This would require exposing checkpoint heights from the checkpoints package
	// For now, return empty
	return []uint64{}
}

// ========== BAN ENFORCEMENT ==========

// Start starts the ban manager
func (bm *BanManager) Start() {
	bm.logger.Info("🛡️  Starting P2P Ban Manager with Checkpoint Protection")
	
	if bm.checkpoints != nil {
		stats := bm.checkpoints.GetStats()
		bm.logger.WithFields(logrus.Fields{
			"totalCheckpoints": stats["totalCheckpoints"],
			"checkpointAuth":   stats["authority"],
			"enforcement":      bm.config.BanCheckpointViolators,
		}).Info("Checkpoint protection enabled")
	}
	
	// Start cleanup goroutine
	go bm.cleanupExpiredBans()
	
	// Start monitoring
	go bm.monitorPeerBehavior()
}

// Stop stops the ban manager
func (bm *BanManager) Stop() {
	bm.logger.Info("🛑 Stopping P2P Ban Manager")
}

// cleanupExpiredBans periodically cleans up expired bans
func (bm *BanManager) cleanupExpiredBans() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	
	for {
		select {
		case <-bm.node.ctx.Done():
			return
		case <-ticker.C:
			bm.mu.Lock()
			now := time.Now()
			expiredCount := 0
			
			for peerID, record := range bm.bannedPeers {
				if now.After(record.ExpiresAt) {
					delete(bm.bannedPeers, peerID)
					delete(bm.violations, peerID)
					delete(bm.violationCounter, peerID)
					delete(bm.checkpointViolations, peerID)
					expiredCount++
				}
			}
			
			bm.mu.Unlock()
			
			if expiredCount > 0 {
				bm.logger.Debugf("Cleaned up %d expired bans", expiredCount)
			}
		}
	}
}

// monitorPeerBehavior monitors ongoing peer behavior
func (bm *BanManager) monitorPeerBehavior() {
	ticker := time.NewTicker(bm.config.CheckInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-bm.node.ctx.Done():
			return
		case <-ticker.C:
			bm.checkPeerConnections()
		}
	}
}

// checkPeerConnections checks all connected peers for suspicious behavior
func (bm *BanManager) checkPeerConnections() {
	if bm.node == nil || bm.node.host == nil {
		return
	}
	
	peers := bm.node.host.Network().Peers()
	for _, peerID := range peers {
		// Check if peer is sending too many messages
		bm.mu.RLock()
		timestamps, exists := bm.lastMessages[peerID]
		bm.mu.RUnlock()
		
		if exists && len(timestamps) > bm.config.RateLimitMessages*2 {
			bm.RecordViolation(peerID, &Violation{
				Type:      "EXCESSIVE_MESSAGES",
				Severity:  6,
				Timestamp: time.Now(),
				Details:   fmt.Sprintf("Sustained high message rate: %d messages", len(timestamps)),
			}, nil)
		}
		
		// Check for connection flooding
		conns := bm.node.host.Network().ConnsToPeer(peerID)
		if len(conns) > 3 {
			bm.RecordViolation(peerID, &Violation{
				Type:      "CONNECTION_FLOOD",
				Severity:  7,
				Timestamp: time.Now(),
				Details:   fmt.Sprintf("Too many connections: %d", len(conns)),
			}, nil)
		}
	}
}

// broadcastBanNotification broadcasts ban to other peers
func (bm *BanManager) broadcastBanNotification(peerID peer.ID, reason string) {
	if bm.node == nil || bm.node.topic == nil {
		return
	}
	
	banMsg := map[string]interface{}{
		"type":       "peer_ban",
		"peerId":     peerID.String(),
		"reason":     reason,
		"timestamp":  time.Now().Unix(),
		"bannedBy":   bm.node.host.ID().String(),
		"expiresAt":  time.Now().Add(bm.config.BanDuration).Unix(),
	}
	
	data, err := json.Marshal(banMsg)
	if err != nil {
		bm.logger.Warnf("Failed to marshal ban notification: %v", err)
		return
	}
	
	// Create message with custom type (using block type as placeholder)
	msg := make([]byte, 1+len(data))
	msg[0] = msgTypeBlock // Reusing block type, could add dedicated ban type
	copy(msg[1:], data)
	
	if err := bm.node.topic.Publish(bm.node.ctx, msg); err != nil {
		bm.logger.Debugf("Failed to broadcast ban notification: %v", err)
	} else {
		bm.logger.Debugf("Broadcast ban notification for peer %s", peerID.String()[:8])
	}
}

// ========== PUBLIC INTERFACE ==========
// IsBanned checks if a peer is currently banned
func (bm *BanManager) IsBanned(peerID peer.ID) bool {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	record, exists := bm.bannedPeers[peerID]
	if !exists {
		return false
	}
	
	// Check if ban has expired
	if time.Now().After(record.ExpiresAt) {
		// Ban expired, remove it
		delete(bm.bannedPeers, peerID)
		delete(bm.violations, peerID)
		delete(bm.violationCounter, peerID)
		delete(bm.checkpointViolations, peerID)
		bm.logger.Debugf("Ban expired for peer %s", peerID.String()[:8])
		return false
	}
	
	return true
}

// GetBanStatus returns ban status for a peer
func (bm *BanManager) GetBanStatus(peerID peer.ID) (BanRecord, bool) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	record, exists := bm.bannedPeers[peerID]
	return record, exists
}

// GetBannedPeers returns list of currently banned peers
func (bm *BanManager) GetBannedPeers() []BanRecord {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	records := make([]BanRecord, 0, len(bm.bannedPeers))
	for _, record := range bm.bannedPeers {
		records = append(records, record)
	}
	return records
}

// UnbanPeer removes a ban from a peer
func (bm *BanManager) UnbanPeer(peerID peer.ID) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	
	if _, exists := bm.bannedPeers[peerID]; exists {
		delete(bm.bannedPeers, peerID)
		delete(bm.violations, peerID)
		delete(bm.violationCounter, peerID)
		delete(bm.checkpointViolations, peerID)
		
		bm.logger.Infof("✅ Unbanned peer %s", peerID.String()[:8])
	}
}

// GetBanStats returns statistics about bans
func (bm *BanManager) GetBanStats() map[string]interface{} {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	stats := map[string]interface{}{
		"totalBanned":           len(bm.bannedPeers),
		"totalViolations":       len(bm.violations),
		"checkpointViolations":  len(bm.checkpointViolations),
		"config":                bm.config,
	}
	
	// Count checkpoint violations
	cpViolations := 0
	for _, violations := range bm.checkpointViolations {
		cpViolations += len(violations)
	}
	stats["totalCheckpointViolations"] = cpViolations
	
	return stats
}

// ========== INTEGRATION WITH EXISTING NODE ==========

// IntegrateBanManager adds ban checking to the existing node with checkpoint support
func (n *Node) IntegrateBanManagerWithCheckpoints(cp *checkpoints.Checkpoints) {
	banConfig := DefaultBanConfig()
	if n.banManager != nil {
		n.banManager.mu.Lock()
		n.banManager.checkpoints = cp
		n.banManager.mu.Unlock()
		n.logger.WithFields(logrus.Fields{
			"checkpointProtection": cp != nil,
			"autoBan":              n.banManager.config.EnableAutoBan,
			"checkpointWeight":     n.banManager.config.CheckpointWeight,
		}).Info("✅ P2P Ban Manager checkpoint protection updated")
		return
	}

	n.banManager = NewBanManager(n, banConfig, cp)
	n.banManager.Start()

	n.logger.WithFields(logrus.Fields{
		"checkpointProtection": cp != nil,
		"autoBan":              banConfig.EnableAutoBan,
		"checkpointWeight":     banConfig.CheckpointWeight,
	}).Info("✅ P2P Ban Manager with DoS Protection integrated")
}

// CheckPeerBeforeProcessingWithCheckpoints enhanced version with checkpoint verification
func (n *Node) CheckPeerBeforeProcessingWithCheckpoints(peerID peer.ID, msgType byte, data []byte) bool {
	if n.banManager == nil {
		return true // No ban manager, allow all
	}
	
	// Check if peer is banned
	if n.banManager.IsBanned(peerID) {
		n.logger.Debugf("Skipping message from banned peer %s", peerID.String()[:8])
		return false
	}
	
	// For block messages, do additional checkpoint verification
	if msgType == msgTypeBlock && n.banManager.checkpoints != nil {
		// Parse block to get height for checkpoint checking
		var blk block.Block
		if err := json.Unmarshal(data[1:], &blk); err == nil && blk.Header != nil {
			height := blk.Header.Number.Uint64()
			blockHash := blk.Hash()
			
			// Quick checkpoint check before full processing
			if cp, exists := n.banManager.checkpoints.GetCheckpoint(height); exists {
				if cp.Hash != blockHash {
					n.logger.Errorf("🚨 Quick checkpoint violation from %s at height %d",
						peerID.String()[:8], height)
					n.banManager.BanPeer(peerID, "CHECKPOINT_HASH_MISMATCH",
						fmt.Sprintf("height=%d expected=%s got=%s", height, cp.Hash.String(), blockHash.String()))
					return false
				}
			}
		}
	}
	
	return true
}
