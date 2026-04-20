// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package p2p

import (
    "bufio"
    "context"
    "encoding/binary"
    "encoding/json"
    "encoding/hex"
    "errors"
    "fmt"
    "io"
    "math"
    "regexp"
    "os"
    "path/filepath"
    "strings"
    "sync"
    "time"
    "math/big"
    "sort"
    "github.com/ethereum/go-ethereum/common"
    "github.com/ipfs/go-cid"
    "github.com/libp2p/go-libp2p"
    "github.com/libp2p/go-libp2p/core/host"
    "github.com/libp2p/go-libp2p/core/network"
    "github.com/libp2p/go-libp2p/core/peer"
    "github.com/libp2p/go-libp2p/core/peerstore"
    "github.com/libp2p/go-libp2p/p2p/discovery/mdns"
    pubsub "github.com/libp2p/go-libp2p-pubsub"
    kd "github.com/libp2p/go-libp2p-kad-dht"
    "github.com/multiformats/go-multiaddr"
    "github.com/sirupsen/logrus"
    "github.com/antdaza/antdchain/antdc/checkpoints"
    "github.com/antdaza/antdchain/antdc/block"
    "github.com/antdaza/antdchain/antdc/tx"
    "github.com/antdaza/antdchain/antdc/reward"
    crypto "github.com/libp2p/go-libp2p/core/crypto"
    "github.com/antdaza/antdchain/antdc/rotatingking"
)

var _ rotatingking.P2PBroadcaster = (*Node)(nil)

const (
    msgTypeBlock = 0x01
    msgTypeTx    = 0x02
    msgTypeKingRotation = 0x03
    msgTypeKingListUpdate = 0x04
    msgTypeDBSyncRequest = 0x05      // NEW: Request database sync
    msgTypeDBSyncResponse = 0x06     // NEW: Database sync response
    msgTypeDBSyncStatus = 0x07       // NEW: Database status
    msgTypeDBSyncAnnounce = 0x08     // NEW: Database sync announcement
    msgTypeKingConfig = 0x09
    msgTypeKingConfigRequest = iota + 30
)

type DBSyncRequest struct {
    RequestID   string `json:"requestId"`
    FromHeight  uint64 `json:"fromHeight"`
    ToHeight    uint64 `json:"toHeight"`
    RequestType string `json:"requestType"` // "full", "incremental", "metadata", "config"
    Timestamp   int64  `json:"timestamp"`
    PeerID      string `json:"peerId"`
}

type DBSyncResponse struct {
    RequestID   string                      `json:"requestId"`
    Status      string                      `json:"status"` // "success", "partial", "error"
    Rotations   []rotatingking.KingRotation `json:"rotations,omitempty"`
    Config      *rotatingking.RotatingKingConfig `json:"config,omitempty"` // ADD THIS LINE
    LatestBlock uint64                      `json:"latestBlock"`
    SyncState   *rotatingking.SyncState     `json:"syncState,omitempty"`
    Timestamp   int64                       `json:"timestamp"`
    Error       string                      `json:"error,omitempty"`
    PeerID      string                      `json:"peerId"` // Who is responding
}

type DBSyncStatus struct {
    PeerID           string                     `json:"peerId"`
    LastSyncedBlock  uint64                     `json:"lastSyncedBlock"`
    SyncState        *rotatingking.SyncState    `json:"syncState"`
    IsSyncing        bool                       `json:"isSyncing"`
    KingCount        int                        `json:"kingCount"`
    LatestRotation   uint64                     `json:"latestRotation"`
    Timestamp        int64                      `json:"timestamp"`
    Version          string                     `json:"version"`
}

type KingConfigSync struct {
    Config      rotatingking.RotatingKingConfig `json:"config"`
    BlockHeight uint64                          `json:"blockHeight"`
    Timestamp   int64                           `json:"timestamp"`
    PeerID      string                          `json:"peerId"`
}

type DBSyncAnnounce struct {
    PeerID          string   `json:"peerId"`
    Capabilities    []string `json:"capabilities"` // "sync", "history", "backup"
    SupportsSync    bool     `json:"supportsSync"`
    MaxBatchSize    int      `json:"maxBatchSize"`
    Timestamp       int64    `json:"timestamp"`
}

// Database sync metrics
type DBSyncMetrics struct {
    SyncAttempts    int           `json:"syncAttempts"`
    SuccessfulSyncs int           `json:"successfulSyncs"`
    FailedSyncs     int           `json:"failedSyncs"`
    LastSyncTime    time.Time     `json:"lastSyncTime"`
    TotalRotations  int           `json:"totalRotations"`
    BytesTransferred int64        `json:"bytesTransferred"`
    PeerCount       int           `json:"peerCount"`
    IsActive        bool          `json:"isActive"`
    LastKingList     []common.Address `json:"lastKingList"`
}

type KingRotationEvent struct {
    BlockHeight        uint64         `json:"height"`
    PreviousKing       common.Address `json:"prevKing"`
    NewKing            common.Address `json:"newKing"`
    Eligible           bool           `json:"eligible"`
    EligibilityBalance *big.Int       `json:"balance"`
    Timestamp          time.Time      `json:"ts"`
    Reason             string         `json:"reason,omitempty"`
}

type rateLimiter struct {
    count     int
    resetTime time.Time
}

const (
    MaxTxPerPeerPerSecond = 50
    MaxTxPerPeerBurst     = 200
    MaxBlocksPerPeerPerSec = 10
)

type Config struct {
    DataDir          string        // Directory for persistent data
    Port             int           // P2P listening port
    BootstrapPeers   []string      // Initial peers to connect to
    EnableMDNS       bool          // Enable mDNS discovery
    EnableDHT        bool          // Enable DHT discovery
    EnableNATService bool          // Enable NAT traversal
    MaxPeers         int           // Maximum number of connected peers
    MinPeers         int           // Minimum peers before discovery
    ConnectionTimeout time.Duration // Timeout for connections
    LogLevel         string        // Log level
 Context context.Context

}

// DefaultConfig returns configuration with sensible defaults
func DefaultConfig() Config {
    return Config{
        DataDir:          "./antdchain-data",
        Port:             4001,
        EnableMDNS:       true,
        EnableDHT:        true,
        EnableNATService: true,
        MaxPeers:         50,
        MinPeers:         5,
        ConnectionTimeout: 30 * time.Second,
        LogLevel:         "info",
    }
}

type Node struct {
    host           host.Host
    pubsub         *pubsub.PubSub
    topic          *pubsub.Topic
    sub            *pubsub.Subscription
    chain          Chain
    logger         *logrus.Logger
    mu             sync.RWMutex
    processMu      sync.Mutex
    syncMu         sync.Mutex
   // orphanPool     map[common.Hash]*block.Block
    synced         bool
    syncHeight     uint64
    ctx            context.Context
    cancel         context.CancelFunc
    dht            *kd.IpfsDHT
    txPerPeer      map[peer.ID]*rateLimiter
    txPerPeerMu    sync.RWMutex
    blockPerPeer   map[peer.ID]*rateLimiter
    blockPerPeerMu sync.RWMutex
    cfg  Config

    knownTxs      map[common.Hash]time.Time
    knownTxsMu    sync.RWMutex
    knownTxsLimit int

    lastSyncTime  time.Time
    syncAttempts  int
    lastSyncPeer  peer.ID

    kingTopic *pubsub.Topic
    kingSub   *pubsub.Subscription
    eventPerPeer      map[peer.ID]*rateLimiter
    eventPerPeerMu    sync.RWMutex

    // Database sync fields
    dbSyncMu          sync.RWMutex
    dbSyncRequests    map[string]*DBSyncRequest    // Track ongoing requests
    dbSyncResponses   map[string]*DBSyncResponse   // Cache responses
    dbSyncPeers       map[string]*DBSyncStatus     // Track peer sync status
    dbSyncMetrics     *DBSyncMetrics               // Sync metrics
    dbSyncInterval    time.Duration                // How often to sync
    lastDBSyncTime    time.Time
    dbSyncTopic       *pubsub.Topic                // Separate topic for DB sync
    dbSyncSub         *pubsub.Subscription         // Subscription for DB sync
    dbSyncEnabled     bool                         // Whether DB sync is enabled
    dbSyncVersion     string                       // Sync protocol version

    // Sync coordination
    isDBSyncing       bool
    currentSyncPeer   string
    syncRetryCount    int
    maxSyncRetries    int

    banManager *BanManager

    lastKingList     []common.Address
    lastKingListMu   sync.RWMutex
}

func (n *Node) IsSynced() bool {
    return !n.chain.IsSyncing() // ← delegate to blockchain atomic flag
}

func LoadOrCreateIdentity(dataDir string) (crypto.PrivKey, peer.ID, error) {
    // Ensure data directory exists
    if err := os.MkdirAll(dataDir, os.ModePerm); err != nil {
        return nil, "", fmt.Errorf("failed to create data directory: %w", err)
    }

    keyPath := filepath.Join(dataDir, "p2p-key.hex")

    // Try to load existing key
    if data, err := os.ReadFile(keyPath); err == nil {
        keyBytes, err := hex.DecodeString(string(data))
        if err != nil {
            return nil, "", fmt.Errorf("corrupted key file: %w", err)
        }

        privKey, err := crypto.UnmarshalPrivateKey(keyBytes)
        if err != nil {
            return nil, "", fmt.Errorf("failed to unmarshal key: %w", err)
        }

        peerID, err := peer.IDFromPrivateKey(privKey)
        if err != nil {
            return nil, "", err
        }

        return privKey, peerID, nil
    }

    // Create new key
    privKey, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 2048)
    if err != nil {
        return nil, "", err
    }

    keyBytes, err := crypto.MarshalPrivateKey(privKey)
    if err != nil {
        return nil, "", err
    }

    // Save to file
    if err := os.WriteFile(keyPath, []byte(hex.EncodeToString(keyBytes)), 0600); err != nil {
        return nil, "", err
    }

    peerID, err := peer.IDFromPrivateKey(privKey)
    if err != nil {
        return nil, "", err
    }

    return privKey, peerID, nil
}

func parseBootstrapPeers(addrs []string, logger *logrus.Logger) []peer.AddrInfo {
    var peers []peer.AddrInfo
    for _, addrStr := range addrs {
        maddr, err := multiaddr.NewMultiaddr(addrStr)
        if err != nil {
            logger.Warnf("Invalid bootstrap address %s: %v", addrStr, err)
            continue
        }
        ai, err := peer.AddrInfoFromP2pAddr(maddr)
        if err != nil {
            logger.Warnf("Failed to parse bootstrap address %s: %v", addrStr, err)
            continue
        }
        peers = append(peers, *ai)
    }
    return peers
}

// BroadcastBlock — secure, efficient, anti-spam block propagation
func (n *Node) BroadcastBlock(b *block.Block) error {
    if b == nil || b.Header == nil {
        return errors.New("nil block or header")
    }

    // Validate block hash
    if b.Hash() != b.Header.Hash() { // ← Fixed: use .Hash() not .ComputeHash()
        return errors.New("invalid block: hash mismatch")
    }

    // Validate checkpoint
    checkpointManager := n.chain.Checkpoints()
    if checkpointManager != nil {
    if err := checkpointManager.ValidateBlock(b.Header.Number.Uint64(), b.Hash()); err != nil {
        n.logger.Warnf("Checkpoint rejected block %d: %v", b.Header.Number.Uint64(), err)
        return fmt.Errorf("checkpoint validation failed: %w", err)
       }
     } else {
      n.logger.Debug("Checkpoint manager not available, skipping validation")
    }

    data, err := json.Marshal(b)
    if err != nil {
        return fmt.Errorf("failed to marshal block: %w", err)
    }

    msg := make([]byte, 1+len(data))
    msg[0] = msgTypeBlock
    copy(msg[1:], data)

    if err := n.topic.Publish(n.ctx, msg); err != nil {
        return fmt.Errorf("gossipsub publish failed: %w", err)
    }

    go n.directPushBlock(b, msg)

    n.logger.Infof("BLOCK BROADCAST #%d | hash=%s | txs=%d | size=%d KB",
        b.Header.Number.Uint64(),
        b.Hash().Hex()[:12],
        len(b.Txs),
        len(data)/1024,
    )

    return nil
}

// directPushBlock sends block directly to recent peers
func (n *Node) directPushBlock(b *block.Block, msg []byte) {
    peers := n.host.Network().Peers()
    count := 0
    for _, pid := range peers {
        if count >= 15 {
            break
        }
        if pid == n.host.ID() {
            continue
        }

        ctx, cancel := context.WithTimeout(n.ctx, 3*time.Second)
        s, err := n.host.NewStream(ctx, pid, "/antdchain/direct/1.0.0")
        cancel()
        if err != nil {
            continue
        }

        if _, err := s.Write(msg); err != nil {
            s.Close()
            continue
        }
        s.Close()
        count++
    }

    if count > 0 {
        n.logger.Debugf("Direct-pushed block %d to %d peers", b.Header.Number.Uint64(), count)
    }
}

// BroadcastTx — secure, efficient, spam-resistant transaction broadcast
func (n *Node) BroadcastTx(t *tx.Tx) error {
    if t == nil {
        return errors.New("nil transaction")
    }

    if err := t.Validate(); err != nil {
        return fmt.Errorf("invalid transaction: %w", err)
    }
    if valid, err := t.Verify(); err != nil || !valid {
        return errors.New("invalid signature")
    }

    hash := t.Hash()

    // Check if we've recently broadcast this transaction
    n.knownTxsMu.RLock()
    _, recentlyBroadcast := n.knownTxs[hash]
    n.knownTxsMu.RUnlock()

    if recentlyBroadcast {
        n.logger.Debugf("Already recently broadcast tx %s, skipping", hash.Hex()[:10])
        return nil
    }

    data, err := json.Marshal(t)
    if err != nil {
        return fmt.Errorf("failed to marshal tx: %w", err)
    }

    msg := make([]byte, 1+len(data))
    msg[0] = msgTypeTx
    copy(msg[1:], data)

    if err := n.topic.Publish(n.ctx, msg); err != nil {
        return fmt.Errorf("failed to publish tx: %w", err)
    }

    // Mark as broadcast
    n.knownTxsMu.Lock()
    n.knownTxs[hash] = time.Now()
    n.knownTxsMu.Unlock()

    n.logger.Infof("Broadcast tx %s (nonce=%d, value=%s)",
        hash.Hex()[:10],
        t.Nonce,
        t.Value.String(),
    )

    return nil
}

// handleMessages processes incoming pubsub messages
func (n *Node) handleMessages() {
    for {
        msg, err := n.sub.Next(n.ctx)
        if err != nil {
            if n.ctx.Err() == nil {
                n.logger.Errorf("Subscription error: %v", err)
            }
            return
        }
        if msg.GetFrom() == n.host.ID() || len(msg.Data) < 1 {
            continue
        }

        // ===== ADD ENHANCED BAN CHECK HERE =====
        if n.banManager != nil {
            shouldBan, violation, cpViolation := n.banManager.CheckMessageWithCheckpoints(msg, msg.Data[0])
            if shouldBan {
                n.banManager.RecordViolation(msg.GetFrom(), violation, cpViolation)
                continue // Skip processing banned peer's message
            }
        }
        // ===== END ENHANCED BAN CHECK =====

        switch msg.Data[0] {
    case msgTypeKingConfigRequest:
      n.logger.Infof("Received king config request from %s - broadcasting our config", msg.GetFrom().String()[:8])
    n.BroadcastCurrentKingConfig()

   case msgTypeKingListUpdate:
    n.logger.Info("Received king list update event")
    if len(msg.Data) < 100 {
        continue
    }
    if !n.allowEventFromPeer(msg.GetFrom()) {
        continue
    }
    var event rotatingking.KingListUpdateEvent
    if err := json.Unmarshal(msg.Data[1:], &event); err != nil {
        n.logger.Warnf("Failed to unmarshal king list update: %v", err)
        continue
    }
    n.logger.Infof("Received king list update: %d kings at height %d", len(event.NewList), event.BlockHeight)

    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    if mgr != nil {
        // Apply new list
        if err := mgr.UpdateKingAddresses(event.NewList); err != nil {
            n.logger.Warnf("Failed to apply king list update: %v", err)
        } else {
            n.logger.Info("King list updated via P2P")
        }
    }
    n.processMu.Unlock()

    case msgTypeKingRotation:
    if len(msg.Data) < 100 {
        continue
    }
    if !n.allowEventFromPeer(msg.GetFrom()) {
        continue
    }
    var event KingRotationEvent
    if err := json.Unmarshal(msg.Data[1:], &event); err != nil {
        n.logger.Warnf("Failed to unmarshal rotation event: %v", err)
        continue
    }
    n.logger.Infof("Received king rotation event: %s → %s at height %d (eligible=%v)",
        event.PreviousKing.Hex()[:8], event.NewKing.Hex()[:8], event.BlockHeight, event.Eligible)

    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    if mgr != nil {
        currentHeight := n.currentHeight()
        if event.BlockHeight != currentHeight && event.BlockHeight != currentHeight+1 {
            n.logger.Warnf("Invalid rotation event height %d (current %d)", event.BlockHeight, currentHeight)
            n.processMu.Unlock()
            continue
        }
        localEligible := mgr.IsEligible(event.BlockHeight)
        if localEligible != event.Eligible {
            n.logger.Warnf("Eligibility mismatch: local=%v event=%v - skipping", localEligible, event.Eligible)
            n.processMu.Unlock()
            continue
        }
        if err := mgr.ForceRotateToAddress(event.NewKing, "p2p-rotation-event"); err != nil {
            n.logger.Warnf("Failed to apply rotation event: %v", err)
        }
    }
    n.processMu.Unlock()

     case msgTypeBlock:
    if len(msg.Data) < 100 {
        continue
    }

    if !n.allowBlockFromPeer(msg.GetFrom()) {
        continue
    }

    var blk block.Block
    if err := json.Unmarshal(msg.Data[1:], &blk); err != nil {
        n.logger.Warnf("Failed to unmarshal block: %v", err)
        continue
    }

    n.logger.Infof("Received block %d | hash=%s | from=%s",
        blk.Header.Number.Uint64(), blk.Hash().Hex()[:12], msg.GetFrom().String()[:8])

    // If we're at height 0 and receive any block, force sync
    currentHeight := n.currentHeight()
    if currentHeight == 0 && blk.Header.Number.Uint64() > 0 {
        n.logger.Warnf("EMERGENCY: At height 0 but received block %d - forcing sync!",
            blk.Header.Number.Uint64())
        go n.forceSync()
    }

    n.processMu.Lock()
    if err := n.processBlock(&blk); err != nil {
        if !strings.Contains(err.Error(), "already known") &&
           !strings.Contains(err.Error(), "parent") {
            n.logger.Warnf("Block %d rejected: %v", blk.Header.Number.Uint64(), err)
        }
    }
    n.processMu.Unlock()

    case msgTypeTx:
    if len(msg.Data) < 100 {
        continue
    }

    if !n.allowTxFromPeer(msg.GetFrom()) {
        continue
    }

    var txObj tx.Tx
    if err := json.Unmarshal(msg.Data[1:], &txObj); err != nil {
        n.logger.Warnf("Failed to unmarshal tx: %v", err)
        continue
    }

    hash := txObj.Hash()

    // Check if we've recently seen this transaction
    n.knownTxsMu.RLock()
    _, recentlySeen := n.knownTxs[hash]
    n.knownTxsMu.RUnlock()

    if recentlySeen {
        n.logger.Debugf("Already recently saw tx %s, ignoring", hash.Hex()[:10])
        continue
    }

    n.logger.Infof("Received tx %s from %s (nonce=%d)",
        hash.Hex()[:10], msg.GetFrom().String()[:8], txObj.Nonce)

    n.processMu.Lock()
    err := n.chain.TxPool().AddTx(&txObj, n.chain)
    wasNew := err == nil
    if err != nil && !strings.Contains(err.Error(), "already in pool") {
        n.logger.Warnf("Tx rejected: %v", err)
    }
    n.processMu.Unlock()

    // Mark as seen before re-broadcasting
    n.knownTxsMu.Lock()
    n.knownTxs[hash] = time.Now()
    // Clean old entries if needed
    if len(n.knownTxs) > n.knownTxsLimit {
        for key := range n.knownTxs {
            delete(n.knownTxs, key)
            break
        }
    }
    n.knownTxsMu.Unlock()

    // Only re-broadcast if it was new to us
    if wasNew {
        go func(tx *tx.Tx) {
            // Small delay to let the local node fully process it first
            time.Sleep(50 * time.Millisecond)
            if err := n.BroadcastTx(tx); err != nil {
                n.logger.Debugf("Failed to re-broadcast tx %s: %v", tx.Hash().Hex()[:10], err)
            } else {
                n.logger.Debugf("Re-broadcasted tx %s to network", tx.Hash().Hex()[:10])
            }
        }(&txObj)
    }


        }
    }
}

// triggerSyncWithPeers finds mining peers and syncs with them
func (n *Node) triggerSyncWithPeers() {
    peers := n.Peers()
    if len(peers) == 0 {
        n.logger.Warn("No peers available for sync")
        return
    }

    // Try each peer
    for _, pid := range peers {
        height, err := n.GetPeerHeight(pid)
        if err != nil {
            continue
        }

        localHeight := n.currentHeight()

        // Only sync if peer is ahead
        if height > localHeight {
            n.logger.Infof("Syncing with peer %s (height: %d)", pid.String()[:12], height)

            // Start sync mode
            n.chain.StartSync(height)

            // Do the sync
            go func(pid peer.ID, target uint64) {
                if err := n.syncMissingBlocks(pid, target); err != nil {
                    n.logger.Errorf("Sync failed: %v", err)
                } else {
                    n.logger.Info("Sync completed successfully")
                    if n.chain.IsSyncing() {
                        n.chain.StopSync()
                    }
                }
            }(pid, height)

            // Only sync with one peer at a time
            break
        }
    }
}

// GetPeerHeight queries a peer for their latest block height
func (n *Node) GetPeerHeight(pid peer.ID) (uint64, error) {
    n.syncMu.Lock()
    defer n.syncMu.Unlock()
    s, err := n.host.NewStream(n.ctx, pid, "/antdchain/sync/1.0.0")
    if err != nil {
        return 0, fmt.Errorf("failed to open stream to %s: %w", pid, err)
    }
    defer s.Close()

    rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))
    var req uint64 = math.MaxUint64
    if err := binary.Write(rw, binary.BigEndian, req); err != nil {
        return 0, fmt.Errorf("failed to send height request: %w", err)
    }
    _ = rw.Flush()

    var height uint64
    if err := binary.Read(rw, binary.BigEndian, &height); err != nil {
        return 0, fmt.Errorf("failed to read height: %w", err)
    }

    localHeight := n.currentHeight()
    difference := int64(height) - int64(localHeight)

    // Smart logging based on difference
    switch {
    case height == 0:
        n.logger.Debugf("Peer %s at genesis", pid.String()[:12])

    case height == localHeight:
        n.logger.Debugf("Peer %s at same height: %d", pid.String()[:12], height)
    case difference > 0 && difference <= 10:
        n.logger.Infof("Peer %s slightly ahead: %d (+%d)",
            pid.String()[:12], height, difference)

    case difference > 10:
        n.logger.Warnf("Peer %s significantly ahead: %d (+%d)",
            pid.String()[:12], height, difference)

    case difference < 0 && difference >= -10:
        n.logger.Infof("Peer %s slightly behind: %d (%d)",
            pid.String()[:12], height, difference)

    case difference < -10:
        n.logger.Infof("Peer %s significantly behind: %d (%d)",
            pid.String()[:12], height, difference)

    default:
        n.logger.Infof("Peer %s height: %d (we're at %d)",
            pid.String()[:12], height, localHeight)
    }

    if height > n.syncHeight {
        n.syncHeight = height
    }
    return height, nil
}


// findCommonAncestorLocked — same as before but called only when syncMu is held
func (n *Node) findCommonAncestorLocked(peerID peer.ID, startHeight uint64) (uint64, error) {
    for h := startHeight; h >= 0; h-- {
        blk, err := n.RequestBlockSync(peerID, h)
        if err != nil {
            continue
        }
        if local := n.chain.GetBlock(h); local != nil && local.Hash() == blk.Hash() {
            return h, nil
        }
    }
    return 0, fmt.Errorf("no common ancestor")
}

func (n *Node) findCommonAncestor(peerID peer.ID, startHeight uint64) (uint64, error) {
    for h := startHeight; h >= 0; h-- {
        blk, err := n.RequestBlockSync(peerID, h)
        if err != nil {
            continue
        }
        if local := n.chain.GetBlock(h); local != nil && local.Hash() == blk.Hash() {
            return h, nil
        }
    }
    // Should never happen — genesis is always common
    return 0, fmt.Errorf("no common ancestor found (genesis mismatch?)")
}

// LatestHeight returns current canonical chain height (0 if empty)
func (n *Node) LatestHeight() uint64 {
    if latest := n.chain.Latest(); latest != nil {
        return latest.Header.Number.Uint64()
    }
    return 0
}

// LatestHash returns current tip hash (zero hash if empty)
func (n *Node) LatestHash() common.Hash {
    if latest := n.chain.Latest(); latest != nil {
        return latest.Hash()
    }
    return common.Hash{} // zero hash = genesis parent
}

//Recursively fetches missing parents until we connect to our chain
func (n *Node) backfillMissingParents(peerID peer.ID, neededHeight uint64, expectedHash common.Hash) error {
    for h := neededHeight; h >= 0; h-- {
        if n.LatestHeight() >= h {
            if n.LatestHash() == expectedHash {
                return nil // we're connected
            }
            // We're at wrong fork — this shouldn't happen during normal sync
            return fmt.Errorf("fork detected during backfill at height %d", h)
        }

        blk, err := n.RequestBlockSync(peerID, h)
        if err != nil {
            return fmt.Errorf("backfill failed at %d: %w", h, err)
        }

        if err := n.chain.AddBlock(blk); err != nil {
            return fmt.Errorf("failed to add backfill block %d: %w", h, err)
        }

        n.logger.Infof("Backfilled missing block %d", h)
    }
    return nil
}

// handleStream handles block sync requests
func (n *Node) handleStream(s network.Stream) {
    defer s.Close()
    rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))

    var request uint64
    if err := binary.Read(rw, binary.BigEndian, &request); err != nil {
        n.logger.Warnf("Failed to read sync request: %v", err)
        return
    }

    if request == math.MaxUint64 {
        height := uint64(0)
        latest := n.chain.Latest()
        if latest != nil {
            height = latest.Header.Number.Uint64()
        }
        if err := binary.Write(rw, binary.BigEndian, height); err != nil {
            n.logger.Warnf("Failed to write height response: %v", err)
        }
        _ = rw.Flush()
        return
    }

    blk := n.chain.GetBlock(request)
    if blk == nil {
        _ = binary.Write(rw, binary.BigEndian, uint32(0))
        _ = rw.Flush()
        return
    }
    if err := n.chain.Checkpoints().ValidateBlock(request, blk.Hash()); err != nil {
        _ = binary.Write(rw, binary.BigEndian, uint32(0))
        _ = rw.Flush()
        return
    }
    data, _ := json.Marshal(blk)
    _ = binary.Write(rw, binary.BigEndian, uint32(len(data)))
    _, _ = rw.Write(data)
    _ = rw.Flush()
}

// RequestBlockSync fetches a block from a peer
func (n *Node) RequestBlockSync(peerID peer.ID, blockNumber uint64) (*block.Block, error) {
    n.syncMu.Lock()
    defer n.syncMu.Unlock()

    // Add timeout context
    ctx, cancel := context.WithTimeout(n.ctx, 10*time.Second) // 10 second timeout
    defer cancel()

    s, err := n.host.NewStream(ctx, peerID, "/antdchain/sync/1.0.0")
    if err != nil {
        return nil, fmt.Errorf("failed to open stream to %s: %w", peerID, err)
    }
    defer s.Close()

    // Set deadline on the stream
    deadline := time.Now().Add(10 * time.Second)
    s.SetDeadline(deadline)

    rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))
    if err := binary.Write(rw, binary.BigEndian, blockNumber); err != nil {
        return nil, fmt.Errorf("failed to send block request: %w", err)
    }
    _ = rw.Flush()

    var length uint32
    if err := binary.Read(rw, binary.BigEndian, &length); err != nil {
        return nil, fmt.Errorf("failed to read block length: %w", err)
    }
    if length == 0 {
        return nil, fmt.Errorf("block %d not found", blockNumber)
    }
    data := make([]byte, length)
    if _, err := io.ReadFull(rw, data); err != nil {
        return nil, fmt.Errorf("failed to read block data: %w", err)
    }
    var blk block.Block
    if err := json.Unmarshal(data, &blk); err != nil {
        return nil, fmt.Errorf("failed to unmarshal block: %w", err)
    }
    if err := n.chain.Checkpoints().ValidateBlock(blockNumber, blk.Hash()); err != nil {
        return nil, fmt.Errorf("checkpoint validation failed for block %d: %w", blockNumber, err)
    }
    return &blk, nil
}

// startMDNSDiscovery starts mDNS discovery
func (n *Node) startMDNSDiscovery() error {
    svc := mdns.NewMdnsService(n.host, "antdchain-mdns", &mdnsNotifee{host: n.host, logger: n.logger, node: n})
    return svc.Start()
}

// mdnsNotifee handles mDNS peer discovery
type mdnsNotifee struct {
    host   host.Host
    logger *logrus.Logger
    node   *Node
}

func (m *mdnsNotifee) HandlePeerFound(pi peer.AddrInfo) {
    m.host.Peerstore().AddAddrs(pi.ID, pi.Addrs, peerstore.TempAddrTTL)
    if err := m.host.Connect(context.Background(), pi); err != nil {
        m.logger.Warnf("Failed to connect to mDNS peer %s: %v", pi.ID, err)
        return
    }
    m.logger.Infof("Connected to mDNS peer %s", pi.ID)
    go m.node.syncIfBehind(pi.ID)
}

// Global DHT peer discovery
func (n *Node) startDHTDiscovery() {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()

    rendezvous := "antdchain-mainnet-v1"
    cidRendezvous := cid.NewCidV1(cid.Raw, []byte(rendezvous))

    for {
        select {
        case <-n.ctx.Done():
            return
        case <-ticker.C:
            ctx, cancel := context.WithTimeout(n.ctx, 15*time.Second)
            peerChan := n.dht.FindProvidersAsync(ctx, cidRendezvous, 50)
            cancel()

            count := 0
            for p := range peerChan {
                if p.ID == n.host.ID() || len(p.Addrs) == 0 {
                    continue
                }
                n.logger.Infof("DHT discovered peer: %s", p.ID.String()[:8])
                go n.host.Connect(n.ctx, p)
                count++
            }
            if count == 0 {
                n.logger.Debug("DHT discovery: no new peers")
            }
        }
    }
}

// Announce ourselves on DHT
func (n *Node) announceOnDHT() {
    time.Sleep(5 * time.Second)
    ctx, cancel := context.WithTimeout(n.ctx, 30*time.Second)
    defer cancel()

    rendezvous := "antdchain-mainnet-v1"
    cidRendezvous := cid.NewCidV1(cid.Raw, []byte(rendezvous))
    n.logger.Infof("Announcing on DHT: %s", rendezvous)

    if err := n.dht.Provide(ctx, cidRendezvous, true); err != nil {
        n.logger.Warnf("DHT Provide failed: %v", err)
    } else {
        n.logger.Info("Successfully announced on DHT")
    }
}

// allowTxFromPeer — rate limiting
func (n *Node) allowTxFromPeer(pid peer.ID) bool {
    n.txPerPeerMu.Lock()
    defer n.txPerPeerMu.Unlock()

    if n.txPerPeer == nil {
        n.txPerPeer = make(map[peer.ID]*rateLimiter)
    }

    rl := n.txPerPeer[pid]
    if rl == nil {
        rl = &rateLimiter{resetTime: time.Now()}
        n.txPerPeer[pid] = rl
    }

    if time.Since(rl.resetTime) > time.Second {
        rl.count = 0
        rl.resetTime = time.Now()
    }

    rl.count++
    return rl.count <= MaxTxPerPeerBurst && (rl.count <= MaxTxPerPeerPerSecond || time.Since(rl.resetTime) > time.Second)
}

// allowBlockFromPeer — rate limiting
func (n *Node) allowBlockFromPeer(pid peer.ID) bool {
    n.blockPerPeerMu.Lock()
    defer n.blockPerPeerMu.Unlock()

    if n.blockPerPeer == nil {
        n.blockPerPeer = make(map[peer.ID]*rateLimiter)
    }

    rl := n.blockPerPeer[pid]
    if rl == nil {
        rl = &rateLimiter{resetTime: time.Now()}
        n.blockPerPeer[pid] = rl
    }

    if time.Since(rl.resetTime) > time.Second {
        rl.count = 0
        rl.resetTime = time.Now()
    }

    rl.count++
    return rl.count <= MaxBlocksPerPeerPerSec
}

// handleDirectPush — direct block push handler
func (n *Node) handleDirectPush(s network.Stream) {
    defer s.Close()

    data, err := io.ReadAll(s)
    if err != nil || len(data) < 1 {
        return
    }

    if data[0] != msgTypeBlock {
        return
    }

    var blk block.Block
    if err := json.Unmarshal(data[1:], &blk); err != nil {
        return
    }

    go func() {
        n.processMu.Lock()
        defer n.processMu.Unlock()
        if err := n.processBlock(&blk); err != nil && !strings.Contains(err.Error(), "already known") {
            n.logger.Warnf("Direct block failed: %v", err)
        }
    }()
}

func (n *Node) Peers() []peer.ID {
    return n.host.Network().Peers()
}

func (n *Node) ID() string {
    if n.host == nil {
        return ""
    }
    return n.host.ID().String() // Get the actual host ID
}

func (n *Node) currentHeight() uint64 {
    if b := n.chain.Latest(); b != nil {
        return b.Header.Number.Uint64()
    }
    return 0
}

func (n *Node) connectToBootstrap(bootstrap []string) int {
    count := 0
    for _, addrStr := range bootstrap {
        maddr, err := multiaddr.NewMultiaddr(addrStr)
        if err != nil {
            n.logger.Warnf("Invalid bootstrap address %s: %v", addrStr, err)
            continue
        }
        ai, err := peer.AddrInfoFromP2pAddr(maddr)
        if err != nil {
            n.logger.Warnf("Failed to parse bootstrap address %s: %v", addrStr, err)
            continue
        }

        n.host.Peerstore().AddAddrs(ai.ID, ai.Addrs, peerstore.PermanentAddrTTL)

        ctx, cancel := context.WithTimeout(n.ctx, n.cfg.ConnectionTimeout)
        defer cancel()

        if err := n.host.Connect(ctx, *ai); err != nil {
            n.logger.Warnf("Failed to connect to bootstrap %s: %v", ai.ID.String()[:8], err)
            continue
        }

        n.logger.Infof("Connected to bootstrap node %s", ai.ID.String()[:8])
        go n.syncIfBehind(ai.ID)
        count++
    }
    return count
}

func (n *Node) manageConnections() {
    ticker := time.NewTicker(20 * time.Second)
    defer ticker.Stop()

    noPeerStartTime := time.Time{}

    for {
        select {
        case <-n.ctx.Done():
            return
        case <-ticker.C:
            peers := n.Peers()
            peerCount := len(peers)

            // Track how long we've had no peers
            if peerCount == 0 {
                if noPeerStartTime.IsZero() {
                    noPeerStartTime = time.Now()
                } else if time.Since(noPeerStartTime) > 60*time.Second {
                    n.logger.Warnf("No peers for 60+ seconds — forcing reconnection")
                    n.ForceBootstrap()
                    n.ForceDHTAnnounce()
                    noPeerStartTime = time.Now() // Reset timer
                }
            } else {
                noPeerStartTime = time.Time{} // Reset
            }

            n.logger.Debugf("Connection check: %d peers connected", peerCount)

            if peerCount < n.cfg.MinPeers {
                n.logger.Infof("Low peer count (%d/%d), triggering discovery",
                    peerCount, n.cfg.MinPeers)
                // Try to connect to bootstrap nodes
                n.connectToBootstrap(n.cfg.BootstrapPeers)
            }

            if peerCount > n.cfg.MaxPeers {
                n.pruneConnections(peerCount - n.cfg.MaxPeers)
            }
        }
    }
}

// pruneConnections disconnects from excess peers
func (n *Node) pruneConnections(count int) {
    peers := n.Peers()
    if len(peers) <= count {
        return
    }

    for i := 0; i < count && i < len(peers); i++ {
        pid := peers[i]
        if pid == n.host.ID() {
            continue
        }

        n.logger.Debugf("Pruning connection to peer %s", pid.String()[:8])
        n.host.Network().ClosePeer(pid)
    }

    n.logger.Infof("Pruned %d connections, now have %d peers",
        count, len(n.Peers()))
}

func NewNode(bc Chain, port int, bootstrap []string) (*Node, error) {
    cfg := Config{
        DataDir:          "./antdchain-data",
        Port:             port,
        BootstrapPeers:   bootstrap,
        EnableMDNS:       true,
        EnableDHT:        true,
        EnableNATService: true,
        MaxPeers:         50,
        MinPeers:         5,
        ConnectionTimeout: 30 * time.Second,
        LogLevel:         "info",
    }

    return NewNodeWithConfig(bc, cfg)
}

// NewNodeWithConfig is the new configurable version
func NewNodeWithConfig(bc Chain, cfg Config) (*Node, error) {
    var ctx context.Context
    var cancel context.CancelFunc

    if cfg.Context != nil {
        ctx, cancel = context.WithCancel(cfg.Context)
    } else {
        ctx, cancel = context.WithCancel(context.Background())
    }

    // Setup logger
    logger := logrus.New()
    logger.SetFormatter(&logrus.TextFormatter{
        FullTimestamp:   true,
        TimestampFormat: "15:04:05.000",
        ForceColors:     true,
    })

    // Set log level
    if level, err := logrus.ParseLevel(cfg.LogLevel); err == nil {
        logger.SetLevel(level)
    }

    // Load or create persistent identity
    var privKey crypto.PrivKey
    var peerID peer.ID
    var err error

    if cfg.DataDir != "" {
        privKey, peerID, err = LoadOrCreateIdentity(cfg.DataDir)
        if err != nil {
            cancel()
            return nil, fmt.Errorf("failed to load/create identity: %w", err)
        }
        logger.Infof("Loaded persistent identity | Peer ID: %s", peerID.String()[:12])
    } else {
        // Generate ephemeral identity
        privKey, _, err = crypto.GenerateKeyPair(crypto.Ed25519, 2048)
        if err != nil {
            cancel()
            return nil, fmt.Errorf("failed to generate key pair: %w", err)
        }
        peerID, err = peer.IDFromPrivateKey(privKey)
        if err != nil {
            cancel()
            return nil, fmt.Errorf("failed to generate peer ID: %w", err)
        }
        logger.Infof("Generated ephemeral identity | Peer ID: %s", peerID.String()[:12])
    }

    // Build libp2p options
    opts := []libp2p.Option{
        libp2p.Identity(privKey),
        libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", cfg.Port)),
    }

    if cfg.EnableNATService {
        opts = append(opts, libp2p.EnableNATService())
        opts = append(opts, libp2p.NATPortMap())
    }

    // Create libp2p host
    h, err := libp2p.New(opts...)
    if err != nil {
        cancel()
        return nil, fmt.Errorf("failed to create libp2p host: %w", err)
    }

    logger.Infof("P2P node started | ID: %s | Addresses:", h.ID().String()[:12])
    for _, addr := range h.Addrs() {
        logger.Infof("  %s/p2p/%s", addr, h.ID())
    }

    // Initialize DHT if enabled
    var dht *kd.IpfsDHT
    if cfg.EnableDHT {
        dht, err = kd.New(ctx, h,
            kd.Mode(kd.ModeAutoServer),
            kd.ProtocolPrefix("/antdchain/kad/1.0.0"),
            kd.BootstrapPeers(parseBootstrapPeers(cfg.BootstrapPeers, logger)...),
        )
        if err != nil {
            h.Close()
            cancel()
            return nil, fmt.Errorf("failed to create DHT: %w", err)
        }

        bootstrapCtx, bootstrapCancel := context.WithTimeout(ctx, 45*time.Second)
        defer bootstrapCancel()

        if err := dht.Bootstrap(bootstrapCtx); err != nil {
            logger.Warnf("DHT bootstrap warning: %v", err)
        } else {
            logger.Info("DHT bootstrapped successfully")
        }
    }

    // Create node instance
    node := &Node{
        host:              h,
        dht:               dht,
        chain:             bc,
        logger:            logger,
        ctx:               ctx,
        cancel:            cancel,
      //  orphanPool:        make(map[common.Hash]*block.Block),
        txPerPeer:         make(map[peer.ID]*rateLimiter),
        blockPerPeer:      make(map[peer.ID]*rateLimiter),
        eventPerPeer:      make(map[peer.ID]*rateLimiter), // For king rotation events
        cfg:               cfg,
        knownTxs:          make(map[common.Hash]time.Time),
        knownTxsLimit:     10000,

    // Database sync initialization
    dbSyncRequests:    make(map[string]*DBSyncRequest),
    dbSyncResponses:   make(map[string]*DBSyncResponse),
    dbSyncPeers:       make(map[string]*DBSyncStatus),
    dbSyncMetrics: &DBSyncMetrics{
        SyncAttempts:    0,
        SuccessfulSyncs: 0,
        FailedSyncs:     0,
        TotalRotations:  0,
        IsActive:        true,
        LastKingList:    []common.Address{},
    },
    dbSyncInterval:    120 * time.Second, // Sync every 2 minutes
    dbSyncEnabled:     true,
    dbSyncVersion:     "1.0.0",
    maxSyncRetries:    3,
    lastKingList:      []common.Address{},
    }

    // Set stream handlers
    h.SetStreamHandler("/antdchain/sync/1.0.0", node.handleStream)
    h.SetStreamHandler("/antdchain/direct/1.0.0", node.handleDirectPush)
    h.SetStreamHandler("/antdchain/king-config/1.0.0", node.handleKingConfigStream)

    // Initialize GossipSub
    ps, err := pubsub.NewGossipSub(ctx, h)
    if err != nil {
        h.Close()
        cancel()
        return nil, fmt.Errorf("failed to create pubsub: %w", err)
    }

    // Join database sync topic (create if it doesn't exist)
    dbSyncTopic, err := ps.Join("antdchain-db-sync-v1")
    if err != nil {
    h.Close()
    cancel()
    return nil, fmt.Errorf("failed to join DB sync topic: %w", err)
    }

    dbSub, err := dbSyncTopic.Subscribe()
    if err != nil {
    dbSyncTopic.Close()
    h.Close()
    cancel()
    return nil, fmt.Errorf("failed to subscribe to DB sync topic: %w", err)
    }

    // Join main topic
    topic, err := ps.Join("antdchain-blocks-txs-v1")
    if err != nil {
        h.Close()
        cancel()
        return nil, err
    }

    // Subscribe to main topic
    sub, err := topic.Subscribe()
    if err != nil {
        h.Close()
        cancel()
        return nil, err
    }

    // NEW: Join dedicated king rotation topic
    kingTopic, err := ps.Join("antdchain-king-rotations-v1")
    if err != nil {
        topic.Close()
        h.Close()
        cancel()
        return nil, fmt.Errorf("failed to join king rotation topic: %w", err)
    }
    kingSub, err := kingTopic.Subscribe()
    if err != nil {
        kingTopic.Close()
        topic.Close()
        h.Close()
        cancel()
        return nil, fmt.Errorf("failed to subscribe to king rotation topic: %w", err)
    }

    node.pubsub = ps
    node.topic = topic
    node.sub = sub
    node.kingTopic = kingTopic
    node.kingSub = kingSub

    node.dbSyncTopic = dbSyncTopic
    node.dbSyncSub = dbSub

    logger.Info("GossipSub initialized (main + king rotation topics)")

    // Connect to bootstrap peers
    if len(cfg.BootstrapPeers) > 0 {
        connected := node.connectToBootstrap(cfg.BootstrapPeers)
        if connected > 0 {
            logger.Infof("Connected to %d bootstrap peers", connected)
        } else {
            logger.Warn("Failed to connect to any bootstrap peers")
        }
    }

    // Start background tasks
    go node.handleMessages()
    go node.handleKingMessages()
    go node.PeriodicSyncCheck()
    go node.FastSyncCheck()
    go node.startConfigurationMonitor()
    // Start database sync handler
    go node.handleDBSyncMessages()

    // Start periodic database sync
    go node.periodicDBSync()
    go node.StartPeriodicKingListSync()
    // Announce our database sync capabilities
    go node.announceDBSyncCapabilities()
    go node.syncKingConfigurationOnStartup()
    node.logger.Info("Database synchronization initialized")
    go node.cleanupKnownTxs()
    go node.startKingListCleanup()
    go func() {
        time.Sleep(2 * time.Second)
        //node.logger.Warn("STARTUP: Triggering immediate sync check")
        node.forceInitialSync()
    }()

    if cfg.EnableMDNS {
        go node.startMDNSDiscovery()
    }
    if cfg.EnableDHT {
        go node.startDHTDiscovery()
        go node.announceOnDHT()
    }

    go node.manageConnections()

    logger.Info("ANTDChain P2P node READY")

    go func() {
    time.Sleep(5 * time.Second) // Wait a bit for connections
    node.BroadcastCurrentKingConfig()
    }()
    go node.startKingListChangeDetector()
    // Start periodic configuration broadcasting
    go node.StartPeriodicConfigBroadcast()
    go node.startConfigurationHealthCheck()

    go node.startConfigurationSyncer()
    go node.StartPeriodicConfigBroadcast() // Change to every 1 minute
    go node.PeriodicKingConfigCheck()      // Already exists

    // Add this to ensure immediate configuration broadcast
    time.AfterFunc(3*time.Second, func() {
    node.logger.Info("🚀 Broadcasting initial king configuration")
    node.BroadcastCurrentKingConfig()
    })

    // Initialize checkpoints system
    checkpointsPath := filepath.Join(cfg.DataDir, "checkpoints.json")

    genesisHash := common.HexToHash("0x31dbbb638d6b5cb0f4350f3479fccd1a749a5313586744d8719a99d13715f539")

    cp, err := checkpoints.NewCheckpoints(cfg.DataDir, checkpointsPath, genesisHash)
    if err != nil {
          node.logger.Warnf("Failed to initialize checkpoints: %v", err)
    // Continue without checkpoints
    } else {
    // Add ban manager with checkpoint support
    node.IntegrateBanManagerWithCheckpoints(cp)
    node.logger.Infof("Checkpoints initialized with genesis hash: %s", genesisHash.Hex())
    }

    return node, nil
}


func (n *Node) Stop() {
    n.logger.Info("Stopping P2P node...")

    // Cancel context first
    if n.cancel != nil {
        n.cancel()
    }

    // Disable database sync
    n.EnableDatabaseSync(false)

    // Close database sync topic
    if n.dbSyncTopic != nil {
        if err := n.dbSyncTopic.Close(); err != nil {
            n.logger.Warnf("Error closing DB sync topic: %v", err)
        }
    }
    if n.dbSyncSub != nil {
        n.dbSyncSub.Cancel()
    }

    // Close main pubsub topic and subscription
    if n.topic != nil {
        if err := n.topic.Close(); err != nil {
            n.logger.Warnf("Error closing main topic: %v", err)
        }
    }
    if n.sub != nil {
        n.sub.Cancel()
    }

    // Close dedicated king rotation topic and subscription
    if n.kingTopic != nil {
        if err := n.kingTopic.Close(); err != nil {
            n.logger.Warnf("Error closing king rotation topic: %v", err)
        }
    }
    if n.kingSub != nil {
        n.kingSub.Cancel()
    }

    // Close DHT
    if n.dht != nil {
        if err := n.dht.Close(); err != nil {
            n.logger.Warnf("Error closing DHT: %v", err)
        }
    }

    // Close host
    if n.host != nil {
        if err := n.host.Close(); err != nil {
            n.logger.Warnf("Error closing libp2p host: %v", err)
        }
    }

    n.logger.Info("P2P node stopped gracefully")
}

func (n *Node) SyncWithPeer(pid peer.ID) {
    go n.syncIfBehind(pid)
}

func (n *Node) ForceBootstrap() {
    if n.cfg.BootstrapPeers != nil {
        n.connectToBootstrap(n.cfg.BootstrapPeers)
    }
}

func (n *Node) ForceDHTAnnounce() {
    go n.announceOnDHT()
}

// quickFindDivergence quickly finds where chains diverged
func (n *Node) quickFindDivergence(peerID peer.ID, maxHeight uint64) (uint64, error) {
    n.logger.Debugf("Quick find divergence up to height %d", maxHeight)

    // Check recent blocks first (forks are usually recent)
    for offset := uint64(0); offset < 10 && maxHeight >= offset; offset++ {
        h := maxHeight - offset

        local := n.chain.GetBlock(h)
        if local == nil {
            continue
        }

        ctx, cancel := context.WithTimeout(n.ctx, 5*time.Second)
        defer cancel()

        // Quick fetch with timeout
        peerBlk, err := n.requestBlockWithContext(ctx, peerID, h)
        if err != nil {
            continue
        }

        if local.Hash() == peerBlk.Hash() {
            n.logger.Infof("Found match at height %d", h)
            return h, nil
        }
    }

    // If no recent match, assume we need to go back to genesis
    n.logger.Warn("No recent match found, truncating to genesis")
    return 0, nil
}

// Fetches a block from a peer with a timeout context
func (n *Node) requestBlockWithContext(ctx context.Context, peerID peer.ID, blockNumber uint64) (*block.Block, error) {
    s, err := n.host.NewStream(ctx, peerID, "/antdchain/sync/1.0.0")
    if err != nil {
        return nil, fmt.Errorf("failed to open stream to %s: %w", peerID, err)
    }
    defer s.Close()

    // Set a reasonable deadline on the stream
    deadline := time.Now().Add(15 * time.Second)
    s.SetDeadline(deadline)

    rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))

    // Send block number request
    if err := binary.Write(rw, binary.BigEndian, blockNumber); err != nil {
        return nil, fmt.Errorf("failed to send block request: %w", err)
    }

    // Flush to ensure data is sent
    if err := rw.Flush(); err != nil {
        return nil, fmt.Errorf("failed to flush request: %w", err)
    }

    // Read response length
    var length uint32
    if err := binary.Read(rw, binary.BigEndian, &length); err != nil {
        // Check if context was cancelled
        if ctx.Err() != nil {
            return nil, fmt.Errorf("context cancelled while reading length: %w", ctx.Err())
        }
        return nil, fmt.Errorf("failed to read block length: %w", err)
    }

    if length == 0 {
        return nil, fmt.Errorf("block %d not found on peer", blockNumber)
    }

    // Read block data
    data := make([]byte, length)
    if _, err := io.ReadFull(rw, data); err != nil {
        if ctx.Err() != nil {
            return nil, fmt.Errorf("context cancelled while reading data: %w", ctx.Err())
        }
        return nil, fmt.Errorf("failed to read block data: %w", err)
    }

    // Parse block
    var blk block.Block
    if err := json.Unmarshal(data, &blk); err != nil {
        return nil, fmt.Errorf("failed to unmarshal block: %w", err)
    }

    // Validate block number matches
    if blk.Header.Number.Uint64() != blockNumber {
        return nil, fmt.Errorf("block number mismatch: requested %d, got %d",
            blockNumber, blk.Header.Number.Uint64())
    }

    // Optional: Validate checkpoint (only if you have checkpoints)
    if checkpoints := n.chain.Checkpoints(); checkpoints != nil {
        if err := checkpoints.ValidateBlock(blockNumber, blk.Hash()); err != nil {
            return nil, fmt.Errorf("checkpoint validation failed: %w", err)
        }
    }

    return &blk, nil
}

func (n *Node) findDivergencePoint(peerID peer.ID, maxHeight uint64) (uint64, error) {
    n.logger.Infof("Finding divergence point up to height %d", maxHeight)

    // If we're at genesis or close to it, just return 0
    if maxHeight <= 10 {
        n.logger.Debugf("Low height %d, assuming genesis is common", maxHeight)
        return 0, nil
    }

    // Try a few strategic heights first (more efficient)
    checkHeights := []uint64{
        maxHeight,                    // Latest block
        maxHeight - 1,                // Previous block
        maxHeight - 10,               // 10 blocks back
        maxHeight / 2,                // Middle of chain
        maxHeight / 4,                // Quarter point
        100, 50, 25, 10, 5, 1, 0,     // Fixed checkpoints
    }

    for _, h := range checkHeights {
        if h > maxHeight {
            continue
        }

        local := n.chain.GetBlock(h)
        if local == nil {
            continue
        }

        n.logger.Debugf("Checking height %d for divergence", h)
        peerBlk, err := n.RequestBlockSync(peerID, h)
        if err != nil {
            n.logger.Debugf("Failed to fetch block %d: %v", h, err)
            continue
        }

        if local.Hash() == peerBlk.Hash() {
            n.logger.Infof("Found matching block at height %d", h)
            return h, nil
        } else {
            n.logger.Debugf("Blocks differ at height %d", h)
        }
    }

    // If we haven't found a match, do a quick binary search
    return n.binarySearchDivergence(peerID, maxHeight)
}

// binarySearchDivergence does a quick binary search for divergence
func (n *Node) binarySearchDivergence(peerID peer.ID, maxHeight uint64) (uint64, error) {
    n.logger.Debugf("Binary search for divergence up to height %d", maxHeight)

    low := uint64(0)
    high := maxHeight
    lastMatch := uint64(0)

    for low <= high && (high-low) > 1 {
        mid := (low + high) / 2

        local := n.chain.GetBlock(mid)
        if local == nil {
            // No local block at mid, search lower half
            high = mid - 1
            continue
        }

        peerBlk, err := n.RequestBlockSync(peerID, mid)
        if err != nil {
            // Can't fetch, assume divergence at or before mid
            high = mid - 1
            continue
        }

        if local.Hash() == peerBlk.Hash() {
            // Match at mid, search upper half
            lastMatch = mid
            low = mid + 1
        } else {
            // Divergence at or before mid, search lower half
            high = mid - 1
        }
    }

    n.logger.Infof("Binary search found last match at height %d", lastMatch)
    return lastMatch, nil
}

// Add this function to p2p.go
func (n *Node) PeriodicSyncCheck() {
    ticker := time.NewTicker(60 * time.Second) // Check every minute
    defer ticker.Stop()

    for {
        select {
        case <-n.ctx.Done():
            return
        case <-ticker.C:
            // If we're already syncing, skip
            if n.chain.IsSyncing() {
                continue
            }

            // Check if we're behind any peer
            for _, pid := range n.Peers() {
                peerHeight, err := n.GetPeerHeight(pid)
                if err != nil {
                    continue
                }

                localHeight := n.currentHeight()
                if peerHeight > localHeight+5 { // If behind by more than 5 blocks
                    n.logger.Warnf("Periodic check: behind peer %s by %d blocks → triggering sync",
                        pid.String()[:12], peerHeight-localHeight)
                    n.chain.StartSync(peerHeight)
                    go n.syncIfBehind(pid)
                    break // Only trigger with one peer
                }
            }
        }
    }
}

func (n *Node) triggerSync() {
    peers := n.Peers()
    if len(peers) == 0 {
        n.logger.Warn("No peers available for sync")
        return
    }

    // Try each peer
    for _, pid := range peers {
        go func(pid peer.ID) {
            height, err := n.GetPeerHeight(pid)
            if err != nil {
                return
            }

            localHeight := n.currentHeight()
            if height > localHeight {
                n.logger.Infof("Triggering sync with peer %s (height: %d)",
                    pid.String()[:12], height)
                n.chain.StartSync(height)
                n.syncIfBehind(pid)
            }
        }(pid)
    }
}

func (n *Node) syncMissingBlocks(peerID peer.ID, targetHeight uint64) error {
    localHeight := n.currentHeight()

    n.logger.Infof("SYNC START → %d blocks needed (%d → %d)",
        targetHeight-localHeight, localHeight+1, targetHeight)

    failures := 0
    for height := localHeight + 1; height <= targetHeight; height++ {
        select {
        case <-n.ctx.Done():
            return n.ctx.Err()
        default:
        }

        // CHECK FIRST: Do we already have this block?
        if existing := n.chain.GetBlock(height); existing != nil {
            n.logger.Debugf("Already have block %d, skipping", height)
            continue // Skip to next block
        }

        n.logger.Debugf("Fetching block %d/%d", height, targetHeight)

        ctx, cancel := context.WithTimeout(n.ctx, 12*time.Second)
        blk, err := n.requestBlockWithContext(ctx, peerID, height)
        cancel()

        if err != nil {
            n.logger.Warnf("Failed to fetch block %d: %v", height, err)
            failures++
            if failures > 10 {
                return fmt.Errorf("too many failures")
            }
            height-- // retry same block
            time.Sleep(300 * time.Millisecond)
            continue
        }

        failures = 0

        n.logger.Debugf("Got block %d, adding to chain...", height)

        err = n.chain.AddBlock(blk)
        if err != nil {
            if strings.Contains(strings.ToLower(err.Error()), "already") ||
               strings.Contains(err.Error(), "known") ||
               strings.Contains(err.Error(), "duplicate") {
                // Block was added by gossip between check and add
                n.logger.Debugf("Block %d added via gossip while syncing — skipping", height)
                continue
            }
            if ancestorHeight, ok := parseParentBranchMissingAncestor(err); ok {
                localHeightNow := n.currentHeight()
                if localHeightNow > ancestorHeight {
                    n.logger.Warnf("Detected branch mismatch while syncing block %d; rewinding local chain from %d to %d",
                        height, localHeightNow, ancestorHeight)
                    if truncErr := n.chain.TruncateTo(ancestorHeight); truncErr != nil {
                        n.logger.Warnf("Failed to truncate chain to %d after branch mismatch: %v", ancestorHeight, truncErr)
                    } else {
                        // Restart fetching from the ancestor height (loop increments by 1).
                        height = ancestorHeight
                        time.Sleep(150 * time.Millisecond)
                        continue
                    }
                }
            }

            n.logger.Warnf("AddBlock failed for %d: %v", height, err)
            time.Sleep(200 * time.Millisecond)
            height-- // retry
            continue
        }

        n.logger.Infof("Synced block %d", height)

        // Progress log
        if height%20 == 0 || height == targetHeight {
            n.logger.Infof("Sync progress: %d/%d (%.1f%%)",
                height-localHeight, targetHeight-localHeight,
                float64(height-localHeight)/float64(targetHeight-localHeight)*100)
        }

        time.Sleep(25 * time.Millisecond)
    }

    // SUCCESS
    n.chain.StopSync()
    n.logger.Infof("SYNC COMPLETE — at height %d", n.currentHeight())
    return nil
}

func min(a, b time.Duration) time.Duration {
    if a < b {
        return a
    }
    return b
}

var parentBranchMissingErrRE = regexp.MustCompile(`parent branch missing at height ([0-9]+)`)

func parseParentBranchMissingAncestor(err error) (uint64, bool) {
    if err == nil {
        return 0, false
    }

    matches := parentBranchMissingErrRE.FindStringSubmatch(strings.ToLower(err.Error()))
    if len(matches) != 2 {
        return 0, false
    }

    var missingHeight uint64
    if _, scanErr := fmt.Sscanf(matches[1], "%d", &missingHeight); scanErr != nil {
        return 0, false
    }

    if missingHeight == 0 {
        return 0, true
    }
    return missingHeight - 1, true
}

// Finds common ancestor when chains diverge
func (n *Node) resolveFork(peerID peer.ID, maxHeight uint64, ctx context.Context) (uint64, error) {
    n.logger.Infof("🔍 Resolving fork, checking up to height %d", maxHeight)

    // Try to find a matching block quickly
    // Check recent blocks first (most likely place for match)
    for offset := uint64(0); offset < 20 && maxHeight >= offset; offset++ {
        h := maxHeight - offset

        // Get local block
        n.processMu.Lock()
        local := n.chain.GetBlock(h)
        n.processMu.Unlock()

        if local == nil {
            continue
        }

        // Try to get peer block
        blockCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
        peerBlk, err := n.requestBlockWithContext(blockCtx, peerID, h)
        cancel()

        if err != nil {
            n.logger.Debugf("Cannot fetch block %d: %v", h, err)
            continue
        }

        if local.Hash() == peerBlk.Hash() {
            n.logger.Infof("Found matching block at height %d", h)
            return h, nil
        }
    }

    // Check strategic points
    checkPoints := []uint64{maxHeight / 2, maxHeight / 4, 100, 50, 10, 1, 0}
    for _, h := range checkPoints {
        if h > maxHeight {
            continue
        }

        n.processMu.Lock()
        local := n.chain.GetBlock(h)
        n.processMu.Unlock()

        if local == nil {
            continue
        }

        blockCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
        peerBlk, err := n.requestBlockWithContext(blockCtx, peerID, h)
        cancel()

        if err == nil && local.Hash() == peerBlk.Hash() {
            n.logger.Infof("Found matching block at height %d", h)
            return h, nil
        }
    }

    // No match found, use genesis
    n.logger.Warn("No common ancestor found, defaulting to genesis")
    return 0, nil
}

// Check if we're making progress
func (n *Node) isSyncProgressing(startHeight uint64, currentHeight uint64) bool {
    if currentHeight > startHeight {
        return true
    }

    // Check if we've added any blocks in the last 30 seconds
    // (We will need to track this with timestamps, continue for now)
    return false
}

// Finds where chains diverged
func (n *Node) quickFindCommonAncestor(peerID peer.ID, maxHeight uint64) (uint64, error) {
    n.logger.Infof("Looking for common ancestor up to height %d", maxHeight)

    // Check a few recent heights first
    for offset := uint64(0); offset < 10 && maxHeight >= offset; offset++ {
        h := maxHeight - offset

        n.processMu.Lock()
        local := n.chain.GetBlock(h)
        n.processMu.Unlock()

        if local == nil {
            continue
        }

        ctx, cancel := context.WithTimeout(n.ctx, 5*time.Second)
        peerBlk, err := n.requestBlockWithContext(ctx, peerID, h)
        cancel()

        if err == nil && local.Hash() == peerBlk.Hash() {
            n.logger.Infof("Found common block at height %d", h)
            return h, nil
        }
    }

    // Default to genesis
    n.logger.Warn("No common ancestor found, using genesis")
    return 0, nil
}

func (n *Node) processBlock(blk *block.Block) error {
    if blk == nil || blk.Header == nil {
        return errors.New("nil block or header")
    }

    num := blk.Header.Number.Uint64()
    hash := blk.Hash()

    // Early duplicate check
    if existing := n.chain.GetBlock(num); existing != nil && existing.Hash() == hash {
        n.logger.Debugf("Block %d already in chain", num)
        return nil
    }

    n.processMu.Lock()
    defer n.processMu.Unlock()

    // Get current chain state
    tip := n.chain.Latest()
    currentHeight := uint64(0)
    expectedParent := common.Hash{}
    if tip != nil {
        currentHeight = tip.Header.Number.Uint64()
        expectedParent = tip.Hash()
    }

    // FAST PATH: Direct chain extension
    if blk.Header.ParentHash == expectedParent && num == currentHeight+1 {
        err := n.chain.AddBlock(blk)

        if err == nil {
            n.logger.Infof("Added block %d via gossip (direct extension)", num)

            // Trigger rotating king database sync for this block
            go n.syncRotatingKingForBlock(num)

            // Also check if we need to sync configuration
            go n.checkAndSyncKingConfig()
            return nil
        }

        if err == nil {
            n.logger.Infof("Added block %d via gossip (direct extension)", num)
        go n.syncDatabaseForNewBlock(num, blk.Hash())

            return nil
        }

        // Handle specific errors
        if strings.Contains(err.Error(), "already") ||
           strings.Contains(err.Error(), "known") ||
           strings.Contains(err.Error(), "duplicate") {
            return nil // Already processed
        }

        n.logger.Warnf("Direct extension failed for block %d: %v", num, err)
        return err
    }

    // ORPHAN CHECK: Parent not in chain
    if !n.chain.HasBlock(blk.Header.ParentHash) {
        n.logger.Warnf("REJECTING ORPHAN: Block %d (parent %s not found)",
            num, blk.Header.ParentHash.Hex()[:8])

        // If this orphan is far ahead, we might need to sync
        if num > currentHeight+10 {
            n.logger.Warnf("Orphan block %d is %d blocks ahead - triggering sync",
                num, num-currentHeight)
            go n.triggerSync()
        }

        return fmt.Errorf("orphan block rejected: parent %s not found",
            blk.Header.ParentHash.Hex()[:8])
    }

    // BLOCK EXISTS AT SAME HEIGHT (fork)
    if existing := n.chain.GetBlock(num); existing != nil {
        if existing.Hash() == hash {
            return nil // Duplicate
        }
        n.logger.Warnf("REJECTING FORK: Different block at height %d (ours: %s, theirs: %s)",
            num, existing.Hash().Hex()[:8], hash.Hex()[:8])
        return fmt.Errorf("fork block rejected at height %d", num)
    }

    // BLOCK IS IN CHAIN BUT NOT DIRECT EXTENSION (gap during sync)
    // Check if we're in sync mode
    if bc, ok := n.chain.(interface{ IsSyncing() bool }); ok && bc.IsSyncing() {
        // Verify parent exists at height-1
        parent := n.chain.GetBlock(num - 1)
        if parent == nil {
            n.logger.Warnf("Gap during sync: parent at height %d missing", num-1)
            return fmt.Errorf("parent missing during sync")
        }

        if parent.Hash() != blk.Header.ParentHash {
            n.logger.Warnf("Wrong parent during sync: expected %s, got %s",
                parent.Hash().Hex()[:8], blk.Header.ParentHash.Hex()[:8])
            return fmt.Errorf("wrong parent during sync")
        }

        // Valid sync block
        err := n.chain.AddBlock(blk)
        if err != nil {
            if strings.Contains(err.Error(), "already") ||
               strings.Contains(err.Error(), "known") {
                return nil
            }
            n.logger.Warnf("Sync block %d rejected: %v", num, err)
            return err
        }

        n.logger.Infof("Added sync block %d via gossip", num)

        // Check if sync completed
        if syncBC, ok := n.chain.(interface{
            IsSyncing() bool
            GetSyncTarget() uint64
            StopSync()
        }); ok {
            if syncBC.IsSyncing() && num >= syncBC.GetSyncTarget() {
                syncBC.StopSync()
                n.logger.Info("Gossip caught us up — sync mode disabled")
            }
        }

        return nil
    }

    // BLOCK IS AHEAD BUT WE'RE NOT SYNCING
    if num > currentHeight+1 {
        n.logger.Warnf("Block %d ahead of us (we're at %d) but not in sync mode",
            num, currentHeight)
        return fmt.Errorf("block ahead but not syncing")
    }

    // BLOCK IS BEHIND OR STALE
    if num <= currentHeight {
        n.logger.Debugf("Stale block %d (we're at %d)", num, currentHeight)
        return fmt.Errorf("stale block")
    }

    // Should not reach here
    n.logger.Warnf("Unexpected block processing state: height=%d, hash=%s", num, hash.Hex()[:8])
    return fmt.Errorf("unexpected block state")
}


func (n *Node) syncIfBehind(pid peer.ID) {
    // Skip sync lock check for genesis nodes - we NEED to sync
    isGenesis := n.currentHeight() == 0

    if !isGenesis {
        // For non-genesis nodes, use normal checks
        if n.chain.IsSyncing() {
            n.logger.Debug("Already syncing, skipping")
            return
        }

        if !n.shouldAttemptSync() {
            return
        }
    }

    localHeight := n.currentHeight()
    peerHeight, err := n.GetPeerHeight(pid)
    if err != nil {
        n.logger.Debugf("Cannot get height from peer %s: %v", pid.String()[:12], err)
        return
    }

    if peerHeight <= localHeight {
        n.logger.Debugf("Not behind peer %s (local=%d, peer=%d)", pid.String()[:12], localHeight, peerHeight)
        if n.chain.IsSyncing() {
            n.chain.StopSync()
        }
        return
    }

    gap := peerHeight - localHeight

    if isGenesis {
        n.logger.Warnf("🚀 GENESIS SYNC: %d → %d with peer %s (gap: %d blocks)",
            localHeight, peerHeight, pid.String()[:12], gap)
    } else {
        n.logger.Warnf("Behind by %d blocks — starting sync with peer %s", gap, pid.String()[:12])
    }

    n.chain.StartSync(peerHeight)

    // Run sync in foreground
    err = n.syncMissingBlocks(pid, peerHeight)
    if err != nil {
        n.logger.Errorf("Sync failed with %s: %v", pid.String()[:12], err)
        if n.chain.IsSyncing() {
            n.chain.StopSync()
        }
        return
    }

    // Verify we caught up
    finalHeight := n.currentHeight()
    if finalHeight >= peerHeight {
        n.chain.StopSync()
        if isGenesis {
            n.logger.Warnf("✅ GENESIS SYNC COMPLETE: Now at height %d", finalHeight)
        } else {
            n.logger.Info("Sync completed successfully")
        }
    } else {
        n.logger.Warnf("Sync ended at %d but target was %d", finalHeight, peerHeight)
    }
}

func (n *Node) syncLoop() {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()

    lastSyncAttempt := time.Now()
    syncCooldown := 30 * time.Second

    for {
        select {
        case <-n.ctx.Done():
            return
        case <-ticker.C:
            // If we're at very low height, be more aggressive
            localHeight := n.currentHeight()
            if localHeight < 10 {
                syncCooldown = 10 * time.Second // Shorter cooldown for new nodes
            } else {
                syncCooldown = 30 * time.Second
            }

            // Skip if we just tried to sync
            if time.Since(lastSyncAttempt) < syncCooldown {
                continue
            }

            // Skip if already syncing
            if n.chain.IsSyncing() {
                continue
            }

            // Check peers
            for _, pid := range n.Peers() {
                height, err := n.GetPeerHeight(pid)
                if err != nil {
                    continue
                }

                gap := height - localHeight

                // Be more aggressive for new/low nodes
                threshold := uint64(5)
                if localHeight < 10 {
                    threshold = 1 // Sync if even 1 block behind
                }

                if gap > threshold {
                    n.logger.Infof("Gap detected: %d blocks behind (threshold=%d)", gap, threshold)
                    lastSyncAttempt = time.Now()
                    go n.syncIfBehind(pid)
                    break // Only sync with one peer
                }
            }
        }
    }
}


func (n *Node) shouldUseSyncMode(peerHeight, localHeight uint64) bool {
    gap := peerHeight - localHeight
    return gap > 50 // Only sync if VERY far behind
}

func (n *Node) cleanupKnownTxs() {
    ticker := time.NewTicker(5 * time.Minute)
    defer ticker.Stop()

    for {
        select {
        case <-n.ctx.Done():
            return
        case <-ticker.C:
            n.knownTxsMu.Lock()
            cutoff := time.Now().Add(-10 * time.Minute) // Keep for 10 minutes

            for hash, timestamp := range n.knownTxs {
                if timestamp.Before(cutoff) {
                    delete(n.knownTxs, hash)
                }
            }

            // Also enforce size limit
            if len(n.knownTxs) > n.knownTxsLimit {
                // delete half of entries
                count := 0
                for hash := range n.knownTxs {
                    delete(n.knownTxs, hash)
                    count++
                    if count >= n.knownTxsLimit/2 {
                        break
                    }
                }
            }
            n.knownTxsMu.Unlock()
            n.logger.Debugf("Cleaned up known transactions cache, now %d entries", len(n.knownTxs))
        }
    }
}

// Checks if we're behind and triggers sync immediately
func (n *Node) triggerImmediateSync() {
    // Don't trigger if already syncing
    if n.chain.IsSyncing() {
        return
    }

    // Check all peers immediately
    peers := n.Peers()
    for _, pid := range peers {
        height, err := n.GetPeerHeight(pid)
        if err != nil {
            continue
        }

        localHeight := n.currentHeight()
        if height > localHeight {
            n.logger.Warnf("IMMEDIATE SYNC TRIGGERED: Peer %s height=%d, our height=%d",
                pid.String()[:12], height, localHeight)
            n.chain.StartSync(height)
            go n.syncIfBehind(pid)
            break // Sync with first peer that's ahead
        }
    }
}

func (n *Node) FastSyncCheck() {
    // First check immediately on startup
    time.Sleep(3 * time.Second) // Give time for connections to establish
    n.logger.Warn("STARTUP: Initial sync check")
    n.forceInitialSync()

    // Then continue with periodic checks
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-n.ctx.Done():
            return
        case <-ticker.C:
            localHeight := n.currentHeight()

            // If we're at genesis, force sync
            if localHeight == 0 && !n.chain.IsSyncing() {
                n.logger.Warn("STILL AT GENESIS - forcing sync!")
                n.forceInitialSync()
                continue
            }

            // Normal sync check
            if n.chain.IsSyncing() {
                continue
            }

            // Check if we're behind any peer
            for _, pid := range n.Peers() {
                peerHeight, err := n.GetPeerHeight(pid)
                if err != nil {
                    continue
                }

                if peerHeight > localHeight {
                    n.logger.Warnf("Behind peer %s: %d -> %d",
                        pid.String()[:12], localHeight, peerHeight)
                    go n.syncIfBehind(pid)
                    break
                }
            }
        }
    }
}

// Ensures we sync even if we think we're already syncing
func (n *Node) forceSync() {
    n.logger.Warn("FORCE SYNC triggered")

    // Force stop any existing sync
    if n.chain.IsSyncing() {
        n.chain.StopSync()
        time.Sleep(100 * time.Millisecond)
    }

    // Get best peer
    var bestPeer peer.ID
    var bestHeight uint64

    for _, pid := range n.Peers() {
        height, err := n.GetPeerHeight(pid)
        if err != nil {
            continue
        }

        if height > bestHeight {
            bestHeight = height
            bestPeer = pid
        }
    }

    if bestHeight == 0 {
        n.logger.Warn("No peers with height > 0")
        return
    }

    localHeight := n.currentHeight()
    if bestHeight <= localHeight {
        n.logger.Infof("Already at or ahead of peers: %d vs %d", localHeight, bestHeight)
        return
    }

    n.logger.Warnf("FORCE SYNC: %d -> %d with peer %s",
        localHeight, bestHeight, bestPeer.String()[:12])

    n.chain.StartSync(bestHeight)

    // Don't use goroutine - run sync in foreground
    err := n.syncMissingBlocks(bestPeer, bestHeight)
    if err != nil {
        n.logger.Errorf("Force sync failed: %v", err)
    }
}

func (n *Node) cleanupAfterSync() {
    n.mu.Lock()
    defer n.mu.Unlock()

    // Log sync completion metrics
    currentHeight := n.currentHeight()

    n.logger.Infof("Sync cleanup completed at height %d", currentHeight)

    // Clean up known transactions cache (if you want to)
    n.knownTxsMu.Lock()
    cutoff := time.Now().Add(-30 * time.Minute)
    count := 0
    for hash, timestamp := range n.knownTxs {
        if timestamp.Before(cutoff) {
            delete(n.knownTxs, hash)
            count++
        }
    }
    n.knownTxsMu.Unlock()

    if count > 0 {
        n.logger.Debugf("Cleaned %d old transactions from cache", count)
    }

    // Reset sync attempt counter
    n.syncAttempts = 0
}

func (n *Node) shouldAttemptSync() bool {
    // Don't sync if we just tried
    if time.Since(n.lastSyncTime) < 10*time.Second {
        return false
    }

    // Don't sync too many times in a row
    if n.syncAttempts > 3 {
        n.logger.Warn("Too many sync attempts recently, cooling down")
        return false
    }

    return true
}

func (n *Node) recordSyncAttempt(pid peer.ID) {
    n.lastSyncTime = time.Now()
    n.syncAttempts++
    n.lastSyncPeer = pid

    // Reset attempts after 30 seconds
    time.AfterFunc(30*time.Second, func() {
        n.syncAttempts = 0
    })
}

func (n *Node) forceInitialSync() {
   // n.logger.Warn("FORCE INITIAL SYNC: Checking all peers")

    // Check all peers and find the highest one
    var bestPeer peer.ID
    var bestHeight uint64

    for _, pid := range n.Peers() {
        height, err := n.GetPeerHeight(pid)
        if err != nil {
            n.logger.Debugf("Can't get height from %s: %v", pid.String()[:12], err)
            continue
        }

        if height > bestHeight {
            bestHeight = height
            bestPeer = pid
        }
    }

    if bestHeight == 0 {
        n.logger.Warn("No peers with blocks found")
        return
    }

    n.logger.Warnf("INITIAL SYNC: Found peer %s at height %d",
        bestPeer.String()[:12], bestHeight)

    // Force sync with this peer
    n.syncIfBehind(bestPeer)
}

func (n *Node) allowEventFromPeer(pid peer.ID) bool {
    n.eventPerPeerMu.Lock()
    defer n.eventPerPeerMu.Unlock()

    rl := n.eventPerPeer[pid]
    if rl == nil {
        rl = &rateLimiter{resetTime: time.Now()}
        n.eventPerPeer[pid] = rl
    }

    if time.Since(rl.resetTime) > time.Second {
        rl.count = 0
        rl.resetTime = time.Now()
    }

    rl.count++
    return rl.count <= 5 // Max 5 events/sec per peer
}

func (n *Node) BroadcastKingRotation(event *rotatingking.KingRotation) error {
    if n.kingTopic == nil {
        return errors.New("king topic not initialized")
    }

    data, err := json.Marshal(event)
    if err != nil {
        return fmt.Errorf("failed to marshal rotation event: %w", err)
    }

    msg := make([]byte, 1+len(data))
    msg[0] = msgTypeKingRotation
    copy(msg[1:], data)

    if err := n.kingTopic.Publish(n.ctx, msg); err != nil {
        return fmt.Errorf("failed to publish rotation event: %w", err)
    }

    n.logger.Infof("Broadcast forced rotation: %s → %s at height %d",
        event.PreviousKing.Hex()[:8], event.NewKing.Hex()[:8], event.BlockHeight)
    return nil
}

func (n *Node) BroadcastKingListUpdate(event *rotatingking.KingListUpdateEvent) error {

    if event.BlockHeight == 0 {
        return errors.New("cannot broadcast rotation with height 0")
    }

    n.logger.Info("Broadcasting king list update")

    if n.kingTopic == nil {
        return errors.New("king topic not initialized")
    }

    data, err := json.Marshal(event)
    if err != nil {
        return err
    }

    msg := make([]byte, 1+len(data))
    msg[0] = msgTypeKingListUpdate
    copy(msg[1:], data)

    return n.kingTopic.Publish(n.ctx, msg)
}

func (n *Node) handleKingMessages() {
    for {
        msg, err := n.kingSub.Next(n.ctx)
        if err != nil {
            if n.ctx.Err() == nil {
                n.logger.Errorf("King subscription error: %v", err)
            }
            return
        }
        if msg.GetFrom() == n.host.ID() || len(msg.Data) < 1 {
            continue
        }

        switch msg.Data[0] {
        case msgTypeKingListUpdate:
            // Check if this is a configuration message
            if len(msg.Data) > 1 {
                var data map[string]interface{}
                if json.Unmarshal(msg.Data[1:], &data) == nil {
                    if configType, ok := data["type"].(string); ok && configType == "king_config" {
                        n.handleKingConfig(msg.Data[1:], msg.GetFrom())
                        continue
                    }
                }
            }
            // Handle regular list update
            n.handleKingListUpdate(msg)

        case msgTypeKingRotation:
            n.handleKingRotation(msg)
        }
    }
}

func (n *Node) logConnections() {
    peers := n.Peers()
    n.logger.Infof("Connected to %d peers:", len(peers))
    for _, pid := range peers {
        n.logger.Infof("  - %s", pid.String())
    }
}

// Processes database sync messages
func (n *Node) handleDBSyncMessages() {
    for {
        msg, err := n.dbSyncSub.Next(n.ctx)
        if err != nil {
            if n.ctx.Err() == nil {
                n.logger.Errorf("DB sync subscription error: %v", err)
            }
            return
        }
        if msg.GetFrom() == n.host.ID() || len(msg.Data) < 1 {
            continue
        }

        // Apply rate limiting for DB sync messages
        if !n.allowEventFromPeer(msg.GetFrom()) {
            n.logger.Debugf("Rate limiting DB sync message from %s", msg.GetFrom().String()[:8])
            continue
        }

        switch msg.Data[0] {
        case msgTypeDBSyncRequest:
            n.handleDBSyncRequest(msg)
        case msgTypeDBSyncResponse:
            n.handleDBSyncResponse(msg)
        case msgTypeDBSyncStatus:
            n.handleDBSyncStatus(msg)
        case msgTypeDBSyncAnnounce:
            n.handleDBSyncAnnounce(msg)
        }
    }
}

func (n *Node) handleDBSyncRequest(msg *pubsub.Message) {
    if !n.dbSyncEnabled {
        return
    }

    n.logger.Debug("Received database sync request")

    var req DBSyncRequest
    if err := json.Unmarshal(msg.Data[1:], &req); err != nil {
        n.logger.Warnf("Failed to unmarshal DB sync request: %v", err)
        return
    }

    if req.RequestType == "config" {
        n.handleConfigSyncRequest(&req, msg.GetFrom())
        return
    }

    // Check if this is a duplicate request
    n.dbSyncMu.RLock()
    _, exists := n.dbSyncRequests[req.RequestID]
    n.dbSyncMu.RUnlock()

    if exists {
        n.logger.Debugf("Duplicate DB sync request %s", req.RequestID[:8])
        return
    }

    // Store the request
    n.dbSyncMu.Lock()
    n.dbSyncRequests[req.RequestID] = &req
    n.dbSyncMu.Unlock()

    // Process the request
    go n.processDBSyncRequest(&req, msg.GetFrom())
}

// Find where response is being used:
func (n *Node) processDBSyncRequest(req *DBSyncRequest, requester peer.ID) {
    n.logger.Infof("Processing DB sync request %s from %s for blocks %d-%d",
        req.RequestID[:8], requester.String()[:8], req.FromHeight, req.ToHeight)

    // Get rotating king manager
    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    n.processMu.Unlock()

    if mgr == nil {
        n.logger.Warn("Cannot process DB sync request: rotating king manager not available")
        n.sendDBSyncErrorResponse(requester, req.RequestID, "rotating king manager not available")
        return
    }

    // Initialize response variable FIRST
    response := DBSyncResponse{
        RequestID:   req.RequestID,
        Timestamp:   time.Now().Unix(),
        LatestBlock: n.currentHeight(),
        PeerID:      n.host.ID().String(),
        Status:      "success", // Default status
    }

    // Validate request range
    if req.ToHeight < req.FromHeight {
        response.Status = "error"
        response.Error = "invalid range: toHeight < fromHeight"
        n.sendDBSyncResponse(requester, response)
        return
    }

    // Limit response size (max 1000 rotations per response)
    maxHeight := req.ToHeight
    if maxHeight > req.FromHeight+1000 {
        maxHeight = req.FromHeight + 1000
        n.logger.Debugf("Limiting response to %d rotations", 1000)
    }

    // Get rotation events from database
    if manager, ok := mgr.(interface{
        GetRotationHistoryFromDB(fromBlock, toBlock uint64) ([]rotatingking.KingRotation, error)
    }); ok {
        rotations, err := manager.GetRotationHistoryFromDB(req.FromHeight, maxHeight)
        if err != nil {
            response.Status = "error"
            response.Error = fmt.Sprintf("database error: %v", err)
        } else {
            response.Status = "success"
            response.Rotations = rotations

            // Get configuration if this is a full sync request
    if configManager, ok := mgr.(interface{ GetConfig() rotatingking.RotatingKingConfig }); ok {
        config := configManager.GetConfig()
        response.Config = &config
    } else if addrManager, ok := mgr.(interface{ GetKingAddresses() []common.Address }); ok {
        // Fallback: create basic config from addresses
        addresses := addrManager.GetKingAddresses()
        config := rotatingking.RotatingKingConfig{
            KingAddresses: addresses,
            RotationInterval: 100, // Default
            MinStakeRequired: rotatingking.EligibilityThreshold,
        }
        response.Config = &config
    }

            // Get sync state
            if syncManager, ok := mgr.(interface{ GetSyncState() (*rotatingking.SyncState, error) }); ok {
                syncState, err := syncManager.GetSyncState()
                if err == nil {
                    response.SyncState = syncState
                }
            }

            n.logger.Debugf("Sending %d rotations in response to %s", len(rotations), requester.String()[:8])
        }
    } else {
        response.Status = "error"
        response.Error = "database access not available"
    }

    // Send response
    n.sendDBSyncResponse(requester, response)

    // Update metrics
    n.dbSyncMu.Lock()
    if response.Status == "success" {
        n.dbSyncMetrics.SuccessfulSyncs++
        n.dbSyncMetrics.TotalRotations += len(response.Rotations)
    } else {
        n.dbSyncMetrics.FailedSyncs++
    }
    n.dbSyncMu.Unlock()
}


func (n *Node) handleDBSyncResponse(msg *pubsub.Message) {
    var resp DBSyncResponse
    if err := json.Unmarshal(msg.Data[1:], &resp); err != nil {
        n.logger.Warnf("Failed to unmarshal DB sync response: %v", err)
        return
    }

    // If response contains configuration
    if resp.Config != nil {
        n.logger.Infof("Received configuration from peer %s with %d addresses",
            msg.GetFrom().String()[:8], len(resp.Config.KingAddresses))

        // Compare with our configuration
        n.processMu.Lock()
        mgr := n.chain.GetRotatingKingManager()
        n.processMu.Unlock()
    //    n.applyKingConfiguration(resp.Config, msg.GetFrom())
        if mgr != nil {
            var ourAddresses []common.Address
            if manager, ok := mgr.(interface{ GetKingAddresses() []common.Address }); ok {
                ourAddresses = manager.GetKingAddresses()
            }

            // If peer has more addresses, adopt their configuration
            if len(resp.Config.KingAddresses) > len(ourAddresses) {
                n.logger.Warnf("🔄 Adopting peer configuration: %d addresses > our %d",
                    len(resp.Config.KingAddresses), len(ourAddresses))

                n.applyKingConfiguration(resp.Config, msg.GetFrom())
            } else if len(resp.Config.KingAddresses) < len(ourAddresses) {
                n.logger.Infof("Peer has fewer addresses (%d) than us (%d), keeping ours",
                    len(resp.Config.KingAddresses), len(ourAddresses))
            }
            n.applyKingConfiguration(resp.Config, msg.GetFrom())
        }
    }
}

func (n *Node) handleDBSyncStatus(msg *pubsub.Message) {
    var status DBSyncStatus
    if err := json.Unmarshal(msg.Data[1:], &status); err != nil {
        n.logger.Warnf("Failed to unmarshal DB sync status: %v", err)
        return
    }

    n.dbSyncMu.Lock()
    n.dbSyncPeers[status.PeerID] = &status
    n.dbSyncMu.Unlock()

    n.logger.Debugf("Peer %s DB status: synced to %d, isSyncing=%v, kings=%d",
        status.PeerID[:8], status.LastSyncedBlock, status.IsSyncing, status.KingCount)
}

func (n *Node) handleDBSyncAnnounce(msg *pubsub.Message) {
    var announce DBSyncAnnounce
    if err := json.Unmarshal(msg.Data[1:], &announce); err != nil {
        n.logger.Warnf("Failed to unmarshal DB sync announce: %v", err)
        return
    }

    n.logger.Debugf("Peer %s announced DB sync capabilities: %v",
        announce.PeerID[:8], announce.Capabilities)
}

// Syncs database with peers
func (n *Node) periodicDBSync() {
    // Wait for initial startup and connections
    time.Sleep(30 * time.Second)

    n.logger.Info("🔄 Starting periodic database synchronization")

    ticker := time.NewTicker(n.dbSyncInterval) // Should be 2 minutes based on your code
    defer ticker.Stop()

    // Initial sync
    n.performDBSyncWithPeers()

    for {
        select {
        case <-n.ctx.Done():
            n.logger.Info("🛑 Stopping periodic database sync")
            return
        case <-ticker.C:
            if n.dbSyncEnabled {
                n.logger.Info("🔄 Running periodic database sync")
                n.performDBSyncWithPeers()
            }
        }
    }
}

// Syncs database with connected peers
func (n *Node) performDBSyncWithPeers() {
    if n.isDBSyncing {
        n.logger.Debug("Database sync already in progress, skipping")
        return
    }

    peers := n.Peers()
    if len(peers) == 0 {
        n.logger.Debug("No peers for database sync")
        return
    }

    n.logger.Infof("🔄 Starting database sync with %d peers", len(peers))

    // Get our current sync state
    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    n.processMu.Unlock()

    if mgr == nil {
        n.logger.Warn("Rotating king manager not available for DB sync")
        return
    }

    // Get our current database sync state
    var ourSyncState *rotatingking.SyncState
    if manager, ok := mgr.(interface{ GetSyncState() (*rotatingking.SyncState, error) }); ok {
        ourSyncState, _ = manager.GetSyncState()
    }

    // Get current blockchain height
    currentBlockHeight := n.currentHeight()

    // If we're already synced to current height, skip
    if ourSyncState != nil && ourSyncState.LastSyncedBlock >= currentBlockHeight {
        n.logger.Debugf("Database already synced to current height %d", currentBlockHeight)
        return
    }

    // Find the best peer to sync from
    bestPeer := n.selectBestSyncPeer()
    if bestPeer == "" {
        n.logger.Debug("No suitable peer found for database sync")
        return
    }

    n.logger.Infof("🔄 Syncing database from peer %s (current height: %d, db synced to: %d)",
        bestPeer[:8], currentBlockHeight,
        func() uint64 {
            if ourSyncState != nil {
                return ourSyncState.LastSyncedBlock
            }
            return 0
        }())

    // Start sync with best peer
    n.startDBSyncWithPeer(bestPeer, ourSyncState, currentBlockHeight)
}

// Selects the best peer to sync from
func (n *Node) selectBestSyncPeer() string {
    n.dbSyncMu.RLock()
    defer n.dbSyncMu.RUnlock()

    var bestPeer string
    var bestHeight uint64

    for peerID, status := range n.dbSyncPeers {
        // Skip if peer is syncing (they might be behind)
        if status.IsSyncing {
            continue
        }

        // Skip if version mismatch
        if status.Version != n.dbSyncVersion {
            continue
        }

        // Choose peer with highest last synced block
        if status.LastSyncedBlock > bestHeight {
            bestHeight = status.LastSyncedBlock
            bestPeer = peerID
        }
    }

    return bestPeer
}

// Starts database sync with a specific peer
func (n *Node) startDBSyncWithPeer(peerID string, ourState *rotatingking.SyncState, currentBlockHeight uint64) {
    n.dbSyncMu.Lock()
    n.isDBSyncing = true
    n.currentSyncPeer = peerID
    n.dbSyncMetrics.SyncAttempts++
    n.dbSyncMu.Unlock()

    defer func() {
        n.dbSyncMu.Lock()
        n.isDBSyncing = false
        n.currentSyncPeer = ""
        n.dbSyncMu.Unlock()
    }()

    // Convert string peerID to peer.ID
    pid, err := peer.Decode(peerID)
    if err != nil {
        n.logger.Warnf("Invalid peer ID %s: %v", peerID[:8], err)
        return
    }

    // Create request
    requestID := fmt.Sprintf("db-sync-%s-%d", n.host.ID().String()[:8], time.Now().UnixNano())

    req := DBSyncRequest{
        RequestID:   requestID,
        FromHeight:  0,
        ToHeight:    currentBlockHeight, // Sync to current blockchain height
        RequestType: "incremental",
        Timestamp:   time.Now().Unix(),
        PeerID:      n.host.ID().String(),
    }

    // If we have a sync state, request from where we left off
    if ourState != nil && ourState.LastSyncedBlock > 0 {
        req.FromHeight = ourState.LastSyncedBlock + 1
    }

    // Don't request if we're already caught up
    if req.FromHeight > req.ToHeight {
        n.logger.Debugf("Database already synced to height %d", req.ToHeight)
        return
    }

    n.logger.Infof("📥 Requesting DB sync from peer %s: blocks %d-%d",
        pid.String()[:8], req.FromHeight, req.ToHeight)

    // Send request
    n.sendDBSyncRequest(pid, req)

    // Wait for response with timeout
    if err := n.waitForDBSyncResponse(requestID, 30*time.Second); err != nil {
        n.logger.Warnf("❌ DB sync timeout with peer %s: %v", pid.String()[:8], err)
        n.dbSyncMu.Lock()
        n.dbSyncMetrics.FailedSyncs++
        n.dbSyncMu.Unlock()

        // Try another peer
        n.tryNextSyncPeer(peerID, ourState, currentBlockHeight)
        return
    }

    n.dbSyncMu.Lock()
    n.dbSyncMetrics.LastSyncTime = time.Now()
    n.lastDBSyncTime = time.Now()
    n.dbSyncMu.Unlock()

    n.logger.Infof("✅ Database sync completed with peer %s up to block %d",
        pid.String()[:8], req.ToHeight)
}

func (n *Node) tryNextSyncPeer(excludedPeer string, ourState *rotatingking.SyncState, currentBlockHeight uint64) {
    n.dbSyncMu.RLock()
    peers := make([]string, 0, len(n.dbSyncPeers))
    for peerID := range n.dbSyncPeers {
        if peerID != excludedPeer {
            peers = append(peers, peerID)
        }
    }
    n.dbSyncMu.RUnlock()

    if len(peers) > 0 {
        n.logger.Debugf("Trying next peer: %s", peers[0][:8])
        n.startDBSyncWithPeer(peers[0], ourState, currentBlockHeight)
    }
}

// Wait for a database sync response
func (n *Node) waitForDBSyncResponse(requestID string, timeout time.Duration) error {
    deadline := time.Now().Add(timeout)

    for time.Now().Before(deadline) {
        n.dbSyncMu.RLock()
        resp, exists := n.dbSyncResponses[requestID]
        n.dbSyncMu.RUnlock()

        if exists {
            if resp.Status == "success" {
                return nil
            }
            return fmt.Errorf("sync failed: %s", resp.Error)
        }

        time.Sleep(100 * time.Millisecond)
    }

    return fmt.Errorf("timeout waiting for response")
}

// Sends a database sync request via pubsub
func (n *Node) sendDBSyncRequest(peerID peer.ID, req DBSyncRequest) {
    data, err := json.Marshal(req)
    if err != nil {
        n.logger.Warnf("Failed to marshal DB sync request: %v", err)
        return
    }

    msg := make([]byte, 1+len(data))
    msg[0] = msgTypeDBSyncRequest
    copy(msg[1:], data)

    if err := n.dbSyncTopic.Publish(n.ctx, msg); err != nil {
        n.logger.Warnf("Failed to publish DB sync request: %v", err)
    }
}

// Sends a database sync response via pubsub
func (n *Node) sendDBSyncResponse(peerID peer.ID, resp DBSyncResponse) {
    data, err := json.Marshal(resp)
    if err != nil {
        n.logger.Warnf("Failed to marshal DB sync response: %v", err)
        return
    }

    msg := make([]byte, 1+len(data))
    msg[0] = msgTypeDBSyncResponse
    copy(msg[1:], data)

    if err := n.dbSyncTopic.Publish(n.ctx, msg); err != nil {
        n.logger.Warnf("Failed to publish DB sync response: %v", err)
    }
}

func (n *Node) sendDBSyncErrorResponse(peerID peer.ID, requestID string, errorMsg string) {
    resp := DBSyncResponse{
        RequestID: requestID,
        Status:    "error",
        Error:     errorMsg,
        Timestamp: time.Now().Unix(),
        PeerID:    n.host.ID().String(),
    }
    n.sendDBSyncResponse(peerID, resp)
}

// Announce our database sync capabilities
func (n *Node) announceDBSyncCapabilities() {
    // Wait for startup
    time.Sleep(10 * time.Second)

    ticker := time.NewTicker(5 * time.Minute)
    defer ticker.Stop()

    for {
        select {
        case <-n.ctx.Done():
            return
        case <-ticker.C:
            n.broadcastDBSyncStatus()
            n.broadcastDBSyncAnnounce()
        }
    }
}

func (n *Node) broadcastDBSyncAnnounce() {
    announce := DBSyncAnnounce{
        PeerID:       n.host.ID().String(),
        Capabilities: []string{"sync", "history", "backup"},
        SupportsSync: true,
        MaxBatchSize: 1000,
        Timestamp:    time.Now().Unix(),
    }

    data, err := json.Marshal(announce)
    if err != nil {
        n.logger.Warnf("Failed to marshal DB sync announce: %v", err)
        return
    }

    msg := make([]byte, 1+len(data))
    msg[0] = msgTypeDBSyncAnnounce
    copy(msg[1:], data)

    if err := n.dbSyncTopic.Publish(n.ctx, msg); err != nil {
        n.logger.Warnf("Failed to publish DB sync announce: %v", err)
    }
}

// Processes received rotation data and updates our database
func (n *Node) processReceivedRotations(rotations []rotatingking.KingRotation, sourcePeer string) {
    if len(rotations) == 0 {
        return
    }

    n.logger.Infof("Processing %d received rotations from peer %s", len(rotations), sourcePeer[:8])

    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    n.processMu.Unlock()

    if mgr == nil {
        return
    }

    // Sort rotations by block height (ensure chronological order)
    sort.Slice(rotations, func(i, j int) bool {
        return rotations[i].BlockHeight < rotations[j].BlockHeight
    })

    // Get current height to avoid processing future data
    currentHeight := n.currentHeight()

    processed := 0
    for _, rotation := range rotations {
        // Skip if too far in the future
        if rotation.BlockHeight > currentHeight+10 {
            n.logger.Debugf("Skipping future rotation at height %d", rotation.BlockHeight)
            continue
        }

        // Save to database if manager supports it
        if manager, ok := mgr.(interface{
            SaveRotationEvent(rotation *rotatingking.KingRotation) error
        }); ok {
            if err := manager.SaveRotationEvent(&rotation); err != nil {
                n.logger.Debugf("Failed to save rotation %d: %v", rotation.BlockHeight, err)
            } else {
                processed++
            }
        }
    }

    n.logger.Infof("Successfully processed %d/%d rotations from peer %s",
        processed, len(rotations), sourcePeer[:8])

    // Update metrics
    n.dbSyncMu.Lock()
    n.dbSyncMetrics.TotalRotations += processed
    n.dbSyncMu.Unlock()
}

// Returns current database sync status
func (n *Node) GetDatabaseSyncStatus() map[string]interface{} {
    n.dbSyncMu.RLock()
    defer n.dbSyncMu.RUnlock()

    status := map[string]interface{}{
        "enabled":           n.dbSyncEnabled,
        "version":           n.dbSyncVersion,
        "isSyncing":         n.isDBSyncing,
        "currentSyncPeer":   n.currentSyncPeer,
        "lastSyncTime":      n.lastDBSyncTime,
        "syncInterval":      n.dbSyncInterval.String(),
        "pendingRequests":   len(n.dbSyncRequests),
        "cachedResponses":   len(n.dbSyncResponses),
        "knownPeers":        len(n.dbSyncPeers),
        "syncRetryCount":    n.syncRetryCount,
    }

    // Add metrics
    if n.dbSyncMetrics != nil {
        status["metrics"] = map[string]interface{}{
            "syncAttempts":     n.dbSyncMetrics.SyncAttempts,
            "successfulSyncs":  n.dbSyncMetrics.SuccessfulSyncs,
            "failedSyncs":      n.dbSyncMetrics.FailedSyncs,
            "totalRotations":   n.dbSyncMetrics.TotalRotations,
            "bytesTransferred": n.dbSyncMetrics.BytesTransferred,
            "peerCount":        n.dbSyncMetrics.PeerCount,
            "isActive":         n.dbSyncMetrics.IsActive,
        }
    }

    // Get sync state from rotating king manager
    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    n.processMu.Unlock()

    if mgr != nil {
        if manager, ok := mgr.(interface{ GetSyncState() (*rotatingking.SyncState, error) }); ok {
            syncState, err := manager.GetSyncState()
            if err == nil && syncState != nil {
                status["databaseState"] = map[string]interface{}{
                    "lastSyncedBlock": syncState.LastSyncedBlock,
                    "lastSyncTime":    syncState.LastSyncTime,
                    "isSyncing":       syncState.IsSyncing,
                    "syncProgress":    syncState.SyncProgress,
                    "syncError":       syncState.SyncError,
                    "peerCount":       syncState.PeerCount,
                }
            }
        }
    }

    return status
}

// Enables or disables database synchronization
func (n *Node) EnableDatabaseSync(enabled bool) {
    n.dbSyncMu.Lock()
    n.dbSyncEnabled = enabled
    n.dbSyncMu.Unlock()

    if enabled {
        n.logger.Info("Database synchronization enabled")
        // Trigger immediate sync
        go n.performDBSyncWithPeers()
    } else {
        n.logger.Info("Database synchronization disabled")
    }
}

func (n *Node) getCurrentKingConfig(mgr interface{}) rotatingking.RotatingKingConfig {
    var config rotatingking.RotatingKingConfig

    // Try to get the full config from manager
    if configManager, ok := mgr.(interface{ GetConfig() rotatingking.RotatingKingConfig }); ok {
        config = configManager.GetConfig()
        if len(config.KingAddresses) > 0 {
            n.logger.Debugf("Got config with %d addresses via GetConfig()", len(config.KingAddresses))
            return config
        }
    }

    // Fallback: use GetKingAddresses()
    if addrManager, ok := mgr.(interface{ GetKingAddresses() []common.Address }); ok {
        addresses := addrManager.GetKingAddresses()
        n.logger.Debugf("Got %d addresses via GetKingAddresses()", len(addresses))

        if len(addresses) > 0 {
            // Create a config with the addresses and default values
            config = rotatingking.RotatingKingConfig{
                KingAddresses:    addresses,
                RotationInterval: 100, // Default from rotatingking
                RotationOffset:   0,
                ActivationDelay:  2,
                MinStakeRequired: rotatingking.EligibilityThreshold,
            }
            return config
        }
    }

    // Last resort: return default config
    n.logger.Warn("Could not get king addresses, returning default config")
    config = rotatingking.DefaultRotatingKingConfig()
    return config
}

func (n *Node) processReceivedConfig(config *rotatingking.RotatingKingConfig, sourcePeer string) {
    if config == nil || len(config.KingAddresses) == 0 {
        return
    }

    n.logger.Infof("Processing configuration from peer %s with %d kings",
        sourcePeer[:8], len(config.KingAddresses))

    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    n.processMu.Unlock()

    if mgr == nil {
        return
    }

    // Update configuration if manager supports it
    if manager, ok := mgr.(interface{ UpdateKingAddresses(newAddresses []common.Address) error }); ok {
        currentAddresses := []common.Address{}
        if currentManager, ok := mgr.(interface{ GetKingAddresses() []common.Address }); ok {
            currentAddresses = currentManager.GetKingAddresses()
        }

        // Only update if different
        if !n.areAddressListsEqual(currentAddresses, config.KingAddresses) {
            n.logger.Infof("Updating king list from %d to %d addresses",
                len(currentAddresses), len(config.KingAddresses))

            if err := manager.UpdateKingAddresses(config.KingAddresses); err != nil {
                n.logger.Warnf("Failed to update king list: %v", err)
            } else {
                n.logger.Infof("Successfully updated king list configuration")
            }
        }
    }
}

func (n *Node) applyKingConfig(config *rotatingking.RotatingKingConfig, source peer.ID) {
    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    n.processMu.Unlock()

    if mgr == nil {
        n.logger.Warn("Cannot apply king config: no manager")
        return
    }

    // Get current addresses
    var currentAddresses []common.Address
    if manager, ok := mgr.(interface{ GetKingAddresses() []common.Address }); ok {
        currentAddresses = manager.GetKingAddresses()
    }

    // Check if we need to update
    if !n.compareAddressLists(currentAddresses, config.KingAddresses) {
        n.logger.Infof("Updating king list from peer %s: %d -> %d addresses",
            source.String()[:8], len(currentAddresses), len(config.KingAddresses))

        // Update the list
        if updater, ok := mgr.(interface{ UpdateKingAddresses([]common.Address) error }); ok {
            if err := updater.UpdateKingAddresses(config.KingAddresses); err != nil {
                n.logger.Warnf("Failed to update king list: %v", err)
            } else {
                n.logger.Info("King list updated from peer sync")
            }
        }
    }
}

func (n *Node) compareAddressLists(a, b []common.Address) bool {
    if len(a) != len(b) {
        return false
    }
    for i := range a {
        if a[i] != b[i] {
            return false
        }
    }
    return true
}

// Sends king configuration to a peer
func (n *Node) sendKingConfig(peerID peer.ID, config rotatingking.RotatingKingConfig) {
    // Create configuration message
    configMsg := map[string]interface{}{
        "type":         "king_config",
        "config":       config,  // This should serialize with capital field names
        "timestamp":    time.Now().Unix(),
        "source_peer":  n.host.ID().String(),
        "block_height": n.currentHeight(),
    }

    n.logger.Infof("Sending king config to %s with %d addresses",
        peerID.String()[:8], len(config.KingAddresses))

    // Log the addresses being sent
    for i, addr := range config.KingAddresses {
        n.logger.Debugf("  Address %d: %s", i+1, addr.Hex())
    }

    data, err := json.Marshal(configMsg)
    if err != nil {
        n.logger.Warnf("Failed to marshal king config: %v", err)
        return
    }

    // Log the EXACT JSON being sent
    n.logger.Debugf("RAW JSON being sent: %s", string(data))

    // Send via the king topic
    if n.kingTopic != nil {
        msg := make([]byte, 1+len(data))
        msg[0] = msgTypeKingListUpdate
        copy(msg[1:], data)

        if err := n.kingTopic.Publish(n.ctx, msg); err != nil {
            n.logger.Warnf("Failed to publish king config: %v", err)
        } else {
            n.logger.Infof("✅ King configuration sent to peer %s (%d addresses)",
                peerID.String()[:8], len(config.KingAddresses))
        }
    } else {
        // Fallback: use direct stream
        n.sendKingConfigDirect(peerID, config)
    }
}

// Sends king configuration via direct stream
func (n *Node) sendKingConfigDirect(peerID peer.ID, config rotatingking.RotatingKingConfig) {
    ctx, cancel := context.WithTimeout(n.ctx, 5*time.Second)
    defer cancel()

    s, err := n.host.NewStream(ctx, peerID, "/antdchain/king-config/1.0.0")
    if err != nil {
        n.logger.Debugf("Failed to open config stream to %s: %v", peerID.String()[:8], err)
        return
    }
    defer s.Close()

    configMsg := map[string]interface{}{
        "config":       config,
        "timestamp":    time.Now().Unix(),
        "source_peer":  n.host.ID().String(),
    }

    data, err := json.Marshal(configMsg)
    if err != nil {
        n.logger.Warnf("Failed to marshal config: %v", err)
        return
    }

    if _, err := s.Write(data); err != nil {
        n.logger.Debugf("Failed to send config to %s: %v", peerID.String()[:8], err)
    } else {
        n.logger.Debugf("Sent direct king config to %s", peerID.String()[:8])
    }
}

func (n *Node) handleConfigSyncRequest(req *DBSyncRequest, requester peer.ID) {
    n.logger.Info("Processing configuration sync request")

    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    n.processMu.Unlock()

    if mgr == nil {
        n.sendDBSyncErrorResponse(requester, req.RequestID, "rotating king manager not available")
        return
    }

    // Get current configuration
    var config rotatingking.RotatingKingConfig
    if configManager, ok := mgr.(interface{ GetConfig() rotatingking.RotatingKingConfig }); ok {
        config = configManager.GetConfig()
    } else if addrManager, ok := mgr.(interface{ GetKingAddresses() []common.Address }); ok {
        addresses := addrManager.GetKingAddresses()
        config = rotatingking.RotatingKingConfig{
            KingAddresses: addresses,
            RotationInterval: 100,
            MinStakeRequired: rotatingking.EligibilityThreshold,
        }
    }

    // Validate configuration before sending
    if len(config.KingAddresses) == 0 {
        n.logger.Warn("Cannot send empty configuration to peer")
        n.sendDBSyncErrorResponse(requester, req.RequestID, "empty configuration")
        return
    }

    // Send configuration using the new method
    n.sendKingConfig(requester, config)

    n.logger.Infof("Sent king configuration to peer %s (%d addresses)",
        requester.String()[:8], len(config.KingAddresses))
}

// Processes a rotatingking.KingRotation
func (n *Node) processKingRotation(rotation *rotatingking.KingRotation, source peer.ID) {
    // FIXED: Don't reject height 0 - it's valid for initial sync
    if rotation.BlockHeight == 0 {
        n.logger.Info("Processing KingRotation with height 0 (initial configuration)")
    }

    n.logger.Infof("Received king rotation: %s → %s at block %d",
        rotation.PreviousKing.Hex()[:8], rotation.NewKing.Hex()[:8], rotation.BlockHeight)

    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    n.processMu.Unlock()

    if mgr == nil {
        return
    }

    // Save rotation to database if supported
    if manager, ok := mgr.(interface{
        SaveRotationEvent(rotation *rotatingking.KingRotation) error
    }); ok {
        if err := manager.SaveRotationEvent(rotation); err != nil {
            n.logger.Debugf("Failed to save rotation event: %v", err)
        } else {
            n.logger.Debug("Rotation event saved to database")
        }
    }
}

//Handles king configuration messages
func (n *Node) parseKingConfig(configData map[string]interface{}) (rotatingking.RotatingKingConfig, error) {
    var config rotatingking.RotatingKingConfig

    n.logger.Debugf("parseKingConfig called with data: %v", configData)

    // Parse KingAddresses - try different possible field names
    var addresses []common.Address

    // Try "KingAddresses" (capital)
    if addrsData, ok := configData["KingAddresses"].([]interface{}); ok {
        n.logger.Debugf("Found KingAddresses array with %d items", len(addrsData))
        for _, addr := range addrsData {
            if addrStr, ok := addr.(string); ok {
                addresses = append(addresses, common.HexToAddress(addrStr))
            }
        }
    } else if addrsData, ok := configData["kingAddresses"].([]interface{}); ok {
        // Try "kingAddresses" (lowercase)
        n.logger.Debugf("Found kingAddresses array with %d items", len(addrsData))
        for _, addr := range addrsData {
            if addrStr, ok := addr.(string); ok {
                addresses = append(addresses, common.HexToAddress(addrStr))
            }
        }
    } else if addrsData, ok := configData["addresses"].([]interface{}); ok {
        // Try "addresses"
        n.logger.Debugf("Found addresses array with %d items", len(addrsData))
        for _, addr := range addrsData {
            if addrStr, ok := addr.(string); ok {
                addresses = append(addresses, common.HexToAddress(addrStr))
            }
        }
    } else {
        n.logger.Warn("No addresses array found in config data")
        // Try to see what fields are available
        for key, value := range configData {
            n.logger.Debugf("Key: %s, Type: %T, Value: %v", key, value, value)
        }
    }

    config.KingAddresses = addresses
    n.logger.Debugf("Parsed %d addresses", len(addresses))

    // Parse RotationInterval
    if interval, ok := configData["RotationInterval"].(float64); ok {
        config.RotationInterval = uint64(interval)
        n.logger.Debugf("RotationInterval: %d", config.RotationInterval)
    } else if interval, ok := configData["rotationInterval"].(float64); ok {
        config.RotationInterval = uint64(interval)
        n.logger.Debugf("rotationInterval: %d", config.RotationInterval)
    } else {
        config.RotationInterval = 100
        n.logger.Debug("Using default RotationInterval: 100")
    }

    // Parse RotationOffset
    if offset, ok := configData["RotationOffset"].(float64); ok {
        config.RotationOffset = uint64(offset)
    } else if offset, ok := configData["rotationOffset"].(float64); ok {
        config.RotationOffset = uint64(offset)
    }

    // Parse ActivationDelay
    if delay, ok := configData["ActivationDelay"].(float64); ok {
        config.ActivationDelay = uint64(delay)
    } else if delay, ok := configData["activationDelay"].(float64); ok {
        config.ActivationDelay = uint64(delay)
    } else {
        config.ActivationDelay = 2
    }

    // Parse MinStakeRequired
    if minStake, ok := configData["MinStakeRequired"].(string); ok {
        if bigInt, ok := new(big.Int).SetString(minStake, 10); ok {
            config.MinStakeRequired = bigInt
        }
    } else if minStake, ok := configData["minStakeRequired"].(string); ok {
        if bigInt, ok := new(big.Int).SetString(minStake, 10); ok {
            config.MinStakeRequired = bigInt
        }
    }

    if config.MinStakeRequired == nil {
        config.MinStakeRequired = rotatingking.EligibilityThreshold
    }

    n.logger.Debugf("Final parsed config: %d addresses, interval=%d, delay=%d, minStake=%s",
        len(config.KingAddresses), config.RotationInterval, config.ActivationDelay,
        config.MinStakeRequired.String())

    return config, nil
}

func (n *Node) applyKingConfiguration(config *rotatingking.RotatingKingConfig, source peer.ID) {
    if config == nil || len(config.KingAddresses) == 0 {
        n.logger.Warn("Received empty king configuration - ignoring")
        return
    }

    mgr := n.chain.GetRotatingKingManager()
    if mgr == nil {
        n.logger.Warn("Rotating king manager not available")
        return
    }

    currentList := mgr.GetKingAddresses()
    currentCount := len(currentList)
    newCount := len(config.KingAddresses)

    n.logger.Infof("Received king config from %s: %d addresses (local: %d)",
        source.String()[:8], newCount, currentCount)

    // ALWAYS accept if larger
    if newCount > currentCount {
        n.logger.Warnf("🔄 ACCEPTING LARGER king list from peer %s: %d → %d addresses",
            source.String()[:8], currentCount, newCount)

        if err := mgr.UpdateKingAddresses(config.KingAddresses); err != nil {
            n.logger.Errorf("Failed to apply larger king list: %v", err)
            return
        }

        n.logger.Infof("✅ King list updated to %d addresses", newCount)

        // Immediately rebroadcast our new configuration
        n.BroadcastCurrentKingConfig()
        n.detectAndBroadcastKingListChanges()
        return
    }

    // If same size but different, check if it's newer/better
    if newCount == currentCount && !n.areAddressListsEqual(currentList, config.KingAddresses) {
        n.logger.Infof("Same size list but different - merging")
        mergedList := n.mergeAddressLists(currentList, config.KingAddresses)
        if len(mergedList) > currentCount {
            if err := mgr.UpdateKingAddresses(mergedList); err != nil {
                n.logger.Errorf("Failed to merge king list: %v", err)
            } else {
                n.logger.Infof("✅ King lists merged: %d → %d addresses",
                    currentCount, len(mergedList))
                n.BroadcastCurrentKingConfig()
            }
        }
        return
    }

    n.logger.Debug("Received config not better than local - ignoring")
}

func (n *Node) startConfigurationSyncer() {
    // Initial wait for connections
    time.Sleep(10 * time.Second)

    ticker := time.NewTicker(15 * time.Second) // Check every 15 seconds
    defer ticker.Stop()

    n.logger.Info("🔄 Starting continuous configuration syncer")

    for {
        select {
        case <-n.ctx.Done():
            return
        case <-ticker.C:
            n.syncConfigWithAllPeers()
        }
    }
}

func (n *Node) syncConfigWithAllPeers() {
    mgr := n.chain.GetRotatingKingManager()
    if mgr == nil {
        return
    }

    currentList := mgr.GetKingAddresses()
    currentCount := len(currentList)

    // Always broadcast our config first
    n.BroadcastCurrentKingConfig()

    // If we have fewer than 10 addresses, aggressively request from all peers
    if currentCount < 10 {
        n.logger.Warnf("🔄 LOW ADDRESS COUNT (%d) - AGGRESSIVELY REQUESTING CONFIG", currentCount)

        for _, pid := range n.Peers() {
            go n.RequestKingConfigFromPeer(pid)
        }
    }
}

// Compare address lists
func (n *Node) areAddressListsEqual(a, b []common.Address) bool {
    if len(a) != len(b) {
        return false
    }
    for i := range a {
        if a[i] != b[i] {
            return false
        }
    }
    return true
}
// Handles king rotation messages
func (n *Node) handleKingRotation(msg *pubsub.Message) {
    if len(msg.Data) < 100 {
        return
    }
    if !n.allowEventFromPeer(msg.GetFrom()) {
        return
    }

    // First try to parse as KingRotationEvent
    var rotationEvent KingRotationEvent
    if err := json.Unmarshal(msg.Data[1:], &rotationEvent); err == nil {
        // Reject height 0 rotations - they must match actual block height
        if rotationEvent.BlockHeight == 0 {
            n.logger.Warnf("❌ Rejecting rotation event with invalid height 0 from %s",
                msg.GetFrom().String()[:8])
            return
        }

        n.processKingRotationEvent(&rotationEvent, msg.GetFrom())
        return
    }

    // Fallback: try to parse as rotatingking.KingRotation
    var kingRotation rotatingking.KingRotation
    if err := json.Unmarshal(msg.Data[1:], &kingRotation); err != nil {
        n.logger.Warnf("Failed to unmarshal rotation event: %v", err)
        return
    }

    // Reject height 0 - rotations must match actual block height
    if kingRotation.BlockHeight == 0 {
        n.logger.Warnf("❌ Rejecting KingRotation with invalid height 0 from %s",
            msg.GetFrom().String()[:8])
        return
    }

    n.processKingRotation(&kingRotation, msg.GetFrom())
}

// Handles king list update messages
func (n *Node) handleKingListUpdate(msg *pubsub.Message) {
    if len(msg.Data) < 100 {
        return
    }
    if !n.allowEventFromPeer(msg.GetFrom()) {
        return
    }

    var event rotatingking.KingListUpdateEvent
    if err := json.Unmarshal(msg.Data[1:], &event); err != nil {
        n.logger.Warnf("Failed to unmarshal king list update from %s: %v",
            msg.GetFrom().String()[:8], err)
        return
    }

    // Reject height 0 - list updates must match actual block height
    if event.BlockHeight == 0 {
        n.logger.Warnf("❌ Rejecting king list update with invalid height 0 from %s",
            msg.GetFrom().String()[:8])
        return
    }

    n.logger.Infof("Received king list update from %s: %d addresses at height %d",
        msg.GetFrom().String()[:8], len(event.NewList), event.BlockHeight)

    n.processMu.Lock()
    defer n.processMu.Unlock()

    mgr := n.chain.GetRotatingKingManager()
    if mgr == nil {
        n.logger.Warn("Rotating king manager not available - cannot apply list update")
        return
    }

    currentHeight := n.currentHeight()

    // Validate height - must be close to our current height
    if event.BlockHeight < currentHeight-100 {
        n.logger.Warnf("Ignoring very old king list update (height %d vs current %d)",
            event.BlockHeight, currentHeight)
        return
    }

    // If it's from the future, it's likely invalid
    if event.BlockHeight > currentHeight+10 {
        n.logger.Warnf("Rejecting future king list update (height %d > our %d + 10)",
            event.BlockHeight, currentHeight)
        return
    }

    // Get current local list
    currentList := mgr.GetKingAddresses()

    // Avoid redundant updates
    if len(currentList) == len(event.NewList) {
        matched := true
        for i := range currentList {
            if currentList[i] != event.NewList[i] {
                matched = false
                break
            }
        }
        if matched {
            n.logger.Debug("Received identical king list - ignoring")
            return
        }
    }

    // Apply the update
    if err := mgr.UpdateKingAddresses(event.NewList); err != nil {
        n.logger.Warnf("Failed to apply king list update: %v", err)
        return
    }

    n.logger.Infof("✅ King list updated via P2P at height %d (now %d addresses)",
        event.BlockHeight, len(event.NewList))
}

// Processes a KingRotationEvent
func (n *Node) processKingRotationEvent(event *KingRotationEvent, source peer.ID) {
    // Height must be valid (not 0)
    if event.BlockHeight == 0 {
        n.logger.Warn("❌ Invalid rotation event: height 0")
        return
    }

    n.logger.Infof("Received king rotation event: %s → %s at height %d (eligible=%v)",
        event.PreviousKing.Hex()[:8], event.NewKing.Hex()[:8], event.BlockHeight, event.Eligible)

    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    n.processMu.Unlock()

    if mgr == nil {
        return
    }

    currentHeight := n.currentHeight()

    // Validate height matches our chain
    if event.BlockHeight != currentHeight && event.BlockHeight != currentHeight+1 {
        n.logger.Warnf("Invalid rotation event height %d (current %d)",
            event.BlockHeight, currentHeight)
        return
    }

    // Check eligibility
    localEligible := false
    if eligibilityChecker, ok := mgr.(interface{ IsEligible(height uint64) bool }); ok {
        localEligible = eligibilityChecker.IsEligible(event.BlockHeight)
    }

    if localEligible != event.Eligible {
        n.logger.Warnf("Eligibility mismatch: local=%v event=%v - skipping",
            localEligible, event.Eligible)
        return
    }

    // Apply rotation
    if rotator, ok := mgr.(interface{ ForceRotateToAddress(newKing common.Address, reason string) error }); ok {
        if err := rotator.ForceRotateToAddress(event.NewKing, "p2p-rotation-event"); err != nil {
            n.logger.Warnf("Failed to apply rotation event: %v", err)
        } else {
            n.logger.Info("✅ King rotation applied via P2P")
        }
    }
}

// Broadcasts king configuration AT CURRENT BLOCK HEIGHT
func (n *Node) BroadcastCurrentKingConfig() {
    mgr := n.chain.GetRotatingKingManager()
    if mgr == nil {
        //n.logger.Warn("Cannot broadcast king config: manager not available")
        return
    }

    currentHeight := n.currentHeight()
    currentList := mgr.GetKingAddresses()

    // Don't broadcast empty lists
    if len(currentList) == 0 {
      //  n.logger.Warn("Cannot broadcast empty king list")
        return
    }

    // Always broadcast if we have a decent list (2+ addresses)
    if len(currentList) >= 2 {
        event := &rotatingking.KingListUpdateEvent{
            BlockHeight: currentHeight,
            NewList:     currentList,
            Timestamp:   time.Now(),
            Reason:      "always_sync_broadcast",
        }

        if err := n.BroadcastKingListUpdate(event); err != nil {
            n.logger.Warnf("Failed to broadcast current king config: %v", err)
        } else {
            n.logger.Infof("📤 ALWAYS SYNC: Broadcast king config at height %d (%d addresses)",
                currentHeight, len(currentList))
        }
    }

    // Also broadcast via direct streams to all peers
    for _, pid := range n.Peers() {
        go n.sendKingConfig(pid, n.getCurrentKingConfig(mgr))
    }
}

// Handles king configuration messages (for database sync, not chain sync)
func (n *Node) handleKingConfig(data []byte, source peer.ID) {
    n.logger.Debugf("Received king config message from %s", source.String()[:8])

    var msg map[string]interface{}
    if err := json.Unmarshal(data, &msg); err != nil {
        n.logger.Debugf("Failed to unmarshal king config: %v", err)
        return
    }

    configType, ok := msg["type"].(string)
    if !ok || configType != "king_config" {
        return
    }

    configData, ok := msg["config"].(map[string]interface{})
    if !ok {
        n.logger.Debug("Invalid config format")
        return
    }

    // Parse the configuration
    config, err := n.parseKingConfig(configData)
    if err != nil {
        n.logger.Debugf("Failed to parse king config: %v", err)
        return
    }

    n.logger.Debugf("Received database config with %d addresses", len(config.KingAddresses))

    // Only use for database sync, not for chain state
    n.applyKingConfiguration(&config, source)
}

// Requests king configuration from a peer
func (n *Node) RequestKingConfiguration(peerID peer.ID) {
    // This requests DATABASE configuration, not chain state
    n.logger.Debugf("Requesting database configuration from peer %s", peerID.String()[:8])

    if n.kingTopic == nil {
        return
    }

    // Send a configuration request
    msg := []byte{msgTypeKingConfigRequest}

    if err := n.kingTopic.Publish(n.ctx, msg); err != nil {
        n.logger.Debugf("Failed to publish king config request: %v", err)
    }
}

// Handles direct configuration stream requests
func (n *Node) handleKingConfigStream(s network.Stream) {
    defer s.Close()

    data, err := io.ReadAll(s)
    if err != nil {
        n.logger.Debugf("Failed to read config stream: %v", err)
        return
    }

    // This is a database configuration request, respond with current state
    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    n.processMu.Unlock()

    if mgr == nil {
        return
    }

    var config rotatingking.RotatingKingConfig
    if configManager, ok := mgr.(interface{ GetConfig() rotatingking.RotatingKingConfig }); ok {
        config = configManager.GetConfig()
    } else if addrManager, ok := mgr.(interface{ GetKingAddresses() []common.Address }); ok {
        addresses := addrManager.GetKingAddresses()
        config = rotatingking.RotatingKingConfig{
            KingAddresses:    addresses,
            RotationInterval: 100,
            MinStakeRequired: rotatingking.EligibilityThreshold,
        }
    }

    // Send response
    response := map[string]interface{}{
        "type":    "king_config",
        "config":  config,
        "height":  n.currentHeight(),
    }

    data, err = json.Marshal(response)
    if err != nil {
        return
    }

    if _, err := s.Write(data); err != nil {
        n.logger.Debugf("Failed to send config: %v", err)
    }
}

func (n *Node) syncKingConfigurationOnStartup() {
    // Wait for connections
    time.Sleep(10 * time.Second)

    n.logger.Info("🔍 Starting king configuration sync")

    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    n.processMu.Unlock()

    if mgr == nil {
        //n.logger.Warn("No rotating king manager")
        return
    }

    // Get current addresses
    var currentAddresses []common.Address
    if manager, ok := mgr.(interface{ GetKingAddresses() []common.Address }); ok {
        currentAddresses = manager.GetKingAddresses()
    }

    n.logger.Infof("Startup configuration: %d addresses", len(currentAddresses))

    // If we have peers, check if we need to sync
    peers := n.Peers()
    if len(peers) == 0 {
        //n.logger.Warn("No peers for configuration sync")
        return
    }

    // Check each peer's configuration
    for _, pid := range peers {
        n.logger.Debugf("Checking configuration with peer %s", pid.String()[:8])

        // Request configuration from peer
        go n.requestAndCompareConfiguration(pid, currentAddresses)
    }

    go func() {
        // Wait a bit for initial peer discovery
        time.Sleep(15 * time.Second)
        n.CheckIfConfigurationSyncNeeded()
    }()
}

func (n *Node) requestAndCompareConfiguration(peerID peer.ID, ourAddresses []common.Address) {
    // Create config request
    req := DBSyncRequest{
        RequestID:   fmt.Sprintf("config-check-%d", time.Now().UnixNano()),
        RequestType: "config",
        Timestamp:   time.Now().Unix(),
        PeerID:      n.host.ID().String(),
    }

    n.logger.Debugf("Requesting configuration from peer %s", peerID.String()[:8])

    // Send request
    n.sendDBSyncRequest(peerID, req)

    // Wait for response (simplified - you might want to use a channel)
    time.Sleep(5 * time.Second)
}

func (n *Node) shouldSyncConfiguration(ourCount, peerCount int, ourAddresses, peerAddresses []common.Address) bool {
    // If counts differ, definitely sync
    if ourCount != peerCount {
        n.logger.Warnf("Configuration mismatch: we have %d, peer has %d addresses",
            ourCount, peerCount)
        return true
    }

    // If counts same but addresses differ, sync
    if !n.areAddressListsEqual(ourAddresses, peerAddresses) {
        n.logger.Warn("Configuration addresses differ (same count but different addresses)")
        return true
    }

    return false
}


func (n *Node) syncKingConfiguration() {
    peers := n.Peers()
    if len(peers) == 0 {
        n.logger.Warn("No peers for configuration sync")
        return
    }

    // Get our configuration first
    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    n.processMu.Unlock()

    if mgr == nil {
        return
    }

    var ourConfig rotatingking.RotatingKingConfig
    if configManager, ok := mgr.(interface{ GetConfig() rotatingking.RotatingKingConfig }); ok {
        ourConfig = configManager.GetConfig()
    } else if addrManager, ok := mgr.(interface{ GetKingAddresses() []common.Address }); ok {
        addresses := addrManager.GetKingAddresses()
        ourConfig = rotatingking.RotatingKingConfig{
            KingAddresses:    addresses,
            RotationInterval: 100,
            MinStakeRequired: rotatingking.EligibilityThreshold,
        }
    }

    ourCount := len(ourConfig.KingAddresses)
    n.logger.Infof("Our configuration: %d addresses", ourCount)

    // Try to get configuration from each peer
    for _, pid := range peers {
        n.logger.Infof("Requesting configuration from peer %s", pid.String()[:8])

        // Request configuration (this will trigger response handling)
        n.RequestKingConfiguration(pid)

        // Wait a bit for response
        time.Sleep(2 * time.Second)
    }
}

// Processes a KingRotationEvent
func (n *Node) ForceDatabaseSync() {
    n.logger.Info("Forcing immediate database synchronization")
    go n.performDBSyncWithPeers()
}

func (n *Node) TriggerManualDBSync() {
    //n.logger.Warn("MANUAL DATABASE SYNC TRIGGERED")
    n.performDBSyncWithPeers()
}

func getKeys(m map[string]interface{}) []string {
    keys := make([]string, 0, len(m))
    for k := range m {
        keys = append(keys, k)
    }
    return keys
}

func (n *Node) DebugKingConfiguration() {
    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    n.processMu.Unlock()

    if mgr == nil {
        n.logger.Warn("No rotating king manager")
        return
    }

    // Try different ways to get addresses
    if manager, ok := mgr.(interface{ GetKingAddresses() []common.Address }); ok {
        addresses := manager.GetKingAddresses()
        n.logger.Infof("DEBUG: GetKingAddresses() returned %d addresses:", len(addresses))
        for i, addr := range addresses {
            n.logger.Infof("  [%d] %s", i, addr.Hex())
        }
    }

    if configManager, ok := mgr.(interface{ GetConfig() rotatingking.RotatingKingConfig }); ok {
        config := configManager.GetConfig()
        n.logger.Infof("DEBUG: GetConfig() returned %d addresses:", len(config.KingAddresses))
        for i, addr := range config.KingAddresses {
            n.logger.Infof("  [%d] %s", i, addr.Hex())
        }
    }
}

func (n *Node) CheckRotationHistory() {
    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    n.processMu.Unlock()

    if mgr == nil {
        return
    }

    if manager, ok := mgr.(interface{
        GetRotationHistory(limit int) []rotatingking.KingRotation
    }); ok {
        history := manager.GetRotationHistory(10)
        n.logger.Infof("Rotation history has %d entries:", len(history))
        for i, rot := range history {
            n.logger.Infof("  [%d] Block %d: %s -> %s",
                i, rot.BlockHeight,
                rot.PreviousKing.Hex()[:8],
                rot.NewKing.Hex()[:8])
        }
    }
}

func (n *Node) BroadcastCurrentConfig() {
    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    n.processMu.Unlock()

    if mgr == nil {
        return
    }

    var config rotatingking.RotatingKingConfig
    if configManager, ok := mgr.(interface{ GetConfig() rotatingking.RotatingKingConfig }); ok {
        config = configManager.GetConfig()
    } else if addrManager, ok := mgr.(interface{ GetKingAddresses() []common.Address }); ok {
        addresses := addrManager.GetKingAddresses()
        config = rotatingking.RotatingKingConfig{
            KingAddresses: addresses,
            RotationInterval: 100,
            MinStakeRequired: rotatingking.EligibilityThreshold,
        }
    }

    n.logger.Infof("Broadcasting configuration with %d addresses", len(config.KingAddresses))

    // Broadcast to all peers
    for _, pid := range n.Peers() {
        n.sendKingConfig(pid, config)
    }
}

func (n *Node) TriggerConfigSync() {
    n.logger.Info("🔍 Manually triggering configuration sync")

    // Get our current configuration
    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    n.processMu.Unlock()

    if mgr == nil {
        n.logger.Warn("No rotating king manager")
        return
    }

    config := n.getCurrentKingConfig(mgr)
    n.logger.Infof("Our config has %d addresses", len(config.KingAddresses))

    // Broadcast our config to all peers
    for _, pid := range n.Peers() {
        n.sendKingConfig(pid, config)
    }

    // Also request config from all peers
    for _, pid := range n.Peers() {
        n.RequestKingConfiguration(pid)
    }
}

func (n *Node) CheckCurrentConfig() {
    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    n.processMu.Unlock()

    if mgr == nil {
        n.logger.Warn("No rotating king manager")
        return
    }

    config := n.getCurrentKingConfig(mgr)

    n.logger.Info("=== CURRENT KING CONFIGURATION ===")
    n.logger.Infof("Addresses: %d", len(config.KingAddresses))
    for i, addr := range config.KingAddresses {
        n.logger.Infof("  [%d] %s", i+1, addr.Hex())
    }
    n.logger.Infof("Rotation Interval: %d", config.RotationInterval)
    n.logger.Infof("Activation Delay: %d", config.ActivationDelay)
    n.logger.Infof("Min Stake Required: %s", config.MinStakeRequired.String())
    n.logger.Info("================================")
}

func (n *Node) syncDatabaseForNewBlock(blockHeight uint64, blockHash common.Hash) {
    if !n.dbSyncEnabled {
        return
    }

    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    n.processMu.Unlock()

    if mgr == nil {
        return
    }

    // Get current sync state
    syncState, err := mgr.GetSyncState()
    if err != nil {
        return
    }

    // If we're behind by more than 10 blocks, trigger sync
    if syncState.LastSyncedBlock < blockHeight-10 {
        n.logger.Infof("🔄 Database behind by %d blocks, triggering sync",
            blockHeight - syncState.LastSyncedBlock)
        go n.performDBSyncWithPeers()
    }
}

func (n *Node) broadcastDBSyncStatus() {
    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    n.processMu.Unlock()

    if mgr == nil {
        return
    }

    // Get current blockchain height
    currentHeight := n.currentHeight()

    status := DBSyncStatus{
        PeerID:          n.host.ID().String(),
        LastSyncedBlock: currentHeight, // Use blockchain height, not just DB sync
        Timestamp:       time.Now().Unix(),
        Version:         n.dbSyncVersion,
    }

    // Get sync state from manager
    if manager, ok := mgr.(interface{ GetSyncState() (*rotatingking.SyncState, error) }); ok {
        syncState, err := manager.GetSyncState()
        if err == nil && syncState != nil {
            status.SyncState = syncState
            status.IsSyncing = syncState.IsSyncing

            // Update last synced block to max of either
            if syncState.LastSyncedBlock > status.LastSyncedBlock {
                status.LastSyncedBlock = syncState.LastSyncedBlock
            }
        }
    }

    // Get king count
    if manager, ok := mgr.(interface{ GetKingAddresses() []common.Address }); ok {
        status.KingCount = len(manager.GetKingAddresses())
    }

    // Broadcast status
    n.sendDBSyncStatusBroadcast(status)
}

func (n *Node) sendDBSyncStatusBroadcast(status DBSyncStatus) {
    data, err := json.Marshal(status)
    if err != nil {
        //n.logger.Warnf("Failed to marshal DB sync status: %v", err)
        return
    }

    msg := make([]byte, 1+len(data))
    msg[0] = msgTypeDBSyncStatus
    copy(msg[1:], data)

    if err := n.dbSyncTopic.Publish(n.ctx, msg); err != nil {
        //n.logger.Warnf("Failed to publish DB sync status: %v", err)
    } else {
        n.logger.Debugf("📤 Broadcast DB sync status: height=%d, syncing=%v",
            status.LastSyncedBlock, status.IsSyncing)
    }
}

func (n *Node) compareAndSyncConfigurations() {
    n.logger.Info("🔄 Actively comparing configurations with peers")

    // Get our configuration
    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    n.processMu.Unlock()

    if mgr == nil {
        return
    }

    var ourConfig rotatingking.RotatingKingConfig
    if configManager, ok := mgr.(interface{ GetConfig() rotatingking.RotatingKingConfig }); ok {
        ourConfig = configManager.GetConfig()
    } else {
        return
    }

    ourCount := len(ourConfig.KingAddresses)

    // Broadcast our configuration first
    n.logger.Infof("📤 Broadcasting our configuration (%d addresses) to peers", ourCount)
    n.BroadcastCurrentConfig()

    // Request configurations from all peers
    for _, pid := range n.Peers() {
        n.logger.Debugf("Requesting configuration from peer %s", pid.String()[:8])
        n.RequestKingConfiguration(pid)
    }

    // Wait for responses and compare
    time.Sleep(10 * time.Second)

    // After responses, check if we should sync
    n.CheckIfConfigurationSyncNeeded()
}


func (n *Node) CheckIfConfigurationSyncNeeded() {
    n.logger.Info("🔍 Checking if configuration sync is needed")

    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    n.processMu.Unlock()

    if mgr == nil {
        //n.logger.Warn("No rotating king manager")
        return
    }

    // Get our current configuration
    var ourConfig rotatingking.RotatingKingConfig
    var ourAddresses []common.Address

    if configManager, ok := mgr.(interface{ GetConfig() rotatingking.RotatingKingConfig }); ok {
        ourConfig = configManager.GetConfig()
        ourAddresses = ourConfig.KingAddresses
    } else if addrManager, ok := mgr.(interface{ GetKingAddresses() []common.Address }); ok {
        ourAddresses = addrManager.GetKingAddresses()
        ourConfig = rotatingking.RotatingKingConfig{
            KingAddresses:    ourAddresses,
            RotationInterval: 100,
            MinStakeRequired: rotatingking.EligibilityThreshold,
        }
    }

    ourCount := len(ourAddresses)
    n.logger.Infof("Our configuration: %d addresses", ourCount)

    // Check cached responses from peers
    n.dbSyncMu.RLock()
    defer n.dbSyncMu.RUnlock()

    if len(n.dbSyncResponses) == 0 {
        n.logger.Debug("No configuration responses cached yet")
        return
    }

    // Analyze peer configurations
    var betterConfigs []*rotatingking.RotatingKingConfig
    var peerCounts []int

    for _, response := range n.dbSyncResponses {
        // Only consider recent responses (last 5 minutes)
        if time.Now().Unix()-response.Timestamp > 300 {
            continue
        }

        if response.Config != nil && len(response.Config.KingAddresses) > 0 {
            peerCount := len(response.Config.KingAddresses)
            peerCounts = append(peerCounts, peerCount)

            n.logger.Debugf("Peer %s has %d addresses in configuration",
                response.PeerID[:8], peerCount)

            // Check if this configuration is "better" than ours
            if n.isConfigurationBetter(response.Config, &ourConfig) {
                betterConfigs = append(betterConfigs, response.Config)
                n.logger.Infof("📊 Peer %s has better configuration (%d > %d addresses)",
                    response.PeerID[:8], peerCount, ourCount)
            }
        }
    }

    if len(peerCounts) == 0 {
        n.logger.Debug("No valid peer configurations found")
        return
    }

    // Calculate statistics
    avgCount := 0
    maxCount := 0
    for _, count := range peerCounts {
        avgCount += count
        if count > maxCount {
            maxCount = count
        }
    }
    avgCount /= len(peerCounts)

    n.logger.Infof("📊 Configuration analysis: Our=%d, AvgPeer=%d, MaxPeer=%d",
        ourCount, avgCount, maxCount)

    // Decision logic
    decision := n.evaluateSyncDecision(ourCount, avgCount, maxCount, betterConfigs)

    switch decision {
    case "sync_needed":
        n.logger.Warnf("🔄 Configuration sync needed: we have %d addresses, peers average %d",
            ourCount, avgCount)
        n.triggerConfigurationSync()

    case "broadcast":
        n.logger.Infof("📤 Our configuration is better (%d addresses), broadcasting to peers",
            ourCount)
        n.BroadcastCurrentConfig()

    case "ok":
        n.logger.Info("✅ Configuration is in sync with network")

    case "inconsistent":
        n.logger.Warn("⚠️  Inconsistent configurations among peers")
        n.resolveConfigurationConflict(peerCounts, betterConfigs)
    }
}

func (n *Node) isConfigurationBetter(peerConfig, ourConfig *rotatingking.RotatingKingConfig) bool {
    peerCount := len(peerConfig.KingAddresses)
    ourCount := len(ourConfig.KingAddresses)

    // More addresses is generally better
    if peerCount > ourCount {
        return true
    }

    // Same count but different addresses might indicate newer config
    if peerCount == ourCount && !n.areAddressListsEqual(peerConfig.KingAddresses, ourConfig.KingAddresses) {
        // Check if peer config is newer (based on rotation count or timestamp)
        return true
    }

    return false
}

func (n *Node) evaluateSyncDecision(ourCount, avgCount, maxCount int, betterConfigs []*rotatingking.RotatingKingConfig) string {
    // If we have significantly fewer addresses than average
    if ourCount < avgCount-1 {
        return "sync_needed"
    }

    // If we have more addresses than most peers, broadcast ours
    if ourCount > avgCount+1 {
        return "broadcast"
    }

    // If we're at average but have different better configurations
    if len(betterConfigs) > 0 {
        // Check if these better configs are actually different, not just larger
        for _, betterConfig := range betterConfigs {
            if len(betterConfig.KingAddresses) == ourCount {
                // Same size but different content
                return "sync_needed"
            }
        }
    }

    // If peer counts vary widely and we're below max
    if maxCount-ourCount > 2 {
        return "sync_needed"
    }

    // If we're within reasonable range
    return "ok"
}

func (n *Node) triggerConfigurationSync() {
    n.logger.Info("🔄 Triggering configuration sync")

    // Find the best configuration from cached responses
    var bestConfig *rotatingking.RotatingKingConfig
    var bestCount int

    n.dbSyncMu.RLock()
    for _, response := range n.dbSyncResponses {
        if response.Config != nil && len(response.Config.KingAddresses) > bestCount {
            bestCount = len(response.Config.KingAddresses)
            bestConfig = response.Config
        }
    }
    n.dbSyncMu.RUnlock()

    if bestConfig != nil {
        n.logger.Infof("📥 Adopting configuration with %d addresses", bestCount)

        // Apply the configuration
        n.processMu.Lock()
        mgr := n.chain.GetRotatingKingManager()
        n.processMu.Unlock()

        if mgr != nil {
            if updater, ok := mgr.(interface{ UpdateKingAddresses([]common.Address) error }); ok {
                if err := updater.UpdateKingAddresses(bestConfig.KingAddresses); err != nil {
                    n.logger.Errorf("Failed to apply configuration: %v", err)
                } else {
                    n.logger.Info("✅ Configuration updated successfully")

                    // Broadcast our new configuration
                    n.BroadcastCurrentConfig()
                }
            }
        }
    } else {
        n.logger.Warn("No suitable configuration found for sync")

        // Request fresh configurations
        n.logger.Info("📤 Requesting fresh configurations from peers")
        for _, pid := range n.Peers() {
            n.RequestKingConfiguration(pid)
        }
    }
}

func (n *Node) resolveConfigurationConflict(peerCounts []int, betterConfigs []*rotatingking.RotatingKingConfig) {
    n.logger.Warn("🔀 Resolving configuration conflict")

    // Find the most common configuration size
    countMap := make(map[int]int)
    for _, count := range peerCounts {
        countMap[count]++
    }

    var commonCount int
    var maxFreq int
    for count, freq := range countMap {
        if freq > maxFreq {
            maxFreq = freq
            commonCount = count
        }
    }

    n.logger.Infof("Most common configuration size: %d addresses (appears in %d peers)",
        commonCount, maxFreq)

    // Find a configuration with the common size
    n.dbSyncMu.RLock()
    var targetConfig *rotatingking.RotatingKingConfig
    for _, response := range n.dbSyncResponses {
        if response.Config != nil && len(response.Config.KingAddresses) == commonCount {
            targetConfig = response.Config
            break
        }
    }
    n.dbSyncMu.RUnlock()

    if targetConfig != nil {
        n.logger.Infof("Adopting consensus configuration with %d addresses", commonCount)
        n.applyConsensusConfiguration(targetConfig)
    } else {
        n.logger.Warn("Could not find consensus configuration")

        // Request vote from peers
        n.initiateConfigurationVote()
    }
}

func (n *Node) applyConsensusConfiguration(config *rotatingking.RotatingKingConfig) {
    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    n.processMu.Unlock()

    if mgr == nil {
        return
    }

    if updater, ok := mgr.(interface{ UpdateKingAddresses([]common.Address) error }); ok {
        if err := updater.UpdateKingAddresses(config.KingAddresses); err != nil {
            n.logger.Errorf("Failed to apply consensus configuration: %v", err)
        } else {
            n.logger.Info("✅ Consensus configuration applied")

            // Broadcast the consensus
            n.BroadcastCurrentConfig()
        }
    }
}

func (n *Node) initiateConfigurationVote() {
    n.logger.Info("🗳️  Initiating configuration vote")

    // Create a vote request
    voteRequest := map[string]interface{}{
        "type":       "config_vote",
        "timestamp":  time.Now().Unix(),
        "our_count":  n.getOurAddressCount(),
        "request_id": fmt.Sprintf("vote-%d", time.Now().UnixNano()),
    }

    data, err := json.Marshal(voteRequest)
    if err != nil {
        n.logger.Warnf("Failed to marshal vote request: %v", err)
        return
    }

    // Broadcast vote request
    msg := make([]byte, 1+len(data))
    msg[0] = msgTypeKingListUpdate
    copy(msg[1:], data)

    if err := n.kingTopic.Publish(n.ctx, msg); err != nil {
        n.logger.Warnf("Failed to publish vote request: %v", err)
    } else {
        n.logger.Info("📤 Configuration vote request broadcasted")
    }
}

func (n *Node) getOurAddressCount() int {
    n.processMu.Lock()
    defer n.processMu.Unlock()

    mgr := n.chain.GetRotatingKingManager()
    if mgr == nil {
        return 0
    }

    if manager, ok := mgr.(interface{ GetKingAddresses() []common.Address }); ok {
        return len(manager.GetKingAddresses())
    }

    return 0
}

func (n *Node) startConfigurationMonitor() {
    // Wait for initial sync
    time.Sleep(10 * time.Second)

    ticker := time.NewTicker(30 * time.Second) // Check every 30 seconds
    defer ticker.Stop()

    n.logger.Info("🔄 Starting aggressive configuration monitor")

    for {
        select {
        case <-n.ctx.Done():
            return
        case <-ticker.C:
            n.checkAndSyncKingConfig()

            // Always broadcast our config
            n.BroadcastCurrentKingConfig()

            // Always request from peers
            peers := n.Peers()
            for _, pid := range peers {
                n.RequestKingConfigFromPeer(pid)
            }
        }
    }
}

func (n *Node) startKingListCleanup() {
    // Wait for startup
    time.Sleep(60 * time.Second)

    ticker := time.NewTicker(10 * time.Minute) // Check every 10 minutes
    defer ticker.Stop()

    n.logger.Info("🔄 Starting king list cleanup monitor")

    for {
        select {
        case <-n.ctx.Done():
            n.logger.Info("🛑 Stopping king list cleanup")
            return
        case <-ticker.C:
            n.processMu.Lock()
            mgr := n.chain.GetRotatingKingManager()
            n.processMu.Unlock()

            if mgr == nil {
                continue
            }

            // Try to cast to the cleanup interface
            if cleaner, ok := mgr.(interface{ CleanupIneligibleKings() ([]common.Address, error) }); ok {
                removed, err := cleaner.CleanupIneligibleKings()
                if err != nil {
                    n.logger.Warnf("Failed to cleanup kings: %v", err)
                    continue
                }

                if len(removed) > 0 {
                    n.logger.Warnf("🔄 Removed %d ineligible kings from rotation list", len(removed))

                    // Broadcast updated list
                    n.BroadcastCurrentConfig()
                }
            }
        }
    }
}

func (n *Node) RequestKingConfigFromPeer(p peer.ID) {
    n.logger.Infof("Requesting king configuration from peer %s", p.String()[:8])

    if n.kingTopic == nil {
        n.logger.Warn("King topic not available for config request")
        return
    }

    msg := []byte{msgTypeKingConfigRequest}

    if err := n.kingTopic.Publish(n.ctx, msg); err != nil {
        n.logger.Warnf("Failed to publish king config request: %v", err)
    }
}

func (n *Node) PeriodicKingConfigCheck() {
    ticker := time.NewTicker(10 * time.Second) // Every 10 seconds
    defer ticker.Stop()

    for range ticker.C {
        peers := n.Peers()
        if len(peers) == 0 {
            continue
        }

        mgr := n.chain.GetRotatingKingManager()
        if mgr == nil {
            continue
        }

        localCount := len(mgr.GetKingAddresses())

        // If we have less than 10 addresses, request from everyone
        if localCount < 10 {
            for _, peerID := range peers {
                n.RequestKingConfigFromPeer(peerID)
            }
        }
    }
}

func (n *Node) onKingListChanged(newList []common.Address) {
    n.logger.Infof("🔄 King list changed to %d addresses - broadcasting immediately", len(newList))

    // Store the last list
    n.lastKingListMu.Lock()
    n.lastKingList = newList
    n.lastKingListMu.Unlock()

    // Broadcast the new configuration
    n.BroadcastCurrentKingConfig()
}

// Broadcasts database sync requests (implements rotatingking.P2PBroadcaster)
func (n *Node) BroadcastDatabaseSync(request *rotatingking.DatabaseSyncRequest) error {
    if n == nil || n.dbSyncTopic == nil {
        return errors.New("p2p node or db sync topic not initialized")
    }

    n.logger.Info("Broadcasting database sync request",
        logrus.Fields{
            "nodeId": request.NodeID[:8],
            "lastSyncedBlock": request.LastSyncedBlock,
            "type": request.RequestType,
        })

    data, err := json.Marshal(request)
    if err != nil {
        return fmt.Errorf("failed to encode sync request: %w", err)
    }

    msg := make([]byte, 1+len(data))
    msg[0] = msgTypeDBSyncRequest
    copy(msg[1:], data)

    if err := n.dbSyncTopic.Publish(n.ctx, msg); err != nil {
        return fmt.Errorf("gossipsub publish failed: %w", err)
    }

    n.logger.Debugf("Database sync request broadcasted to network")
    return nil
}

// Broadcasts rotation proposals (implements rotatingking.P2PBroadcaster)
func (n *Node) BroadcastRotationProposal(proposal *rotatingking.RotationProposal) error {
    if n == nil || n.kingTopic == nil {
        return errors.New("p2p node or king topic not initialized")
    }

    n.logger.Debug("Broadcasting rotation proposal",
        logrus.Fields{
            "proposalId": proposal.ProposalID[:8],
            "height": proposal.BlockHeight,
            "currentKing": proposal.CurrentKing.Hex()[:8],
            "nextKing": proposal.NextKing.Hex()[:8],
        })

    data, err := json.Marshal(proposal)
    if err != nil {
        return fmt.Errorf("failed to encode rotation proposal: %w", err)
    }

    msg := make([]byte, 1+len(data))
    msg[0] = msgTypeKingRotation
    copy(msg[1:], data)

    if err := n.kingTopic.Publish(n.ctx, msg); err != nil {
        return fmt.Errorf("gossipsub publish failed: %w", err)
    }

    n.logger.Debugf("Rotation proposal broadcasted to network")
    return nil
}

// Broadcasts rotation votes.
func (n *Node) BroadcastRotationVote(vote *rotatingking.RotationVote) error {
    if n == nil || n.kingTopic == nil {
        return errors.New("p2p node or king topic not initialized")
    }

    n.logger.Debug("Broadcasting rotation vote",
        logrus.Fields{
            "proposalId": vote.ProposalID[:8],
            "voter": vote.VoterNodeID,
            "approved": vote.Approved,
        })

    data, err := json.Marshal(vote)
    if err != nil {
        return fmt.Errorf("failed to encode rotation vote: %w", err)
    }

    msg := make([]byte, 1+len(data))
    msg[0] = msgTypeKingRotation
    copy(msg[1:], data)

    if err := n.kingTopic.Publish(n.ctx, msg); err != nil {
        return fmt.Errorf("gossipsub publish failed: %w", err)
    }

    n.logger.Debugf("Rotation vote broadcasted to network")
    return nil
}

// Broadcasts consensus results.
func (n *Node) BroadcastConsensusResult(result *rotatingking.ConsensusResult) error {
    if n == nil || n.kingTopic == nil {
        return errors.New("p2p node or king topic not initialized")
    }

    n.logger.Info("Broadcasting consensus result",
        logrus.Fields{
            "proposalId": result.ProposalID[:8],
            "approved": result.Approved,
            "approvalCount": result.ApprovalCount,
            "totalPeers": result.TotalPeers,
        })

    data, err := json.Marshal(result)
    if err != nil {
        return fmt.Errorf("failed to encode consensus result: %w", err)
    }

    msg := make([]byte, 1+len(data))
    msg[0] = msgTypeKingRotation
    copy(msg[1:], data)

    if err := n.kingTopic.Publish(n.ctx, msg); err != nil {
        return fmt.Errorf("gossipsub publish failed: %w", err)
    }

    n.logger.Infof("Consensus result broadcasted to network")
    return nil
}

// GetPeerCount returns the number of connected peers (implements rotatingking.P2PBroadcaster)
func (n *Node) GetPeerCount() int {
    if n == nil || n.host == nil {
        return 0
    }
    return len(n.host.Network().Peers())
}

// GetPeers returns list of connected peers (implements rotatingking.P2PBroadcaster)
func (n *Node) GetPeers() []string {
    if n == nil || n.host == nil {
        return []string{}
    }

    peers := n.host.Network().Peers()
    peerStrings := make([]string, len(peers))
    for i, peerID := range peers {
        peerStrings[i] = peerID.String()
    }
    return peerStrings
}

// Broadcasts rotating king state.
func (n *Node) BroadcastKingState(event *rotatingking.KingStateBroadcast) error {
    if n == nil || n.kingTopic == nil {
        return errors.New("p2p node or king topic not initialized")
    }

    n.logger.Info("Broadcasting king state",
        logrus.Fields{
            "blockHeight": event.BlockHeight,
            "currentKingIndex": event.CurrentKingIndex,
            "kingCount": len(event.KingAddresses),
        })

    // Create a KingListUpdateEvent from the state (for compatibility)
    updateEvent := &rotatingking.KingListUpdateEvent{
        BlockHeight: event.BlockHeight,
        NewList:     event.KingAddresses,
        Timestamp:   event.BroadcastTimestamp,
        Reason:      "state_broadcast",
    }

    return n.BroadcastKingListUpdate(updateEvent)
}

func (n *Node) BroadcastRotation(event *rotatingking.KingRotationBroadcast) error {
    // Convert KingRotationBroadcast to KingRotation
    rotation := &rotatingking.KingRotation{
        BlockHeight:  event.BlockHeight,
        PreviousKing: event.PreviousKing,
        NewKing:      event.NewKing,
        Timestamp:    event.Timestamp,
        Reward:       big.NewInt(0),
        WasEligible:  true,
        Reason:       "broadcast",
    }

    // Use the existing BroadcastKingRotation method
    return n.BroadcastKingRotation(rotation)
}

func (n *Node) isImportantAddress(addr common.Address) bool {
    // Define important addresses (main king, known validators, etc.)
    importantAddresses := []common.Address{
        common.HexToAddress("0xb007d5cde43250cA61E87799ed3416A0B20f4FC2"), // Main King
        // Add other important addresses HERE
    }

    for _, important := range importantAddresses {
        if addr == important {
            return true
        }
    }
    return false
}

func (n *Node) addressInList(addr common.Address, list []common.Address) bool {
    for _, a := range list {
        if a == addr {
            return true
        }
    }
    return false
}

func (n *Node) checkAndSyncKingConfig() {
    mgr := n.chain.GetRotatingKingManager()
    if mgr == nil {
        return
    }

    localList := mgr.GetKingAddresses()
    localCount := len(localList)
    localKing := mgr.GetCurrentKing()

    n.logger.Infof("🔍 Checking rotating king config: %d addresses, current king: %s",
        localCount, localKing.Hex()[:10])

    // Check if we have the minimum expected configuration
    if localCount < 2 {
        n.logger.Warnf("⚠️ LOW ADDRESS COUNT (%d) - REQUESTING CONFIG FROM PEERS", localCount)

        // Broadcast our config request
        for _, peer := range n.Peers() {
            n.logger.Infof("📤 Requesting config from peer %s", peer.String()[:8])
            go n.RequestKingConfigFromPeer(peer)
        }

        // Also broadcast our current state (even if minimal)
        n.BroadcastCurrentKingConfig()
    }

    // Check if current king is valid
    if localKing == (common.Address{}) {
        n.logger.Warn("⚠️ NO CURRENT ROTATING KING - TRIGGERING EMERGENCY SYNC")
        n.triggerEmergencyConfigSync()
    }
}

func (n *Node) triggerEmergencyConfigSync() {
    n.logger.Warn("🚨 EMERGENCY CONFIGURATION SYNC TRIGGERED")

    // Broadcast urgent config request
    for _, pid := range n.Peers() {
        n.logger.Infof("🚨 URGENT: Requesting config from peer %s", pid.String()[:8])
        go func(p peer.ID) {
            // Send multiple requests to ensure response
            for i := 0; i < 3; i++ {
                n.RequestKingConfigFromPeer(p)
                time.Sleep(1 * time.Second)
            }
        }(pid)
    }

    // Wait for responses then force update
    time.AfterFunc(5*time.Second, func() {
        n.processMu.Lock()
        mgr := n.chain.GetRotatingKingManager()
        n.processMu.Unlock()

        if mgr == nil {
            return
        }

        // Get configuration from cache or use defaults
        n.applyEmergencyDefaultConfiguration(mgr)
    })
}

func (n *Node) applyEmergencyDefaultConfiguration(mgr reward.RotatingKingManager) {
    n.logger.Warn("🔄 Applying emergency default configuration")

    // Try to get any configuration from cache first
    var bestConfig *rotatingking.RotatingKingConfig

    n.dbSyncMu.RLock()
    for _, response := range n.dbSyncResponses {
        if response.Config != nil && len(response.Config.KingAddresses) >= 2 {
            bestConfig = response.Config
            break
        }
    }
    n.dbSyncMu.RUnlock()

    // If no config in cache, use hardcoded defaults
    if bestConfig == nil {
        n.logger.Warn("No cached configuration found, using hardcoded defaults")

        // Default addresses
        defaultAddresses := []common.Address{

        }

        // Create default config
        config := rotatingking.RotatingKingConfig{
            KingAddresses:    defaultAddresses,
            RotationInterval: 100,
            RotationOffset:   0,
            ActivationDelay:  2,
            MinStakeRequired: rotatingking.EligibilityThreshold,
        }
        bestConfig = &config
    }

    // Apply the configuration
    if updater, ok := mgr.(interface{ UpdateKingAddresses([]common.Address) error }); ok {
        if err := updater.UpdateKingAddresses(bestConfig.KingAddresses); err != nil {
            n.logger.Errorf("Failed to apply emergency configuration: %v", err)
        } else {
            n.logger.Infof("✅ Emergency configuration applied: %d addresses",
                len(bestConfig.KingAddresses))

            // Broadcast our new configuration
            n.BroadcastCurrentKingConfig()
        }
    }
}

func (n *Node) startConfigurationHealthCheck() {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-n.ctx.Done():
            return
        case <-ticker.C:
            n.healthCheckRotatingKingConfig()
        }
    }
}

func (n *Node) healthCheckRotatingKingConfig() {
    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    n.processMu.Unlock()

    if mgr == nil {
        return
    }

    currentList := mgr.GetKingAddresses()
    currentKing := mgr.GetCurrentKing()

    // Minimum address count
    if len(currentList) < 2 {
        n.logger.Warnf("⚠️ CONFIGURATION HEALTH: Only %d addresses (minimum 2 required)",
            len(currentList))
        n.triggerEmergencyConfigSync()
        return
    }

    //Current king exists in list
    kingFound := false
    for _, addr := range currentList {
        if addr == currentKing {
            kingFound = true
            break
        }
    }

    if !kingFound && currentKing != (common.Address{}) {
        n.logger.Warnf("⚠️ CONFIGURATION HEALTH: Current king %s not in address list",
            currentKing.Hex()[:10])
        // Reset to first address
        if len(currentList) > 0 {
            mgr.ForceRotateToAddress(currentList[0], "health-check-repair")
        }
    }

    // Broadcast our config if healthy
    if len(currentList) >= 2 && kingFound {
        n.logger.Debugf("✅ Configuration healthy: %d addresses, king=%s",
            len(currentList), currentKing.Hex()[:10])
        // Periodically broadcast to help other nodes
        n.BroadcastCurrentKingConfig()
    }
}

// Syncs rotating king database for a specific block
func (n *Node) syncRotatingKingForBlock(blockHeight uint64) {
    if n.chain.GetRotatingKingManager() == nil {
        return
    }

    n.logger.Debugf("🔄 Syncing rotating king database for block %d", blockHeight)

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    if err := n.chain.GetRotatingKingManager().SyncBlocks(ctx, blockHeight); err != nil {
        n.logger.Warnf("Rotating king sync failed for block %d: %v", blockHeight, err)
    } else {
        n.logger.Debugf("✅ Rotating king database synced to block %d", blockHeight)
    }
}

func (n *Node) EnsureKingConfigBroadcast() {
    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    n.processMu.Unlock()

    if mgr == nil {
        return
    }

    currentList := mgr.GetKingAddresses()
    if len(currentList) == 0 {
        return
    }

    // Always broadcast when list changes
    event := &rotatingking.KingListUpdateEvent{
        BlockHeight: n.currentHeight(),
        NewList:     currentList,
        Timestamp:   time.Now(),
        Reason:      "periodic_broadcast",
    }

    if err := n.BroadcastKingListUpdate(event); err != nil {
        n.logger.Debugf("Failed to broadcast king config: %v", err)
    } else {
        n.logger.Debugf("Periodic king config broadcast: %d addresses", len(currentList))
    }
}

func (n *Node) StartPeriodicConfigBroadcast() {
    ticker := time.NewTicker(5 * time.Minute) // Broadcast every 5 minutes
    defer ticker.Stop()

    for {
        select {
        case <-n.ctx.Done():
            return
        case <-ticker.C:
            n.EnsureKingConfigBroadcast()
        }
    }
}

func (n *Node) detectAndBroadcastKingListChanges() {
    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    n.processMu.Unlock()

    if mgr == nil {
        return
    }

    currentList := mgr.GetKingAddresses()

    n.lastKingListMu.Lock()
    lastList := n.lastKingList
    n.lastKingListMu.Unlock()

    // Check if list has changed
    if !n.areAddressListsEqual(lastList, currentList) {
        n.onKingListChanged(currentList)
    }
}

func (n *Node) startKingListChangeDetector() {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-n.ctx.Done():
            return
        case <-ticker.C:
            n.detectAndBroadcastKingListChanges()
        }
    }
}

// Compares king lists with all peers and merges them
func (n *Node) CompareAndSyncKingLists() {
    n.processMu.Lock()
    mgr := n.chain.GetRotatingKingManager()
    n.processMu.Unlock()

    if mgr == nil {
        n.logger.Warn("Cannot sync king lists: rotating king manager not available")
        return
    }

    // Get our current list
    ourList := mgr.GetKingAddresses()
    ourCount := len(ourList)
    
    n.logger.Infof("🔍 Starting king list comparison: we have %d addresses", ourCount)

    // Collect lists from all peers
    allAddresses := make(map[common.Address]int) // address -> count of peers that have it
    allLists := make(map[string][]common.Address) // peerID -> address list
    
    // Start with our own list
    for _, addr := range ourList {
        allAddresses[addr]++
    }
    allLists[n.host.ID().String()] = ourList

    // Get lists from connected peers
    peers := n.Peers()
    for _, peerID := range peers {
        // Try to get config from peer
        if config := n.getKingConfigFromPeer(peerID); config != nil && len(config.KingAddresses) > 0 {
            peerList := config.KingAddresses
            allLists[peerID.String()] = peerList
            
            // Count addresses
            for _, addr := range peerList {
                allAddresses[addr]++
            }
            
            n.logger.Debugf("Peer %s has %d addresses", peerID.String()[:8], len(peerList))
        }
    }

    // Analyze the collected data
    n.analyzeAndMergeKingLists(ourList, allAddresses, allLists, mgr)
}

// Tries to get king config from a peer
func (n *Node) getKingConfigFromPeer(peerID peer.ID) *rotatingking.RotatingKingConfig {
    // First check if we have a cached response
    n.dbSyncMu.RLock()
    for _, resp := range n.dbSyncResponses {
        if resp.Config != nil && resp.PeerID == peerID.String() {
            // Check if response is recent (last 5 minutes)
            if time.Now().Unix()-resp.Timestamp < 300 {
                n.dbSyncMu.RUnlock()
                return resp.Config
            }
        }
    }
    n.dbSyncMu.RUnlock()

    // If no cached response, request it
    n.RequestKingConfigFromPeer(peerID)
    
    // Wait for response
    time.Sleep(2 * time.Second)
    
    // Check again after waiting
    n.dbSyncMu.RLock()
    defer n.dbSyncMu.RUnlock()
    
    for _, resp := range n.dbSyncResponses {
        if resp.Config != nil && resp.PeerID == peerID.String() {
            return resp.Config
        }
    }
    
    return nil
}

// Analyzes collected lists and merges if needed
func (n *Node) analyzeAndMergeKingLists(ourList []common.Address, 
    allAddresses map[common.Address]int, 
    allLists map[string][]common.Address,
    mgr reward.RotatingKingManager) {
    
    totalPeers := len(allLists)
    if totalPeers < 2 {
        n.logger.Debug("Not enough peers for meaningful comparison")
        return
    }

    // Find addresses that appear in multiple lists (consensus addresses)
    consensusThreshold := totalPeers/2 + 1 // More than half of peers
    consensusAddresses := make([]common.Address, 0)
    allUniqueAddresses := make([]common.Address, 0)
    
    for addr, count := range allAddresses {
        allUniqueAddresses = append(allUniqueAddresses, addr)
        if count >= consensusThreshold {
            consensusAddresses = append(consensusAddresses, addr)
        }
    }

    n.logger.Infof("📊 King list analysis: %d unique addresses across %d peers, %d consensus addresses",
        len(allUniqueAddresses), totalPeers, len(consensusAddresses))

    // Find the largest list
    largestList := ourList
    largestPeer := "us"
    largestCount := len(ourList)
    
    for peerID, list := range allLists {
        if len(list) > largestCount {
            largestCount = len(list)
            largestList = list
            largestPeer = peerID[:8]
        }
    }

    // Check if we need to update
    if largestPeer != "us" {
        n.logger.Warnf("🔄 Peer %s has larger list (%d vs our %d)", 
            largestPeer, largestCount, len(ourList))
        
        // Merge our list with the largest list
        mergedList := n.mergeAddressLists(ourList, largestList)
        
        if len(mergedList) > len(ourList) {
            n.logger.Infof("Merging lists: %d → %d addresses", len(ourList), len(mergedList))
            
            if err := mgr.UpdateKingAddresses(mergedList); err != nil {
                n.logger.Errorf("Failed to merge king list: %v", err)
            } else {
                n.logger.Info("✅ King list merged successfully")
                
                // Broadcast updated list
                n.BroadcastCurrentKingConfig()
            }
        }
    }

    // If we have consensus addresses, ensure they're in our list
    if len(consensusAddresses) > 0 {
        missingConsensus := n.findMissingAddresses(ourList, consensusAddresses)
        if len(missingConsensus) > 0 {
            n.logger.Warnf("We're missing %d consensus addresses", len(missingConsensus))
            
            // Add missing consensus addresses
            updatedList := append(ourList, missingConsensus...)
            
            if err := mgr.UpdateKingAddresses(updatedList); err != nil {
                n.logger.Errorf("Failed to add consensus addresses: %v", err)
            } else {
                n.logger.Infof("✅ Added %d consensus addresses", len(missingConsensus))
                n.BroadcastCurrentKingConfig()
            }
        }
    }

    // Create a comprehensive list that includes all addresses
    if len(allUniqueAddresses) > len(ourList) {
        // Check if we should create a super-set list
        n.considerCreatingSuperSet(ourList, allUniqueAddresses, mgr)
    }
}

// Finds addresses in target that are not in source
func (n *Node) findMissingAddresses(source, target []common.Address) []common.Address {
    missing := make([]common.Address, 0)
    
    for _, targetAddr := range target {
        found := false
        for _, sourceAddr := range source {
            if sourceAddr == targetAddr {
                found = true
                break
            }
        }
        if !found {
            missing = append(missing, targetAddr)
        }
    }
    
    return missing
}

// Decides whether to create a comprehensive list
func (n *Node) considerCreatingSuperSet(ourList, allAddresses []common.Address, mgr reward.RotatingKingManager) {
    // Only create super-set if we're missing significant addresses
    missingCount := len(allAddresses) - len(ourList)
    
    if missingCount > 0 {
        n.logger.Infof("We're missing %d addresses that other peers have", missingCount)
        
        // Ask for user/configuration decision
        // For now, auto-merge if we're missing more than 20% of addresses
        threshold := len(allAddresses) / 5
        
        if missingCount > threshold {
            n.logger.Warnf("Missing %d addresses (>%d threshold) - creating super-set", 
                missingCount, threshold)
            
            // Create merged list
            mergedList := n.mergeAddressLists(ourList, allAddresses)
            
            if err := mgr.UpdateKingAddresses(mergedList); err != nil {
                n.logger.Errorf("Failed to create super-set: %v", err)
            } else {
                n.logger.Infof("✅ Created comprehensive list with %d addresses", len(mergedList))
                n.BroadcastCurrentKingConfig()
            }
        }
    }
}

// Merges two address lists, removing duplicates
func (n *Node) mergeAddressLists(list1, list2 []common.Address) []common.Address {
    merged := make([]common.Address, 0, len(list1)+len(list2))
    seen := make(map[common.Address]bool)
    
    // Add all from list1
    for _, addr := range list1 {
        if !seen[addr] {
            merged = append(merged, addr)
            seen[addr] = true
        }
    }
    
    // Add from list2 if not already present
    for _, addr := range list2 {
        if !seen[addr] {
            merged = append(merged, addr)
            seen[addr] = true
        }
    }
    
    return merged
}

// Starts periodic king list comparison
func (n *Node) StartPeriodicKingListSync() {
    // Wait for initial connections
    time.Sleep(30 * time.Second)
    
    ticker := time.NewTicker(2 * time.Minute) // Compare every 2 minutes
    defer ticker.Stop()
    
    n.logger.Info("🔄 Starting periodic king list synchronization")
    
    for {
        select {
        case <-n.ctx.Done():
            return
        case <-ticker.C:
            n.CompareAndSyncKingLists()
        }
    }
}

func (n *Node) TriggerImmediateKingListSync() {
    n.logger.Info("🚀 Triggering immediate king list sync")
    go n.CompareAndSyncKingLists()
}
