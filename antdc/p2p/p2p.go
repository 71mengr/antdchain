// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package p2p

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/antdaza/antdchain/antdc/block"
	"github.com/antdaza/antdchain/antdc/checkpoints"
	"github.com/antdaza/antdchain/antdc/reward"
	"github.com/antdaza/antdchain/antdc/rotatingking"
	"github.com/antdaza/antdchain/antdc/tx"
        "github.com/antdaza/antdchain/common"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	kd "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	libp2pnoise "github.com/libp2p/go-libp2p/p2p/security/noise"
	libp2ptls "github.com/libp2p/go-libp2p/p2p/security/tls"
	"github.com/multiformats/go-multiaddr"
	"github.com/sirupsen/logrus"
	"io"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

var _ rotatingking.P2PBroadcaster = (*Node)(nil)

const (
	msgTypeBlock             = 0x01
	msgTypeTx                = 0x02
	msgTypeKingRotation      = 0x03
	msgTypeKingListUpdate    = 0x04
	msgTypeDBSyncRequest     = 0x05 // NEW: Request database sync
	msgTypeDBSyncResponse    = 0x06 // NEW: Database sync response
	msgTypeDBSyncStatus      = 0x07 // NEW: Database status
	msgTypeDBSyncAnnounce    = 0x08 // NEW: Database sync announcement
	msgTypeKingConfig        = 0x09
	msgTypeKingConfigRequest = iota + 30
	orphanBlockTTL           = 2 * time.Minute
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
	RequestID   string                           `json:"requestId"`
	Status      string                           `json:"status"` // "success", "partial", "error"
	Rotations   []rotatingking.KingRotation      `json:"rotations,omitempty"`
	Config      *rotatingking.RotatingKingConfig `json:"config,omitempty"` // ADD THIS LINE
	LatestBlock uint64                           `json:"latestBlock"`
	SyncState   *rotatingking.SyncState          `json:"syncState,omitempty"`
	Timestamp   int64                            `json:"timestamp"`
	Error       string                           `json:"error,omitempty"`
	PeerID      string                           `json:"peerId"` // Who is responding
}

type DBSyncStatus struct {
	PeerID          string                  `json:"peerId"`
	LastSyncedBlock uint64                  `json:"lastSyncedBlock"`
	SyncState       *rotatingking.SyncState `json:"syncState"`
	IsSyncing       bool                    `json:"isSyncing"`
	KingCount       int                     `json:"kingCount"`
	LatestRotation  uint64                  `json:"latestRotation"`
	Timestamp       int64                   `json:"timestamp"`
	Version         string                  `json:"version"`
}

type KingConfigSync struct {
	Config      rotatingking.RotatingKingConfig `json:"config"`
	BlockHeight uint64                          `json:"blockHeight"`
	Timestamp   int64                           `json:"timestamp"`
	PeerID      string                          `json:"peerId"`
}

type DBSyncAnnounce struct {
	PeerID       string   `json:"peerId"`
	Capabilities []string `json:"capabilities"` // "sync", "history", "backup"
	SupportsSync bool     `json:"supportsSync"`
	MaxBatchSize int      `json:"maxBatchSize"`
	Timestamp    int64    `json:"timestamp"`
}

// Database sync metrics
type DBSyncMetrics struct {
	SyncAttempts     int                     `json:"syncAttempts"`
	SuccessfulSyncs  int                     `json:"successfulSyncs"`
	FailedSyncs      int                     `json:"failedSyncs"`
	LastSyncTime     time.Time               `json:"lastSyncTime"`
	TotalRotations   int                     `json:"totalRotations"`
	BytesTransferred int64                   `json:"bytesTransferred"`
	PeerCount        int                     `json:"peerCount"`
	IsActive         bool                    `json:"isActive"`
	LastKingList     []common.QuantumAddress `json:"lastKingList"`
}

type KingRotationEvent struct {
	BlockHeight        uint64                `json:"height"`
	PreviousKing       common.QuantumAddress `json:"prevKing"`
	NewKing            common.QuantumAddress `json:"newKing"`
	Eligible           bool                  `json:"eligible"`
	EligibilityBalance *big.Int              `json:"balance"`
	Timestamp          time.Time             `json:"ts"`
	Reason             string                `json:"reason,omitempty"`
}

type rateLimiter struct {
	count     int
	resetTime time.Time
}

type minedBlockCandidate struct {
	Block      *block.Block
	Source     string
	ReceivedAt time.Time
}

type minedBlockProposalValidator interface {
	ValidateMinedBlockProposal(*block.Block) error
}

const (
	MaxTxPerPeerPerSecond  = 50
	MaxTxPerPeerBurst      = 200
	MaxBlocksPerPeerPerSec = 10
	DefaultMaxPeers        = 100
	MaxDirectPushBytes     = 4 << 20
	MaxConfigStreamBytes   = 64 << 10
	MaxSyncResponseBytes   = 16 << 20
	MaxConnsPerPeer        = 3
	DefaultNetworkNamespace = "antdchain"
)

type Config struct {
	DataDir           string        // Directory for persistent data
	Port              int           // P2P listening port
	BootstrapPeers    []string      // Initial peers to connect to
	EnableMDNS        bool          // Enable mDNS discovery
	EnableDHT         bool          // Enable DHT discovery
	EnableNATService  bool          // Enable NAT traversal
	MaxPeers          int           // Maximum number of connected peers
	MinPeers          int           // Minimum peers before discovery
	ConnectionTimeout time.Duration // Timeout for connections
	NetworkNamespace string        // Namespace for protocols, topics, and discovery
	LogLevel          string        // Log level
	LogOutput         io.Writer     // Optional writer for logs
	Context           context.Context
}

// DefaultConfig returns configuration with sensible defaults
func DefaultConfig() Config {
	return Config{
		DataDir:           "./antdchain-data",
		Port:              3000,
		EnableMDNS:        true,
		EnableDHT:         true,
		EnableNATService:  true,
		MaxPeers:          DefaultMaxPeers,
		MinPeers:          5,
		ConnectionTimeout: 30 * time.Second,
		NetworkNamespace: DefaultNetworkNamespace,
		LogLevel:          defaultP2PLogLevel(),
	}
}

func defaultP2PLogLevel() string {
	for _, arg := range os.Args[1:] {
		if arg == "--log-info" {
			return "info"
		}
	}

	return "error"
}


func normalizeNetworkNamespace(namespace string) string {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return DefaultNetworkNamespace
	}

	namespace = strings.ToLower(namespace)
	re := regexp.MustCompile(`[^a-z0-9-]+`)
	namespace = re.ReplaceAllString(namespace, "-")
	namespace = strings.Trim(namespace, "-")
	if namespace == "" {
		return DefaultNetworkNamespace
	}

	return namespace
}

func (n *Node) protocolID(name string) protocol.ID {
	return protocol.ID(fmt.Sprintf("/%s/%s/1.0.0", normalizeNetworkNamespace(n.cfg.NetworkNamespace), name))
}

func namespacedValue(namespace, suffix string) string {
	return normalizeNetworkNamespace(namespace) + suffix
}

func (n *Node) topicName(suffix string) string {
	return namespacedValue(n.cfg.NetworkNamespace, suffix)
}

func (n *Node) dhtRendezvous() string {
	return namespacedValue(n.cfg.NetworkNamespace, "-mainnet-v1")
}

type orphanBlockEntry struct {
	block     *block.Block
	expiresAt time.Time
	reason    string
}

type Node struct {
	host      host.Host
	pubsub    *pubsub.PubSub
	topic     *pubsub.Topic
	sub       *pubsub.Subscription
	chain     Chain
	logger    *logrus.Logger
	mu        sync.RWMutex
	publishMu sync.RWMutex
	processMu sync.Mutex
	syncMu    sync.Mutex
	orphanPool   map[common.Hash]orphanBlockEntry
	orphanPoolMu sync.Mutex
	synced         bool
	syncHeight     uint64
	ctx            context.Context
	cancel         context.CancelFunc
	dht            *kd.IpfsDHT
	txPerPeer      map[peer.ID]*rateLimiter
	txPerPeerMu    sync.RWMutex
	blockPerPeer   map[peer.ID]*rateLimiter
	blockPerPeerMu sync.RWMutex
	cfg            Config

	knownTxs      map[common.Hash]time.Time
	knownTxsMu    sync.RWMutex
	knownTxsLimit int

	lastSyncTime time.Time
	syncAttempts int
	lastSyncPeer peer.ID

	kingTopic      *pubsub.Topic
	kingSub        *pubsub.Subscription
	eventPerPeer   map[peer.ID]*rateLimiter
	eventPerPeerMu sync.RWMutex

	// Database sync fields
	dbSyncMu        sync.RWMutex
	dbSyncRequests  map[string]*DBSyncRequest  // Track ongoing requests
	dbSyncResponses map[string]*DBSyncResponse // Cache responses
	dbSyncPeers     map[string]*DBSyncStatus   // Track peer sync status
	dbSyncMetrics   *DBSyncMetrics             // Sync metrics
	dbSyncInterval  time.Duration              // How often to sync
	lastDBSyncTime  time.Time
	dbSyncTopic     *pubsub.Topic        // Separate topic for DB sync
	dbSyncSub       *pubsub.Subscription // Subscription for DB sync
	dbSyncEnabled   bool                 // Whether DB sync is enabled
	dbSyncVersion   string               // Sync protocol version

	// Sync coordination
	isDBSyncing     bool
	currentSyncPeer string
	syncRetryCount  int
	maxSyncRetries  int

	banManager *BanManager

	lastKingList   []common.QuantumAddress
	lastKingListMu sync.RWMutex

	minedBlockMu         sync.RWMutex
	minedBlockCandidates map[uint64]map[common.Hash]minedBlockCandidate
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


func configuredExternalIP() string {
	for _, envName := range []string{"ANTD_EXTERNAL_IP", "ANTDCHAIN_EXTERNAL_IP"} {
		if externalIP := strings.TrimSpace(os.Getenv(envName)); externalIP != "" {
			return externalIP
		}
	}
	return ""
}

func readLimitedStream(r io.Reader, maxBytes int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("stream exceeds %d byte limit", maxBytes)
	}
	return data, nil
}

func (n *Node) allowStreamFromPeer(pid peer.ID, streamType string) bool {
	if n == nil || pid == "" {
		return false
	}
	if n.banManager != nil {
		if n.banManager.IsBanned(pid) {
			n.logger.Debugf("Rejecting %s stream from banned peer %s", streamType, pid.String()[:8])
			return false
		}
		if violation := n.banManager.checkRateLimit(pid); violation != nil {
			violation.Type = "STREAM_" + violation.Type
			violation.Details = fmt.Sprintf("%s stream rate limit exceeded: %s", streamType, violation.Details)
			n.banManager.RecordViolation(pid, violation, nil)
			return false
		}
	}
	return true
}

func (n *Node) recordPeerViolation(pid peer.ID, violationType string, severity int, details string) {
	if n == nil || n.banManager == nil || pid == "" {
		return
	}
	n.banManager.RecordViolation(pid, &Violation{
		Type:      violationType,
		Severity:  severity,
		Timestamp: time.Now(),
		Details:   details,
	}, nil)
}

func (n *Node) connectDiscoveredPeer(pi peer.AddrInfo) {
	if n == nil || n.host == nil || pi.ID == n.host.ID() {
		return
	}
	if n.banManager != nil && n.banManager.IsBanned(pi.ID) {
		return
	}
	if len(n.Peers()) >= n.cfg.MaxPeers {
		return
	}

	n.host.Peerstore().AddAddrs(pi.ID, pi.Addrs, peerstore.TempAddrTTL)
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.ConnectionTimeout)
	defer cancel()
	if err := n.host.Connect(ctx, pi); err != nil {
		n.logger.Debugf("Failed to connect discovered peer %s: %v", pi.ID.String()[:8], err)
		return
	}
	n.logger.Infof("Connected to discovered peer %s", pi.ID.String()[:8])
	go n.syncIfBehind(pi.ID)
}

func (n *Node) handlePeerConnected(_ network.Network, conn network.Conn) {
	if n == nil || n.host == nil {
		return
	}
	pid := conn.RemotePeer()
	if pid == "" || pid == n.host.ID() {
		return
	}
	if n.banManager != nil && n.banManager.IsBanned(pid) {
		_ = n.host.Network().ClosePeer(pid)
		return
	}
	if conns := n.host.Network().ConnsToPeer(pid); len(conns) > MaxConnsPerPeer {
		n.recordPeerViolation(pid, "CONNECTION_FLOOD", 7, fmt.Sprintf("too many connections: %d", len(conns)))
		_ = conn.Close()
	}
}

func (n *Node) GossipSubReady() error {
	if n == nil {
		return errors.New("p2p node not initialized")
	}
	if n.ctx == nil {
		return errors.New("p2p context not initialized")
	}
	select {
	case <-n.ctx.Done():
		return fmt.Errorf("p2p context canceled: %w", n.ctx.Err())
	default:
	}
	if n.pubsub == nil {
		return errors.New("p2p pubsub not initialized")
	}
	if n.topic == nil {
		return errors.New("p2p topic not initialized")
	}
	if n.sub == nil {
		return errors.New("p2p subscription not initialized")
	}
	return nil
}

func (n *Node) rememberMinedBlockCandidate(b *block.Block, source string) {
	if n == nil || b == nil || b.Header == nil || b.Header.Number == nil {
		return
	}

	height := b.Header.Number.Uint64()
	hash := b.Hash()
	if source == "" {
		source = "unknown"
	}

	n.minedBlockMu.Lock()
	if n.minedBlockCandidates == nil {
		n.minedBlockCandidates = make(map[uint64]map[common.Hash]minedBlockCandidate)
	}
	if n.minedBlockCandidates[height] == nil {
		n.minedBlockCandidates[height] = make(map[common.Hash]minedBlockCandidate)
	}

	_, existed := n.minedBlockCandidates[height][hash]
	n.minedBlockCandidates[height][hash] = minedBlockCandidate{
		Block:      b,
		Source:     source,
		ReceivedAt: time.Now(),
	}
	candidateCount := len(n.minedBlockCandidates[height])
	for candidateHeight, candidates := range n.minedBlockCandidates {
		if candidateHeight+128 < height {
			delete(n.minedBlockCandidates, candidateHeight)
			continue
		}
		if len(candidates) == 0 {
			delete(n.minedBlockCandidates, candidateHeight)
		}
	}
	n.minedBlockMu.Unlock()

	if !existed && candidateCount > 1 && n.logger != nil {
		n.logger.Infof("P2P is tracking %d mined block candidates at height %d; fork-choice will compare protocol-valid blocks before acceptance", candidateCount, height)
	}
}

func (n *Node) minedBlockCandidateCount(height uint64) int {
	if n == nil {
		return 0
	}
	n.minedBlockMu.RLock()
	defer n.minedBlockMu.RUnlock()
	return len(n.minedBlockCandidates[height])
}

func (n *Node) validateMinedBlockProposal(b *block.Block) error {
	validator, ok := n.chain.(minedBlockProposalValidator)
	if !ok || validator == nil {
		return nil
	}
	if err := validator.ValidateMinedBlockProposal(b); err != nil {
		return fmt.Errorf("mined block proposal failed network protocol validation: %w", err)
	}
	return nil
}

// BroadcastBlock — secure, efficient, anti-spam block propagation
func (n *Node) BroadcastBlock(b *block.Block) error {
	if n == nil {
		return errors.New("p2p node not initialized")
	}
	if b == nil || b.Header == nil {
		return errors.New("nil block or header")
	}

	n.publishMu.RLock()
	defer n.publishMu.RUnlock()

	if err := n.GossipSubReady(); err != nil {
		return err
	}

	// Validate block hash
	if b.Hash() != b.Header.Hash() { // ← Fixed: use .Hash() not .ComputeHash()
		return errors.New("invalid block: hash mismatch")
	}

	n.rememberMinedBlockCandidate(b, "local-miner")

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

	publishCtx, cancel := context.WithTimeout(n.ctx, 5*time.Second)
	defer cancel()
	if err := n.topic.Publish(publishCtx, msg); err != nil {
		return fmt.Errorf("gossipsub publish failed: %w", err)
	}

	go n.directPushBlock(b, msg)

	n.logger.Infof("BLOCK BROADCAST #%d | hash=%s | txs=%d | size=%d KB",
		b.Header.Number.Uint64(),
		b.Hash().String()[:12],
		len(b.Txs),
		len(data)/1024,
	)

	return nil
}

// directPushBlock sends block directly to recent peers.
func (n *Node) directPushBlock(b *block.Block, msg []byte) {
	count := n.directPushMessage(msg, 15)
	if count > 0 {
		n.logger.Debugf("Direct-pushed block %d to %d peers", b.Header.Number.Uint64(), count)
	}
}

// directPushTx sends a transaction directly to connected peers in addition to
// GossipSub. This makes wallet-originated transactions visible to peers even
// when the GossipSub mesh has not fully formed yet.
func (n *Node) directPushTx(t *tx.Tx, msg []byte) {
	count := n.directPushMessage(msg, 15)
	if count > 0 {
		n.logger.Debugf("Direct-pushed tx %s to %d peers", t.Hash().Hex()[:10], count)
	}
}

// directPushMessage sends a pre-encoded network message over the direct push
// stream protocol and returns the number of peers that accepted the write.
func (n *Node) directPushMessage(msg []byte, limit int) int {
	if n == nil || n.host == nil || n.ctx == nil {
		return 0
	}
	peers := n.host.Network().Peers()
	count := 0
	for _, pid := range peers {
		if count >= limit {
			break
		}
		if pid == n.host.ID() {
			continue
		}

		ctx, cancel := context.WithTimeout(n.ctx, 3*time.Second)
		s, err := n.host.NewStream(ctx, pid, n.protocolID("direct"))
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

	return count
}

// BroadcastTx — secure, efficient, spam-resistant transaction broadcast
func (n *Node) BroadcastTx(t *tx.Tx) error {
	if t == nil {
		return errors.New("nil transaction")
	}

	// Add entry logging
	n.logger.Debugf("BroadcastTx called for tx %s", t.Hash().Hex()[:10])

	if err := t.Validate(); err != nil {
		return fmt.Errorf("invalid transaction: %w", err)
	}
	if valid, err := t.Verify(); err != nil || !valid {
		return errors.New("invalid signature")
	}

	hash := t.Hash()
	n.logger.Debugf("Tx %s validation passed", hash.Hex()[:10])

	// Check context
	if n.ctx == nil {
		n.logger.Error("P2P context is nil")
		return errors.New("p2p context not initialized")
	}

	// Check context not canceled
	select {
	case <-n.ctx.Done():
		n.logger.Errorf("P2P context canceled: %v", n.ctx.Err())
		return fmt.Errorf("p2p context canceled: %w", n.ctx.Err())
	default:
		n.logger.Debug("P2P context is valid")
	}

	// Check if we've recently broadcast this transaction
	n.knownTxsMu.RLock()
	broadcastTime, recentlyBroadcast := n.knownTxs[hash]
	n.knownTxsMu.RUnlock()

	if recentlyBroadcast {
		elapsed := time.Since(broadcastTime)
		if elapsed < 30*time.Second { // Only skip if really recent
			n.logger.Debugf("Already broadcast tx %s within %v, skipping", hash.Hex()[:10], elapsed)
			return nil
		}
	}

	// Check topic
	if n.topic == nil {
		n.logger.Error("P2P topic is nil")
		return errors.New("p2p topic not initialized")
	}

	data, err := json.Marshal(t)
	if err != nil {
		n.logger.Errorf("Failed to marshal tx: %v", err)
		return fmt.Errorf("failed to marshal tx: %w", err)
	}

	msg := make([]byte, 1+len(data))
	msg[0] = msgTypeTx
	copy(msg[1:], data)

	n.logger.Debugf("Publishing tx %s, msg size=%d bytes", hash.Hex()[:10], len(msg))

	// Publish with timeout
	publishDone := make(chan error, 1)
	go func() {
		publishDone <- n.topic.Publish(n.ctx, msg)
	}()

	select {
	case err := <-publishDone:
		if err != nil {
			n.logger.Errorf("Failed to publish tx: %v", err)
			return fmt.Errorf("failed to publish tx: %w", err)
		}
		n.logger.Infof("Successfully published tx %s", hash.Hex()[:10])
	case <-time.After(5 * time.Second):
		n.logger.Errorf("Publish timeout for tx %s", hash.Hex()[:10])
		return errors.New("publish timeout")
	}

	// Mark as broadcast
	n.knownTxsMu.Lock()
	n.knownTxs[hash] = time.Now()
	n.knownTxsMu.Unlock()

	go n.directPushTx(t, msg)

	n.logger.Infof("Broadcast tx %s (nonce=%d, value=%s)",
		hash.Hex()[:10],
		t.Nonce,
		t.Value.String(),
	)

	// Clean up old entries periodically
	go n.cleanupKnownTxs()

	return nil
}

// Add cleanup function
func (n *Node) cleanupKnownTxs() {
    n.knownTxsMu.Lock()
    defer n.knownTxsMu.Unlock()
    
    cutoff := time.Now().Add(-5 * time.Minute)
    for hash, timestamp := range n.knownTxs {
        if timestamp.Before(cutoff) {
            delete(n.knownTxs, hash)
        }
    }
}

// BroadcastTxForce publishes a transaction even if it was recently broadcast.
// Useful for explicit rebroadcast flows where stronger propagation is required.
func (n *Node) BroadcastTxForce(t *tx.Tx) error {
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

	go n.directPushTx(t, msg)

	n.knownTxsMu.Lock()
	n.knownTxs[hash] = time.Now()
	n.knownTxsMu.Unlock()

	n.logger.Infof("Force-broadcast tx %s (nonce=%d, value=%s)",
		hash.String()[:10],
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
				currentList := mgr.GetKingAddresses()
				mergedList := n.mergeAddressLists(currentList, event.NewList)

				// Never shrink local knowledge from a single remote update; only merge.
				if len(mergedList) == len(currentList) && n.areAddressListsEqual(currentList, mergedList) {
					n.logger.Debug("Received king list update is subset/identical - no changes applied")
					n.processMu.Unlock()
					continue
				}

				if err := mgr.UpdateKingAddresses(mergedList); err != nil {
					n.logger.Warnf("Failed to apply king list update: %v", err)
				} else {
					n.logger.Infof("King list updated via P2P (%d -> %d addresses)", len(currentList), len(mergedList))
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
				event.PreviousKing.String()[:8], event.NewKing.String()[:8], event.BlockHeight, event.Eligible)

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
				blk.Header.Number.Uint64(), blk.Hash().String()[:12], msg.GetFrom().String()[:8])

			n.rememberMinedBlockCandidate(&blk, msg.GetFrom().String())

			// If we're at height 0 and receive any block, force sync
			currentHeight := n.currentHeight()
			if currentHeight == 0 && blk.Header.Number.Uint64() > 0 {
				n.logger.Warnf("EMERGENCY: At height 0 but received block %d - forcing sync!",
					blk.Header.Number.Uint64())
				go n.forceSync()
			}

			if err := n.processBlock(&blk); err != nil {
				if !strings.Contains(err.Error(), "already known") &&
					!strings.Contains(err.Error(), "parent") {
					n.logger.Warnf("Block %d rejected: %v", blk.Header.Number.Uint64(), err)
				}
			}

		case msgTypeTx:
			if len(msg.Data) < 2 {
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

			n.processIncomingTx(&txObj, msg.GetFrom())

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
	s, err := n.host.NewStream(n.ctx, pid, n.protocolID("sync"))
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

// Recursively fetches missing parents until we connect to our chain
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
	remotePeer := s.Conn().RemotePeer()
	if !n.allowStreamFromPeer(remotePeer, "sync") {
		return
	}
	_ = s.SetDeadline(time.Now().Add(10 * time.Second))
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
	if checkpoints := n.chain.Checkpoints(); checkpoints != nil {
		if err := checkpoints.ValidateBlock(request, blk.Hash()); err != nil {
			_ = binary.Write(rw, binary.BigEndian, uint32(0))
			_ = rw.Flush()
			return
		}
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

	s, err := n.host.NewStream(ctx, peerID, n.protocolID("sync"))
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
	if length > MaxSyncResponseBytes {
		if n.banManager != nil {
			n.banManager.BanPeer(peerID, "OVERSIZED_SYNC_RESPONSE", fmt.Sprintf("size=%d", length))
		}
		return nil, fmt.Errorf("sync response too large: %d bytes", length)
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(rw, data); err != nil {
		return nil, fmt.Errorf("failed to read block data: %w", err)
	}
	var blk block.Block
	if err := json.Unmarshal(data, &blk); err != nil {
		return nil, fmt.Errorf("failed to unmarshal block: %w", err)
	}
	if checkpoints := n.chain.Checkpoints(); checkpoints != nil {
		if err := checkpoints.ValidateBlock(blockNumber, blk.Hash()); err != nil {
			if n.banManager != nil {
				n.banManager.BanPeer(peerID, "CHECKPOINT_SYNC_MISMATCH", err.Error())
			}
			return nil, fmt.Errorf("checkpoint validation failed for block %d: %w", blockNumber, err)
		}
	}
	return &blk, nil
}

// startMDNSDiscovery starts mDNS discovery
func (n *Node) startMDNSDiscovery() error {
	svc := mdns.NewMdnsService(n.host, n.topicName("-mdns"), &mdnsNotifee{host: n.host, logger: n.logger, node: n})
	return svc.Start()
}

// mdnsNotifee handles mDNS peer discovery
type mdnsNotifee struct {
	host   host.Host
	logger *logrus.Logger
	node   *Node
}

func (m *mdnsNotifee) HandlePeerFound(pi peer.AddrInfo) {
	if m.node == nil {
		return
	}
	m.node.connectDiscoveredPeer(pi)
}

// Global DHT peer discovery
func (n *Node) startDHTDiscovery() {
	if n.dht == nil {
		return
	}
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	rendezvous := n.dhtRendezvous()
	cidRendezvous := cid.NewCidV1(cid.Raw, []byte(rendezvous))

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			if len(n.Peers()) >= n.cfg.MaxPeers {
				continue
			}
			ctx, cancel := context.WithTimeout(n.ctx, 15*time.Second)
			peerChan := n.dht.FindProvidersAsync(ctx, cidRendezvous, n.cfg.MaxPeers)

			count := 0
			for p := range peerChan {
				if len(n.Peers()) >= n.cfg.MaxPeers {
					break
				}
				if p.ID == n.host.ID() || len(p.Addrs) == 0 {
					continue
				}
				n.logger.Infof("DHT discovered peer: %s", p.ID.String()[:8])
				go n.connectDiscoveredPeer(p)
				count++
			}
			cancel()
			if count == 0 {
				n.logger.Debug("DHT discovery: no new peers")
			}
		}
	}
}

// Announce ourselves on DHT
func (n *Node) announceOnDHT() {
	if n.dht == nil {
		return
	}
	time.Sleep(5 * time.Second)
	ctx, cancel := context.WithTimeout(n.ctx, 30*time.Second)
	defer cancel()

	rendezvous := n.dhtRendezvous()
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

// handleDirectPush handles direct block and transaction pushes.
func (n *Node) handleDirectPush(s network.Stream) {
	defer s.Close()
	remotePeer := s.Conn().RemotePeer()
	if !n.allowStreamFromPeer(remotePeer, "direct") {
		return
	}
	_ = s.SetReadDeadline(time.Now().Add(5 * time.Second))

	data, err := readLimitedStream(s, MaxDirectPushBytes)
	if err != nil || len(data) < 1 {
		if err != nil {
			n.recordPeerViolation(remotePeer, "OVERSIZED_DIRECT_PUSH", 8, err.Error())
		}
		return
	}
	if !n.CheckPeerBeforeProcessingWithCheckpoints(remotePeer, data[0], data) {
		return
	}

	switch data[0] {
	case msgTypeBlock:
		if !n.allowBlockFromPeer(remotePeer) {
			n.recordPeerViolation(remotePeer, "DIRECT_BLOCK_RATE_LIMIT", 6, "direct block rate limit exceeded")
			return
		}

		var blk block.Block
		if err := json.Unmarshal(data[1:], &blk); err != nil {
			n.recordPeerViolation(remotePeer, "MALFORMED_DIRECT_BLOCK", 9, err.Error())
			return
		}

		n.rememberMinedBlockCandidate(&blk, remotePeer.String())

		go func() {
			if err := n.processBlock(&blk); err != nil && !strings.Contains(err.Error(), "already known") {
				n.logger.Warnf("Direct block failed: %v", err)
			}
		}()
	case msgTypeTx:
		if !n.allowTxFromPeer(remotePeer) {
			n.recordPeerViolation(remotePeer, "DIRECT_TX_RATE_LIMIT", 5, "direct transaction rate limit exceeded")
			return
		}

		var txObj tx.Tx
		if err := json.Unmarshal(data[1:], &txObj); err != nil {
			n.recordPeerViolation(remotePeer, "MALFORMED_DIRECT_TX", 9, err.Error())
			return
		}

		go n.processIncomingTx(&txObj, remotePeer)
	}
}

// processIncomingTx validates, stores, marks, and forwards a transaction received
// from either GossipSub or the direct push stream.
func (n *Node) processIncomingTx(txObj *tx.Tx, from peer.ID) {
	if txObj == nil {
		return
	}

	hash := txObj.Hash()

	// Check if we've recently seen this transaction.
	n.knownTxsMu.RLock()
	_, recentlySeen := n.knownTxs[hash]
	n.knownTxsMu.RUnlock()

	if recentlySeen {
		n.logger.Debugf("Already recently saw tx %s, ignoring", hash.String()[:10])
		return
	}

	n.logger.Infof("Received tx %s from %s (nonce=%d)",
		hash.String()[:10], from.String()[:8], txObj.Nonce)

	n.processMu.Lock()
	err := n.chain.TxPool().AddTransaction(txObj, n.chain)
	wasNew := err == nil
	if err != nil && !strings.Contains(err.Error(), "already in pool") {
		n.logger.Warnf("Tx rejected: %v", err)
	}
	n.processMu.Unlock()

	// Mark as seen after local pool processing. Explicit rebroadcast uses the
	// force path so this marker prevents loops without suppressing forwarding.
	n.knownTxsMu.Lock()
	n.knownTxs[hash] = time.Now()
	if len(n.knownTxs) > n.knownTxsLimit {
		for key := range n.knownTxs {
			delete(n.knownTxs, key)
			break
		}
	}
	n.knownTxsMu.Unlock()

	// Only re-broadcast if it was new to us. Use the force path because the tx is
	// now intentionally present in knownTxs to prevent future duplicate handling.
	if wasNew {
		go func(tx *tx.Tx) {
			time.Sleep(50 * time.Millisecond)
			if err := n.BroadcastTxForce(tx); err != nil {
				n.logger.Debugf("Failed to re-broadcast tx %s: %v", tx.Hash().String()[:10], err)
			} else {
				n.logger.Debugf("Re-broadcasted tx %s to network", tx.Hash().String()[:10])
			}
		}(txObj)
	}
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
	if len(bootstrap) == 0 {
		return 0
	}

	availableSlots := n.cfg.MaxPeers - len(n.Peers())
	if availableSlots <= 0 {
		n.logger.Debugf("Skipping bootstrap connections: max peers reached (%d/%d)", len(n.Peers()), n.cfg.MaxPeers)
		return 0
	}

	peerInfos := make([]peer.AddrInfo, 0, len(bootstrap))
	seen := make(map[peer.ID]struct{}, len(bootstrap))
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
		if ai.ID == n.host.ID() {
			continue
		}
		if _, ok := seen[ai.ID]; ok {
			continue
		}
		seen[ai.ID] = struct{}{}
		n.host.Peerstore().AddAddrs(ai.ID, ai.Addrs, peerstore.PermanentAddrTTL)
		peerInfos = append(peerInfos, *ai)
	}

	if len(peerInfos) > availableSlots {
		peerInfos = peerInfos[:availableSlots]
	}

	var wg sync.WaitGroup
	results := make(chan peer.ID, len(peerInfos))
	for _, ai := range peerInfos {
		ai := ai
		wg.Add(1)
		go func() {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(n.ctx, n.cfg.ConnectionTimeout)
			defer cancel()

			if err := n.host.Connect(ctx, ai); err != nil {
				n.logger.Warnf("Failed to connect to bootstrap %s: %v", ai.ID.String()[:8], err)
				return
			}

			n.logger.Infof("Connected to bootstrap node %s", ai.ID.String()[:8])
			go n.syncIfBehind(ai.ID)
			results <- ai.ID
		}()
	}

	wg.Wait()
	close(results)

	count := 0
	for range results {
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
		DataDir:           "./antdchain-data",
		Port:              port,
		BootstrapPeers:    ResolveBootstrapPeers(bootstrap),
		EnableMDNS:        true,
		EnableDHT:         true,
		EnableNATService:  true,
		MaxPeers:          DefaultMaxPeers,
		MinPeers:          5,
		ConnectionTimeout: 30 * time.Second,
		NetworkNamespace: DefaultNetworkNamespace,
		LogLevel:          defaultP2PLogLevel(),
	}

	return NewNodeWithConfig(bc, cfg)
}

// NewNodeWithConfig is the new configurable version
func NewNodeWithConfig(bc Chain, cfg Config) (*Node, error) {
	cfg.BootstrapPeers = ResolveBootstrapPeers(cfg.BootstrapPeers)
	cfg.NetworkNamespace = normalizeNetworkNamespace(cfg.NetworkNamespace)
	if cfg.MaxPeers <= 0 {
		cfg.MaxPeers = DefaultMaxPeers
	}
	if cfg.MinPeers < 0 {
		cfg.MinPeers = 0
	}
	if cfg.MinPeers > cfg.MaxPeers {
		cfg.MinPeers = cfg.MaxPeers
	}

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
	if level, err := logrus.ParseLevel(cfg.LogLevel); err == nil {
		logger.SetLevel(level)
	}
	if cfg.LogOutput != nil {
		logger.SetOutput(cfg.LogOutput)
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
		libp2p.ListenAddrStrings(
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", cfg.Port),
			fmt.Sprintf("/ip6/::/tcp/%d", cfg.Port),
		),
		libp2p.Security(libp2pnoise.ID, libp2pnoise.New),
		libp2p.Security(libp2ptls.ID, libp2ptls.New),
	}

	connectionManager, err := connmgr.NewConnManager(cfg.MinPeers, cfg.MaxPeers)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create connection manager: %w", err)
	}
	opts = append(opts, libp2p.ConnectionManager(connectionManager))

	// NAT options for public reachability when running behind routers/firewalls.
	if cfg.EnableNATService {
		opts = append(opts,
			libp2p.NATPortMap(),         // UPnP / NAT-PMP port mapping
			libp2p.EnableNATService(),   // Enable the AutoNAT service
			libp2p.EnableRelay(),        // Enable circuit relay for NAT traversal
			libp2p.EnableHolePunching(), // Enable hole punching
		)
	}

	// Optional external address announcement. Set ANTD_EXTERNAL_IP when auto-detection is wrong.
	if externalIP := configuredExternalIP(); externalIP != "" {
		externalAddr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", externalIP, cfg.Port))
		if err != nil {
			cancel()
			return nil, fmt.Errorf("invalid ANTD_EXTERNAL_IP %q: %w", externalIP, err)
		}
		opts = append(opts, libp2p.AddrsFactory(func(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
			return append(addrs, externalAddr)
		}))
		logger.Infof("Configured external address: %s", externalAddr)
	}
	// ---------------------------------

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
			kd.ProtocolPrefix(protocol.ID(fmt.Sprintf("/%s/kad/1.0.0", cfg.NetworkNamespace))),
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
		host:            h,
		dht:             dht,
		chain:           bc,
		logger:          logger,
		ctx:             ctx,
		cancel:          cancel,
		txPerPeer:       make(map[peer.ID]*rateLimiter),
		blockPerPeer:    make(map[peer.ID]*rateLimiter),
		eventPerPeer:    make(map[peer.ID]*rateLimiter),
		cfg:             cfg,
		knownTxs:        make(map[common.Hash]time.Time),
		knownTxsLimit:   10000,
		minedBlockCandidates: make(map[uint64]map[common.Hash]minedBlockCandidate),
		orphanPool:      make(map[common.Hash]orphanBlockEntry),
		dbSyncRequests:  make(map[string]*DBSyncRequest),
		dbSyncResponses: make(map[string]*DBSyncResponse),
		dbSyncPeers:     make(map[string]*DBSyncStatus),
		dbSyncMetrics: &DBSyncMetrics{
			SyncAttempts:    0,
			SuccessfulSyncs: 0,
			FailedSyncs:     0,
			TotalRotations:  0,
			IsActive:        true,
			LastKingList:    []common.QuantumAddress{},
		},
		dbSyncInterval: 120 * time.Second,
		dbSyncEnabled:  true,
		dbSyncVersion:  "1.0.0",
		maxSyncRetries: 3,
		lastKingList:   []common.QuantumAddress{},
	}

	// Enable peer protection before accepting streams.
	node.IntegrateBanManagerWithCheckpoints(nil)
	h.Network().Notify(&network.NotifyBundle{ConnectedF: node.handlePeerConnected})

	// Set stream handlers
	h.SetStreamHandler(node.protocolID("sync"), node.handleStream)
	h.SetStreamHandler(node.protocolID("direct"), node.handleDirectPush)
	h.SetStreamHandler(node.protocolID("king-config"), node.handleKingConfigStream)

	// Initialize GossipSub
	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		h.Close()
		cancel()
		return nil, fmt.Errorf("failed to create pubsub: %w", err)
	}

	// Join topics.
	dbSyncTopic, err := ps.Join(node.topicName("-db-sync-v1"))
	if err != nil {
		h.Close()
		cancel()
		return nil, fmt.Errorf("failed to join db sync topic: %w", err)
	}
	dbSub, err := dbSyncTopic.Subscribe()
	if err != nil {
		h.Close()
		cancel()
		return nil, fmt.Errorf("failed to subscribe to db sync topic: %w", err)
	}
	topic, err := ps.Join(node.topicName("-blocks-txs-v1"))
	if err != nil {
		h.Close()
		cancel()
		return nil, fmt.Errorf("failed to join main pubsub topic: %w", err)
	}
	sub, err := topic.Subscribe()
	if err != nil {
		h.Close()
		cancel()
		return nil, fmt.Errorf("failed to subscribe to main pubsub topic: %w", err)
	}
	kingTopic, err := ps.Join(node.topicName("-king-rotations-v1"))
	if err != nil {
		h.Close()
		cancel()
		return nil, fmt.Errorf("failed to join king rotation topic: %w", err)
	}
	kingSub, err := kingTopic.Subscribe()
	if err != nil {
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

	// Initialize checkpoints system before the node begins syncing.
	checkpointsPath := filepath.Join(cfg.DataDir, "checkpoints.json")
	genesisHash := common.HexToHash("0xc78fbaf000cc0023fcb2cef07f0fa8aa35ccc437279510d803306f60447fcb09")
	cp, err := checkpoints.NewCheckpoints(cfg.DataDir, checkpointsPath, genesisHash)
	if err != nil {
		node.Stop()
		return nil, fmt.Errorf("failed to initialize checkpoints: %w", err)
	}
	node.IntegrateBanManagerWithCheckpoints(cp)
	node.logger.Infof("Checkpoints initialized with genesis hash: %s", genesisHash.String())

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
	go node.handleDBSyncMessages()
	go node.periodicDBSync()
	go node.StartPeriodicKingListSync()
	go node.announceDBSyncCapabilities()
	go node.syncKingConfigurationOnStartup()
	node.logger.Info("Database synchronization initialized")
	go node.cleanupKnownTxs()
	go node.startKingListCleanup()
	go func() {
		time.Sleep(2 * time.Second)
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
		time.Sleep(5 * time.Second)
		node.BroadcastCurrentKingConfig()
	}()
	go node.startKingListChangeDetector()

	// You had two calls; keep only one.
	go node.StartPeriodicConfigBroadcast()
	go node.startConfigurationHealthCheck()
	go node.startConfigurationSyncer()
	go node.PeriodicKingConfigCheck()

	time.AfterFunc(3*time.Second, func() {
		node.logger.Info("🚀 Broadcasting initial king configuration")
		node.BroadcastCurrentKingConfig()
	})

	return node, nil
}

func (n *Node) Stop() {
	n.logger.Info("Stopping P2P node...")

	n.publishMu.Lock()
	defer n.publishMu.Unlock()
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
	s, err := n.host.NewStream(ctx, peerID, n.protocolID("sync"))
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
		maxHeight,                // Latest block
		maxHeight - 1,            // Previous block
		maxHeight - 10,           // 10 blocks back
		maxHeight / 2,            // Middle of chain
		maxHeight / 4,            // Quarter point
		100, 50, 25, 10, 5, 1, 0, // Fixed checkpoints
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
				n.syncIfBehind(pid)
			}
		}(pid)
	}
}

func (n *Node) syncMissingBlocks(peerID peer.ID, targetHeight uint64) error {
	localHeight := n.currentHeight()

	if targetHeight <= localHeight {
		n.logger.Debugf("Sync target %d is not ahead of local height %d", targetHeight, localHeight)
		return nil
	}

	n.logger.Infof("SYNC START → %d blocks needed (%d → %d)",
		targetHeight-localHeight, localHeight+1, targetHeight)

	failures := 0
	for height := localHeight + 1; height <= targetHeight; height++ {
		select {
		case <-n.ctx.Done():
			return n.ctx.Err()
		default:
		}

		if height <= n.currentHeight() {
			n.logger.Debugf("Already applied block %d, skipping", height)
			continue
		}

		n.logger.Debugf("Fetching block %d/%d from peer %s", height, targetHeight, peerID.String()[:12])

		ctx, cancel := context.WithTimeout(n.ctx, 12*time.Second)
		blk, err := n.requestBlockWithContext(ctx, peerID, height)
		cancel()

		if err != nil {
			n.logger.Warnf("Failed to fetch block %d: %v", height, err)
			failures++
			if failures > 10 {
				return fmt.Errorf("too many failures fetching block %d from %s", height, peerID.String()[:12])
			}
			height-- // retry same block
			time.Sleep(300 * time.Millisecond)
			continue
		}

		if existing := n.chain.GetBlock(height); existing != nil && existing.Hash() == blk.Hash() && height <= n.currentHeight() {
			n.logger.Debugf("Block %d was applied while fetching; skipping", height)
			continue
		}

		failures = 0

		n.logger.Debugf("Got block %d from peer %s, adding to chain...", height, peerID.String()[:12])

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

			if isDeterministicSyncValidationFailure(err) {
				return fmt.Errorf("failed to add block %d from %s: %w", height, peerID.String()[:12], err)
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

func (n *Node) rememberOrphanBlock(blk *block.Block, reason string) {
	if n == nil || blk == nil || blk.Header == nil {
		return
	}
	n.orphanPoolMu.Lock()
	defer n.orphanPoolMu.Unlock()
	if n.orphanPool == nil {
		n.orphanPool = make(map[common.Hash]orphanBlockEntry)
	}
	now := time.Now()
	for hash, entry := range n.orphanPool {
		if !entry.expiresAt.After(now) {
			delete(n.orphanPool, hash)
		}
	}
	hash := blk.Hash()
	expiresAt := now.Add(orphanBlockTTL)
	n.orphanPool[hash] = orphanBlockEntry{
		block:     blk,
		expiresAt: expiresAt,
		reason:    reason,
	}
	go n.deleteOrphanBlockAfter(hash, expiresAt)
	if n.logger != nil {
		n.logger.Warnf("Moved block %d hash=%s to orphan pool for %s; expires in %s",
			blk.Header.Number.Uint64(), hash.String()[:8], reason, orphanBlockTTL)
	}
}

func (n *Node) deleteOrphanBlockAfter(hash common.Hash, expiresAt time.Time) {
	timer := time.NewTimer(time.Until(expiresAt))
	defer timer.Stop()
	<-timer.C
	n.orphanPoolMu.Lock()
	defer n.orphanPoolMu.Unlock()
	entry, ok := n.orphanPool[hash]
	if ok && !entry.expiresAt.After(time.Now()) {
		delete(n.orphanPool, hash)
	}
}

func (n *Node) validateProtocolCandidate(blk *block.Block, parent *block.Block) error {
	if blk == nil || blk.Header == nil {
		return errors.New("nil block or header")
	}
	if blk.Hash() != blk.Header.Hash() {
		return errors.New("block hash does not match canonical header hash")
	}
	if parent == nil {
		return errors.New("missing parent for protocol candidate")
	}
	if blk.Header.ParentHash != parent.Hash() {
		return fmt.Errorf("parent hash mismatch: expected %s, got %s", parent.Hash().String()[:8], blk.Header.ParentHash.String()[:8])
	}
	if blk.Header.Number == nil || parent.Header == nil || parent.Header.Number == nil {
		return errors.New("missing block number")
	}
	if blk.Header.Number.Uint64() != parent.Header.Number.Uint64()+1 {
		return fmt.Errorf("height mismatch: parent=%d child=%d", parent.Header.Number.Uint64(), blk.Header.Number.Uint64())
	}
	if checkpoints := n.chain.Checkpoints(); checkpoints != nil {
		if err := checkpoints.ValidateBlock(blk.Header.Number.Uint64(), blk.Hash()); err != nil {
			return fmt.Errorf("checkpoint validation failed: %w", err)
		}
	}
	if err := blk.Validate(parent); err != nil {
		return fmt.Errorf("block validation failed: %w", err)
	}
	return nil
}

func (n *Node) strongestProtocolBlock(parent *block.Block, candidates ...*block.Block) (*block.Block, error) {
	var strongest *block.Block
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		if strongest == nil {
			if err := n.validateProtocolCandidate(candidate, parent); err != nil {
				return nil, err
			}
			strongest = candidate
			continue
		}

		chosen, orphan, err := n.selectProtocolBlock(strongest, candidate, parent)
		if err != nil {
			return nil, err
		}
		if orphan != nil && orphan.Hash() != chosen.Hash() {
			n.rememberOrphanBlock(orphan, "mining-tip consensus lost deterministic protocol selection")
		}
		strongest = chosen
	}
	if strongest == nil {
		return nil, errors.New("no block candidates to compare")
	}
	return strongest, nil
}

// ResolveMiningTipConsensus makes local mining wait for all currently connected
// peers to converge on one canonical tip before the next block is mined. It
// compares same-height local and peer blocks, reorganizes this node to the
// superior protocol-valid hash when needed, and re-broadcasts that winner so
// peers that still advertise a weaker hash can reorg before mining continues.
func (n *Node) ResolveMiningTipConsensus() error {
	if n == nil || n.chain == nil {
		return nil
	}
	if n.host == nil {
		return nil
	}

	localTip := n.chain.Latest()
	if localTip == nil || localTip.Header == nil || localTip.Header.Number == nil {
		return nil
	}

	height := localTip.Header.Number.Uint64()
	if height == 0 {
		return nil
	}

	peers := n.Peers()
	if len(peers) == 0 {
		return nil
	}

	parent := n.chain.GetBlock(height - 1)
	if parent == nil {
		return fmt.Errorf("cannot resolve mining tip at height %d: missing parent", height)
	}

	candidates := []*block.Block{localTip}
	peerBlocks := make(map[peer.ID]*block.Block, len(peers))
	for _, pid := range peers {
		peerHeight, err := n.GetPeerHeight(pid)
		if err != nil {
			n.logger.Debugf("Skipping mining-tip comparison with %s: height unavailable: %v", pid.String()[:12], err)
			continue
		}
		if peerHeight < height {
			continue
		}

		peerBlock, err := n.RequestBlockSync(pid, height)
		if err != nil {
			n.logger.Debugf("Skipping mining-tip comparison with %s at height %d: %v", pid.String()[:12], height, err)
			continue
		}
		if peerBlock == nil || peerBlock.Header == nil {
			continue
		}
		peerBlocks[pid] = peerBlock
		candidates = append(candidates, peerBlock)
	}

	strongest, err := n.strongestProtocolBlock(parent, candidates...)
	if err != nil {
		return fmt.Errorf("mining-tip candidate comparison failed at height %d: %w", height, err)
	}

	strongestHash := strongest.Hash()
	localHash := localTip.Hash()
	if strongestHash != localHash {
		n.logger.Warnf("Mining-tip consensus selected superior block at height %d: local=%s superior=%s; reorganizing before next mine",
			height, localHash.String()[:8], strongestHash.String()[:8])
		if err := n.processBlock(strongest); err != nil {
			return fmt.Errorf("failed to reorganize to superior mining-tip hash %s at height %d: %w", strongestHash.String(), height, err)
		}
		localTip = n.chain.Latest()
		if localTip == nil || localTip.Hash() != strongestHash {
			return fmt.Errorf("local tip did not converge to superior mining-tip hash %s at height %d", strongestHash.String(), height)
		}
	}

	divergedPeers := 0
	for pid, peerBlock := range peerBlocks {
		if peerBlock.Hash() == strongestHash {
			continue
		}
		divergedPeers++
		n.logger.Warnf("Peer %s has weaker mining-tip hash at height %d: peer=%s superior=%s; rebroadcasting winner and pausing mining",
			pid.String()[:12], height, peerBlock.Hash().String()[:8], strongestHash.String()[:8])
	}
	if divergedPeers > 0 {
		if err := n.BroadcastBlock(strongest); err != nil {
			n.logger.Warnf("Failed to rebroadcast superior mining-tip block %d %s: %v", height, strongestHash.String()[:8], err)
		}
		return fmt.Errorf("waiting for %d connected peers to reorganize to mining-tip hash %s at height %d", divergedPeers, strongestHash.String(), height)
	}

	return nil
}

func (n *Node) selectProtocolBlock(local *block.Block, remote *block.Block, parent *block.Block) (*block.Block, *block.Block, error) {
	localErr := n.validateProtocolCandidate(local, parent)
	remoteErr := n.validateProtocolCandidate(remote, parent)

	if localErr != nil && remoteErr != nil {
		return nil, nil, fmt.Errorf("no same-height block meets network protocol: local=%v remote=%v", localErr, remoteErr)
	}
	if remoteErr != nil {
		return local, remote, nil
	}
	if localErr != nil {
		return remote, local, nil
	}

	localDifficulty := local.Header.Difficulty
	if localDifficulty == nil {
		localDifficulty = big.NewInt(0)
	}
	remoteDifficulty := remote.Header.Difficulty
	if remoteDifficulty == nil {
		remoteDifficulty = big.NewInt(0)
	}
	if cmp := remoteDifficulty.Cmp(localDifficulty); cmp > 0 {
		return remote, local, nil
	} else if cmp < 0 {
		return local, remote, nil
	}

	if remote.Header.Time < local.Header.Time {
		return remote, local, nil
	}
	if local.Header.Time < remote.Header.Time {
		return local, remote, nil
	}

	if strings.Compare(remote.Hash().String(), local.Hash().String()) < 0 {
		return remote, local, nil
	}
	return local, remote, nil
}

func (n *Node) processBlock(blk *block.Block) error {
	if blk == nil || blk.Header == nil {
		return errors.New("nil block or header")
	}
	if blk.Header.Number == nil {
		return errors.New("nil block number")
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
		if err := n.validateMinedBlockProposal(blk); err != nil {
			return err
		}
		if err := n.validateProtocolCandidate(blk, tip); err != nil {
			n.rememberOrphanBlock(blk, fmt.Sprintf("protocol rejection: %v", err))
			return err
		}

		err := n.chain.AddBlock(blk)

		if err == nil {
			n.logger.Infof("Added block %d via gossip (direct extension)", num)

			// Trigger rotating king database sync for this block
			go n.syncRotatingKingForBlock(num)
			go n.syncDatabaseForNewBlock(num, blk.Hash())

			// Also check if we need to sync configuration
			go n.checkAndSyncKingConfig()
			return nil
		}

		// Handle specific errors
		if strings.Contains(err.Error(), "already") ||
			strings.Contains(err.Error(), "known") ||
			strings.Contains(err.Error(), "duplicate") {
			return nil // Already processed
		}

		n.rememberOrphanBlock(blk, fmt.Sprintf("direct extension rejected: %v", err))
		n.logger.Warnf("Direct extension failed for block %d: %v", num, err)
		return err
	}

	// ORPHAN CHECK: Parent not in chain
	if !n.chain.HasBlock(blk.Header.ParentHash) {
		n.rememberOrphanBlock(blk, "missing parent")
		n.logger.Warnf("Deferred orphan block %d (parent %s not found)",
			num, blk.Header.ParentHash.String()[:8])

		// Any announced block ahead of our tip means the network has at least
		// one block we are missing. Trigger sync immediately instead of waiting
		// for the periodic peer-height check; this is especially important while
		// the local miner is busy on an obsolete parent.
		if num > currentHeight {
			n.logger.Warnf("Orphan block %d is %d blocks ahead - triggering sync",
				num, num-currentHeight)
			n.triggerSyncToHeight(num)
		}

		return nil
	}

	// BLOCK EXISTS AT SAME HEIGHT (fork)
	if existing := n.chain.GetBlock(num); existing != nil {
		if existing.Hash() == hash {
			return nil // Duplicate
		}

		parent := n.chain.GetBlock(num - 1)
		chosen, orphan, err := n.selectProtocolBlock(existing, blk, parent)
		if err != nil {
			n.rememberOrphanBlock(blk, fmt.Sprintf("same-height fork without protocol-valid winner: %v", err))
			return err
		}
		if orphan != nil {
			n.rememberOrphanBlock(orphan, "same-height fork lost deterministic protocol selection")
		}
		if chosen.Hash() == existing.Hash() {
			n.logger.Warnf("Same-height fork at height %d kept local protocol block %s; orphaned remote %s",
				num, existing.Hash().String()[:8], hash.String()[:8])
			return nil
		}

		n.logger.Warnf("Same-height fork at height %d (ours: %s, theirs: %s); comparing %d known mined candidates before chain fork-choice",
			num, existing.Hash().String()[:8], hash.String()[:8], n.minedBlockCandidateCount(num))
		if err := n.validateMinedBlockProposal(blk); err != nil {
			return err
		}
		if err := n.chain.AddBlock(blk); err != nil {
			n.rememberOrphanBlock(blk, fmt.Sprintf("selected fork rejected: %v", err))
			return fmt.Errorf("fork block rejected at height %d: %w", num, err)
		}
		return nil
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
				parent.Hash().String()[:8], blk.Header.ParentHash.String()[:8])
			return fmt.Errorf("wrong parent during sync")
		}

		// Valid sync block
		if err := n.validateMinedBlockProposal(blk); err != nil {
			return err
		}
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
		if syncBC, ok := n.chain.(interface {
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
		n.triggerSyncToHeight(num)
		return fmt.Errorf("block ahead but not syncing")
	}

	// BLOCK IS BEHIND OR STALE
	if num <= currentHeight {
		n.logger.Debugf("Stale block %d (we're at %d)", num, currentHeight)
		return fmt.Errorf("stale block")
	}

	// Should not reach here
	n.logger.Warnf("Unexpected block processing state: height=%d, hash=%s", num, hash.String()[:8])
	return fmt.Errorf("unexpected block state")
}

func (n *Node) triggerSyncToHeight(targetHeight uint64) {
	if n == nil || n.chain == nil {
		return
	}

	localHeight := n.currentHeight()
	if targetHeight <= localHeight {
		return
	}
	if n.chain.IsSyncing() {
		return
	}

	if n.host == nil {
		n.chain.StartSync(targetHeight)
		return
	}

	peers := n.Peers()
	if len(peers) == 0 {
		n.logger.Warnf("No peers available for sync to announced height %d", targetHeight)
		return
	}

	go func() {
		bestPeer := peer.ID("")
		bestHeight := uint64(0)
		for _, pid := range peers {
			height, err := n.GetPeerHeight(pid)
			if err != nil {
				n.logger.Debugf("Cannot get height from peer %s while syncing to announced block %d: %v",
					pid.String()[:12], targetHeight, err)
				continue
			}
			if height >= targetHeight && height > bestHeight {
				bestPeer = pid
				bestHeight = height
			}
		}

		if bestPeer == "" {
			n.logger.Warnf("No peer reported announced height %d yet; falling back to regular sync", targetHeight)
			n.triggerSync()
			return
		}

		n.logger.Warnf("SYNC TRIGGERED BY ANNOUNCED BLOCK: peer %s height=%d, our height=%d, announced=%d",
			bestPeer.String()[:12], bestHeight, n.currentHeight(), targetHeight)
		n.syncIfBehind(bestPeer)
	}()
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
		if isDeterministicSyncValidationFailure(err) {
			n.logger.Warnf("Detected deterministic validation failure while syncing from %s, trying alternate peer",
				pid.String()[:12])
			if recovered := n.retrySyncWithAlternatePeer(pid, finalSyncTarget(peerHeight, n.currentHeight())); recovered {
				return
			}
		}
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

func isDeterministicSyncValidationFailure(err error) bool {
	if err == nil {
		return false
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "pos validation failed") ||
		strings.Contains(msg, "miner eligibility verification failed") ||
		strings.Contains(msg, "address not eligible to mine") ||
		strings.Contains(msg, "state root mismatch") ||
		strings.Contains(msg, "block validation failed")
}

func finalSyncTarget(initialTarget, localHeight uint64) uint64 {
	if initialTarget > localHeight {
		return initialTarget
	}
	return localHeight
}

func (n *Node) retrySyncWithAlternatePeer(exclude peer.ID, targetHeight uint64) bool {
	peers := n.Peers()
	for _, candidate := range peers {
		if candidate == exclude {
			continue
		}

		peerHeight, err := n.GetPeerHeight(candidate)
		if err != nil {
			continue
		}
		if peerHeight < targetHeight {
			continue
		}

		n.logger.Warnf("Retrying sync with alternate peer %s (target=%d)",
			candidate.String()[:12], targetHeight)
		n.chain.StartSync(targetHeight)
		if err := n.syncMissingBlocks(candidate, targetHeight); err != nil {
			n.logger.Warnf("Alternate peer %s sync failed: %v", candidate.String()[:12], err)
			continue
		}

		finalHeight := n.currentHeight()
		if finalHeight >= targetHeight {
			n.chain.StopSync()
			n.logger.Infof("Alternate peer sync succeeded at height %d", finalHeight)
			return true
		}
	}

	return false
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

				if height <= localHeight {
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

/*func (n *Node) cleanupKnownTxs() {
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
}*/

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
		event.PreviousKing.String()[:8], event.NewKing.String()[:8], event.BlockHeight)
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
	if manager, ok := mgr.(interface {
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
			if configManager, ok := mgr.(interface {
				GetConfig() rotatingking.RotatingKingConfig
			}); ok {
				config := configManager.GetConfig()
				response.Config = &config
			} else if addrManager, ok := mgr.(interface {
			    GetKingAddresses() []common.QuantumAddress
			}); ok {
				// Fallback: create basic config from addresses
				addresses := addrManager.GetKingAddresses()
				config := rotatingking.RotatingKingConfig{
					KingAddresses:    addresses,
					RotationInterval: 100, // Default
					MinStakeRequired: rotatingking.EligibilityThreshold,
				}
				response.Config = &config
			}

			// Get sync state
			if syncManager, ok := mgr.(interface {
				GetSyncState() (*rotatingking.SyncState, error)
			}); ok {
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
			var ourAddresses []common.QuantumAddress
			if manager, ok := mgr.(interface {
				GetKingAddresses() []common.QuantumAddress
			}); ok {
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
	if manager, ok := mgr.(interface {
		GetSyncState() (*rotatingking.SyncState, error)
	}); ok {
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
		if manager, ok := mgr.(interface {
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
		"enabled":         n.dbSyncEnabled,
		"version":         n.dbSyncVersion,
		"isSyncing":       n.isDBSyncing,
		"currentSyncPeer": n.currentSyncPeer,
		"lastSyncTime":    n.lastDBSyncTime,
		"syncInterval":    n.dbSyncInterval.String(),
		"pendingRequests": len(n.dbSyncRequests),
		"cachedResponses": len(n.dbSyncResponses),
		"knownPeers":      len(n.dbSyncPeers),
		"syncRetryCount":  n.syncRetryCount,
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
		if manager, ok := mgr.(interface {
			GetSyncState() (*rotatingking.SyncState, error)
		}); ok {
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
	if configManager, ok := mgr.(interface {
		GetConfig() rotatingking.RotatingKingConfig
	}); ok {
		config = configManager.GetConfig()
		if len(config.KingAddresses) > 0 {
			n.logger.Debugf("Got config with %d addresses via GetConfig()", len(config.KingAddresses))
			return config
		}
	}

	// Fallback: use GetKingAddresses()
	if addrManager, ok := mgr.(interface {
		GetKingAddresses() []common.QuantumAddress
	}); ok {
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
	if manager, ok := mgr.(interface {
		UpdateKingAddresses(newAddresses []common.QuantumAddress) error
	}); ok {
		currentAddresses := []common.QuantumAddress{}
		if currentManager, ok := mgr.(interface {
			GetKingAddresses() []common.QuantumAddress
		}); ok {
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
	var currentAddresses []common.QuantumAddress
	if manager, ok := mgr.(interface {
		GetKingAddresses() []common.QuantumAddress
	}); ok {
		currentAddresses = manager.GetKingAddresses()
	}

	// Check if we need to update
	if !n.compareAddressLists(currentAddresses, config.KingAddresses) {
		n.logger.Infof("Updating king list from peer %s: %d -> %d addresses",
			source.String()[:8], len(currentAddresses), len(config.KingAddresses))

		// Update the list
		if updater, ok := mgr.(interface {
			UpdateKingAddresses([]common.QuantumAddress) error
		}); ok {
			if err := updater.UpdateKingAddresses(config.KingAddresses); err != nil {
				n.logger.Warnf("Failed to update king list: %v", err)
			} else {
				n.logger.Info("King list updated from peer sync")
			}
		}
	}
}

func (n *Node) compareAddressLists(a, b []common.QuantumAddress) bool {
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
		"config":       config, // This should serialize with capital field names
		"timestamp":    time.Now().Unix(),
		"source_peer":  n.host.ID().String(),
		"block_height": n.currentHeight(),
	}

	n.logger.Infof("Sending king config to %s with %d addresses",
		peerID.String()[:8], len(config.KingAddresses))

	// Log the addresses being sent
	for i, addr := range config.KingAddresses {
		n.logger.Debugf("  Address %d: %s", i+1, addr.String())
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

	s, err := n.host.NewStream(ctx, peerID, n.protocolID("king-config"))
	if err != nil {
		n.logger.Debugf("Failed to open config stream to %s: %v", peerID.String()[:8], err)
		return
	}
	defer s.Close()

	configMsg := map[string]interface{}{
		"config":      config,
		"timestamp":   time.Now().Unix(),
		"source_peer": n.host.ID().String(),
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
	if configManager, ok := mgr.(interface {
		GetConfig() rotatingking.RotatingKingConfig
	}); ok {
		config = configManager.GetConfig()
	} else if addrManager, ok := mgr.(interface {
	GetKingAddresses() []common.QuantumAddress
	}); ok {
		addresses := addrManager.GetKingAddresses()
		config = rotatingking.RotatingKingConfig{
			KingAddresses:    addresses,
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
		rotation.PreviousKing.String()[:8], rotation.NewKing.String()[:8], rotation.BlockHeight)

	n.processMu.Lock()
	mgr := n.chain.GetRotatingKingManager()
	n.processMu.Unlock()

	if mgr == nil {
		return
	}

	// Save rotation to database if supported
	if manager, ok := mgr.(interface {
		SaveRotationEvent(rotation *rotatingking.KingRotation) error
	}); ok {
		if err := manager.SaveRotationEvent(rotation); err != nil {
			n.logger.Debugf("Failed to save rotation event: %v", err)
		} else {
			n.logger.Debug("Rotation event saved to database")
		}
	}
}

// Handles king configuration messages
func (n *Node) parseKingConfig(configData map[string]interface{}) (rotatingking.RotatingKingConfig, error) {
	var config rotatingking.RotatingKingConfig

	n.logger.Debugf("parseKingConfig called with data: %v", configData)

	// Parse KingAddresses - try different possible field names
	var addresses []common.QuantumAddress

	// Helper to parse a list of address strings
	parseAddressList := func(addrsData []interface{}) {
		for _, addr := range addrsData {
			addrStr, ok := addr.(string)
			if !ok {
				continue
			}
			quantumAddr, err := common.ParseQuantumAddress(addrStr)
			if err != nil {
				n.logger.Warnf("Skipping invalid quantum address in config: %s (%v)", addrStr, err)
				continue
			}
			addresses = append(addresses, quantumAddr)
		}
	}

	// Try different field names
	if addrsData, ok := configData["KingAddresses"].([]interface{}); ok {
		n.logger.Debugf("Found KingAddresses array with %d items", len(addrsData))
		parseAddressList(addrsData)
	} else if addrsData, ok := configData["kingAddresses"].([]interface{}); ok {
		n.logger.Debugf("Found kingAddresses array with %d items", len(addrsData))
		parseAddressList(addrsData)
	} else if addrsData, ok := configData["addresses"].([]interface{}); ok {
		n.logger.Debugf("Found addresses array with %d items", len(addrsData))
		parseAddressList(addrsData)
	} else {
		n.logger.Warn("No addresses array found in config data")
		// Debug: show available fields
		for key, value := range configData {
			n.logger.Debugf("Key: %s, Type: %T, Value: %v", key, value, value)
		}
	}

	config.KingAddresses = addresses
	n.logger.Debugf("Parsed %d valid addresses", len(addresses))

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
func (n *Node) areAddressListsEqual(a, b []common.QuantumAddress) bool {
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
	if len(msg.Data) < 2 {
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

	// Validate height - allow zero-height bootstrap updates before chain starts.
	if event.BlockHeight > 0 {
		// Guard against uint64 underflow when currentHeight is below the stale-window size.
		var minValidHeight uint64
		if currentHeight > 100 {
			minValidHeight = currentHeight - 100
		}
		if event.BlockHeight < minValidHeight {
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
	}

	// Get current local list and merge instead of replacing.
	currentList := mgr.GetKingAddresses()
	mergedList := n.mergeAddressLists(currentList, event.NewList)

	// Avoid redundant updates and ignore shrinking subsets from peers.
	if len(mergedList) == len(currentList) && n.areAddressListsEqual(currentList, mergedList) {
		n.logger.Debug("Received identical/subset king list - ignoring")
		return
	}

	if err := mgr.UpdateKingAddresses(mergedList); err != nil {
		n.logger.Warnf("Failed to apply king list update: %v", err)
		return
	}

	n.logger.Infof("✅ King list updated via P2P at height %d (%d -> %d addresses)",
		event.BlockHeight, len(currentList), len(mergedList))
}

// Processes a KingRotationEvent
func (n *Node) processKingRotationEvent(event *KingRotationEvent, source peer.ID) {
	// Height must be valid (not 0)
	if event.BlockHeight == 0 {
		n.logger.Warn("❌ Invalid rotation event: height 0")
		return
	}

	n.logger.Infof("Received king rotation event: %s → %s at height %d (eligible=%v)",
		event.PreviousKing.String()[:8], event.NewKing.String()[:8], event.BlockHeight, event.Eligible)

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
	if rotator, ok := mgr.(interface {
		ForceRotateToAddress(newKing common.QuantumAddress, reason string) error
	}); ok {
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
	remotePeer := s.Conn().RemotePeer()
	if !n.allowStreamFromPeer(remotePeer, "king-config") {
		return
	}
	_ = s.SetDeadline(time.Now().Add(5 * time.Second))

	_, err := readLimitedStream(s, MaxConfigStreamBytes)
	if err != nil {
		n.recordPeerViolation(remotePeer, "OVERSIZED_CONFIG_STREAM", 7, err.Error())
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
	if configManager, ok := mgr.(interface {
		GetConfig() rotatingking.RotatingKingConfig
	}); ok {
		config = configManager.GetConfig()
	} else if addrManager, ok := mgr.(interface {
		GetKingAddresses() []common.QuantumAddress
	}); ok {
		addresses := addrManager.GetKingAddresses()
		config = rotatingking.RotatingKingConfig{
			KingAddresses:    addresses,
			RotationInterval: 100,
			MinStakeRequired: rotatingking.EligibilityThreshold,
		}
	}

	// Send response
	response := map[string]interface{}{
		"type":   "king_config",
		"config": config,
		"height": n.currentHeight(),
	}

	data, err := json.Marshal(response)
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
	var currentAddresses []common.QuantumAddress
	if manager, ok := mgr.(interface {
		GetKingAddresses() []common.QuantumAddress
	}); ok {
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

func (n *Node) requestAndCompareConfiguration(peerID peer.ID, ourAddresses []common.QuantumAddress) {
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

func (n *Node) shouldSyncConfiguration(ourCount, peerCount int, ourAddresses, peerAddresses []common.QuantumAddress) bool {
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
	if configManager, ok := mgr.(interface {
		GetConfig() rotatingking.RotatingKingConfig
	}); ok {
		ourConfig = configManager.GetConfig()
	} else if addrManager, ok := mgr.(interface {
		GetKingAddresses() []common.QuantumAddress
	}); ok {
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
	if manager, ok := mgr.(interface {
		GetKingAddresses() []common.QuantumAddress
	}); ok {
		addresses := manager.GetKingAddresses()
		n.logger.Infof("DEBUG: GetKingAddresses() returned %d addresses:", len(addresses))
		for i, addr := range addresses {
			n.logger.Infof("  [%d] %s", i, addr.String())
		}
	}

	if configManager, ok := mgr.(interface {
		GetConfig() rotatingking.RotatingKingConfig
	}); ok {
		config := configManager.GetConfig()
		n.logger.Infof("DEBUG: GetConfig() returned %d addresses:", len(config.KingAddresses))
		for i, addr := range config.KingAddresses {
			n.logger.Infof("  [%d] %s", i, addr.String())
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

	if manager, ok := mgr.(interface {
		GetRotationHistory(limit int) []rotatingking.KingRotation
	}); ok {
		history := manager.GetRotationHistory(10)
		n.logger.Infof("Rotation history has %d entries:", len(history))
		for i, rot := range history {
			n.logger.Infof("  [%d] Block %d: %s -> %s",
				i, rot.BlockHeight,
				rot.PreviousKing.String()[:8],
				rot.NewKing.String()[:8])
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
	if configManager, ok := mgr.(interface {
		GetConfig() rotatingking.RotatingKingConfig
	}); ok {
		config = configManager.GetConfig()
	} else if addrManager, ok := mgr.(interface {
		GetKingAddresses() []common.QuantumAddress
	}); ok {
		addresses := addrManager.GetKingAddresses()
		config = rotatingking.RotatingKingConfig{
			KingAddresses:    addresses,
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
		n.logger.Infof("  [%d] %s", i+1, addr.String())
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
			blockHeight-syncState.LastSyncedBlock)
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
	if manager, ok := mgr.(interface {
		GetSyncState() (*rotatingking.SyncState, error)
	}); ok {
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
	if manager, ok := mgr.(interface {
		GetKingAddresses() []common.QuantumAddress
	}); ok {
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
	if configManager, ok := mgr.(interface {
		GetConfig() rotatingking.RotatingKingConfig
	}); ok {
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
	var ourAddresses []common.QuantumAddress

	if configManager, ok := mgr.(interface {
		GetConfig() rotatingking.RotatingKingConfig
	}); ok {
		ourConfig = configManager.GetConfig()
		ourAddresses = ourConfig.KingAddresses
	} else if addrManager, ok := mgr.(interface {
		GetKingAddresses() []common.QuantumAddress
	}); ok {
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
			if updater, ok := mgr.(interface {
				UpdateKingAddresses([]common.QuantumAddress) error
			}); ok {
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

	if updater, ok := mgr.(interface {
		UpdateKingAddresses([]common.QuantumAddress) error
	}); ok {
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

	if manager, ok := mgr.(interface {
		GetKingAddresses() []common.QuantumAddress
	}); ok {
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
			if cleaner, ok := mgr.(interface {
				CleanupIneligibleKings() ([]common.QuantumAddress, error)
			}); ok {
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

func (n *Node) onKingListChanged(newList []common.QuantumAddress) {
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
			"nodeId":          request.NodeID[:8],
			"lastSyncedBlock": request.LastSyncedBlock,
			"type":            request.RequestType,
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
			"proposalId":  proposal.ProposalID[:8],
			"height":      proposal.BlockHeight,
			"currentKing": proposal.CurrentKing.String()[:8],
			"nextKing":    proposal.NextKing.String()[:8],
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
			"voter":      vote.VoterNodeID,
			"approved":   vote.Approved,
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
			"proposalId":    result.ProposalID[:8],
			"approved":      result.Approved,
			"approvalCount": result.ApprovalCount,
			"totalPeers":    result.TotalPeers,
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
			"blockHeight":      event.BlockHeight,
			"currentKingIndex": event.CurrentKingIndex,
			"kingCount":        len(event.KingAddresses),
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

func (n *Node) isImportantAddress(addr common.QuantumAddress) bool {
    // Pre-parse important addresses
    mainKing, err := common.ParseQuantumAddress("0qANA3c85k94LTyTXLGDdEzmLE32b1qhYZF")
    if err != nil {
        // Fallback to zero address if parsing fails (should never happen with valid string)
        mainKing = common.QuantumAddress{}
    }

    importantAddresses := []common.QuantumAddress{
        mainKing,
        // Add other important addresses here
    }

    for _, important := range importantAddresses {
        if addr == important {
            return true
        }
    }
    return false
}

func (n *Node) addressInList(addr common.QuantumAddress, list []common.QuantumAddress) bool {
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
		localCount, localKing.String()[:10])

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
	if localKing == (common.QuantumAddress{}) {
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
		defaultAddresses := []common.QuantumAddress{}
		mainKing, err := common.ParseQuantumAddress("0qANA3c85k94LTyTXLGDdEzmLE32b1qhYZF")
		if err != nil {
			n.logger.Errorf("Failed to parse emergency fallback king address: %v", err)
		} else {
			defaultAddresses = append(defaultAddresses, mainKing)
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
	if updater, ok := mgr.(interface {
		UpdateKingAddresses([]common.QuantumAddress) error
	}); ok {
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

	if !kingFound && currentKing != (common.QuantumAddress{}) {
		n.logger.Warnf("⚠️ CONFIGURATION HEALTH: Current king %s not in address list",
			currentKing.String()[:10])
		// Reset to first address
		if len(currentList) > 0 {
			mgr.ForceRotateToAddress(currentList[0], "health-check-repair")
		}
	}

	// Broadcast our config if healthy
	if len(currentList) >= 2 && kingFound {
		n.logger.Debugf("✅ Configuration healthy: %d addresses, king=%s",
			len(currentList), currentKing.String()[:10])
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
	allAddresses := make(map[common.QuantumAddress]int)  // address -> count of peers that have it
	allLists := make(map[string][]common.QuantumAddress) // peerID -> address list

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
func (n *Node) analyzeAndMergeKingLists(ourList []common.QuantumAddress,
	allAddresses map[common.QuantumAddress]int,
	allLists map[string][]common.QuantumAddress,
	mgr reward.RotatingKingManager) {

	totalPeers := len(allLists)
	if totalPeers < 2 {
		n.logger.Debug("Not enough peers for meaningful comparison")
		return
	}

	// Find addresses that appear in multiple lists (consensus addresses)
	consensusThreshold := totalPeers/2 + 1 // More than half of peers
	consensusAddresses := make([]common.QuantumAddress, 0)
	allUniqueAddresses := make([]common.QuantumAddress, 0)

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
func (n *Node) findMissingAddresses(source, target []common.QuantumAddress) []common.QuantumAddress {
	missing := make([]common.QuantumAddress, 0)

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
func (n *Node) considerCreatingSuperSet(ourList, allAddresses []common.QuantumAddress, mgr reward.RotatingKingManager) {
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
func (n *Node) mergeAddressLists(list1, list2 []common.QuantumAddress) []common.QuantumAddress {
	merged := make([]common.QuantumAddress, 0, len(list1)+len(list2))
	seen := make(map[common.QuantumAddress]bool)

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
