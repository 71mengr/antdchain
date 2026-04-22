// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package mining

import (
    "crypto/ecdsa"
    "errors"
    "fmt"
    "log"
    "math/big"
    "sync"
    "time"
    "os"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/ethereum/go-ethereum/accounts"
    "github.com/ethereum/go-ethereum/accounts/keystore"
    "github.com/ethereum/go-ethereum/common"
    "github.com/ethereum/go-ethereum/crypto"
    "github.com/antdaza/antdchain/antdc/block"
    "github.com/antdaza/antdchain/antdc/chain"
    "github.com/antdaza/antdchain/antdc/p2p"
    "github.com/antdaza/antdchain/antdc/pow"
    chaincommon "github.com/antdaza/antdchain/common"
)

// Configuration constants (can be made configurable via environment variables)
const (
    DefaultMiningInterval    = 2 * time.Second
    DefaultBroadcastMaxRetries = 5
    DefaultBroadcastInitialBackoff = 100 * time.Millisecond
    DefaultBroadcastMaxBackoff     = 2 * time.Second
    LogEligibilityCheckInterval   = 10 // Log every 10th eligibility check
    LogSyncStatusInterval         = 10 * time.Second
)

var (
    // Prometheus metrics
    miningBlocksTotal = prometheus.NewCounter(
        prometheus.CounterOpts{
            Name: "antdchain_mining_blocks_total",
            Help: "Total blocks successfully mined by this node",
        },
    )
    
    miningEligibilityChecks = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "antdchain_mining_eligibility_checks_total",
            Help: "Eligibility checks by result",
        },
        []string{"result"}, // "eligible", "not_eligible", "error"
    )
    
    miningBroadcastSuccess = prometheus.NewCounter(
        prometheus.CounterOpts{
            Name: "antdchain_mining_broadcast_success_total",
            Help: "Successful block broadcasts",
        },
    )
    
    miningBroadcastFailures = prometheus.NewCounter(
        prometheus.CounterOpts{
            Name: "antdchain_mining_broadcast_failures_total",
            Help: "Failed block broadcasts",
        },
    )
    
    miningSessionDuration = prometheus.NewGauge(
        prometheus.GaugeOpts{
            Name: "antdchain_mining_session_duration_seconds",
            Help: "Current mining session duration in seconds",
        },
    )
    
    miningUptime = prometheus.NewGauge(
        prometheus.GaugeOpts{
            Name: "antdchain_mining_uptime_seconds",
            Help: "Total mining uptime in seconds",
        },
    )
    
    miningRewardsTotal = prometheus.NewGauge(
        prometheus.GaugeOpts{
            Name: "antdchain_mining_rewards_total_antd",
            Help: "Total mining rewards earned in ANTD",
        },
    )
)

func init() {
    // Register Prometheus metrics
    prometheus.MustRegister(miningBlocksTotal)
    prometheus.MustRegister(miningEligibilityChecks)
    prometheus.MustRegister(miningBroadcastSuccess)
    prometheus.MustRegister(miningBroadcastFailures)
    prometheus.MustRegister(miningSessionDuration)
    prometheus.MustRegister(miningUptime)
    prometheus.MustRegister(miningRewardsTotal)
}

var lastSyncLog time.Time = time.Now()

// PosMiningState manages Proof-of-Stake mining
type PosMiningState struct {
    mining       bool
    enabled      bool
    minerAddress common.Address
    powEngine    *pow.PoW
    privateKey   *ecdsa.PrivateKey

    blocksMined  uint64
    totalRewards *big.Int
    
    // Configuration
    miningInterval           time.Duration
    broadcastMaxRetries      int
    broadcastInitialBackoff  time.Duration
    broadcastMaxBackoff      time.Duration

    mu sync.RWMutex
    onSyncChange func(isSyncing bool)
}

func NewPosMiningState(powEngine *pow.PoW) *PosMiningState {
    return &PosMiningState{
        enabled:      true,
        powEngine:    powEngine,
        totalRewards: big.NewInt(0),
        
        // Configurable values (can be set via environment variables)
        miningInterval:          DefaultMiningInterval,
        broadcastMaxRetries:     DefaultBroadcastMaxRetries,
        broadcastInitialBackoff: DefaultBroadcastInitialBackoff,
        broadcastMaxBackoff:     DefaultBroadcastMaxBackoff,
    }
}

func (ms *PosMiningState) IsMining() bool    { return ms.mining }
func (ms *PosMiningState) IsEnabled() bool   { return ms.enabled }
func (ms *PosMiningState) SetEnabled(v bool) { ms.enabled = v }
func (ms *PosMiningState) SetMining(v bool)  { ms.mining = v }

// SetMiningInterval sets the mining check interval
func (ms *PosMiningState) SetMiningInterval(interval time.Duration) {
    ms.mu.Lock()
    defer ms.mu.Unlock()
    ms.miningInterval = interval
}

// SetBroadcastRetryConfig sets broadcast retry configuration
func (ms *PosMiningState) SetBroadcastRetryConfig(maxRetries int, initialBackoff, maxBackoff time.Duration) {
    ms.mu.Lock()
    defer ms.mu.Unlock()
    ms.broadcastMaxRetries = maxRetries
    ms.broadcastInitialBackoff = initialBackoff
    ms.broadcastMaxBackoff = maxBackoff
}

func (ms *PosMiningState) SetMinerAddress(addr common.Address) error {
    ms.mu.Lock()
    defer ms.mu.Unlock()

    if ms.powEngine == nil {
        return errors.New("PoS engine not initialized")
    }

    ms.minerAddress = addr
    if qAddr, err := chaincommon.NewQuantumAddressFromBytes(addr.Bytes()); err == nil {
        log.Printf("[miner] PoS miner address set → %s", qAddr.String())
    } else {
        log.Printf("[miner] PoS miner address set → %s", addr.Hex())
    }
    return nil
}

func (ms *PosMiningState) SetPrivateKey(privKey *ecdsa.PrivateKey) {
    ms.mu.Lock()
    defer ms.mu.Unlock()
    ms.privateKey = privKey
    log.Printf("[miner] Private key set for address: %s", crypto.PubkeyToAddress(privKey.PublicKey).Hex())
}

func (ms *PosMiningState) SetPrivateKeyFromBytes(keyBytes []byte) error {
    privKey, err := crypto.ToECDSA(keyBytes)
    if err != nil {
        return fmt.Errorf("failed to parse private key: %w", err)
    }
    ms.SetPrivateKey(privKey)
    return nil
}

func (ms *PosMiningState) SetPrivateKeyFromHex(hexKey string) error {
    keyBytes := common.FromHex(hexKey)
    return ms.SetPrivateKeyFromBytes(keyBytes)
}

func (ms *PosMiningState) GetMinerAddress() common.Address {
    ms.mu.RLock()
    defer ms.mu.RUnlock()
    return ms.minerAddress
}

func (ms *PosMiningState) GetPublicKey() *ecdsa.PublicKey {
    ms.mu.RLock()
    defer ms.mu.RUnlock()
    if ms.privateKey == nil {
        return nil
    }
    return &ms.privateKey.PublicKey
}

// Starts the Proof-of-Stake mining process
func StartPosMining(bc *chain.Blockchain, state *PosMiningState, rewardAddr common.Address, p2pNode *p2p.Node) {
    if bc == nil || rewardAddr == (common.Address{}) || state.powEngine == nil {
        log.Println("[miner] Missing required components")
        return
    }

    if !state.enabled {
        log.Println("[miner] Mining disabled")
        return
    }

    if state.mining {
        state.mining = false
        time.Sleep(200 * time.Millisecond)
    }

    if rewardAddr == (common.Address{}) {
        log.Println("[miner] Invalid miner address")
        return
    }

    if err := state.SetMinerAddress(rewardAddr); err != nil {
        log.Printf("[miner] Cannot set miner address: %v", err)
        return
    }

    // Automatic staking check — no manual registration needed
    currentBalance := bc.State().GetBalance(rewardAddr)
    bc.Pow().AutoRegisterIfEligible(rewardAddr, currentBalance)

    log.Printf("[miner] Auto-checked staking eligibility for %s (balance: %s ANTD)",
        rewardAddr.Hex()[:12],
        new(big.Int).Div(currentBalance, big.NewInt(1e18)).String())

    state.mining = true
    log.Printf("[miner] PoS Mining STARTED → %s", rewardAddr.Hex())
    go posMiningLoop(bc, state, rewardAddr, p2pNode)
}

func StopMining(state *PosMiningState) {
    if state != nil {
        state.mining = false
        log.Println("[miner] Mining STOPPED")
    }
}

func posMiningLoop(bc *chain.Blockchain, ms *PosMiningState, _ common.Address, p2pNode *p2p.Node) {
    // Get configuration values
    ms.mu.RLock()
    miningInterval := ms.miningInterval
    ms.mu.RUnlock()
    
    ticker := time.NewTicker(miningInterval)
    defer ticker.Stop()

    log.Println("[miner] Dynamic wallet mining started — will mine with any eligible address in wallet")

    var (
        consecutiveMisses int    = 0
        totalMined        uint64 = 0
        startTime         time.Time = time.Now()
        sessionStartTime  time.Time = time.Now()
        eligibilityChecks int    = 0
    )
    
    // Update session duration metric
    go func() {
        for ms.mining {
            sessionDuration := time.Since(sessionStartTime).Seconds()
            miningSessionDuration.Set(sessionDuration)
            time.Sleep(1 * time.Second)
        }
    }()

    for ms.mining {
        <-ticker.C

        if bc.IsSyncing() {
            // Optional: log only occasionally to reduce spam
            if time.Since(lastSyncLog) > LogSyncStatusInterval {
                log.Printf("[miner] Sync in progress (height %d → %d) — mining paused",
                    bc.GetChainHeight(), bc.GetSyncTarget())
                lastSyncLog = time.Now()
            }
            time.Sleep(1 * time.Second)
            continue
        }

        parent := bc.Latest()
        if parent == nil {
            continue
        }

        height := parent.Header.Number.Uint64() + 1
        if height <= bc.GetChainHeight() {
            continue
        }

        if ms.powEngine == nil {
            continue
        }

        // Get who the network expects to mine this block
        expectedMiner, err := ms.powEngine.GetNextMiner(parent.Hash(), height)
        if err != nil {
            log.Printf("[miner] Failed to get next miner for block %d: %v", height, err)
            miningEligibilityChecks.WithLabelValues("error").Inc()
            continue
        }

        // Check if the expected miner has a loaded private key in this node.
        // We accept either:
        //   1) a configured miner address match, or
        //   2) a direct match with the address derived from the loaded key.
        // This prevents false "waiting" states when minerAddress and key address drift.
        var (
            eligiblePrivKey  *ecdsa.PrivateKey
            configuredMiner  common.Address
            loadedKeyAddress common.Address
        )
        ms.mu.RLock()
        configuredMiner = ms.minerAddress
        if ms.privateKey != nil {
            loadedKeyAddress = crypto.PubkeyToAddress(ms.privateKey.PublicKey)
            if expectedMiner == configuredMiner || expectedMiner == loadedKeyAddress {
                eligiblePrivKey = ms.privateKey
            }
        }
        ms.mu.RUnlock()

        eligibilityChecks++
        if eligiblePrivKey == nil {
            // Not our turn — or we don't have the key for the expected miner
            consecutiveMisses++
            if consecutiveMisses == 1 || consecutiveMisses%LogEligibilityCheckInterval == 0 {
                hasKey := loadedKeyAddress != (common.Address{})
                log.Printf("[miner] Waiting — expected: %s configured: %s key: %s (loaded key: %v)",
                    expectedMiner.Hex()[:12],
                    configuredMiner.Hex()[:12],
                    loadedKeyAddress.Hex()[:12],
                    hasKey,
                )
            }
            miningEligibilityChecks.WithLabelValues("not_eligible").Inc()
            continue
        }

        // YES! It's our turn and we have the private key
        consecutiveMisses = 0
        miningEligibilityChecks.WithLabelValues("eligible").Inc()
        log.Printf("[miner] ✅ OUR TURN! Mining block %d as %s", height, expectedMiner.Hex()[:12])

        currentTime := uint64(time.Now().Unix())
        eligible, err := ms.powEngine.VerifyMinerEligibility(expectedMiner, parent.Hash(), height, currentTime)
        if err != nil || !eligible {
            log.Printf("[miner] Eligibility failed: %v", err)
            ms.powEngine.RecordMissedBlock(expectedMiner)
            continue
        }

        // Create block using the eligible address
        newBlock, _, err := bc.CreatePoSBlock(expectedMiner)
        if err != nil || newBlock == nil {
            log.Printf("[miner] Block creation failed: %v", err)
            continue
        }

        // Sign with our private key
        timestamp := newBlock.Header.Time
        signature, err := generateBlockSignature(expectedMiner, parent.Hash(), height, timestamp, eligiblePrivKey)
        if err != nil {
            log.Printf("[miner] Signing failed: %v", err)
            continue
        }

        if len(signature) > 0 {
            sigMarker := []byte("|SIG|")
            extra := append(newBlock.Header.Extra, sigMarker...)
            extra = append(extra, signature...)
            newBlock.Header.Extra = extra
            log.Printf("[miner] Signed block %d", height)
        }

        // Submit
        if err := bc.AddBlock(newBlock); err != nil {
            log.Printf("[miner] AddBlock failed: %v", err)
            if bc.GetBlock(height) != nil {
                ms.powEngine.RecordMissedBlock(expectedMiner)
            }
            continue
        }

        // SUCCESS! Update rewards and metrics
        totalMined++
        ms.blocksMined++
        
        // Calculate and record rewards (simplified - adjust based on your reward logic)
        blockReward := calculateBlockReward(height, bc)
        ms.mu.Lock()
        ms.totalRewards.Add(ms.totalRewards, blockReward)
        ms.mu.Unlock()
        
        // Update metrics
        miningBlocksTotal.Inc()
        miningRewardsTotal.Set(float64(new(big.Int).Div(ms.totalRewards, big.NewInt(1e18)).Int64()))
        miningUptime.Set(time.Since(startTime).Seconds())

        ms.powEngine.RecordBlockMined(expectedMiner, height)

        log.Printf("[miner] 🎉 BLOCK #%d MINED by %s! Reward: %s ANTD",
            height, expectedMiner.Hex()[:12],
            new(big.Int).Div(blockReward, big.NewInt(1e18)).String())
        log.Printf("[miner]   Total mined this session: %d", totalMined)

        if p2pNode != nil {
            go broadcastMinedBlock(p2pNode, newBlock, ms)
        }

        if totalMined%5 == 0 {
            uptime := time.Since(startTime)
            avg := uptime.Seconds() / float64(totalMined)
            log.Printf("[miner] 📊 Mined %d blocks (avg %.1fs/block)", totalMined, avg)
        }
    }

    log.Println("[miner] Mining stopped")
}

// Creates a PoS signature for a block using ECDSA
func generateBlockSignature(
    miner common.Address,
    parentHash common.Hash,
    height uint64,
    timestamp uint64,
    privateKey *ecdsa.PrivateKey,
) ([]byte, error) {
    if privateKey == nil {
        log.Println("[miner] Warning: No private key provided for block signing")
        return []byte{}, nil
    }

    // Use the engine's GenerateBlockSignature method
    if privateKey == nil {
        return nil, errors.New("private key required")
    }
    
    // Create the message to sign
    msg := crypto.Keccak256Hash(
        []byte("ANTDChain-PoS-Block"),
        parentHash.Bytes(),
        common.LeftPadBytes(big.NewInt(int64(height)).Bytes(), 32),
        common.LeftPadBytes(big.NewInt(int64(timestamp)).Bytes(), 32),
        miner.Bytes(),
    ).Bytes()
    
    // Sign the message
    signature, err := crypto.Sign(msg, privateKey)
    if err != nil {
        return nil, fmt.Errorf("failed to sign block: %w", err)
    }

    log.Printf("[miner] Generated valid ECDSA signature for block %d", height)
    return signature, nil
}

// verifyBlockSignature verifies a PoS block signature using ECDSA
func verifyBlockSignature(
    miner common.Address,
    parentHash common.Hash,
    height uint64,
    timestamp uint64,
    signature []byte,
    expectedPublicKey *ecdsa.PublicKey,
) (bool, error) {
    if len(signature) == 0 {
        return true, nil // Empty signature allowed for unsigned blocks
    }

    // Use the engine's VerifyBlockSignature method
    if len(signature) != 65 {
        return false, errors.New("invalid signature length (expected 65 bytes)")
    }

    // Create the message that was signed
    msg := crypto.Keccak256(
        []byte("ANTDChain-PoS-Block"),
        parentHash.Bytes(),
        common.LeftPadBytes(big.NewInt(int64(height)).Bytes(), 32),
        common.LeftPadBytes(big.NewInt(int64(timestamp)).Bytes(), 32),
        miner.Bytes(),
    )

    // Recover the public key
    pubKeyBytes, err := crypto.Ecrecover(msg, signature)
    if err != nil {
        return false, fmt.Errorf("failed to recover public key: %w", err)
    }

    // Verify the signature
    if !crypto.VerifySignature(pubKeyBytes, msg, signature[:64]) {
        return false, errors.New("signature verification failed")
    }

    // If expected public key is provided, verify it matches
    if expectedPublicKey != nil {
        recoveredPubKey, err := crypto.UnmarshalPubkey(pubKeyBytes)
        if err != nil {
            return false, fmt.Errorf("invalid recovered public key: %w", err)
        }
        
        recoveredAddr := crypto.PubkeyToAddress(*recoveredPubKey)
        expectedAddr := crypto.PubkeyToAddress(*expectedPublicKey)
        
        if recoveredAddr != expectedAddr {
            return false, fmt.Errorf("signature public key mismatch: recovered %s, expected %s",
                recoveredAddr.Hex(), expectedAddr.Hex())
        }
    }

    return true, nil
}

// broadcastMinedBlock with exponential backoff
func broadcastMinedBlock(p *p2p.Node, blk *block.Block, ms *PosMiningState) {
    if p == nil || blk == nil {
        return
    }

    // Get retry configuration
    ms.mu.RLock()
    maxRetries := ms.broadcastMaxRetries
    backoff := ms.broadcastInitialBackoff
    maxBackoff := ms.broadcastMaxBackoff
    ms.mu.RUnlock()

    for i := 0; i < maxRetries; i++ {
        if err := p.BroadcastBlock(blk); err != nil {
            log.Printf("[miner] Broadcast attempt %d/%d failed: %v", i+1, maxRetries, err)
            time.Sleep(backoff)
            
            // Exponential backoff
            backoff *= 2
            if backoff > maxBackoff {
                backoff = maxBackoff
            }
        } else {
            log.Printf("[miner] Block %d broadcasted successfully (miner: %s)",
                blk.Header.Number.Uint64(), blk.Header.Coinbase.Hex()[:12])
            miningBroadcastSuccess.Inc()
            return
        }
    }
    
    log.Printf("[miner] Failed to broadcast block %d after %d attempts",
        blk.Header.Number.Uint64(), maxRetries)
    miningBroadcastFailures.Inc()
}

// extractSignatureFromBlock extracts the signature from a block's Extra field
func extractSignatureFromBlock(blk *block.Block) []byte {
    if blk == nil || blk.Header == nil {
        return nil
    }

    extra := blk.Header.Extra
    sigMarker := []byte("|SIG|")

    for i := 0; i <= len(extra)-len(sigMarker); i++ {
        if string(extra[i:i+len(sigMarker)]) == string(sigMarker) {
            return extra[i+len(sigMarker):]
        }
    }

    return nil
}

// Verifies the signature of a mined block
func (ms *PosMiningState) VerifyBlockSignature(blk *block.Block, expectedPublicKey *ecdsa.PublicKey) (bool, error) {
    if blk == nil || blk.Header == nil {
        return false, errors.New("nil block or header")
    }

    signature := extractSignatureFromBlock(blk)
    if len(signature) == 0 {
        return false, errors.New("no signature found in block")
    }

    return verifyBlockSignature(
        blk.Header.Coinbase,
        blk.Header.ParentHash,
        blk.Header.Number.Uint64(),
        blk.Header.Time,
        signature,
        expectedPublicKey,
    )
}

// Returns current mining stats
func (ms *PosMiningState) GetMiningStatistics() map[string]interface{} {
    ms.mu.RLock()
    defer ms.mu.RUnlock()

    stats := map[string]interface{}{
        "mining_enabled":          ms.enabled,
        "is_mining":               ms.mining,
        "miner_address":           ms.minerAddress.Hex(),
        "blocks_mined":            ms.blocksMined,
        "total_rewards_antd":       formatWei(ms.totalRewards),
        "total_rewards_wei":       ms.totalRewards.String(),
        "has_private_key":         ms.privateKey != nil,
        "mining_interval_seconds": ms.miningInterval.Seconds(),
        "broadcast_max_retries":   ms.broadcastMaxRetries,
    }

    if ms.powEngine != nil {
        posStats := ms.powEngine.GetMiningStatistics()
        for k, v := range posStats {
            stats["pos_"+k] = v
        }
    }

    return stats
}

// Loads and decrypts the private key from keystore
func (ms *PosMiningState) LoadPrivateKeyFromKeystore(keystoreStore *keystore.KeyStore, password string) error {
    ms.mu.Lock()
    defer ms.mu.Unlock()

    if ms.minerAddress == (common.Address{}) {
        return errors.New("miner address not set")
    }

    if keystoreStore == nil {
        return errors.New("keystore is nil")
    }

    var targetAccount accounts.Account
    found := false
    for _, acc := range keystoreStore.Accounts() {
        if acc.Address == ms.minerAddress {
            targetAccount = acc
            found = true
            break
        }
    }

    if !found {
        return fmt.Errorf("address %s not found in keystore", ms.minerAddress.Hex())
    }

    keyjson, err := os.ReadFile(targetAccount.URL.Path)
    if err != nil {
        return fmt.Errorf("failed to read keystore file: %w", err)
    }

    key, err := keystore.DecryptKey(keyjson, password)
    if err != nil {
        return fmt.Errorf("failed to decrypt key: %w", err)
    }

    ms.privateKey = key.PrivateKey
    log.Printf("[miner] ✓ Loaded private key from keystore for %s", ms.minerAddress.Hex())
    log.Printf("[miner]   Keystore file: %s", targetAccount.URL.Path)

    return nil
}

// Loads a private key from a keystore file
func (ms *PosMiningState) LoadPrivateKeyFromFile(filepath, password string) error {
    ms.mu.Lock()
    defer ms.mu.Unlock()

    if ms.minerAddress == (common.Address{}) {
        return errors.New("miner address not set")
    }

    keyjson, err := os.ReadFile(filepath)
    if err != nil {
        return fmt.Errorf("failed to read keystore file: %w", err)
    }

    key, err := keystore.DecryptKey(keyjson, password)
    if err != nil {
        return fmt.Errorf("failed to decrypt key: %w", err)
    }

    keyAddress := crypto.PubkeyToAddress(key.PrivateKey.PublicKey)
    if keyAddress != ms.minerAddress {
        return fmt.Errorf("key address mismatch: expected %s, got %s",
            ms.minerAddress.Hex(), keyAddress.Hex())
    }

    ms.privateKey = key.PrivateKey
    log.Printf("[miner] ✓ Loaded private key from %s", filepath)

    return nil
}

func formatWei(wei *big.Int) string {
    if wei == nil {
        return "0"
    }
    antd := new(big.Float).SetInt(wei)
    antd.Quo(antd, big.NewFloat(1e18))
    return antd.Text('f', 6)
}

func (ms *PosMiningState) SetSyncCallback(cb func(isSyncing bool)) {
    ms.mu.Lock()
    defer ms.mu.Unlock()
    ms.onSyncChange = cb
}

func (ms *PosMiningState) PauseMining() {
    ms.mu.Lock()
    defer ms.mu.Unlock()
    if ms.mining {
        ms.mining = false
        log.Println("[miner] Mining PAUSED due to sync")
        if ms.onSyncChange != nil {
            ms.onSyncChange(true)
        }
    }
}

func (ms *PosMiningState) ResumeMining() {
    ms.mu.Lock()
    defer ms.mu.Unlock()
    if !ms.mining {
        ms.mining = true
        log.Println("[miner] Mining RESUMED")
        if ms.onSyncChange != nil {
            ms.onSyncChange(false)
        }
    }
}

// CheckMiningEligibility checks if the current miner address is eligible to mine
func (ms *PosMiningState) CheckMiningEligibility(bc *chain.Blockchain) (bool, error) {
    if bc == nil || ms.powEngine == nil {
        return false, errors.New("blockchain or PoS engine not initialized")
    }

    ms.mu.RLock()
    minerAddr := ms.minerAddress
    ms.mu.RUnlock()

    if minerAddr == (common.Address{}) {
        return false, errors.New("miner address not set")
    }

    // Get current chain state
    parent := bc.Latest()
    if parent == nil {
        return false, errors.New("no parent block found")
    }

    height := parent.Header.Number.Uint64() + 1
    currentTime := uint64(time.Now().Unix())

    // Check if miner is eligible
    eligible, err := ms.powEngine.VerifyMinerEligibility(minerAddr, parent.Hash(), height, currentTime)
    if err != nil {
        return false, fmt.Errorf("eligibility check failed: %w", err)
    }

    return eligible, nil
}

// GetNextMiningSlot estimates when this miner will get to mine next
func (ms *PosMiningState) GetNextMiningSlot(bc *chain.Blockchain) (uint64, time.Duration, error) {
    if bc == nil || ms.powEngine == nil {
        return 0, 0, errors.New("blockchain or PoS engine not initialized")
    }

    ms.mu.RLock()
    minerAddr := ms.minerAddress
    ms.mu.RUnlock()

    if minerAddr == (common.Address{}) {
        return 0, 0, errors.New("miner address not set")
    }

    // Get current chain state
    parent := bc.Latest()
    if parent == nil {
        return 0, 0, errors.New("no parent block found")
    }

    currentHeight := parent.Header.Number.Uint64()
    
    // Check if miner is in the validator set
    isValidator := ms.powEngine.IsKing(minerAddr)
    if !isValidator {
        return 0, 0, errors.New("address is not in validator set")
    }

    // Get validator statistics
    stats := ms.powEngine.GetMiningStatistics()
    activeStakers, _ := stats["active_stakers"].(int)
    if activeStakers <= 0 {
        return 0, 0, errors.New("no active validators")
    }

    // Estimate: each validator gets a turn every (BlocksPerMiner * activeStakers) blocks
    blocksUntilTurn := uint64(pow.BlocksPerMiner * activeStakers)
    estimatedBlocks := blocksUntilTurn // rough estimate
    
    // Convert to time (using target block time)
    estimatedTime := time.Duration(estimatedBlocks * pow.TargetBlockTimeSeconds) * time.Second
    
    return currentHeight + estimatedBlocks, estimatedTime, nil
}


func calculateBlockReward(height uint64, bc *chain.Blockchain) *big.Int {
    baseReward := new(big.Int).Mul(big.NewInt(200), big.NewInt(1e18)) // 200 ANTD
    
    // TODO:look into this
    if height > 0 && height%1000000 == 0 {
        baseReward.Div(baseReward, big.NewInt(2))
    }
    
    return baseReward
}

// UpdateTotalRewards updates the total rewards with additional reward
func (ms *PosMiningState) UpdateTotalRewards(additionalReward *big.Int) {
    if additionalReward == nil || additionalReward.Sign() == 0 {
        return
    }
    
    ms.mu.Lock()
    ms.totalRewards.Add(ms.totalRewards, additionalReward)
    ms.mu.Unlock()
    
    // Update metrics
    miningRewardsTotal.Set(float64(new(big.Int).Div(ms.totalRewards, big.NewInt(1e18)).Int64()))
}
