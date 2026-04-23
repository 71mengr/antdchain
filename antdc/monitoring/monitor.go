// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package monitoring
import (
    "encoding/json"
    "fmt"
    "log"
    "math/big"
    "os"
    "sort"
    "strings"
    "sync"
    "time"

    "github.com/antdaza/antdchain/common"
)

// Tracks total supply and distribution
type SupplyMonitor struct {
    mu               sync.RWMutex
    totalSupply      *big.Int
    distribution     map[common.QuantumAddress]*big.Int
    transactionCount map[common.QuantumAddress]int
    alerts           chan TransactionAlert
    alertHistory     []TransactionAlert
    config           MonitorConfig
    blockchain       BlockchainProvider
    isMainKing       bool
    running          bool
    lastBlock        uint64
    alertCounts      map[string]int
    logger           *log.Logger
}

// Creates a new supply monitor
func NewSupplyMonitor(bc BlockchainProvider, config MonitorConfig, isMainKing bool) *SupplyMonitor {
    monitor := &SupplyMonitor{
        totalSupply:      big.NewInt(0),
        distribution:     make(map[common.QuantumAddress]*big.Int),
        transactionCount: make(map[common.QuantumAddress]int),
        alerts:           make(chan TransactionAlert, 1000),
        alertHistory:     make([]TransactionAlert, 0),
        config:           config,
        blockchain:       bc,
        isMainKing:       isMainKing,
        alertCounts:      make(map[string]int),
    }

    // Initialize logger
    if config.LogFile != "" {
        monitor.logger = log.New(os.Stdout, "MONITOR: ", log.Ldate|log.Ltime|log.Lshortfile)
    } else {
        monitor.logger = log.New(os.Stdout, "MONITOR: ", log.Ldate|log.Ltime)
    }

    return monitor
}

// Starts the transaction monitoring
func (sm *SupplyMonitor) StartMonitoring() {
    sm.mu.Lock()
    if sm.running {
        sm.mu.Unlock()
        return
    }
    sm.running = true
    sm.mu.Unlock()

    if !sm.isMainKing {
        sm.logger.Printf("⚠️  Not Main King - monitoring in read-only mode")
    } else {
        sm.logger.Printf("👑 Main King starting transaction monitoring...")
    }

    // Initialize from current chain state
    sm.initializeFromChain()

    // Start background monitoring
    go sm.monitorBlocks()
    go sm.processAlerts()

    if sm.config.EnableRealTime {
        go sm.realTimeMonitoring()
    }

    sm.logger.Printf("✅ Monitoring started - CheckInterval: %v", sm.config.CheckInterval)
}

// StopMonitoring stops the monitoring
func (sm *SupplyMonitor) StopMonitoring() {
    sm.mu.Lock()
    defer sm.mu.Unlock()

    if !sm.running {
        return
    }

    sm.running = false
    close(sm.alerts)
    sm.logger.Printf("🛑 Monitoring stopped")
}

// Initializes monitoring state from current chain
func (sm *SupplyMonitor) initializeFromChain() {
    sm.mu.Lock()
    defer sm.mu.Unlock()

    currentHeight := sm.blockchain.GetChainHeight()
    sm.logger.Printf("Initializing monitor from chain height: %d", currentHeight)

    // Process all blocks to build initial state
    for i := uint64(0); i <= currentHeight; i++ {
        block, err := sm.blockchain.GetBlockByNumber(i)
        if err != nil {
            sm.logger.Printf("❌ Failed to get block %d: %v", i, err)
            continue
        }

        for _, transaction := range block.GetTransactions() {
            sm.processTransactionForInit(transaction)
        }
    }

    sm.lastBlock = currentHeight
    sm.logger.Printf("✅ Monitor initialized - Total supply: %s ANTD, Unique addresses: %d",
        sm.formatANTD(sm.totalSupply), len(sm.distribution))
}

// Processes transaction for initialization (no alerts)
func (sm *SupplyMonitor) processTransactionForInit(t TransactionProvider) {
    // Update distribution
    sm.updateDistribution(t.GetFrom(), new(big.Int).Neg(t.GetValue())) // Subtract from sender
    if t.GetTo() != nil {
        sm.updateDistribution(*t.GetTo(), t.GetValue()) // Add to receiver
    }

    // Update transaction count
    sm.transactionCount[t.GetFrom()]++
}

// Monitors new blocks for suspicious transactions
func (sm *SupplyMonitor) monitorBlocks() {
    ticker := time.NewTicker(sm.config.CheckInterval)
    defer ticker.Stop()

    for range ticker.C {
        if !sm.isRunning() {
            return
        }

        currentBlock := sm.blockchain.GetChainHeight()

        // Check new blocks since last check
        for blockNum := sm.lastBlock + 1; blockNum <= currentBlock; blockNum++ {
            sm.analyzeBlock(blockNum)
        }

        sm.lastBlock = currentBlock
    }
}

// Analyzes a block for suspicious transactions
func (sm *SupplyMonitor) analyzeBlock(blockNumber uint64) {
    block, err := sm.blockchain.GetBlockByNumber(blockNumber)
    if err != nil {
        sm.logger.Printf("❌ Failed to get block %d: %v", blockNumber, err)
        return
    }

    sm.logger.Printf("🔍 Analyzing block %d with %d transactions", blockNumber, len(block.GetTransactions()))

    for _, transaction := range block.GetTransactions() {
        sm.analyzeTransaction(transaction, blockNumber, block.GetHeader().GetTime())
    }

    // Update supply after block processing
    sm.updateSupply()
}

// Analyzes a single transaction
func (sm *SupplyMonitor) analyzeTransaction(t TransactionProvider, blockNumber uint64, blockTime uint64) {
    // Update transaction count
    sm.mu.Lock()
    sm.transactionCount[t.GetFrom()]++
    sm.mu.Unlock()

    // Check for suspicious amount
    if sm.config.AlertThreshold != nil && t.GetValue().Cmp(sm.config.AlertThreshold) > 0 {
        toAddr := common.QuantumAddress{}
        if t.GetTo() != nil {
            toAddr = *t.GetTo()
        }

        sm.alerts <- TransactionAlert{
            Type:        "LARGE_TRANSACTION",
            Severity:    "high",
            Message:     fmt.Sprintf("Large transaction detected: %s ANTD", sm.formatANTD(t.GetValue())),
            TxHash:      t.GetHash(),
            Amount:      new(big.Int).Set(t.GetValue()),
            From:        t.GetFrom(),
            To:          toAddr,
            BlockNumber: blockNumber,
            Timestamp:   time.Unix(int64(blockTime), 0),
        }
    }

    // Check for potential supply manipulation
    if sm.isSupplyManipulation(t) {
        data, _ := json.Marshal(map[string]interface{}{
            "transaction_count": sm.transactionCount[t.GetFrom()],
        })

        toAddr := common.QuantumAddress{}
        if t.GetTo() != nil {
            toAddr = *t.GetTo()
        }

        sm.alerts <- TransactionAlert{
            Type:        "SUPPLY_MANIPULATION",
            Severity:    "critical",
            Message:     "Potential supply manipulation detected",
            TxHash:      t.GetHash(),
            Amount:      new(big.Int).Set(t.GetValue()),
            From:        t.GetFrom(),
            To:          toAddr,
            BlockNumber: blockNumber,
            Timestamp:   time.Unix(int64(blockTime), 0),
            Data:        data,
        }
    }

    // Check for spam patterns
    if sm.isSpamTransaction(t) {
        toAddr := common.QuantumAddress{}
        if t.GetTo() != nil {
            toAddr = *t.GetTo()
        }

        sm.alerts <- TransactionAlert{
            Type:        "SPAM_TRANSACTION",
            Severity:    "medium",
            Message:     "Potential spam transaction detected",
            TxHash:      t.GetHash(),
            Amount:      new(big.Int).Set(t.GetValue()),
            From:        t.GetFrom(),
            To:          toAddr,
            BlockNumber: blockNumber,
            Timestamp:   time.Unix(int64(blockTime), 0),
        }
    }

    // Update distribution tracking
    sm.updateDistribution(t.GetFrom(), new(big.Int).Neg(t.GetValue())) // Subtract from sender
    if t.GetTo() != nil {
        sm.updateDistribution(*t.GetTo(), t.GetValue()) // Add to receiver
    }
}

// Checks for potential supply manipulation patterns
func (sm *SupplyMonitor) isSupplyManipulation(t TransactionProvider) bool {
    // Check for circular transactions (same address sending to itself)
    if t.GetTo() != nil && t.GetFrom() == *t.GetTo() && t.GetValue().Sign() > 0 {
        return true
    }

    // Check for rapid transactions from same address
    sm.mu.RLock()
    count := sm.transactionCount[t.GetFrom()]
    sm.mu.RUnlock()

    if count > 100 { // More than 100 transactions from same address
        return true
    }

    return false
}

// Checks for spam patterns
func (sm *SupplyMonitor) isSpamTransaction(t TransactionProvider) bool {
    // Dust transactions (very small amounts)
    if t.GetValue().Cmp(big.NewInt(1e12)) < 0 { // Less than 0.000001 ANTD
        sm.mu.RLock()
        count := sm.transactionCount[t.GetFrom()]
        sm.mu.RUnlock()

        // If many dust transactions from same address
        return count > 10
    }

    return false
}

// Updates the distribution tracking
func (sm *SupplyMonitor) updateDistribution(addr common.QuantumAddress, amount *big.Int) {
    sm.mu.Lock()
    defer sm.mu.Unlock()

    if current, exists := sm.distribution[addr]; exists {
        sm.distribution[addr] = new(big.Int).Add(current, amount)
    } else {
        sm.distribution[addr] = new(big.Int).Set(amount)
    }
}

// Recalculates total supply
func (sm *SupplyMonitor) updateSupply() {
    sm.mu.Lock()
    defer sm.mu.Unlock()

    // Reset total supply
    sm.totalSupply = big.NewInt(0)

    // Sum all balances
    for _, balance := range sm.distribution {
        if balance.Sign() > 0 { // Only count positive balances
            sm.totalSupply.Add(sm.totalSupply, balance)
        }
    }

    // Check if supply exceeds maximum
    if sm.config.MaxSupply != nil && sm.totalSupply.Cmp(sm.config.MaxSupply) > 0 {
        data, _ := json.Marshal(map[string]interface{}{
            "current_supply": sm.totalSupply.String(),
            "max_supply":     sm.config.MaxSupply.String(),
        })

        sm.alerts <- TransactionAlert{
            Type:      "SUPPLY_EXCEEDED",
            Severity:  "critical",
            Message:   fmt.Sprintf("Total supply exceeded maximum: %s > %s",
                sm.formatANTD(sm.totalSupply), sm.formatANTD(sm.config.MaxSupply)),
            Timestamp: time.Now(),
            Data:      data,
        }
    }
}

// Processes monitoring alerts
func (sm *SupplyMonitor) processAlerts() {
    for alert := range sm.alerts {
        sm.handleAlert(alert)
    }
}

//Handles a monitoring alert
func (sm *SupplyMonitor) handleAlert(alert TransactionAlert) {
    // Store in history
    sm.mu.Lock()
    sm.alertHistory = append(sm.alertHistory, alert)
    sm.alertCounts[alert.Type]++
    // Keep only last 1000 alerts
    if len(sm.alertHistory) > 1000 {
        sm.alertHistory = sm.alertHistory[1:]
    }
    sm.mu.Unlock()

    // Log alert
    sm.logAlert(alert)

    if !sm.isMainKing {
        return // Only Main King takes action
    }

    // Take action based on alert severity and type
    switch alert.Severity {
    case "critical":
        sm.handleCriticalAlert(alert)
    case "high":
        sm.handleHighAlert(alert)
    case "medium":
        sm.handleMediumAlert(alert)
    case "low":
        sm.handleLowAlert(alert)
    }

    // Send external notification if configured
    if sm.config.WebhookURL != "" {
        go sm.sendWebhookNotification(alert)
    }
}

// Handles critical alerts
func (sm *SupplyMonitor) handleCriticalAlert(alert TransactionAlert) {
    sm.logger.Printf("💥 CRITICAL ALERT - Immediate action required!")

    switch alert.Type {
    case "SUPPLY_MANIPULATION":
        sm.logger.Printf("🛑 Supply manipulation detected from %s - consider freezing address", alert.From.String())

    case "SUPPLY_EXCEEDED":
        sm.logger.Printf("🛑 MAXIMUM SUPPLY EXCEEDED - Emergency measures required!")
        // Trigger emergency protocols

    case "LARGE_TRANSACTION":
        sm.logger.Printf("🛑 Very large transaction: %s ANTD from %s", sm.formatANTD(alert.Amount), alert.From.String())
    }
}

// Handles high severity alerts
func (sm *SupplyMonitor) handleHighAlert(alert TransactionAlert) {
    sm.logger.Printf("⚠️  HIGH ALERT - %s", alert.Message)
    // Log for investigation
}

// handleMediumAlert handles medium severity alerts
func (sm *SupplyMonitor) handleMediumAlert(alert TransactionAlert) {
    sm.logger.Printf("📊 MEDIUM ALERT - %s", alert.Message)
    // Monitor situation
}

// handleLowAlert handles low severity alerts
func (sm *SupplyMonitor) handleLowAlert(alert TransactionAlert) {
    sm.logger.Printf("📝 LOW ALERT - %s", alert.Message)
    // Informational only
}

// realTimeMonitoring monitors transactions in real-time
func (sm *SupplyMonitor) realTimeMonitoring() {
    sm.logger.Printf("🔍 Real-time transaction monitoring enabled")
    // In production, you'd subscribe to pending transactions
}

// logAlert logs an alert with appropriate formatting
func (sm *SupplyMonitor) logAlert(alert TransactionAlert) {
    var emoji string
    switch alert.Severity {
    case "critical":
        emoji = "💥"
    case "high":
        emoji = "⚠️"
    case "medium":
        emoji = "📊"
    case "low":
        emoji = "📝"
    default:
        emoji = "🔔"
    }

    sm.logger.Printf("%s ALERT [%s/%s]: %s", emoji, alert.Type, alert.Severity, alert.Message)

    if alert.TxHash != (common.Hash{}) {
        sm.logger.Printf("   Transaction: %s", alert.TxHash.String())
        sm.logger.Printf("   Amount: %s ANTD", sm.formatANTD(alert.Amount))
        sm.logger.Printf("   From: %s", alert.From.String())
        if alert.To != (common.QuantumAddress{}) {
            sm.logger.Printf("   To: %s", alert.To.String())
        }
        sm.logger.Printf("   Block: %d", alert.BlockNumber)
    }
}

// sendWebhookNotification sends alert to webhook
func (sm *SupplyMonitor) sendWebhookNotification(alert TransactionAlert) {
    //TODO: implement webhook sending
}

// Current supply statistics
func (sm *SupplyMonitor) GetSupplyStats() *SupplyStats {
    sm.mu.RLock()
    defer sm.mu.RUnlock()

    stats := &SupplyStats{
        TotalSupply:     sm.formatANTD(sm.totalSupply),
        UniqueAddresses: len(sm.distribution),
        Distribution:    make(map[common.QuantumAddress]string),
        AlertCounts:     make(map[string]int),
        LastUpdated:     time.Now(),
    }

    // Copy distribution with formatted balances
    for addr, balance := range sm.distribution {
        if balance.Sign() > 0 {
            stats.Distribution[addr] = sm.formatANTD(balance)
        }
    }

    // Get top holders
    stats.TopHolders = sm.getTopHolders(10)

    // Copy alert counts
    for k, v := range sm.alertCounts {
        stats.AlertCounts[k] = v
    }

    return stats
}

// Returns the top N holders by balance
func (sm *SupplyMonitor) getTopHolders(n int) []Holder {
    type holder struct {
        address common.QuantumAddress
        balance *big.Int
    }

    var holders []holder
    for addr, bal := range sm.distribution {
        if bal.Sign() > 0 {
            holders = append(holders, holder{address: addr, balance: bal})
        }
    }

    // Sort by balance (descending)
    sort.Slice(holders, func(i, j int) bool {
        return holders[i].balance.Cmp(holders[j].balance) > 0
    })

    var result []Holder
    totalSupply := sm.totalSupply
    if totalSupply.Sign() == 0 {
        totalSupply = big.NewInt(1) // Avoid division by zero
    }

    for i := 0; i < len(holders) && i < n; i++ {
        percent := new(big.Float).Quo(
            new(big.Float).SetInt(holders[i].balance),
            new(big.Float).SetInt(totalSupply),
        )
        percent.Mul(percent, big.NewFloat(100))

        result = append(result, Holder{
            Address: holders[i].address.String(),
            Balance: sm.formatANTD(holders[i].balance),
            Percent: fmt.Sprintf("%.2f%%", percent),
        })
    }

    return result
}

// Returns recent alerts
func (sm *SupplyMonitor) GetAlertHistory(limit int) []TransactionAlert {
    sm.mu.RLock()
    defer sm.mu.RUnlock()

    if limit <= 0 || limit > len(sm.alertHistory) {
        limit = len(sm.alertHistory)
    }

    start := len(sm.alertHistory) - limit
    if start < 0 {
        start = 0
    }

    result := make([]TransactionAlert, limit)
    copy(result, sm.alertHistory[start:])
    return result
}

// Returns statistics for a specific address
func (sm *SupplyMonitor) GetAddressStats(addr common.QuantumAddress) map[string]interface{} {
    sm.mu.RLock()
    defer sm.mu.RUnlock()

    stats := make(map[string]interface{})
    stats["address"] = addr.String()

    if balance, exists := sm.distribution[addr]; exists {
        stats["balance"] = sm.formatANTD(balance)
        stats["balance_wei"] = balance.String()
    } else {
        stats["balance"] = "0"
        stats["balance_wei"] = "0"
    }

    stats["transaction_count"] = sm.transactionCount[addr]
    stats["is_main_king"] = addr == sm.config.MainKingAddress

    return stats
}

// isRunning checks if monitor is running
func (sm *SupplyMonitor) isRunning() bool {
    sm.mu.RLock()
    defer sm.mu.RUnlock()
    return sm.running
}

// formatANTD formats wei amount to ANTD string
func (sm *SupplyMonitor) formatANTD(amount *big.Int) string {
    if amount == nil {
        return "0"
    }

    oneANTD := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
    whole := new(big.Int).Div(amount, oneANTD)
    remainder := new(big.Int).Mod(amount, oneANTD)

    if remainder.Sign() == 0 {
        return whole.String()
    }

    fractional := new(big.Float).SetInt(remainder)
    divisor := new(big.Float).SetInt(oneANTD)
    fractional.Quo(fractional, divisor)

    fractionalStr := fractional.Text('f', 6)
    if len(fractionalStr) > 2 && fractionalStr[:2] == "0." {
        fractionalStr = fractionalStr[2:]
    }

    fractionalStr = strings.TrimRight(fractionalStr, "0")
    if fractionalStr == "" {
        return whole.String()
    }

    return whole.String() + "." + fractionalStr
}
