// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package chain

import (
	"container/heap"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/antdaza/antdchain/antdc/p2p"
	"github.com/antdaza/antdchain/antdc/tx"
	"github.com/antdaza/antdchain/common"
	"github.com/prometheus/client_golang/prometheus"
)

// Configuration constants (can be made configurable via environment variables)
const (
	DefaultMaxPoolSize       = 10000
	DefaultMaxTxsPerSender   = 64
	DefaultMaxTxSize         = 128 * 1024    // 128KB
	DefaultMinGasPrice       = 1_000_000_000 // 1 Gwei
	DefaultMaxFutureNonceGap = 1024
	DefaultMinConfirmations  = 50
	DefaultStaleBlockAge     = 1000
	DefaultTxTTL             = 24 * time.Hour
	DefaultCleanupInterval   = 2 * time.Minute
	DefaultSaveInterval      = 60 * time.Second
)

// Prometheus metrics
var (
	txPoolSizeGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "antdchain_txpool_size",
			Help: "Current number of transactions in pool",
		},
	)

	txAddedCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "antdchain_txpool_added_total",
			Help: "Total transactions added to pool",
		},
	)

	txDroppedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "antdchain_txpool_dropped_total",
			Help: "Transactions dropped by reason",
		},
		[]string{"reason"}, // "pool_full", "invalid", "stale", "expired", "mined"
	)

	txValidationErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "antdchain_txpool_validation_errors_total",
			Help: "Transaction validation errors by type",
		},
		[]string{"error_type"},
	)

	txPoolOperations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "antdchain_txpool_operations_total",
			Help: "TxPool operations count",
		},
		[]string{"operation"}, // "add", "remove", "cleanup", "save"
	)

	txPoolLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "antdchain_txpool_operation_duration_seconds",
			Help:    "TxPool operation latency in seconds",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		},
		[]string{"operation"},
	)
)

func init() {
	// Register Prometheus metrics
	prometheus.MustRegister(txPoolSizeGauge)
	prometheus.MustRegister(txAddedCounter)
	prometheus.MustRegister(txDroppedCounter)
	prometheus.MustRegister(txValidationErrors)
	prometheus.MustRegister(txPoolOperations)
	prometheus.MustRegister(txPoolLatency)
}

// TxHeap implements a priority queue for transactions sorted by gas price
type TxHeap []*tx.Tx

func (h TxHeap) Len() int           { return len(h) }
func (h TxHeap) Less(i, j int) bool { return h[i].GasPrice.Cmp(h[j].GasPrice) > 0 }
func (h TxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *TxHeap) Push(x interface{}) {
	*h = append(*h, x.(*tx.Tx))
}

func (h *TxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type TxPool struct {
	mu           sync.RWMutex
	txs          map[common.Hash]*tx.Tx
	bySender     map[common.QuantumAddress][]*tx.Tx
	submitHeight map[common.Hash]uint64
	submitTime   map[common.Hash]time.Time
	nonceTracker map[common.QuantumAddress]uint64

	// Priority queue for pending transactions
	pendingHeap *TxHeap

	// Configuration
	maxPoolSize       int
	maxTxsPerSender   int
	maxTxSize         int
	minGasPrice       *big.Int
	maxFutureNonceGap uint64
	minConfirmations  uint64
	staleBlockAge     uint64
	txTTL             time.Duration
	cleanupInterval   time.Duration
	saveInterval      time.Duration

	chain *Blockchain
}

func NewTxPool() *TxPool {
	h := &TxHeap{}
	heap.Init(h)

	return &TxPool{
		txs:          make(map[common.Hash]*tx.Tx),
		bySender:     make(map[common.QuantumAddress][]*tx.Tx),
		submitHeight: make(map[common.Hash]uint64),
		submitTime:   make(map[common.Hash]time.Time),
		nonceTracker: make(map[common.QuantumAddress]uint64),
		pendingHeap:  h,

		// Default configuration
		maxPoolSize:       DefaultMaxPoolSize,
		maxTxsPerSender:   DefaultMaxTxsPerSender,
		maxTxSize:         DefaultMaxTxSize,
		minGasPrice:       big.NewInt(DefaultMinGasPrice),
		maxFutureNonceGap: DefaultMaxFutureNonceGap,
		minConfirmations:  DefaultMinConfirmations,
		staleBlockAge:     DefaultStaleBlockAge,
		txTTL:             DefaultTxTTL,
		cleanupInterval:   DefaultCleanupInterval,
		saveInterval:      DefaultSaveInterval,
	}
}

// Configuration setters
func (p *TxPool) SetMaxPoolSize(size int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.maxPoolSize = size
}

func (p *TxPool) SetMinGasPrice(gasPrice *big.Int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.minGasPrice = gasPrice
}

func (p *TxPool) SetTxTTL(ttl time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.txTTL = ttl
}

func (p *TxPool) SetBlockchain(bc *Blockchain) { p.chain = bc }
func (p *TxPool) SetChain(bc *Blockchain)      { p.chain = bc }

func (p *TxPool) AddTx(t *tx.Tx, _ p2p.Chain) error          { return p.addTx(t) }
func (p *TxPool) AddTransaction(t *tx.Tx, _ p2p.Chain) error { return p.addTx(t) }

func (p *TxPool) GetConfirmedTxs(_ p2p.Chain, min uint64) []*tx.Tx { return p.getConfirmed(min) }
func (p *TxPool) CleanupStale(_ p2p.Chain, age uint64)             { p.cleanupStale(age) }

func (p *TxPool) Remove(hash common.Hash)            { p.remove(hash) }
func (p *TxPool) RemoveTx(hash common.Hash)          { p.remove(hash) }
func (p *TxPool) RemoveTxs(txs []*tx.Tx)             { p.removeTxs(txs) }
func (p *TxPool) RemoveTransaction(hash common.Hash) { p.remove(hash) }

func (p *TxPool) Clear()                    { p.clear() }
func (p *TxPool) Size() int                 { return p.size() }
func (p *TxPool) GetTransactionCounts() int { return p.size() }

func (p *TxPool) GetTransactionCount(addr common.QuantumAddress) int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.bySender[ethToQuantumAddress(addr)])
}

func (p *TxPool) GetPendingTransactionsByNonce(addr common.QuantumAddress) []*tx.Tx {
	p.mu.RLock()
	defer p.mu.RUnlock()
	list := p.bySender[ethToQuantumAddress(addr)]
	cpy := make([]*tx.Tx, len(list))
	copy(cpy, list)
	return cpy
}

func (p *TxPool) GetNextNonce(addr common.QuantumAddress, _ p2p.Chain) uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.chain == nil || p.chain.State() == nil {
		return 0
	}
	sender := ethToQuantumAddress(addr)
	stateNonce := p.chain.State().GetNonce(common.BytesToQuantumAddress(sender.Bytes()))
	if h := p.nonceTracker[sender]; h >= stateNonce {
		return h + 1
	}
	return stateNonce
}

func (p *TxPool) CleanupInvalidTxs(_ p2p.Chain) int {
	return p.cleanupInvalid()
}

func (p *TxPool) ReplaceTransaction(*tx.Tx, p2p.Chain) error { return nil }

func (p *TxPool) GetSubmitTime(hash common.Hash) (time.Time, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	t, ok := p.submitTime[hash]
	return t, ok
}

func (p *TxPool) addTx(t *tx.Tx) error {
	startTime := time.Now()
	defer func() {
		txPoolLatency.WithLabelValues("add").Observe(time.Since(startTime).Seconds())
	}()

	txPoolOperations.WithLabelValues("add").Inc()

	if t == nil {
		txValidationErrors.WithLabelValues("nil_tx").Inc()
		return errors.New("nil tx")
	}

	if err := t.Validate(); err != nil {
		txValidationErrors.WithLabelValues("validation").Inc()
		return err
	}

	if valid, err := t.Verify(); err != nil || !valid {
		txValidationErrors.WithLabelValues("signature").Inc()
		return errors.New("invalid signature")
	}

	hash := t.Hash()
	sender := t.From

	if p.chain == nil || p.chain.State() == nil {
		txValidationErrors.WithLabelValues("chain_unavailable").Inc()
		return errors.New("chain unavailable")
	}

	if p.chain.monitor != nil && p.chain.monitor.IsAddressBlocked(sender) {
		txValidationErrors.WithLabelValues("blocked_sender").Inc()
		return errors.New("sender blocked due to forged amount activity")
	}

	// BALANCE / NONCE CHECKS
	// IMPORTANT: query chain state before taking the txpool mutex to avoid lock-order
	// inversions that can look like hangs while adding a transaction.
	senderAddress := common.BytesToQuantumAddress(sender.Bytes())
	stateNonce := p.chain.State().GetNonce(senderAddress)
	balance := p.chain.State().GetBalance(senderAddress)

	// Capture latest block height before taking the txpool mutex. Calling
	// chain.Latest() while holding p.mu can invert lock order with chain code that
	// already holds chain locks and then touches the txpool.
	latestHeight := uint64(0)
	if latest := p.chain.Latest(); latest != nil && latest.Header != nil {
		latestHeight = latest.Header.Number.Uint64()
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.txs) >= p.maxPoolSize {
		txDroppedCounter.WithLabelValues("pool_full").Inc()
		return errors.New("pool full")
	}

	if len(t.Data) > p.maxTxSize {
		txDroppedCounter.WithLabelValues("tx_too_large").Inc()
		return errors.New("tx too large")
	}

	if t.GasPrice == nil || t.GasPrice.Cmp(p.minGasPrice) < 0 {
		txValidationErrors.WithLabelValues("gas_price").Inc()
		return fmt.Errorf("gas price too low (min: %s)", p.minGasPrice.String())
	}

	if _, exists := p.txs[hash]; exists {
		txValidationErrors.WithLabelValues("known_tx").Inc()
		return errors.New("known tx")
	}

	senderTxs := p.bySender[sender]
	if len(senderTxs) >= p.maxTxsPerSender {
		txDroppedCounter.WithLabelValues("sender_limit").Inc()
		return errors.New("too many pending from sender")
	}

	expected := stateNonce
	if len(senderTxs) > 0 {
		expected = senderTxs[len(senderTxs)-1].Nonce + 1
	}
	gasCost := new(big.Int).Mul(new(big.Int).SetUint64(t.Gas), t.GasPrice)
	totalCost := new(big.Int).Add(t.Value, gasCost)
	if balance.Cmp(totalCost) < 0 {
		txValidationErrors.WithLabelValues("insufficient_balance").Inc()
		return fmt.Errorf("insufficient balance: have %s, need %s",
			formatBalance(balance), formatBalance(totalCost))
	}

	if t.Nonce != expected {
		txValidationErrors.WithLabelValues("wrong_nonce").Inc()
		return fmt.Errorf("wrong nonce: want %d got %d", expected, t.Nonce)
	}

	if t.Nonce > stateNonce+p.maxFutureNonceGap {
		txValidationErrors.WithLabelValues("nonce_too_high").Inc()
		return errors.New("nonce too high")
	}

	// All good — add to pool
	p.txs[hash] = t
	p.bySender[sender] = append(senderTxs, t)
	p.submitTime[hash] = time.Now()

	p.submitHeight[hash] = latestHeight

	if t.Nonce > p.nonceTracker[sender] {
		p.nonceTracker[sender] = t.Nonce
	}

	// Add to priority queue
	heap.Push(p.pendingHeap, t)

	// Update metrics
	txAddedCounter.Inc()
	txPoolSizeGauge.Set(float64(len(p.txs)))

	log.Printf("[txpool] + %s | %s | nonce=%d | value=%s | gasPrice=%s",
		hash.Hex()[:10], sender.String()[:10], t.Nonce,
		formatBalance(t.Value), t.GasPrice.String())

	return nil
}

func (p *TxPool) getConfirmed(minConfirmations uint64) []*tx.Tx {
	startTime := time.Now()
	defer func() {
		txPoolLatency.WithLabelValues("get_confirmed").Observe(time.Since(startTime).Seconds())
	}()

	p.mu.RLock()
	defer p.mu.RUnlock()

	cur := uint64(0)
	if l := p.chain.Latest(); l != nil {
		cur = l.Header.Number.Uint64()
	}

	// Use configured minimum confirmations
	if minConfirmations == 0 {
		minConfirmations = p.minConfirmations
	}

	var list []*tx.Tx
	for hash, t := range p.txs {
		if h, ok := p.submitHeight[hash]; ok && cur >= h {
			confirmations := cur - h + 1
			if confirmations >= minConfirmations {
				list = append(list, t)
			}
		}
	}

	return list
}

func (p *TxPool) remove(hash common.Hash) {
	startTime := time.Now()
	defer func() {
		txPoolLatency.WithLabelValues("remove").Observe(time.Since(startTime).Seconds())
	}()

	txPoolOperations.WithLabelValues("remove").Inc()

	p.mu.Lock()
	defer p.mu.Unlock()
	p.removeLocked(hash)
}

func (p *TxPool) removeTxs(txs []*tx.Tx) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, t := range txs {
		p.removeLocked(t.Hash())
	}
	p.rebuildNonceTracker()
	p.rebuildPendingHeap()
}

func (p *TxPool) removeLocked(hash common.Hash) {
	t, ok := p.txs[hash]
	if !ok {
		return
	}

	delete(p.txs, hash)
	delete(p.submitHeight, hash)
	delete(p.submitTime, hash)
	txDroppedCounter.WithLabelValues("removed").Inc()
	txPoolSizeGauge.Set(float64(len(p.txs)))

	if list := p.bySender[t.From]; len(list) > 0 {
		for i := range list {
			if list[i].Hash() == hash {
				p.bySender[t.From] = append(list[:i], list[i+1:]...)
				break
			}
		}
		if len(p.bySender[t.From]) == 0 {
			delete(p.bySender, t.From)
		}
	}

	log.Printf("[txpool] - %s | removed", hash.Hex()[:10])
}

func (p *TxPool) clear() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.txs = make(map[common.Hash]*tx.Tx)
	p.bySender = make(map[common.QuantumAddress][]*tx.Tx)
	p.submitHeight = make(map[common.Hash]uint64)
	p.submitTime = make(map[common.Hash]time.Time)
	p.nonceTracker = make(map[common.QuantumAddress]uint64)
	p.pendingHeap = &TxHeap{}
	heap.Init(p.pendingHeap)

	txPoolSizeGauge.Set(0)
	log.Printf("[txpool] Pool cleared")
}

func (p *TxPool) size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.txs)
}

// GetPending returns all pending transactions sorted by gas price
func (p *TxPool) GetPending() []*tx.Tx {
	startTime := time.Now()
	defer func() {
		txPoolLatency.WithLabelValues("get_pending").Observe(time.Since(startTime).Seconds())
	}()

	p.mu.RLock()
	defer p.mu.RUnlock()

	// Create a copy of the heap
	out := make([]*tx.Tx, len(*p.pendingHeap))
	copy(out, *p.pendingHeap)

	log.Printf("[txpool] GetPending returning %d transactions", len(out))
	return out
}

// GetMiningTransactions alias for clarity
func (p *TxPool) GetMiningTransactions() []*tx.Tx {
	return p.GetPending()
}

func (p *TxPool) rebuildNonceTracker() {
	p.nonceTracker = make(map[common.QuantumAddress]uint64)
	for _, t := range p.txs {
		if t.Nonce > p.nonceTracker[t.From] {
			p.nonceTracker[t.From] = t.Nonce
		}
	}
}

func (p *TxPool) rebuildPendingHeap() {
	h := &TxHeap{}
	for _, t := range p.txs {
		heap.Push(h, t)
	}
	heap.Init(h)
	p.pendingHeap = h
}

func (p *TxPool) cleanupStale(maxAge uint64) {
	startTime := time.Now()
	defer func() {
		txPoolLatency.WithLabelValues("cleanup_stale").Observe(time.Since(startTime).Seconds())
	}()

	txPoolOperations.WithLabelValues("cleanup").Inc()

	p.mu.Lock()
	defer p.mu.Unlock()

	cur := uint64(0)
	if l := p.chain.Latest(); l != nil {
		cur = l.Header.Number.Uint64()
	}

	removed := 0
	now := time.Now()

	for h := range p.txs {
		// Remove by block age
		if sh, ok := p.submitHeight[h]; ok && cur > sh && cur-sh > maxAge {
			p.removeLocked(h)
			removed++
			continue
		}

		// Remove by TTL
		if submitTime, ok := p.submitTime[h]; ok && now.Sub(submitTime) > p.txTTL {
			p.removeLocked(h)
			removed++
			txDroppedCounter.WithLabelValues("expired").Inc()
		}
	}

	if removed > 0 {
		p.rebuildNonceTracker()
		p.rebuildPendingHeap()
		log.Printf("[txpool] Cleaned up %d stale transactions", removed)
	}
}

func (p *TxPool) cleanupInvalid() int {
	startTime := time.Now()
	defer func() {
		txPoolLatency.WithLabelValues("cleanup_invalid").Observe(time.Since(startTime).Seconds())
	}()

	txPoolOperations.WithLabelValues("cleanup").Inc()

	p.mu.Lock()
	defer p.mu.Unlock()

	removed := 0
	cur := uint64(0)
	if l := p.chain.Latest(); l != nil {
		cur = l.Header.Number.Uint64()
	}

	now := time.Now()

	for h, t := range p.txs {
		// Check for invalid nonce
		if t.Nonce < p.chain.State().GetNonce(common.BytesToQuantumAddress(t.From.Bytes())) {
			p.removeLocked(h)
			removed++
			txDroppedCounter.WithLabelValues("invalid_nonce").Inc()
			continue
		}

		// Check for very old blocks
		if p.submitHeight[h] > 0 && cur-p.submitHeight[h] > p.staleBlockAge {
			p.removeLocked(h)
			removed++
			txDroppedCounter.WithLabelValues("stale").Inc()
			continue
		}

		// Check TTL
		if now.Sub(p.submitTime[h]) > p.txTTL {
			p.removeLocked(h)
			removed++
			txDroppedCounter.WithLabelValues("expired").Inc()
		}
	}

	if removed > 0 {
		p.rebuildNonceTracker()
		p.rebuildPendingHeap()
		log.Printf("[txpool] Cleaned up %d invalid transactions", removed)
	}

	return removed
}

type storedTx struct {
	Tx           *tx.Tx `json:"tx"`
	SubmitHeight uint64 `json:"submit_height"`
	SubmitTime   int64  `json:"submit_time"`
}

func (p *TxPool) SaveToDisk(dataDir string) error {
	startTime := time.Now()
	defer func() {
		txPoolLatency.WithLabelValues("save").Observe(time.Since(startTime).Seconds())
	}()

	txPoolOperations.WithLabelValues("save").Inc()

	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.txs) == 0 {
		return nil
	}

	var list []storedTx
	for h, t := range p.txs {
		list = append(list, storedTx{
			Tx:           t,
			SubmitHeight: p.submitHeight[h],
			SubmitTime:   p.submitTime[h].Unix(),
		})
	}

	b, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		log.Printf("[txpool] Failed to marshal transactions: %v", err)
		return err
	}

	path := filepath.Join(dataDir, "txpool", "pending.json")
	_ = os.MkdirAll(filepath.Dir(path), os.ModePerm)

	if err := os.WriteFile(path, b, 0600); err != nil {
		log.Printf("[txpool] Failed to save transactions: %v", err)
		return err
	}

	log.Printf("[txpool] Saved %d transactions to disk", len(list))
	return nil
}

func (p *TxPool) LoadFromDisk(dataDir string, _ *Blockchain) error {
	startTime := time.Now()
	defer func() {
		txPoolLatency.WithLabelValues("load").Observe(time.Since(startTime).Seconds())
	}()

	txPoolOperations.WithLabelValues("load").Inc()

	path := filepath.Join(dataDir, "txpool", "pending.json")
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var list []storedTx
	if err := json.Unmarshal(b, &list); err != nil {
		log.Printf("[txpool] Failed to unmarshal saved transactions: %v", err)
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	loaded := 0
	for _, st := range list {
		if st.Tx == nil {
			continue
		}

		if valid, _ := st.Tx.Verify(); !valid {
			txDroppedCounter.WithLabelValues("invalid_signature").Inc()
			continue
		}

		if st.Tx.Nonce < p.chain.State().GetNonce(common.BytesToQuantumAddress(st.Tx.From.Bytes())) {
			txDroppedCounter.WithLabelValues("stale_nonce").Inc()
			continue
		}

		h := st.Tx.Hash()
		p.txs[h] = st.Tx
		p.submitHeight[h] = st.SubmitHeight
		p.submitTime[h] = time.Unix(st.SubmitTime, 0)
		p.bySender[st.Tx.From] = append(p.bySender[st.Tx.From], st.Tx)
		heap.Push(p.pendingHeap, st.Tx)

		loaded++
	}

	p.rebuildNonceTracker()

	if loaded > 0 {
		txPoolSizeGauge.Set(float64(len(p.txs)))
		log.Printf("[txpool] Loaded %d transactions from disk", loaded)
	}

	// Clean up the file after successful load
	_ = os.Remove(path)

	return nil
}

func (p *TxPool) StartBackgroundCleanup(_ p2p.Chain) {
	go func() {
		t := time.NewTicker(p.cleanupInterval)
		defer t.Stop()

		for range t.C {
			p.cleanupStale(p.staleBlockAge)
			p.cleanupInvalid()
		}
	}()
}

func (p *TxPool) SaveTransactionsPeriodically(dataDir string) {
	go func() {
		t := time.NewTicker(p.saveInterval)
		defer t.Stop()

		for range t.C {
			if err := p.SaveToDisk(dataDir); err != nil {
				log.Printf("[txpool] Periodic save failed: %v", err)
			}
		}
	}()
}

// Removes transactions that have been included in a block
func (p *TxPool) CleanupMinedTransactions(minedTxs []*tx.Tx) int {
	startTime := time.Now()
	defer func() {
		txPoolLatency.WithLabelValues("cleanup_mined").Observe(time.Since(startTime).Seconds())
	}()

	txPoolOperations.WithLabelValues("cleanup_mined").Inc()

	p.mu.Lock()
	defer p.mu.Unlock()

	if len(minedTxs) == 0 {
		return 0
	}

	removed := 0

	// Create a set of mined transaction hashes for fast lookup
	minedHashes := make(map[common.Hash]bool)
	for _, minedTx := range minedTxs {
		minedHashes[minedTx.Hash()] = true
	}

	// Remove all mined transactions from the pool
	for hash := range minedHashes {
		if _, exists := p.txs[hash]; exists {
			p.removeLocked(hash)
			removed++
			txDroppedCounter.WithLabelValues("mined").Inc()
		}
	}

	// After removing mined transactions, rebuild structures
	if removed > 0 {
		p.rebuildNonceTracker()
		p.rebuildPendingHeap()
		log.Printf("[txpool] Removed %d mined transactions", removed)
	}

	return removed
}

// Removes transactions that have nonce gaps after cleanup
func (p *TxPool) cleanupInvalidNonces() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for sender, txs := range p.bySender {
		if len(txs) == 0 {
			continue
		}

		// Get current state nonce
		stateNonce := p.chain.State().GetNonce(common.BytesToQuantumAddress(sender.Bytes()))

		// Check if first transaction has correct nonce
		if len(txs) > 0 && txs[0].Nonce > stateNonce {
			// There's a gap - remove all transactions from this sender
			for _, tx := range txs {
				delete(p.txs, tx.Hash())
				delete(p.submitHeight, tx.Hash())
				delete(p.submitTime, tx.Hash())
				txDroppedCounter.WithLabelValues("nonce_gap").Inc()

				log.Printf("[txpool] - %s | removed due to nonce gap (have %d, want %d)",
					tx.Hash().Hex()[:10], tx.Nonce, stateNonce)
			}
			delete(p.bySender, sender)
			delete(p.nonceTracker, sender)
		}
	}

	// Rebuild heap after cleanup
	p.rebuildPendingHeap()
	txPoolSizeGauge.Set(float64(len(p.txs)))
}

func (p *TxPool) GetPendingTransactions() []*tx.Tx {
	return p.GetPending()
}

func ethToQuantumAddress(addr common.QuantumAddress) common.QuantumAddress {
	var out common.QuantumAddress
	copy(out[:], addr.Bytes())
	return out
}

// GetPoolStats returns comprehensive pool statistics
func (p *TxPool) GetPoolStats() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	totalValue := big.NewInt(0)
	totalGasPrice := big.NewInt(0)
	senders := 0

	for _, t := range p.txs {
		totalValue.Add(totalValue, t.Value)
		totalGasPrice.Add(totalGasPrice, t.GasPrice)
	}

	for range p.bySender {
		senders++
	}

	avgGasPrice := big.NewInt(0)
	if len(p.txs) > 0 {
		avgGasPrice.Div(totalGasPrice, big.NewInt(int64(len(p.txs))))
	}

	return map[string]interface{}{
		"total_transactions": len(p.txs),
		"unique_senders":     senders,
		"total_value_wei":    totalValue.String(),
		"total_value_antd":   formatBalance(totalValue),
		"average_gas_price":  avgGasPrice.String(),
		"max_pool_size":      p.maxPoolSize,
		"max_txs_per_sender": p.maxTxsPerSender,
		"min_gas_price":      p.minGasPrice.String(),
		"tx_ttl_hours":       p.txTTL.Hours(),
		"pending_heap_size":  p.pendingHeap.Len(),
	}
}
