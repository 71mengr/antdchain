// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package main

import (
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/antdaza/antdchain/antdc/pow"
	"github.com/antdaza/antdchain/antdc/reward"
	"github.com/antdaza/antdchain/common"
	"github.com/gorilla/mux"
)

//go:embed static/* templates/*
var webContent embed.FS

func (ws *WebServer) setupRoutes() {
	// Static files
	ws.router.PathPrefix("/static/").Handler(http.FileServer(http.FS(webContent)))

	// =========================================================================
	// API ENDPOINTS
	// =========================================================================
	ws.router.HandleFunc("/api/health", ws.apiHealth).Methods("GET")
	ws.router.HandleFunc("/api/chain/status", ws.apiChainStatus).Methods("GET")
	ws.router.HandleFunc("/api/chain/stats", ws.apiChainStats).Methods("GET")
	ws.router.HandleFunc("/api/blocks", ws.apiBlocks).Methods("GET")
	ws.router.HandleFunc("/api/blocks/{height:[0-9]+}", ws.apiBlockByHeight).Methods("GET")
	ws.router.HandleFunc("/api/blocks/hash/{hash}", ws.apiBlockByHash).Methods("GET")
	ws.router.HandleFunc("/api/transactions", ws.apiTransactions).Methods("GET")
	ws.router.HandleFunc("/api/transactions/{hash}", ws.apiTransactionByHash).Methods("GET")
	ws.router.HandleFunc("/api/mempool", ws.apiMempool).Methods("GET")
	ws.router.HandleFunc("/api/address/{address}", ws.apiAddress).Methods("GET")
	ws.router.HandleFunc("/api/validators", ws.apiValidators).Methods("GET")
	ws.router.HandleFunc("/api/rotatingking", ws.apiRotatingKing).Methods("GET")
	ws.router.HandleFunc("/api/search", ws.apiSearch).Methods("GET")
	ws.router.HandleFunc("/health", ws.apiHealth).Methods("GET")
	ws.router.HandleFunc("/status", ws.apiChainStatus).Methods("GET")

	// Web pages – all served by the SPA
	ws.router.HandleFunc("/", ws.serveSPA).Methods("GET")
	ws.router.HandleFunc("/blocks", ws.serveSPA).Methods("GET")
	ws.router.PathPrefix("/block/").HandlerFunc(ws.serveSPA)
	ws.router.PathPrefix("/tx/").HandlerFunc(ws.serveSPA)
	ws.router.PathPrefix("/address/").HandlerFunc(ws.serveSPA)
	ws.router.PathPrefix("/mempool").HandlerFunc(ws.serveSPA)
	ws.router.PathPrefix("/validators").HandlerFunc(ws.serveSPA)
}

func (ws *WebServer) serveSPA(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	data, _ := webContent.ReadFile("templates/index.html")
	w.Write(data)
}

// =============================================================================
// API HANDLERS
// =============================================================================

func (ws *WebServer) apiHealth(w http.ResponseWriter, r *http.Request) {
	height := uint64(0)
	if latest := ws.node.Blockchain().Latest(); latest != nil {
		height = latest.Header.Number.Uint64()
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "online",
		"version":   "2.0.0",
		"height":    height,
		"timestamp": time.Now().Unix(),
		"uptime":    time.Since(startTime).Seconds(),
	})
}

func (ws *WebServer) apiChainStatus(w http.ResponseWriter, r *http.Request) {
	bc := ws.node.Blockchain()
	latest := bc.Latest()

	status := map[string]interface{}{
		"height":     uint64(0),
		"hash":       "",
		"difficulty": "0",
		"syncing":    bc.IsSyncing(),
		"syncTarget": bc.GetSyncTarget(),
		"peers":      0,
	}

	if ws.node.P2PNode() != nil {
		status["peers"] = len(ws.node.P2PNode().Peers())
	}

	if latest != nil && latest.Header != nil {
		status["height"] = latest.Header.Number.Uint64()
		status["hash"] = latest.Hash().Hex()
		status["difficulty"] = pow.DisplayDifficulty(latest.Header.Difficulty)
		status["timestamp"] = latest.Header.Time
		status["gasLimit"] = latest.Header.GasLimit
		status["gasUsed"] = latest.Header.GasUsed
	}

	if pool := bc.TxPool(); pool != nil {
		status["mempoolSize"] = len(pool.GetPending())
	}

	json.NewEncoder(w).Encode(status)
}

func (ws *WebServer) apiChainStats(w http.ResponseWriter, r *http.Request) {
	bc := ws.node.Blockchain()
	latest := bc.Latest()

	stats := map[string]interface{}{
		"totalBlocks":       uint64(0),
		"totalTransactions": uint64(0),
		"avgBlockTime":      float64(0),
		"totalStaked":       "0",
		"activeValidators":  0,
		"total_supply":      "0",
		"totalSupplyANTD":   "0",
	}

	if latest != nil && latest.Header != nil {
		height := latest.Header.Number.Uint64()
		stats["totalBlocks"] = height + 1

		var totalTx uint64
		start := uint64(0)
		if height > 1000 {
			start = height - 1000
		}
		for i := start; i <= height; i++ {
			if blk := bc.GetBlock(i); blk != nil {
				totalTx += uint64(len(blk.Txs))
			}
		}
		stats["totalTransactions"] = totalTx

		circulatingSupply := reward.CalculateCirculatingSupply(height)
		stats["total_supply"] = circulatingSupply.String()
		stats["totalSupplyANTD"] = formatBalance(circulatingSupply)

		if height >= 10 {
			oldest := bc.GetBlock(height - 10)
			if oldest != nil {
				diff := latest.Header.Time - oldest.Header.Time
				stats["avgBlockTime"] = float64(diff) / 10.0
			}
		}
	}

	if powEngine := bc.Pow(); powEngine != nil {
		posStats := powEngine.GetMiningStatistics()
		if v, ok := posStats["total_staked_antd"]; ok {
			stats["totalStaked"] = v
		}
		if v, ok := posStats["active_stakers"]; ok {
			stats["activeValidators"] = v
		}
	}

	json.NewEncoder(w).Encode(stats)
}

func (ws *WebServer) apiBlocks(w http.ResponseWriter, r *http.Request) {
	limitStr := r.URL.Query().Get("limit")
	offsetStr := r.URL.Query().Get("offset")

	limit := 20
	if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 100 {
		limit = l
	}
	offset := 0
	if o, err := strconv.Atoi(offsetStr); err == nil && o >= 0 {
		offset = o
	}

	bc := ws.node.Blockchain()
	latest := bc.Latest()
	if latest == nil {
		json.NewEncoder(w).Encode(map[string]interface{}{"blocks": []interface{}{}, "total": 0})
		return
	}

	maxHeight := latest.Header.Number.Uint64()
	startHeight := maxHeight - uint64(offset)
	if startHeight > maxHeight {
		startHeight = 0
	}
	endHeight := startHeight + 1
	if startHeight >= uint64(limit) {
		endHeight = startHeight - uint64(limit) + 1
	} else {
		endHeight = 0
	}

	var blocks []map[string]interface{}
	for height := startHeight; height >= endHeight && height <= maxHeight; height-- {
		blk := bc.GetBlock(height)
		if blk == nil {
			continue
		}
		blockData := map[string]interface{}{
			"height":       height,
			"hash":         blk.Hash().Hex(),
			"parentHash":   blk.Header.ParentHash.Hex(),
			"miner":        blk.Header.Coinbase.String(),
			"timestamp":    blk.Header.Time,
			"timestampUTC": time.Unix(int64(blk.Header.Time), 0).UTC().Format(time.RFC3339),
			"difficulty":   pow.DisplayDifficulty(blk.Header.Difficulty),
			"gasLimit":     blk.Header.GasLimit,
			"gasUsed":      blk.Header.GasUsed,
			"txCount":      len(blk.Txs),
			"size":         blk.Size(),
		}
		blocks = append(blocks, blockData)
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"blocks": blocks,
		"total":  maxHeight + 1,
		"limit":  limit,
		"offset": offset,
	})
}

func (ws *WebServer) apiBlockByHeight(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	height, err := strconv.ParseUint(vars["height"], 10, 64)
	if err != nil {
		http.Error(w, `{"error":"invalid block height"}`, http.StatusBadRequest)
		return
	}

	blk := ws.node.Blockchain().GetBlock(height)
	if blk == nil {
		http.Error(w, `{"error":"block not found"}`, http.StatusNotFound)
		return
	}

	blockData := map[string]interface{}{
		"height":       height,
		"hash":         blk.Hash().Hex(),
		"parentHash":   blk.Header.ParentHash.Hex(),
		"miner":        blk.Header.Coinbase.String(),
		"timestamp":    blk.Header.Time,
		"timestampUTC": time.Unix(int64(blk.Header.Time), 0).UTC().Format(time.RFC3339),
		"difficulty":   pow.DisplayDifficulty(blk.Header.Difficulty),
		"nonce":        hex.EncodeToString(blk.Header.Nonce[:]),
		"mixHash":      blk.Header.MixDigest.Hex(),
		"stateRoot":    blk.Header.Root.Hex(),
		"txRoot":       blk.Header.TxHash.Hex(),
		"gasLimit":     blk.Header.GasLimit,
		"gasUsed":      blk.Header.GasUsed,
		"extraData":    string(blk.Header.Extra),
		"size":         blk.Size(),
	}

	var txs []map[string]interface{}
	for _, tx := range blk.Txs {
		to := ""
		if tx.To != nil {
			to = tx.To.String()
		}
		txData := map[string]interface{}{
			"hash":        tx.Hash().Hex(),
			"from":        tx.From.String(),
			"to":          to,
			"value":       tx.Value.String(),
			"gas":         tx.Gas,
			"gasPrice":    tx.GasPrice.String(),
			"nonce":       tx.Nonce,
			"data":        hex.EncodeToString(tx.Data),
			"blockHeight": height,
		}
		txs = append(txs, txData)
	}
	blockData["transactions"] = txs

	json.NewEncoder(w).Encode(blockData)
}

func (ws *WebServer) apiBlockByHash(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	hash := vars["hash"]

	bc := ws.node.Blockchain()
	latest := bc.Latest()
	if latest == nil {
		http.Error(w, `{"error":"chain empty"}`, http.StatusNotFound)
		return
	}

	for height := uint64(0); height <= latest.Header.Number.Uint64(); height++ {
		blk := bc.GetBlock(height)
		if blk != nil && blk.Hash().Hex() == hash {
			http.Redirect(w, r, fmt.Sprintf("/api/blocks/%d", height), http.StatusFound)
			return
		}
	}
	http.Error(w, `{"error":"block not found"}`, http.StatusNotFound)
}

func (ws *WebServer) apiTransactions(w http.ResponseWriter, r *http.Request) {
	limitStr := r.URL.Query().Get("limit")
	address := strings.TrimSpace(r.URL.Query().Get("address"))

	limit := 50
	if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 200 {
		limit = l
	}

	bc := ws.node.Blockchain()
	latest := bc.Latest()
	if latest == nil {
		json.NewEncoder(w).Encode(map[string]interface{}{"transactions": []interface{}{}})
		return
	}

	var allTxs []map[string]interface{}
	for height := latest.Header.Number.Uint64(); height >= 0 && len(allTxs) < limit; height-- {
		blk := bc.GetBlock(height)
		if blk == nil {
			continue
		}
		for _, tx := range blk.Txs {
			to := ""
			if tx.To != nil {
				to = tx.To.String()
			}
			txFrom := tx.From.String()
			txTo := to

			if address != "" && txFrom != address && txTo != address {
				continue
			}

			txData := map[string]interface{}{
				"hash":         tx.Hash().Hex(),
				"from":         txFrom,
				"to":           txTo,
				"value":        tx.Value.String(),
				"gas":          tx.Gas,
				"gasPrice":     tx.GasPrice.String(),
				"nonce":        tx.Nonce,
				"data":         hex.EncodeToString(tx.Data),
				"blockHeight":  height,
				"timestamp":    blk.Header.Time,
				"timestampUTC": time.Unix(int64(blk.Header.Time), 0).UTC().Format(time.RFC3339),
				"status":       "confirmed",
			}
			allTxs = append(allTxs, txData)
			if len(allTxs) >= limit {
				break
			}
		}
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"transactions": allTxs,
		"count":        len(allTxs),
	})
}

func (ws *WebServer) apiTransactionByHash(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	hash := vars["hash"]

	bc := ws.node.Blockchain()
	latest := bc.Latest()
	if latest == nil {
		http.Error(w, `{"error":"chain empty"}`, http.StatusNotFound)
		return
	}

	for height := uint64(0); height <= latest.Header.Number.Uint64(); height++ {
		blk := bc.GetBlock(height)
		if blk == nil {
			continue
		}
		for _, tx := range blk.Txs {
			if tx.Hash().Hex() == hash {
				to := ""
				if tx.To != nil {
					to = tx.To.String()
				}
				json.NewEncoder(w).Encode(map[string]interface{}{
					"hash":         tx.Hash().Hex(),
					"from":         tx.From.String(),
					"to":           to,
					"value":        tx.Value.String(),
					"gas":          tx.Gas,
					"gasPrice":     tx.GasPrice.String(),
					"nonce":        tx.Nonce,
					"data":         hex.EncodeToString(tx.Data),
					"blockHeight":  height,
					"blockHash":    blk.Hash().Hex(),
					"timestamp":    blk.Header.Time,
					"timestampUTC": time.Unix(int64(blk.Header.Time), 0).UTC().Format(time.RFC3339),
					"status":       "confirmed",
				})
				return
			}
		}
	}
	http.Error(w, `{"error":"transaction not found"}`, http.StatusNotFound)
}

func (ws *WebServer) apiMempool(w http.ResponseWriter, r *http.Request) {
	pending := ws.node.Blockchain().TxPool().GetPending()
	var txs []map[string]interface{}
	for _, tx := range pending {
		to := ""
		if tx.To != nil {
			to = tx.To.String()
		}
		txData := map[string]interface{}{
			"hash":     tx.Hash().Hex(),
			"from":     tx.From.String(),
			"to":       to,
			"value":    tx.Value.String(),
			"gas":      tx.Gas,
			"gasPrice": tx.GasPrice.String(),
			"nonce":    tx.Nonce,
		}
		txs = append(txs, txData)
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"count":        len(txs),
		"transactions": txs,
	})
}

func (ws *WebServer) apiAddress(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	address := vars["address"]

	addr, err := common.ParseQuantumAddress(address)
	if err != nil {
		http.Error(w, `{"error":"invalid address"}`, http.StatusBadRequest)
		return
	}

	bc := ws.node.Blockchain()
	balance := bc.GetAccountBalance(addr)
	nonce := bc.State().GetNonce(addr)

	latest := bc.Latest()
	var txs []map[string]interface{}
	if latest != nil {
		count := 0
		for height := latest.Header.Number.Uint64(); height >= 0 && count < 50; height-- {
			blk := bc.GetBlock(height)
			if blk == nil {
				continue
			}
			for _, tx := range blk.Txs {
				txFrom := tx.From.String()
				to := ""
				if tx.To != nil {
					to = tx.To.String()
				}
				if txFrom == address || to == address {
					txs = append(txs, map[string]interface{}{
						"hash":         tx.Hash().Hex(),
						"from":         txFrom,
						"to":           to,
						"value":        tx.Value.String(),
						"gas":          tx.Gas,
						"gasPrice":     tx.GasPrice.String(),
						"nonce":        tx.Nonce,
						"blockHeight":  height,
						"timestamp":    blk.Header.Time,
						"timestampUTC": time.Unix(int64(blk.Header.Time), 0).UTC().Format(time.RFC3339),
						"direction":    direction(txFrom, address),
					})
					count++
					if count >= 50 {
						break
					}
				}
			}
		}
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"address":      address,
		"balance":      balance.String(),
		"balanceANTD":  formatBalance(balance),
		"nonce":        nonce,
		"transactions": txs,
		"txCount":      len(txs),
	})
}

func direction(from, address string) string {
	if from == address {
		return "out"
	}
	return "in"
}

func (ws *WebServer) apiValidators(w http.ResponseWriter, r *http.Request) {
	bc := ws.node.Blockchain()
	var validators []map[string]interface{}

	if powEngine := bc.Pow(); powEngine != nil {
		stats := powEngine.GetMiningStatistics()
		validators = append(validators, map[string]interface{}{
			"activeStakers":        stats["active_stakers"],
			"totalStakers":         stats["total_stakers"],
			"totalStakedANTD":      stats["total_staked_antd"],
			"currentMiner":         stats["current_miner"],
			"blocksThisTurn":       stats["blocks_this_turn"],
			"blocksPerTurn":        stats["blocks_per_turn"],
			"rotations":            stats["rotations"],
			"missedBlocks":         stats["missed_blocks"],
			"difficulty":           stats["current_difficulty"],
			"avgBlockTime":         stats["average_block_time"],
			"unbondingValidators":  stats["unbonding_validators"],
		})

		kingAddrs := powEngine.GetKingAddresses()
		for _, addr := range kingAddrs {
			balance := bc.GetAccountBalance(addr)
			validators = append(validators, map[string]interface{}{
				"address":     addr.String(),
				"balance":     balance.String(),
				"balanceANTD": formatBalance(balance),
				"isKing":      powEngine.IsKing(addr),
			})
		}
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"validators": validators,
	})
}

func (ws *WebServer) apiRotatingKing(w http.ResponseWriter, r *http.Request) {
	bc := ws.node.Blockchain()
	rkManager := bc.GetRotatingKingManager()
	if rkManager == nil {
		http.Error(w, `{"error":"rotating king manager not available"}`, http.StatusServiceUnavailable)
		return
	}

	currentKing := rkManager.GetCurrentKing()
	nextKing := rkManager.GetNextKing()
	rotationInfo := rkManager.GetRotationInfo(bc.GetChainHeight())
	kingAddresses := rkManager.GetKingAddresses()

	var blocksUntilRotation uint64
	if v, ok := rotationInfo["blocksUntilRotation"]; ok {
		if val, ok := v.(uint64); ok {
			blocksUntilRotation = val
		}
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"currentKing":            currentKing.String(),
		"nextKing":               nextKing.String(),
		"kingCount":              len(kingAddresses),
		"blocksUntilRotation":    blocksUntilRotation,
		"rotationHeight":         rotationInfo["rotationHeight"],
		"rotationInterval":       rotationInfo["rotationInterval"],
		"kingAddresses":          kingAddresses,
		"totalRewardsDistributed": func() string {
			if v := rkManager.GetTotalRewardsDistributed(); v != nil {
				return v.String()
			}
			return "0"
		}(),
	})
}

func (ws *WebServer) apiSearch(w http.ResponseWriter, r *http.Request) {
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if query == "" {
		json.NewEncoder(w).Encode(map[string]interface{}{"type": "none"})
		return
	}

	bc := ws.node.Blockchain()

	// Try block height
	if h, err := strconv.ParseUint(query, 10, 64); err == nil {
		if blk := bc.GetBlock(h); blk != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"type": "block",
				"data": map[string]interface{}{
					"height": h,
					"hash":   blk.Hash().Hex(),
				},
			})
			return
		}
	}

	// Try address
	if strings.HasPrefix(query, "0q") || strings.HasPrefix(query, "0x") {
		addr, err := common.ParseQuantumAddress(query)
		if err == nil {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"type": "address",
				"data": map[string]interface{}{
					"address": addr.String(),
				},
			})
			return
		}
	}

	// Try hash (block or transaction)
	latest := bc.Latest()
	if latest != nil {
		for height := uint64(0); height <= latest.Header.Number.Uint64(); height++ {
			blk := bc.GetBlock(height)
			if blk == nil {
				continue
			}
			if blk.Hash().Hex() == query {
				json.NewEncoder(w).Encode(map[string]interface{}{
					"type": "block",
					"data": map[string]interface{}{
						"height": height,
						"hash":   blk.Hash().Hex(),
					},
				})
				return
			}
			for _, tx := range blk.Txs {
				if tx.Hash().Hex() == query {
					json.NewEncoder(w).Encode(map[string]interface{}{
						"type": "transaction",
						"data": map[string]interface{}{
							"hash": tx.Hash().Hex(),
						},
					})
					return
				}
			}
		}
	}

	json.NewEncoder(w).Encode(map[string]interface{}{"type": "not_found"})
}

// formatBalance formats wei to human-readable ANTD
func formatBalance(amount *big.Int) string {
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
