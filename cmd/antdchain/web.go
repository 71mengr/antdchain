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

"github.com/ethereum/go-ethereum/common"
"github.com/gorilla/mux"
)

//go:embed static/* templates/*
var webContent embed.FS

func (ws *WebServer) setupRoutes() {
// Static files
ws.router.PathPrefix("/static/").Handler(http.FileServer(http.FS(webContent)))

// API endpoints
ws.router.HandleFunc("/api/health", ws.apiHealth).Methods("GET")
ws.router.HandleFunc("/api/chain/status", ws.apiChainStatus).Methods("GET")
ws.router.HandleFunc("/api/blocks", ws.apiBlocks).Methods("GET")
ws.router.HandleFunc("/api/blocks/{height:[0-9]+}", ws.apiBlockByHeight).Methods("GET")
ws.router.HandleFunc("/api/transactions", ws.apiTransactions).Methods("GET")
ws.router.HandleFunc("/api/mempool", ws.apiMempool).Methods("GET")
ws.router.HandleFunc("/api/wallet/list", ws.apiListWallets).Methods("GET")
ws.router.HandleFunc("/api/wallet/balance/{address}", ws.apiWalletBalance).Methods("GET")

// Web pages
ws.router.HandleFunc("/", ws.pageDashboard).Methods("GET")
ws.router.HandleFunc("/blocks", ws.pageBlocks).Methods("GET")
ws.router.HandleFunc("/blocks/{height}", ws.pageBlock).Methods("GET")
ws.router.HandleFunc("/wallet", ws.pageWallet).Methods("GET")
}

// ============================================================================
// API HANDLERS
// ============================================================================

func (ws *WebServer) apiHealth(w http.ResponseWriter, r *http.Request) {
height := uint64(0)
if latest := ws.node.Blockchain().Latest(); latest != nil {
height = latest.Header.Number.Uint64()
}

json.NewEncoder(w).Encode(map[string]interface{}{
"status":    "online",
"version":   "1.0.0",
"height":    height,
"timestamp": time.Now().Unix(),
"uptime":    time.Since(startTime).Seconds(),
})
}

func (ws *WebServer) apiChainStatus(w http.ResponseWriter, r *http.Request) {
bc := ws.node.Blockchain()
latest := bc.Latest()

status := map[string]interface{}{
"height":       uint64(0),
"hash":         "",
"difficulty":   "0",
"total_tx":     0,
"total_blocks": 0,
"syncing":      bc.IsSyncing(),
"sync_target":  bc.GetSyncTarget(),
}

if latest != nil && latest.Header != nil {
status["height"] = latest.Header.Number.Uint64()
status["hash"] = latest.Hash().Hex()
status["difficulty"] = latest.Header.Difficulty.String()

// Count total transactions
totalTx := 0
totalBlocks := 0
for i := uint64(0); i <= latest.Header.Number.Uint64(); i++ {
if blk := bc.GetBlock(i); blk != nil {
totalTx += len(blk.Txs)
totalBlocks++
}
}
status["total_tx"] = totalTx
status["total_blocks"] = totalBlocks
}

json.NewEncoder(w).Encode(status)
}

func (ws *WebServer) apiBlocks(w http.ResponseWriter, r *http.Request) {
limitStr := r.URL.Query().Get("limit")

limit := 20
if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 100 {
limit = l
}

bc := ws.node.Blockchain()
latest := bc.Latest()
if latest == nil {
json.NewEncoder(w).Encode([]interface{}{})
return
}

maxHeight := latest.Header.Number.Uint64()
var blocks []map[string]interface{}

for height := maxHeight; height > maxHeight-uint64(limit) && height <= maxHeight; height-- {
blk := bc.GetBlock(height)
if blk == nil {
continue
}

blockData := map[string]interface{}{
"height":      height,
"hash":        blk.Hash().Hex(),
"parent_hash": blk.Header.ParentHash.Hex(),
"miner":       blk.Header.Coinbase.Hex(),
"timestamp":   blk.Header.Time,
"difficulty":  blk.Header.Difficulty.String(),
"gas_limit":   blk.Header.GasLimit,
"gas_used":    blk.Header.GasUsed,
"tx_count":    len(blk.Txs),
}
blocks = append(blocks, blockData)
}

json.NewEncoder(w).Encode(map[string]interface{}{
"blocks": blocks,
"total":  maxHeight + 1,
})
}

func (ws *WebServer) apiBlockByHeight(w http.ResponseWriter, r *http.Request) {
vars := mux.Vars(r)
heightStr := vars["height"]

height, err := strconv.ParseUint(heightStr, 10, 64)
if err != nil {
http.Error(w, "Invalid block height", http.StatusBadRequest)
return
}

blk := ws.node.Blockchain().GetBlock(height)
if blk == nil {
http.Error(w, "Block not found", http.StatusNotFound)
return
}

blockData := map[string]interface{}{
"height":      height,
"hash":        blk.Hash().Hex(),
"parent_hash": blk.Header.ParentHash.Hex(),
"miner":       blk.Header.Coinbase.Hex(),
"timestamp":   blk.Header.Time,
"difficulty":  blk.Header.Difficulty.String(),
"nonce":       hex.EncodeToString(blk.Header.Nonce[:]),
"mix_hash":    blk.Header.MixDigest.Hex(),
"gas_limit":   blk.Header.GasLimit,
"gas_used":    blk.Header.GasUsed,
"extra_data":  string(blk.Header.Extra),
}

// Add transactions
var txs []map[string]interface{}
for _, tx := range blk.Txs {
to := ""
if tx.To != nil {
to = tx.To.String()
}

txData := map[string]interface{}{
"hash":      tx.Hash().Hex(),
"from":      tx.From.String(),
"to":        to,
"value":     tx.Value.String(),
"gas":       tx.Gas,
"gas_price": tx.GasPrice.String(),
"nonce":     tx.Nonce,
"data":      hex.EncodeToString(tx.Data),
}
txs = append(txs, txData)
}
blockData["transactions"] = txs

json.NewEncoder(w).Encode(blockData)
}

func (ws *WebServer) apiTransactions(w http.ResponseWriter, r *http.Request) {
limitStr := r.URL.Query().Get("limit")
address := r.URL.Query().Get("address")

limit := 50
if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 100 {
limit = l
}

bc := ws.node.Blockchain()
latest := bc.Latest()
if latest == nil {
json.NewEncoder(w).Encode([]interface{}{})
return
}

var allTxs []map[string]interface{}

// Collect transactions from blocks
for height := latest.Header.Number.Uint64(); height >= 0 && len(allTxs) < limit; height-- {
blk := bc.GetBlock(height)
if blk == nil {
continue
}

for _, tx := range blk.Txs {
// Filter by address if specified
if address != "" {
addr := common.HexToAddress(address)
txFrom := common.BytesToAddress(tx.From.Bytes())
txTo := common.Address{}
if tx.To != nil {
txTo = common.BytesToAddress(tx.To.Bytes())
}
if txFrom != addr && (tx.To == nil || txTo != addr) {
continue
}
}

to := ""
if tx.To != nil {
to = tx.To.String()
}

txData := map[string]interface{}{
"hash":      tx.Hash().Hex(),
"from":      tx.From.String(),
"to":        to,
"value":     tx.Value.String(),
"gas":       tx.Gas,
"gas_price": tx.GasPrice.String(),
"nonce":     tx.Nonce,
"block":     height,
"timestamp": blk.Header.Time,
"status":    "confirmed",
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

func (ws *WebServer) apiMempool(w http.ResponseWriter, r *http.Request) {
pending := ws.node.Blockchain().TxPool().GetPending()

var txs []map[string]interface{}
for _, tx := range pending {
to := ""
if tx.To != nil {
to = tx.To.String()
}

txData := map[string]interface{}{
"hash":      tx.Hash().Hex(),
"from":      tx.From.String(),
"to":        to,
"value":     tx.Value.String(),
"gas":       tx.Gas,
"gas_price": tx.GasPrice.String(),
"nonce":     tx.Nonce,
}
txs = append(txs, txData)
}

json.NewEncoder(w).Encode(map[string]interface{}{
"count":        len(txs),
"transactions": txs,
})
}

func (ws *WebServer) apiListWallets(w http.ResponseWriter, r *http.Request) {
// Get accounts from node's keystore instead of wallet manager
var walletList []map[string]interface{}

// Try to get accounts from the node's keystore
if keystore := ws.node.Keystore(); keystore != nil {
accounts := keystore.Accounts()
for _, acc := range accounts {
balance := ws.node.Blockchain().GetAccountBalance(acc.Address)

walletData := map[string]interface{}{
"address":      acc.Address.Hex(),
"name":         "Wallet", // You might need to get the actual name
"balance":      balance.String(),
"balance_antd": formatBalance(balance),
"nonce":        ws.node.Blockchain().State().GetNonce(acc.Address),
}
walletList = append(walletList, walletData)
}
}

json.NewEncoder(w).Encode(walletList)
}

func (ws *WebServer) apiWalletBalance(w http.ResponseWriter, r *http.Request) {
vars := mux.Vars(r)
address := vars["address"]

addr := common.HexToAddress(address)
balance := ws.node.Blockchain().GetAccountBalance(addr)

json.NewEncoder(w).Encode(map[string]interface{}{
"address":      address,
"balance":      balance.String(),
"balance_antd": formatBalance(balance),
})
}

// ============================================================================
// WEB PAGES
// ============================================================================

func (ws *WebServer) pageDashboard(w http.ResponseWriter, r *http.Request) {
w.Header().Set("Content-Type", "text/html")

html := `<!DOCTYPE html>
<html>
<head>
    <title>ANTDChain Dashboard</title>
    <style>
        body { font-family: sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { background: #2c3e50; color: white; padding: 20px; border-radius: 5px; }
        .nav { margin: 20px 0; }
        .nav a { margin-right: 15px; color: #3498db; text-decoration: none; }
        .card { background: white; padding: 20px; margin-bottom: 20px; border-radius: 5px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .stats { display: flex; gap: 20px; margin: 20px 0; }
        .stat-box { flex: 1; background: white; padding: 20px; border-radius: 5px; text-align: center; }
        .stat-value { font-size: 2rem; font-weight: bold; color: #3498db; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #eee; }
        th { background: #f8f9fa; }
        tr:hover { background: #f8f9fa; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>ANTDChain Dashboard</h1>
            <p>Real-time blockchain monitoring</p>
        </div>
        
        <div class="nav">
            <a href="/">Dashboard</a>
            <a href="/blocks">Blocks</a>
            <a href="/wallet">Wallet</a>
        </div>
        
        <div class="stats">
            <div class="stat-box">
                <h3>Block Height</h3>
                <div class="stat-value" id="stat-height">0</div>
            </div>
            <div class="stat-box">
                <h3>Mempool Size</h3>
                <div class="stat-value" id="stat-mempool">0</div>
            </div>
            <div class="stat-box">
                <h3>Total Transactions</h3>
                <div class="stat-value" id="stat-total-tx">0</div>
            </div>
        </div>
        
        <div class="card">
            <h3>Latest Blocks</h3>
            <div id="latest-blocks">Loading...</div>
        </div>
        
        <div class="card">
            <h3>Latest Transactions</h3>
            <div id="latest-transactions">Loading...</div>
        </div>
    </div>
    
    <script>
        async function updateStats() {
            try {
                // Get chain status
                const statusRes = await fetch('/api/chain/status');
                const status = await statusRes.json();
                
                document.getElementById('stat-height').textContent = status.height;
                document.getElementById('stat-total-tx').textContent = status.total_tx;
                
                // Get mempool
                const mempoolRes = await fetch('/api/mempool');
                const mempool = await mempoolRes.json();
                document.getElementById('stat-mempool').textContent = mempool.count;
                
                // Get latest blocks
                const blocksRes = await fetch('/api/blocks?limit=10');
                const blocksData = await blocksRes.json();
                
                let blocksHtml = '<table>';
                blocksHtml += '<tr><th>Height</th><th>Hash</th><th>Miner</th><th>TXs</th><th>Time</th></tr>';
                
                blocksData.blocks.forEach(function(block) {
                    const time = new Date(block.timestamp * 1000).toLocaleTimeString();
                    blocksHtml += '<tr>';
                    blocksHtml += '<td><a href="/blocks/' + block.height + '">' + block.height + '</a></td>';
                    blocksHtml += '<td><small>' + block.hash.substring(0, 16) + '...</small></td>';
                    blocksHtml += '<td><small>' + block.miner.substring(0, 16) + '...</small></td>';
                    blocksHtml += '<td>' + block.tx_count + '</td>';
                    blocksHtml += '<td>' + time + '</td>';
                    blocksHtml += '</tr>';
                });
                blocksHtml += '</table>';
                document.getElementById('latest-blocks').innerHTML = blocksHtml;
                
                // Get latest transactions
                const txsRes = await fetch('/api/transactions?limit=10');
                const txsData = await txsRes.json();
                
                let txsHtml = '<table>';
                txsHtml += '<tr><th>Hash</th><th>From</th><th>To</th><th>Value</th></tr>';
                
                txsData.transactions.forEach(function(tx) {
                    txsHtml += '<tr>';
                    txsHtml += '<td><small>' + tx.hash.substring(0, 16) + '...</small></td>';
                    txsHtml += '<td><small>' + tx.from.substring(0, 16) + '...</small></td>';
                    if (tx.to) {
                        txsHtml += '<td><small>' + tx.to.substring(0, 16) + '...</small></td>';
                    } else {
                        txsHtml += '<td><small>Contract</small></td>';
                    }
                    txsHtml += '<td>' + tx.value + '</td>';
                    txsHtml += '</tr>';
                });
                txsHtml += '</table>';
                document.getElementById('latest-transactions').innerHTML = txsHtml;
                
            } catch (error) {
                console.error('Error updating stats:', error);
            }
        }
        
        // Update every 5 seconds
        updateStats();
        setInterval(updateStats, 5000);
    </script>
</body>
</html>`

w.Write([]byte(html))
}

func (ws *WebServer) pageBlocks(w http.ResponseWriter, r *http.Request) {
w.Header().Set("Content-Type", "text/html")

html := `<!DOCTYPE html>
<html>
<head>
    <title>ANTDChain Blocks</title>
    <style>
        body { font-family: sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { background: #2c3e50; color: white; padding: 20px; border-radius: 5px; }
        .nav { margin: 20px 0; }
        .nav a { margin-right: 15px; color: #3498db; text-decoration: none; }
        table { width: 100%; background: white; border-collapse: collapse; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #eee; }
        th { background: #f8f9fa; }
        tr:hover { background: #f8f9fa; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>ANTDChain Blocks</h1>
        </div>
        
        <div class="nav">
            <a href="/">Dashboard</a>
            <a href="/blocks">Blocks</a>
            <a href="/wallet">Wallet</a>
        </div>
        
        <div id="blocks-container">Loading blocks...</div>
    </div>
    
    <script>
        async function loadBlocks() {
            try {
                const res = await fetch('/api/blocks?limit=50');
                const data = await res.json();
                
                let html = '<table>';
                html += '<tr><th>Height</th><th>Hash</th><th>Miner</th><th>TXs</th><th>Time</th><th>Difficulty</th></tr>';
                
                data.blocks.forEach(function(block) {
                    const time = new Date(block.timestamp * 1000).toLocaleString();
                    html += '<tr>';
                    html += '<td><a href="/blocks/' + block.height + '">' + block.height + '</a></td>';
                    html += '<td><small>' + block.hash.substring(0, 16) + '...</small></td>';
                    html += '<td><small>' + block.miner.substring(0, 16) + '...</small></td>';
                    html += '<td>' + block.tx_count + '</td>';
                    html += '<td>' + time + '</td>';
                    html += '<td>' + block.difficulty + '</td>';
                    html += '</tr>';
                });
                html += '</table>';
                
                document.getElementById('blocks-container').innerHTML = html;
            } catch (error) {
                document.getElementById('blocks-container').innerHTML = 'Error loading blocks';
                console.error(error);
            }
        }
        
        loadBlocks();
    </script>
</body>
</html>`

w.Write([]byte(html))
}

func (ws *WebServer) pageBlock(w http.ResponseWriter, r *http.Request) {
vars := mux.Vars(r)
height := vars["height"]

w.Header().Set("Content-Type", "text/html")

html := fmt.Sprintf(`<!DOCTYPE html>
<html>
<head>
    <title>ANTDChain Block %s</title>
    <style>
        body { font-family: sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { background: #2c3e50; color: white; padding: 20px; border-radius: 5px; }
        .nav { margin: 20px 0; }
        .nav a { margin-right: 15px; color: #3498db; text-decoration: none; }
        .card { background: white; padding: 20px; margin-bottom: 20px; border-radius: 5px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .info { display: grid; grid-template-columns: 150px 1fr; gap: 10px; margin: 10px 0; }
        .info label { font-weight: bold; }
        table { width: 100%%; border-collapse: collapse; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #eee; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Block %s</h1>
        </div>
        
        <div class="nav">
            <a href="/">Dashboard</a>
            <a href="/blocks">Blocks</a>
            <a href="/wallet">Wallet</a>
        </div>
        
        <div class="card">
            <h3>Block Information</h3>
            <div id="block-info">Loading...</div>
        </div>
        
        <div class="card">
            <h3>Transactions</h3>
            <div id="block-transactions">Loading...</div>
        </div>
    </div>
    
    <script>
        async function loadBlock() {
            try {
                const res = await fetch('/api/blocks/%s');
                const block = await res.json();
                
                // Block info
                const time = new Date(block.timestamp * 1000).toLocaleString();
                let infoHtml = '<div class="info">';
                infoHtml += '<div><label>Hash:</label></div><div><small>' + block.hash + '</small></div>';
                infoHtml += '<div><label>Parent Hash:</label></div><div><small>' + block.parent_hash + '</small></div>';
                infoHtml += '<div><label>Miner:</label></div><div><small>' + block.miner + '</small></div>';
                infoHtml += '<div><label>Timestamp:</label></div><div>' + time + '</div>';
                infoHtml += '<div><label>Difficulty:</label></div><div>' + block.difficulty + '</div>';
                infoHtml += '<div><label>Gas Used:</label></div><div>' + block.gas_used + ' / ' + block.gas_limit + '</div>';
                infoHtml += '</div>';
                
                document.getElementById('block-info').innerHTML = infoHtml;
                
                // Transactions
                if (block.transactions && block.transactions.length > 0) {
                    let txsHtml = '<table>';
                    txsHtml += '<tr><th>Hash</th><th>From</th><th>To</th><th>Value</th><th>Nonce</th></tr>';
                    
                    block.transactions.forEach(function(tx) {
                        txsHtml += '<tr>';
                        txsHtml += '<td><small>' + tx.hash.substring(0, 16) + '...</small></td>';
                        txsHtml += '<td><small>' + tx.from.substring(0, 16) + '...</small></td>';
                        if (tx.to) {
                            txsHtml += '<td><small>' + tx.to.substring(0, 16) + '...</small></td>';
                        } else {
                            txsHtml += '<td><small>Contract</small></td>';
                        }
                        txsHtml += '<td>' + tx.value + '</td>';
                        txsHtml += '<td>' + tx.nonce + '</td>';
                        txsHtml += '</tr>';
                    });
                    txsHtml += '</table>';
                    document.getElementById('block-transactions').innerHTML = txsHtml;
                } else {
                    document.getElementById('block-transactions').innerHTML = 'No transactions in this block';
                }
                
            } catch (error) {
                document.getElementById('block-info').innerHTML = 'Error loading block';
                console.error(error);
            }
        }
        
        loadBlock();
    </script>
</body>
</html>`, height, height, height)

w.Write([]byte(html))
}

func (ws *WebServer) pageWallet(w http.ResponseWriter, r *http.Request) {
w.Header().Set("Content-Type", "text/html")

html := `<!DOCTYPE html>
<html>
<head>
    <title>ANTDChain Wallet</title>
    <style>
        body { font-family: sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { background: #2c3e50; color: white; padding: 20px; border-radius: 5px; }
        .nav { margin: 20px 0; }
        .nav a { margin-right: 15px; color: #3498db; text-decoration: none; }
        .card { background: white; padding: 20px; margin-bottom: 20px; border-radius: 5px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #eee; }
        .wallet-address { font-family: monospace; background: #f8f9fa; padding: 5px; border-radius: 3px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>ANTDChain Wallet</h1>
        </div>
        
        <div class="nav">
            <a href="/">Dashboard</a>
            <a href="/blocks">Blocks</a>
            <a href="/wallet">Wallet</a>
        </div>
        
        <div class="card">
            <h3>Your Wallets</h3>
            <div id="wallets-list">Loading wallets...</div>
        </div>
    </div>
    
    <script>
        async function loadWallets() {
            try {
                const res = await fetch('/api/wallet/list');
                const wallets = await res.json();
                
                if (wallets.length === 0) {
                    document.getElementById('wallets-list').innerHTML = 'No wallets found';
                    return;
                }
                
                let html = '<table>';
                html += '<tr><th>Name</th><th>Address</th><th>Balance</th><th>Balance (ANTD)</th></tr>';
                
                wallets.forEach(function(wallet) {
                    const name = wallet.name || 'Wallet';
                    html += '<tr>';
                    html += '<td>' + name + '</td>';
                    html += '<td><span class="wallet-address">' + wallet.address + '</span></td>';
                    html += '<td>' + wallet.balance + '</td>';
                    html += '<td>' + wallet.balance_antd + ' ANTD</td>';
                    html += '</tr>';
                });
                html += '</table>';
                
                document.getElementById('wallets-list').innerHTML = html;
            } catch (error) {
                document.getElementById('wallets-list').innerHTML = 'Error loading wallets';
                console.error(error);
            }
        }
        
        loadWallets();
    </script>
</body>
</html>`

w.Write([]byte(html))
}

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

// Global start time - make sure this is defined in main.go
//var startTime = time.Now()
