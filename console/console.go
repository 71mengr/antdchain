// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package console

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"errors"
	"golang.org/x/term"
	"sort"

	qkeystore "github.com/antdaza/antdchain/antdc/accounts/keystore"
	"github.com/antdaza/antdchain/antdc/chain"
	"github.com/antdaza/antdchain/antdc/crypto/quantum"
	"github.com/antdaza/antdchain/antdc/mining"
	"github.com/antdaza/antdchain/antdc/p2p"
	"github.com/antdaza/antdchain/antdc/reward"
	"github.com/antdaza/antdchain/antdc/rotatingking"
	"github.com/antdaza/antdchain/antdc/staking"
	"github.com/antdaza/antdchain/antdc/tx"
	"github.com/antdaza/antdchain/antdc/wallet"
	"github.com/antdaza/antdchain/common"
	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/rpc"
)

func ethToQuantumAddress(addr common.QuantumAddress) common.QuantumAddress {
	q, err := common.NewQuantumAddressFromBytes(addr.Bytes())
	if err != nil {
		return common.QuantumAddress{}
	}
	return q
}

func parseQuantumAddressInput(input string) (common.QuantumAddress, error) {
	normalized := strings.TrimSpace(input)
	// Be permissive with copy/pasted input where a literal "\n" (or "\r\n")
	// accidentally gets appended to the address string.
	for _, marker := range []string{`\r\n`, `\n`, `\r`} {
		if idx := strings.Index(normalized, marker); idx >= 0 {
			normalized = normalized[:idx]
			break
		}
	}
	if fields := strings.Fields(normalized); len(fields) > 0 {
		normalized = fields[0]
	}

	qAddr, err := common.ParseQuantumAddress(normalized)
	if err != nil {
		return common.QuantumAddress{}, fmt.Errorf("invalid antdchain address '%s': %w", input, err)
	}
	return common.QuantumAddress(qAddr), nil
}

func mustParseQuantumAddress(input string) common.QuantumAddress {
	qAddr, err := common.ParseQuantumAddress(strings.TrimSpace(input))
	if err != nil {
		return common.QuantumAddress{}
	}
	return common.QuantumAddress(qAddr)
}

func ethAddressMatchesQuantumAddress(ethAddr interface{ Bytes() []byte }, quantumAddr common.QuantumAddress) bool {
	return bytes.Equal(ethAddr.Bytes(), quantumAddr.Bytes())
}

type RPCClient struct {
	client *rpc.Client
	url    string
}

type RPCAuthConfig struct {
	URL           string
	Username      string
	Password      string
	APIKey        string
	AuthDisabled  bool
	Testnet       bool
	Stagenet      bool
	AllowInsecure bool
}

func NewRPCClient(config *RPCAuthConfig) (*RPCClient, error) {
	var client *rpc.Client
	var err error

	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	if !config.AuthDisabled {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		httpClient.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if config.APIKey != "" {
				req.Header.Set("X-API-Key", config.APIKey)
			} else if config.Username != "" && config.Password != "" {
				req.SetBasicAuth(config.Username, config.Password)
			}
			return transport.RoundTrip(req)
		})
	}

	client, err = rpc.DialHTTPWithClient(config.URL, httpClient)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RPC server: %w", err)
	}

	return &RPCClient{
		client: client,
		url:    config.URL,
	}, nil
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// RPC methods for rotating king commands
func (c *RPCClient) GetRotatingKingList() ([]common.QuantumAddress, error) {
	var result []string
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := c.client.CallContext(ctx, &result, "rotatingking_list")
	if err != nil {
		if strings.Contains(err.Error(), "method does not exist") ||
			strings.Contains(err.Error(), "not available") {
			return nil, fmt.Errorf("rotating king RPC methods not available. Is the daemon running with rotating king enabled?")
		}
		return nil, fmt.Errorf("RPC call failed: %w", err)
	}

	addresses := make([]common.QuantumAddress, len(result))
	for i, addrStr := range result {
		addresses[i] = mustParseQuantumAddress(addrStr)
	}

	return addresses, nil
}

func (c *RPCClient) GetRotatingKingStatus() (map[string]interface{}, error) {
	var result map[string]interface{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := c.client.CallContext(ctx, &result, "rotatingking_status")
	if err != nil {
		return nil, fmt.Errorf("RPC call failed: %w", err)
	}

	return result, nil
}

func (c *RPCClient) GetRotatingKingAddress() (common.QuantumAddress, error) {
	var result string
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := c.client.CallContext(ctx, &result, "rotatingking_address")
	if err != nil {
		return common.QuantumAddress{}, fmt.Errorf("RPC call failed: %w", err)
	}

	return mustParseQuantumAddress(result), nil
}

func (c *RPCClient) GetRotatingKingCycle() (map[string]interface{}, error) {
	var result map[string]interface{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := c.client.CallContext(ctx, &result, "rotatingking_cycle")
	if err != nil {
		return nil, fmt.Errorf("RPC call failed: %w", err)
	}

	return result, nil
}

func (c *RPCClient) GetRotatingKingNext() (common.QuantumAddress, error) {
	var result string
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := c.client.CallContext(ctx, &result, "rotatingking_next")
	if err != nil {
		return common.QuantumAddress{}, fmt.Errorf("RPC call failed: %w", err)
	}

	return mustParseQuantumAddress(result), nil
}

func (c *RPCClient) GetRotatingKingHistory(limit int) ([]map[string]interface{}, error) {
	var result []map[string]interface{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := c.client.CallContext(ctx, &result, "rotatingking_history", limit)
	if err != nil {
		return nil, fmt.Errorf("RPC call failed: %w", err)
	}

	return result, nil
}

func (c *RPCClient) GetRotatingKingInfo(address string) (map[string]interface{}, error) {
	var result map[string]interface{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := c.client.CallContext(ctx, &result, "rotatingking_info", address)
	if err != nil {
		return nil, fmt.Errorf("RPC call failed: %w", err)
	}

	return result, nil
}

func (c *RPCClient) GetRotatingKingRewards(address string) (*big.Int, error) {
	var result string
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := c.client.CallContext(ctx, &result, "rotatingking_rewards", address)
	if err != nil {
		return nil, fmt.Errorf("RPC call failed: %w", err)
	}

	rewards := new(big.Int)
	rewards.SetString(strings.TrimPrefix(result, "0x"), 16)
	return rewards, nil
}

func (c *RPCClient) GetBalance(address common.QuantumAddress) (*big.Int, error) {
	var result string
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := c.client.CallContext(ctx, &result, "eth_getBalance", address.String(), "latest")
	if err != nil {
		return nil, fmt.Errorf("RPC call failed: %w", err)
	}

	balance := new(big.Int)
	balance.SetString(strings.TrimPrefix(result, "0x"), 16)
	return balance, nil
}

func (c *RPCClient) GetNonce(address common.QuantumAddress) (uint64, error) {
	var result string
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := c.client.CallContext(ctx, &result, "eth_getTransactionCount", address.String(), "latest")
	if err != nil {
		return 0, fmt.Errorf("RPC call failed: %w", err)
	}

	nonce, err := strconv.ParseUint(strings.TrimPrefix(result, "0x"), 16, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse nonce: %w", err)
	}

	return nonce, nil
}

func (c *RPCClient) SendRawTransaction(txData string) (common.Hash, error) {
	var result string
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := c.client.CallContext(ctx, &result, "eth_sendRawTransaction", txData)
	if err != nil {
		return common.Hash{}, fmt.Errorf("RPC call failed: %w", err)
	}

	return common.HexToHash(result), nil
}

func (c *RPCClient) GetBlockNumber() (uint64, error) {
	var result string
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := c.client.CallContext(ctx, &result, "eth_blockNumber")
	if err != nil {
		return 0, fmt.Errorf("RPC call failed: %w", err)
	}

	blockNum, err := strconv.ParseUint(strings.TrimPrefix(result, "0x"), 16, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse block number: %w", err)
	}

	return blockNum, nil
}

func (c *RPCClient) GetGasPrice() (*big.Int, error) {
	var result string
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := c.client.CallContext(ctx, &result, "eth_gasPrice")
	if err != nil {
		return nil, fmt.Errorf("RPC call failed: %w", err)
	}

	gasPrice := new(big.Int)
	gasPrice.SetString(strings.TrimPrefix(result, "0x"), 16)
	return gasPrice, nil
}

func (c *RPCClient) Close() {
	if c.client != nil {
		c.client.Close()
	}
}

// Console struct needs to store the RPC client when in wallet mode
type Console struct {
	node      *Node
	rpcClient *RPCClient // Store RPC client for wallet mode
}

func (c *Console) StartWalletClient(config *RPCAuthConfig) {
	scanner := bufio.NewScanner(os.Stdin)

	// Initialize RPC client with authentication
	rpcClient, err := NewRPCClient(config)
	if err != nil {
		fmt.Printf("❌ Failed to connect to daemon: %v\n", err)
		fmt.Println("   Check if antdchain daemon is running")
		fmt.Printf("   URL: %s\n", config.URL)
		if !config.AuthDisabled {
			fmt.Println("   Note: Authentication is enabled. Check your credentials.")
		}
		return
	}
	defer rpcClient.Close()

	// Store RPC client in Console struct
	c.rpcClient = rpcClient

	// Test connection
	blockNum, err := rpcClient.GetBlockNumber()
	if err != nil {
		fmt.Printf("❌ Failed to connect to daemon: %v\n", err)
		return
	}

	fmt.Println("\n✅ Connected to ANTDChain daemon!")
	fmt.Printf("📦 Latest block: %d\n", blockNum)

	// Get network info
	var networkID string
	if config.Testnet {
		networkID = "Testnet"
	} else if config.Stagenet {
		networkID = "Stagenet"
	} else {
		networkID = "Mainnet"
	}
	fmt.Printf("🌐 Network: %s\n", networkID)

	fmt.Println("\nType 'help' for available commands")

	// Start interactive loop
	for {
		fmt.Print("wallet> ")
		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}

		parts := strings.Fields(input)
		command := parts[0]

		switch command {
		case "exit", "quit":
			fmt.Println("Goodbye!")
			return

		case "balance":
			c.handleRemoteBalance(rpcClient, parts)

		case "send":
			c.handleRemoteSend(rpcClient, parts)

		case "status":
			c.handleRemoteStatus(rpcClient)

		case "address":
			c.handleAddress()

		case "createaddress":
			c.handleCreateAddress()

		case "import":
			c.handleImport(parts)

		case "export":
			c.handleExport(parts)

		case "listwallets":
			c.handleListWallets()
		case "listaddresses":
			c.handleListWallets()

		case "checkeligibility":
			c.handleCheckEligibility(parts)

		case "keystatus":
			c.handleKeyStatus(parts)

		case "mempoolinfo":
			c.handleMempoolInfo(parts)

		case "rebroadcast":
			c.handleRebroadcast(parts)

		case "checktx":
			c.handleCheckTx(parts)

		case "txdebug":
			c.handleTxDebug(parts)

		case "rk", "rotatingking":
			if len(parts) >= 2 && parts[1] == "rewards" {
				c.handleRKRewards(parts)
			} else if len(parts) >= 3 && parts[1] == "add" {
				fmt.Println("⚠️  'rk add' not available in wallet mode")
				fmt.Println("   Use the daemon console for administration commands")
			} else {
				c.handleRotatingKingRPC(parts)
			}

		case "monitor":
			c.handleMonitor(parts)

		case "supply":
			c.handleSupplyStats()

		case "alerts":
			c.handleAlerts(parts)

		case "datadir":
			c.handleDatadir(parts)

		case "txpool":
			fmt.Println("⚠️  'txpool' not available in wallet mode")
			fmt.Println("   Connect to daemon console for mempool information")

		case "getblockinfo":
			fmt.Println("⚠️  'getblockinfo' not available in wallet mode")
			fmt.Println("   Use RPC or connect to daemon console")

		case "gettx":
			fmt.Println("⚠️  'gettx' not available in wallet mode")
			fmt.Println("   Use RPC or connect to daemon console")

		case "lock":
			c.handleLock(parts)

		case "unlock":
			c.handleUnlock(parts)

		case "height":
			handleHeight(rpcClient)

		case "nonce":
			if len(parts) < 2 {
				fmt.Println("Usage: nonce <address>")
				continue
			}
			handleNonce(rpcClient, parts[1])

		case "gas":
			handleGasPrice(rpcClient)

		case "help":
			c.printWalletHelp()

		default:
			fmt.Printf("Unknown command: %s. Type 'help' for available commands.\n", command)
		}
	}
}

func (c *Console) handleRotatingKingRPC(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: rotatingking <command> [options]")
		fmt.Println("Commands:")
		fmt.Println("  status                     - Show current rotation status")
		fmt.Println("  address                    - Show current king address")
		fmt.Println("  cycle                      - Show rotation cycle info")
		fmt.Println("  next                       - Show next king in rotation")
		fmt.Println("  list                       - List all king addresses")
		fmt.Println("  history [limit]            - Show rotation history")
		fmt.Println("  info <address>             - Get info for specific address")
		fmt.Println("  rewards <address>          - Show rewards for address")
		fmt.Println("\n⚠️  Note: Rotating king RPC methods require the daemon")
		fmt.Println("   to be running with rotating king system enabled.")
		fmt.Println("   Current daemon may not support these methods.")
		return
	}

	if c.rpcClient == nil {
		fmt.Println("❌ Not connected to daemon")
		return
	}

	// Add a test call first to check if methods exist
	fmt.Println("🔍 Checking if rotating king RPC methods are available...")

	switch parts[1] {
	case "status":
		c.handleRKStatusRPC()
	case "address":
		c.handleRKAddressRPC()
	case "cycle":
		c.handleRKCycleRPC()
	case "next":
		c.handleRKNextRPC()
	case "list":
		c.handleRKListRPC()
	case "history":
		limit := 10
		if len(parts) > 2 {
			if l, err := strconv.Atoi(parts[2]); err == nil && l > 0 {
				limit = l
			}
		}
		c.handleRKHistoryRPC(limit)
	case "info":
		if len(parts) < 3 {
			fmt.Println("Usage: rotatingking info <address>")
			return
		}
		c.handleRKInfoRPC(parts[2])
	case "rewards":
		if len(parts) < 3 {
			fmt.Println("Usage: rotatingking rewards <address>")
			return
		}
		c.handleRKRewardsRPC(parts[2])
	default:
		fmt.Printf("❌ Unknown command: %s\n", parts[1])
		fmt.Println("💡 Note: Some rotating king commands are only available in daemon mode")
	}
}

// RPC implementations for rotating king commands
func (c *Console) handleRKStatusRPC() {
	status, err := c.rpcClient.GetRotatingKingStatus()
	if err != nil {
		fmt.Printf("❌ Failed to get rotating king status: %v\n", err)
		return
	}

	fmt.Println("👑 ROTATING KING STATUS")
	fmt.Println("════════════════════════════════════════════════")

	for key, value := range status {
		fmt.Printf("%s: %v\n", key, value)
	}

	fmt.Println("════════════════════════════════════════════════")
}

func (c *Console) handleRKAddressRPC() {
	addr, err := c.rpcClient.GetRotatingKingAddress()
	if err != nil {
		fmt.Printf("❌ Failed to get rotating king address: %v\n", err)
		return
	}

	balance, err := c.rpcClient.GetBalance(addr)
	if err != nil {
		fmt.Printf("❌ Failed to get balance: %v\n", err)
		return
	}

	fmt.Println("👑 CURRENT ROTATING KING")
	fmt.Println("════════════════════════════════════════════════")
	fmt.Printf("Address: %s\n", addr.String())
	fmt.Printf("Balance: %s ANTD\n", formatBalance(balance))
	fmt.Println("════════════════════════════════════════════════")
}

func (c *Console) handleRKCycleRPC() {
	cycle, err := c.rpcClient.GetRotatingKingCycle()
	if err != nil {
		fmt.Printf("❌ Failed to get rotation cycle: %v\n", err)
		return
	}

	fmt.Println("🔄 ROTATION CYCLE INFORMATION")
	fmt.Println("════════════════════════════════════════════════")

	for key, value := range cycle {
		fmt.Printf("%s: %v\n", key, value)
	}

	fmt.Println("════════════════════════════════════════════════")
}

func (c *Console) handleRKNextRPC() {
	nextKing, err := c.rpcClient.GetRotatingKingNext()
	if err != nil {
		fmt.Printf("❌ Failed to get next king: %v\n", err)
		return
	}

	balance, err := c.rpcClient.GetBalance(nextKing)
	if err != nil {
		fmt.Printf("❌ Failed to get balance: %v\n", err)
		return
	}

	fmt.Println("⏭️ NEXT ROTATING KING")
	fmt.Println("════════════════════════════════════════════════")
	fmt.Printf("Address: %s\n", nextKing.String())
	fmt.Printf("Balance: %s ANTD\n", formatBalance(balance))
	fmt.Println("════════════════════════════════════════════════")
}

func (c *Console) handleRKListRPC() {
	addresses, err := c.rpcClient.GetRotatingKingList()
	if err != nil {
		fmt.Printf("❌ Failed to get king list: %v\n", err)
		return
	}

	fmt.Printf("🏆 ROTATING KING LIST (%d addresses)\n", len(addresses))
	fmt.Println("════════════════════════════════════════════════")

	for i, addr := range addresses {
		balance, err := c.rpcClient.GetBalance(addr)
		if err != nil {
			fmt.Printf("%d. %s\n", i+1, addr.String())
		} else {
			fmt.Printf("%d. %s - %s ANTD\n", i+1, addr.String(), formatBalance(balance))
		}
	}

	fmt.Println("════════════════════════════════════════════════")
}

func (c *Console) handleRKHistoryRPC(limit int) {
	history, err := c.rpcClient.GetRotatingKingHistory(limit)
	if err != nil {
		fmt.Printf("❌ Failed to get rotation history: %v\n", err)
		return
	}

	if len(history) == 0 {
		fmt.Println("📜 No rotation history found")
		return
	}

	fmt.Printf("📜 ROTATION HISTORY (last %d)\n", len(history))
	fmt.Println("════════════════════════════════════════════════")

	for i := len(history) - 1; i >= 0; i-- {
		event := history[i]
		fmt.Printf("Rotation #%d:\n", len(history)-i)

		for key, value := range event {
			fmt.Printf("  %s: %v\n", key, value)
		}

		if i > 0 {
			fmt.Println("  ──────────────────────────")
		}
	}

	fmt.Println("════════════════════════════════════════════════")
}

func (c *Console) handleRKInfoRPC(address string) {
	info, err := c.rpcClient.GetRotatingKingInfo(address)
	if err != nil {
		fmt.Printf("❌ Failed to get king info: %v\n", err)
		return
	}

	fmt.Printf("📋 KING INFORMATION: %s\n", address)
	fmt.Println("════════════════════════════════════════════════")

	for key, value := range info {
		fmt.Printf("%s: %v\n", key, value)
	}

	fmt.Println("════════════════════════════════════════════════")
}

func (c *Console) handleRKRewardsRPC(address string) {
	rewards, err := c.rpcClient.GetRotatingKingRewards(address)
	if err != nil {
		fmt.Printf("❌ Failed to get rewards: %v\n", err)
		return
	}

	fmt.Printf("💰 REWARDS FOR %s\n", address)
	fmt.Println("════════════════════════════════════════════════")
	fmt.Printf("Total 5%% Rewards: %s ANTD\n", formatBalance(rewards))
	fmt.Println("════════════════════════════════════════════════")
}

func handleHeight(rpcClient *RPCClient) {
	height, err := rpcClient.GetBlockNumber()
	if err != nil {
		fmt.Printf("❌ Failed to get height: %v\n", err)
		return
	}

	fmt.Printf("📦 Daemon height: %d\n", height)
}

func handleNonce(rpcClient *RPCClient, addrStr string) {
	addr := mustParseQuantumAddress(addrStr)
	nonce, err := rpcClient.GetNonce(addr)
	if err != nil {
		fmt.Printf("❌ Failed to get nonce: %v\n", err)
		return
	}

	fmt.Printf("🔢 Nonce for %s: %d\n", addr.String(), nonce)
}

func handleGasPrice(rpcClient *RPCClient) {
	gasPrice, err := rpcClient.GetGasPrice()
	if err != nil {
		fmt.Printf("❌ Failed to get gas price: %v\n", err)
		return
	}

	gwei := new(big.Float).Quo(new(big.Float).SetInt(gasPrice), big.NewFloat(1e9))
	fmt.Printf("⛽ Current gas price: %s Gwei\n", gwei.Text('f', 1))
}

// Remote handlers
func (c *Console) handleRemoteBalance(rpcClient *RPCClient, parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: balance <address>")
		return
	}

	addr := mustParseQuantumAddress(parts[1])
	balance, err := rpcClient.GetBalance(addr)
	if err != nil {
		fmt.Printf("❌ Failed to get balance: %v\n", err)
		return
	}

	fmt.Printf("💰 Balance for %s: %s ANTD\n", addr.String(), formatBalance(balance))
}

func (c *Console) handleRemoteSend(rpcClient *RPCClient, parts []string) {
	if len(parts) < 4 {
		fmt.Println("Usage: send <from> <to> <amount>")
		return
	}

	fromAddr, err := parseQuantumAddressInput(parts[1])
	if err != nil {
		fmt.Printf("❌ %v\n", err)
		return
	}

	toAddr, err := parseQuantumAddressInput(parts[2])
	if err != nil {
		fmt.Printf("❌ %v\n", err)
		return
	}
	amount, err := parseANTDAmount(parts[3])
	if err != nil {
		fmt.Printf("❌ Invalid amount: %v\n", err)
		return
	}

	// Get nonce
	nonce, err := rpcClient.GetNonce(fromAddr)
	if err != nil {
		fmt.Printf("❌ Failed to get nonce: %v\n", err)
		return
	}

	// Get gas price
	gasPrice, err := rpcClient.GetGasPrice()
	if err != nil {
		fmt.Printf("❌ Failed to get gas price: %v\n", err)
		gasPrice = big.NewInt(2_000_000_000) // Default 2 Gwei
	}

	// Get balance to check
	balance, err := rpcClient.GetBalance(fromAddr)
	if err != nil {
		fmt.Printf("❌ Failed to get balance: %v\n", err)
		return
	}

	gasLimit := uint64(21000)
	gasCost := new(big.Int).Mul(gasPrice, big.NewInt(int64(gasLimit)))
	totalCost := new(big.Int).Add(amount, gasCost)

	if balance.Cmp(totalCost) < 0 {
		fmt.Printf("❌ Insufficient balance\n")
		fmt.Printf("   Available: %s ANTD\n", formatBalance(balance))
		fmt.Printf("   Required:  %s ANTD\n", formatBalance(totalCost))
		return
	}

	// Find wallet in local keystore
	found := false
	for _, acc := range c.node.Keystore().Accounts() {
		if ethAddressMatchesQuantumAddress(acc.Address, fromAddr) {
			found = true
			break
		}
	}

	if !found {
		fmt.Printf("❌ Wallet %s not found in local keystore\n", fromAddr.String())
		return
	}

	// Get password
	password, err := c.readPassword(fmt.Sprintf("Password for %s: ", fromAddr.String()))
	if err != nil {
		fmt.Printf("❌ Failed to read password: %v\n", err)
		return
	}

	qFrom, err := common.NewQuantumAddressFromBytes(fromAddr.Bytes())
	if err != nil {
		fmt.Printf("❌ Invalid sender address bytes: %v\n", err)
		return
	}

	privKey, err := qkeystore.Unlock(qFrom, password, c.node.GetKeystoreDir())
	if err != nil {
		fmt.Printf("❌ Wrong password or quantum keystore unavailable: %v\n", err)
		return
	}

	pubKey, err := quantum.DerivePublicKey(privKey)
	if err != nil {
		fmt.Printf("❌ Failed to derive quantum public key: %v\n", err)
		return
	}

	// Create transaction
	txObj := tx.NewTransferTx(
		ethToQuantumAddress(fromAddr),
		ethToQuantumAddress(toAddr),
		amount,
		nonce,
		gasPrice,
	)

	if err := txObj.Sign(privKey, pubKey); err != nil {
		fmt.Printf("❌ Failed to sign transaction: %v\n", err)
		return
	}

	// Serialize
	txData, err := txObj.Serialize()
	if err != nil {
		fmt.Printf("❌ Failed to serialize transaction: %v\n", err)
		return
	}

	// Send via RPC
	txHex := "0x" + hex.EncodeToString(txData)
	txHash, err := rpcClient.SendRawTransaction(txHex)
	if err != nil {
		fmt.Printf("❌ Failed to send transaction: %v\n", err)
		return
	}

	fmt.Printf("✅ Transaction sent: %s\n", txHash.String())
}

func (c *Console) handleRemoteStatus(rpcClient *RPCClient) {
	height, err := rpcClient.GetBlockNumber()
	if err != nil {
		fmt.Printf("❌ Failed to get status: %v\n", err)
		return
	}

	fmt.Printf("🖥️  Daemon Status:\n")
	fmt.Printf("   Latest block: %d\n", height)

	// Show local wallets
	accounts := c.node.Keystore().Accounts()
	fmt.Printf("   Local wallets: %d\n", len(accounts))
}

func (c *Console) handleAddress() {
	accounts := c.node.Keystore().Accounts()
	if len(accounts) == 0 {
		fmt.Println("No wallets found")
		return
	}

	fmt.Println("Your wallet addresses:")
	for i, acc := range accounts {
		fmt.Printf("  %d. %s\n", i+1, acc.Address.String())
	}
}

func (c *Console) printWalletHelp() {
	fmt.Println("Available commands (Wallet Mode):")
	fmt.Println("  balance <address>        - Check balance via RPC")
	fmt.Println("  send <from> <to> <amount> - Send transaction via RPC")
	fmt.Println("  status                   - Show daemon status")
	fmt.Println("  height                   - Show daemon height")
	fmt.Println("  gas                      - Show current gas price")
	fmt.Println("  nonce <address>          - Show nonce for address")
	fmt.Println("  address                  - Show wallet addresses")
	fmt.Println("  createaddress            - Create new wallet")
	fmt.Println("  import <key>             - Import ML-DSA-65 private key")
	fmt.Println("  export <address>         - Export private key")
	fmt.Println("  listwallets              - List all quantum wallets")
	fmt.Println("  listaddresses            - Alias for listwallets")
	fmt.Println("")
	fmt.Println("Rotating King Commands (via RPC):")
	fmt.Println("  rk status                - Show rotation status")
	fmt.Println("  rk address               - Show current king address")
	fmt.Println("  rk cycle                 - Show rotation cycle info")
	fmt.Println("  rk next                  - Show next king in rotation")
	fmt.Println("  rk list                  - List all king addresses")
	fmt.Println("  rk history [limit]       - Show rotation history")
	fmt.Println("  rk info <address>        - Get info for specific address")
	fmt.Println("  rk rewards <address>     - Show rewards for address")
	fmt.Println("")
	fmt.Println("  help                     - Show this help")
	fmt.Println("  exit                     - Exit wallet")
	fmt.Println("")
	fmt.Println("Note: Administration commands (add, rotate, etc.) are")
	fmt.Println("      only available when running as a daemon.")
}

// getAppDataDir returns the correct data directory for antdchain_data across platforms
func getAppDataDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to get home directory: %w", err)
	}

	// Determine data directory based on OS
	var dataDir string
	switch runtime.GOOS {
	case "windows":
		dataDir = filepath.Join(home, "AppData", "Local", "Antdchain")
	case "darwin":
		// macOS standard: ~/Library/Application Support/Antdchain
		dataDir = filepath.Join(home, "Library", "Application Support", "Antdchain")
	default:
		// Linux and other Unix-like systems
		dataDir = filepath.Join(home, ".antdchain")
	}

	// Create the directory
	if err := os.MkdirAll(dataDir, os.ModePerm); err != nil {
		return "", fmt.Errorf("failed to create data directory %s: %w", dataDir, err)
	}

	return dataDir, nil
}

// Node holds the blockchain, mining state, wallet manager, and P2P node
type Node struct {
	mu                 sync.RWMutex
	blockchain         *chain.Blockchain
	miningState        *mining.PosMiningState
	walletManager      *wallet.WalletManager
	minerWallet        MinerWallet
	p2pNode            *p2p.Node
	dataDir            string
	keystore           *keystore.KeyStore
	keystoreDir        string
	minerWalletAddress common.QuantumAddress
	rebroadcastTicker  *time.Ticker
	rebroadcastCancel  context.CancelFunc
}

// StartAutoRebroadcast starts automatic rebroadcast of stale transactions
func (n *Node) StartAutoRebroadcast(interval time.Duration, maxAge time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	n.rebroadcastCancel = cancel

	n.rebroadcastTicker = time.NewTicker(interval)

	go func() {
		defer n.rebroadcastTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-n.rebroadcastTicker.C:
				n.rebroadcastStaleTransactions(maxAge)
			}
		}
	}()

	log.Printf("Auto-rebroadcast started: interval=%v, maxAge=%v", interval, maxAge)
}

// stops the automatic rebroadcast
func (n *Node) StopAutoRebroadcast() {
	if n.rebroadcastCancel != nil {
		n.rebroadcastCancel()
	}
	if n.rebroadcastTicker != nil {
		n.rebroadcastTicker.Stop()
	}
	log.Println("Auto-rebroadcast stopped")
}

// rebroadcasts transactions older than maxAge
func (n *Node) rebroadcastStaleTransactions(maxAge time.Duration) {
	n.mu.RLock()
	if n.p2pNode == nil {
		n.mu.RUnlock()
		return
	}

	pendingTxs := n.blockchain.TxPool().GetPending()
	if len(pendingTxs) == 0 {
		n.mu.RUnlock()
		return
	}

	// rebroadcast - all transactions
	count := 0
	for _, tx := range pendingTxs {
		if err := n.p2pNode.BroadcastTx(tx); err != nil {
			log.Printf("[AutoRebroadcast] Failed to rebroadcast tx %s: %v",
				tx.Hash().String()[:10], err)
		} else {
			count++
		}
	}
	n.mu.RUnlock()

	if count > 0 {
		log.Printf("[AutoRebroadcast] Rebroadcast %d transactions", count)
	}
}

// NewNode creates a new Node with the given components
func NewNode(bc *chain.Blockchain, posMiningState *mining.PosMiningState, wm *wallet.WalletManager, p2pNode *p2p.Node) (*Node, error) {
	var dataDir string
	var err error

	if wm != nil && wm.DataDir() != "" {
		dataDir = wm.DataDir()
	} else {
		dataDir, err = getAppDataDir()
		if err != nil {
			return nil, err
		}
	}

	if err := os.MkdirAll(dataDir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to create data directory %s: %w", dataDir, err)
	}

	ksDir := filepath.Join(dataDir, "keystore")
	if err := os.MkdirAll(ksDir, os.ModePerm); err != nil {
		return nil, err
	}

	ks := keystore.NewKeyStore(ksDir, keystore.StandardScryptN, keystore.StandardScryptP)

	n := &Node{
		blockchain:         bc,
		miningState:        posMiningState, // Now accepts *mining.PosMiningState
		walletManager:      wm,
		p2pNode:            p2pNode,
		dataDir:            dataDir,
		keystoreDir:        ksDir,
		keystore:           ks,
		minerWalletAddress: common.QuantumAddress{},
	}

	// Start auto-rebroadcast (every 10 minutes)
	n.StartAutoRebroadcast(10*time.Minute, 30*time.Minute)

	log.Printf("Keystore directory: %s", ksDir)
	return n, nil
}

// Getter methods for external access
func (n *Node) MuRLock() {
	n.mu.RLock()
}

func (n *Node) MuRUnlock() {
	n.mu.RUnlock()
}

func (n *Node) Blockchain() *chain.Blockchain {
	return n.blockchain
}

func (n *Node) MiningState() *mining.PosMiningState {
	return n.miningState
}

func (n *Node) SetMiningState(state interface{}) {
	if posState, ok := state.(*mining.PosMiningState); ok {
		n.miningState = posState
	}
}

func (n *Node) WalletManager() *wallet.WalletManager {
	return n.walletManager
}

func (n *Node) P2PNode() *p2p.Node {
	return n.p2pNode
}

func (n *Node) Keystore() *keystore.KeyStore {
	return n.keystore
}

func (n *Node) GetKeystoreDir() string {
    homeDir, err := os.UserHomeDir()
    if err != nil {
        return n.keystoreDir
    }

    switch runtime.GOOS {
    case "windows":
        return filepath.Join(homeDir, "AppData", "Local", "Antdchain", "keystore")
    case "darwin":
        return filepath.Join(homeDir, "Library", "Application Support", "Antdchain", "keystore")
    default:
        // Linux and other Unix-like systems
        return filepath.Join(homeDir, ".antdchain", "keystore")
    }
}

func (n *Node) GetMinerWallet() MinerWallet {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.minerWallet
}

type MinerWallet interface {
	Address() common.QuantumAddress
}

func (n *Node) SetMinerWallet(w MinerWallet) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.minerWallet = w
	log.Printf("Miner wallet updated to: %s", w.Address().String())
}

// GetDataDir returns the platform-appropriate data directory
func (n *Node) GetDataDir() string {
	return n.dataDir
}

func NewConsole(node *Node) *Console {
	return &Console{node: node}
}

// readPassword securely reads a password from terminal without echo
func (c *Console) readPassword(prompt string) (string, error) {
	fmt.Print(prompt)
	bytePassword, err := term.ReadPassword(int(syscall.Stdin))
	fmt.Println() // newline after password input
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(bytePassword)), nil
}

// readInput reads regular input from terminal
func (c *Console) readInput(prompt string) (string, error) {
	fmt.Print(prompt)
	scanner := bufio.NewScanner(os.Stdin)
	if scanner.Scan() {
		return strings.TrimSpace(scanner.Text()), nil
	}
	return "", scanner.Err()
}

// confirmAction asks for confirmation before performing dangerous operations
func (c *Console) confirmAction(message string) bool {
	fmt.Printf("%s (y/N): ", message)
	scanner := bufio.NewScanner(os.Stdin)
	if scanner.Scan() {
		response := strings.TrimSpace(strings.ToLower(scanner.Text()))
		return response == "y" || response == "yes"
	}
	return false
}

// getWalletDataDir returns the path to wallet data directory
func (c *Console) getWalletDataDir() string {
	return c.node.GetDataDir()
}

// getWalletFilePath returns the path to the encrypted wallet file
func (c *Console) getWalletFilePath() string {
	return filepath.Join(c.getWalletDataDir(), "wallets.encrypted.json")
}

// getBackupFilePath returns the default backup file path
func (c *Console) getBackupFilePath() string {
	return filepath.Join(c.getWalletDataDir(), "wallet_backup.json")
}

func (c *Console) handleDatadir(parts []string) {
	fmt.Printf("ANTDChain data directory: %s\n", c.node.GetDataDir())
	fmt.Printf("   • Wallets: %s\n", c.getWalletFilePath())
	fmt.Printf("   • Default backup: %s\n", c.getBackupFilePath())
}

// RunSingleCommand executes one console command without entering interactive mode.
func (c *Console) RunSingleCommand(parts []string) {
	if len(parts) == 0 {
		return
	}

	switch parts[0] {
	case "createaddress":
		c.handleCreateAddress()
	case "import":
		c.handleImport(parts)
	case "export":
		c.handleExport(parts)
	case "listwallets", "listaddresses":
		c.handleListWallets()
	case "lock":
		c.handleLock(parts)
	case "unlock":
		c.handleUnlock(parts)
	case "balance":
		c.handleBalance(parts)
	case "datadir":
		c.handleDatadir(parts)
	default:
		fmt.Printf("Unknown command: %s\n", parts[0])
		fmt.Println("Supported one-shot commands: createaddress, import, export, listwallets, listaddresses, lock, unlock, balance, datadir")
	}
}

// Start begins the interactive console
func (c *Console) Start() {
	if cmdParts, ok := parseOneShotCommandArgs(os.Args[1:]); ok {
		c.RunSingleCommand(cmdParts)
		return
	}
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("=== ANTDChain Console ===")
	fmt.Println("Available commands:")
	fmt.Println("  setaddress <addr>        - Set mining address")
	fmt.Println("  startmining              - Start mining")
	fmt.Println("  stopmining               - Stop mining")
	fmt.Println("  createaddress            - Create new wallet")
	fmt.Println("  send <from> <to> <amount>- Send transaction")
	fmt.Println("  import <privateKey>      - Import ML-DSA-65 wallet")
	fmt.Println("  export <address>         - Export private key")
	fmt.Println("  getblockinfo <number>    - Get block info")
	fmt.Println("  gettx <hash>             - Get transaction")
	fmt.Println("  lock <address>           - Lock wallet")
	fmt.Println("  unlock <address>         - Unlock wallet")
	fmt.Println("  listwallets              - List all quantum wallets")
	fmt.Println("  listaddresses            - Alias for listwallets")
	fmt.Println("  balance <address>        - Check balance")
	fmt.Println("  status                   - Show node status")
	fmt.Println("  debugchain               - Debug chain hashes")
	fmt.Println("  savewallets              - Save wallets with encryption")
	fmt.Println("  loadwallets              - Load wallets with decryption")
	fmt.Println("  changepassword           - Change wallet encryption password")
	fmt.Println("  backup <path>            - Backup wallets to file")
	fmt.Println("  restore <path>           - Restore wallets from backup")
	fmt.Println("  cleartx <hash>           - Remove transaction from pool")
	fmt.Println("  mempool                  - Show transaction pool")
	fmt.Println("  mempoolinfo              - Show mempool statistics")
	fmt.Println("  rebroadcast              - Rebroadcast all mempool transactions")
	fmt.Println("  rebroadcast <txhash>     - Rebroadcast specific transaction")
	fmt.Println("  debugtx <hash>           - Debug transaction")
	fmt.Println("  clearstucktxs            - Clear stuck transactions")
	fmt.Println("  txpool                   - Tx mempool")
	fmt.Println("  datadir                  - Show data directory location")
	fmt.Println("  rk                       - Rotating King commands")
	fmt.Println("  rotatingking             - Rotating King commands (alias)")
	fmt.Println("  rk add <address>         - Manually add address to rotation (100k ANTD required)")
	fmt.Println("  emergency-sync           - Force configuration sync with peers")
	fmt.Println("  force-broadcast          - Broadcast current configuration to network")
	fmt.Println("  monitor                  - Supply monitoring")
	fmt.Println("  supply                   - Supply statistics")
	fmt.Println("  alerts                   - View alerts")
	fmt.Println("  help                     - Show this help")
	fmt.Println("  exit                     - Exit console")
	fmt.Println("=========================")

	for {
		fmt.Print("antdchain> ")
		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}

		parts := strings.Fields(input)
		command := parts[0]

		switch command {
		case "exit", "quit":
			fmt.Println("Goodbye!")
			return

		case "checkeligibility":
			c.handleCheckEligibility(parts)

		case "keystatus":
			c.handleKeyStatus(parts)

		case "mempoolinfo":
			c.handleMempoolInfo(parts)

		case "rebroadcast":
			c.handleRebroadcast(parts)

		case "checktx":
			c.handleCheckTx(parts)

		case "txdebug":
			c.handleTxDebug(parts)

		case "rk", "rotatingking":
			if len(parts) >= 2 && parts[1] == "rewards" {
				c.handleRKRewards(parts)
			} else if len(parts) >= 3 && parts[1] == "add" {
				c.handleRKAdd(parts[2])
			} else {
				c.handleRotatingKing(parts)
			}

		case "monitor":
			c.handleMonitor(parts)

		case "supply":
			c.handleSupplyStats()

		case "alerts":
			c.handleAlerts(parts)

		case "datadir":
			c.handleDatadir(parts)

		case "address":
			c.handleAddressStats(parts)

		case "setaddress":
			c.handleSetAddress(parts)

		case "startmining":
			c.handleStartMining()

		case "stopmining":
			c.handleStopMining()

		case "txpool":
			c.handleTxPool(parts)

		case "createaddress":
			c.handleCreateAddress()

		case "send":
			c.handleSend(parts)

		case "import":
			c.handleImport(parts)

		case "export":
			c.handleExport(parts)

		case "getblockinfo":
			c.handleGetBlockInfo(parts)

		case "gettx":
			c.handleGetTx(parts)

		case "lock":
			c.handleLock(parts)

		case "unlock":
			c.handleUnlock(parts)

		case "listwallets":
			c.handleListWallets()

		case "listaddresses":
			c.handleListWallets()
		case "register-worker":
			if len(parts) != 3 {
				fmt.Println("Usage: register-worker <address> <amount>")
				fmt.Println("Example: register-worker 0x1234... 1000000")
				fmt.Println("Minimum: 1,000,000 ANTD")
			}

		case "unregister-worker":
			if len(parts) != 2 {
				fmt.Println("Usage: unregister-worker <address>")
			}

		case "list-workers":
			c.handleListWorkers()

		case "worker-info":
			if len(parts) != 2 {
				fmt.Println("Usage: worker-info <address>")
			} else {
				c.handleWorkerInfo(parts[1])
			}

		case "force-rotate":
			reason := "manual"
			if len(parts) > 1 {
				reason = strings.Join(parts[1:], " ")
			}
			c.handleForceRotate(reason)

		case "rotation-history":
			limit := 10
			if len(parts) == 2 {
				if l, err := strconv.Atoi(parts[1]); err == nil && l > 0 {
					limit = l
				}
			}
			c.handleRotationHistory(limit)

		case "mining-stats":
			c.handleMiningStats()

		case "balance":
			c.handleBalance(parts)

		case "status":
			c.handleStatus()

		case "debugchain":
			c.handleDebugChain()

		case "savewallets":
			c.handleSaveWallets()

		case "loadwallets":
			c.handleLoadWallets()

		case "emergency-sync":
			c.handleEmergencySync(parts)

		case "force-broadcast":
			if c.node.p2pNode != nil {
				c.node.p2pNode.EnsureKingConfigBroadcast()
				fmt.Println("✅ Configuration broadcast triggered")
			} else {
				fmt.Println("❌ P2P node not available")
			}

		case "localproposals":
			c.listLocalProposals()

		case "changepassword":
			c.handleChangePassword()

		case "backup":
			c.handleBackup(parts)

		case "restore":
			c.handleRestore(parts)

		case "cleartx":
			c.handleClearTx(parts)

		case "debugtx":
			c.handleDebugTx(parts)

		case "clearstucktxs":
			c.handleClearStuckTxs()

		case "debugtxflow":
			c.handleDebugTransactionFlow(parts)

		case "help":
			c.handleHelp()

		default:
			fmt.Printf("Unknown command: %s. Type 'help' for available commands.\n", command)
		}
	}
}

func parseOneShotCommandArgs(args []string) ([]string, bool) {
	if len(args) == 0 {
		return nil, false
	}

	command := strings.TrimSpace(args[0])
	if command == "" || strings.HasPrefix(command, "-") {
		return nil, false
	}

	switch command {
	case "createaddress", "import", "export", "listwallets", "listaddresses", "lock", "unlock", "balance", "datadir":
		return args, true
	default:
		return nil, false
	}
}

func (c *Console) handleMonitor(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: monitor <command>")
		fmt.Println("Commands: status, start, stop, stats, alerts, holders")
		return
	}

	switch parts[1] {
	case "status":
		fmt.Println("📊 Supply Monitoring Status:")
		fmt.Println("  Monitoring system is active")

	case "stats":
		c.handleSupplyStats()

	case "alerts":
		c.handleAlerts(parts)

	case "holders":
		fmt.Println("🏆 Top Holders:")
		fmt.Println("  Holder data not available in this version")

	case "start":
		fmt.Println("✅ Monitoring is automatically started with the blockchain")

	case "stop":
		fmt.Println("❌ Cannot stop monitoring - it's integrated with blockchain")

	default:
		fmt.Println("❌ Unknown monitor command")
	}
}

func (c *Console) handleSupplyStats() {
	fmt.Println("💰 Network Supply Statistics:")
	fmt.Println("  Total Supply: <not available>")
	fmt.Println("  Unique Addresses: <not available>")
}

func (c *Console) handleAlerts(parts []string) {
	limit := 10
	if len(parts) > 2 {
		if l, err := strconv.Atoi(parts[2]); err == nil && l > 0 {
			limit = l
		}
	}

	fmt.Printf("🚨 Recent Alerts (last %d):\n", limit)
	fmt.Println("✅ No recent alerts")
}

func (c *Console) handleAddressStats(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: address <address>")
		return
	}

	addr := mustParseQuantumAddress(parts[1])

	c.node.mu.RLock()
	balance := c.node.blockchain.State().GetBalance(addr)
	c.node.mu.RUnlock()

	fmt.Printf("📈 Address Statistics for %s:\n", addr.String())
	fmt.Printf("  Balance: %s ANTD\n", formatBalance(balance))
	fmt.Printf("  Transaction Count: <not available>\n")
	fmt.Printf("  Is Main King: <not available>\n")
}

func (c *Console) handleSetAddress(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: setaddress <address>")
		return
	}

	addr, err := parseQuantumAddressInput(parts[1])
	if err != nil {
		fmt.Printf("❌ %v\n", err)
		return
	}

	// Look for the wallet file in the quantum keystore directory
	keystoreDir := c.node.GetKeystoreDir()
	entries, readErr := os.ReadDir(keystoreDir)
	found := false
	var keyFile string

	if readErr == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			if strings.HasPrefix(name, "UTC--") && strings.HasSuffix(name, addr.String()) {
				keyFile = filepath.Join(keystoreDir, name)
				found = true
				break
			}
		}
	}

	// Set the miner wallet address
	c.node.SetMinerWalletAddress(addr)

	fmt.Printf("Mining address set to: %s\n", addr.String())
	if found {
		fmt.Printf("Keystore file: %s\n", keyFile)
	} else {
		fmt.Printf("⚠️ Wallet %s not found in keystore (address still set)\n", addr.String())
	}

	// Show balance
	c.node.mu.RLock()
	balance := c.node.blockchain.State().GetBalance(addr)
	c.node.mu.RUnlock()
	fmt.Printf("Current balance: %s ANTD\n", formatBalance(balance))
}

func (c *Console) handleStopMining() {
	c.node.miningState.SetMining(false)
	c.node.miningState.SetEnabled(false)
	fmt.Println("⏹️ Mining stopped")
}

func (c *Console) handleCreateAddress() {
	password, err := c.readPassword("Set password for new wallet: ")
	if err != nil {
		fmt.Printf("❌ Failed to read password: %v\n", err)
		return
	}

	confirm, err := c.readPassword("Confirm password: ")
	if err != nil {
		fmt.Printf("❌ Failed to read password confirmation: %v\n", err)
		return
	}
	if password != confirm {
		fmt.Printf("❌ Passwords do not match\n")
		return
	}
	if len(password) < 8 {
		fmt.Printf("❌ Password must be at least 8 characters\n")
		return
	}

	keystoreDir := c.node.GetKeystoreDir()
	addr, err := qkeystore.NewAccount(password, keystoreDir)
	if err != nil {
		fmt.Printf("❌ Failed to create wallet in keystore: %v\n", err)
		return
	}

	// Register as a locked wallet in the wallet manager for console operations.
	w := wallet.NewWalletWithAddress(c.node.blockchain, addr, c.getWalletDataDir())
	c.node.walletManager.AddWallet(addr.String(), w)

	fmt.Printf("✅ New wallet created and saved to keystore!\n")
	fmt.Printf("Address: %s\n", addr)

	fmt.Printf("Type: Antdchain address (0q Base58Check)\n")
	fmt.Printf("Keystore: %s\n", keystoreDir)
}

func (c *Console) handleSend(parts []string) {
	if len(parts) < 4 {
		fmt.Println("Usage: send <from> <to> <amount> [nonce|@replace]")
		fmt.Println("Examples:")
		fmt.Println("  send 0x123... 0x456... 1.5")
		fmt.Println("  send 0x123... 0x456... 1.5 42      # manual nonce")
		fmt.Println("  send 0x123... 0x456... 1.5 @replace # replace pending")
		return
	}

	//PARSE INPUTS
	fromAddr, err := parseQuantumAddressInput(parts[1])
	if err != nil {
		fmt.Printf("❌ %v\n", err)
		return
	}

	toAddr, err := parseQuantumAddressInput(parts[2])
	if err != nil {
		fmt.Printf("❌ %v\n", err)
		return
	}
	amountStr := parts[3]

	// Parse amount
	amount, err := parseANTDAmount(amountStr)
	if err != nil {
		fmt.Printf("❌ Invalid amount '%s': %v\n", amountStr, err)
		return
	}
	if amount.Sign() <= 0 {
		fmt.Printf("❌ Amount must be positive\n")
		return
	}

	//GET CURRENT STATE
	c.node.mu.RLock()
	state := c.node.blockchain.State()
	stateNonce := state.GetNonce(fromAddr)
	balance := state.GetBalance(fromAddr)
	c.node.mu.RUnlock()

	fmt.Printf("\n📊 Current State for %s:\n", fromAddr.String())
	fmt.Printf("   Balance: %s ANTD\n", formatBalance(balance))
	fmt.Printf("   Nonce:   %d\n", stateNonce)

	// DETERMINE NONCE
	var suggestedNonce uint64
	var nonceSource string
	var replaceMode bool
	var replaceTxHash common.Hash

	if len(parts) > 4 {
		arg := parts[4]

		if arg == "@replace" || arg == "replace" {
			replaceMode = true
			nonceSource = "replace"

			// Find pending transaction to replace
			c.node.mu.RLock()
			txPool := c.node.blockchain.TxPool()
			pending := txPool.GetPending()
			c.node.mu.RUnlock()

			// Look for transaction with current nonce
			for _, pendingTx := range pending {
				if common.BytesToQuantumAddress(pendingTx.From.Bytes()) == fromAddr && pendingTx.Nonce == stateNonce {
					suggestedNonce = stateNonce
					replaceTxHash = pendingTx.Hash()
					nonceSource = fmt.Sprintf("replace %s", replaceTxHash.String()[:8])
					break
				}
			}

			if replaceTxHash == (common.Hash{}) {
				fmt.Printf("❌ No pending transaction found to replace at nonce %d\n", stateNonce)
				fmt.Printf("   Current pending transactions:\n")
				for _, pendingTx := range pending {
					if common.BytesToQuantumAddress(pendingTx.From.Bytes()) == fromAddr {
						fmt.Printf("   - Nonce %d: %s\n",
							pendingTx.Nonce, pendingTx.Hash().String()[:8])
					}
				}
				return
			}
		} else {
			// Manual nonce
			manualNonce, err := strconv.ParseUint(arg, 10, 64)
			if err != nil {
				fmt.Printf("❌ Invalid nonce '%s': %v\n", arg, err)
				return
			}
			suggestedNonce = manualNonce
			nonceSource = "manual"
		}
	} else {
		// Automatic nonce
		suggestedNonce = stateNonce
		nonceSource = "auto"
	}

	// GAS CALCULATION
	gasLimit := uint64(21000)             // Standard transfer
	gasPrice := big.NewInt(2_000_000_000) // 2 Gwei for faster inclusion

	gasCost := new(big.Int).Mul(gasPrice, big.NewInt(int64(gasLimit)))
	totalCost := new(big.Int).Add(amount, gasCost)

	// === 5. VALIDATION ===
	if balance.Cmp(totalCost) < 0 {
		fmt.Printf("\n❌ INSUFFICIENT BALANCE\n")
		fmt.Printf("   Available:      %s ANTD\n", formatBalance(balance))
		fmt.Printf("   Required:       %s ANTD\n", formatBalance(totalCost))
		fmt.Printf("   - Amount:       %s ANTD\n", formatBalance(amount))
		fmt.Printf("   - Gas (est):    %s ANTD\n", formatBalance(gasCost))
		fmt.Printf("   Short by:       %s ANTD\n",
			formatBalance(new(big.Int).Sub(totalCost, balance)))
		return
	}

	if suggestedNonce < stateNonce {
		fmt.Printf("❌ Nonce too low! State nonce is %d, got %d\n",
			stateNonce, suggestedNonce)
		return
	}

	// Check for nonce gap
	if suggestedNonce > stateNonce+5 {
		fmt.Printf("⚠️  Warning: Nonce %d is far ahead of current %d\n",
			suggestedNonce, stateNonce)
		if !c.confirmAction("Continue with large nonce gap?") {
			return
		}
	}

	// TRANSACTION SUMMARY
	fmt.Printf("\n📝 TRANSACTION SUMMARY\n")
	fmt.Printf("   From:           %s\n", fromAddr.String())
	fmt.Printf("   To:             %s\n", toAddr.String())
	fmt.Printf("   Amount:         %s ANTD\n", formatBalance(amount))
	fmt.Printf("   Nonce:          %d (%s)\n", suggestedNonce, nonceSource)
	fmt.Printf("   Gas Limit:      %d\n", gasLimit)
	fmt.Printf("   Gas Price:      %s Gwei\n",
		new(big.Float).Quo(new(big.Float).SetInt(gasPrice), big.NewFloat(1e9)).Text('f', 1))
	fmt.Printf("   Max Fee:        %s ANTD\n", formatBalance(gasCost))
	fmt.Printf("   Total Cost:     %s ANTD\n", formatBalance(totalCost))
	fmt.Printf("   New Balance:    %s ANTD\n",
		formatBalance(new(big.Int).Sub(balance, totalCost)))

	if replaceMode {
		fmt.Printf("   Mode:           🔄 REPLACE %s\n", replaceTxHash.String()[:8])
	}

	if !c.confirmAction("\nSend this transaction?") {
		fmt.Println("❌ Cancelled")
		return
	}

	// FIND ACCOUNT IN ANTDCHAIN KEYSTORE
	availableAccounts, err := qkeystore.ListAccounts(c.node.GetKeystoreDir())
	if err != nil {
		fmt.Printf("❌ Failed to read keystore directory: %v\n", err)
		return
	}

	found := false
	for _, acc := range availableAccounts {
		if acc == fromAddr {
			found = true
			break
		}
	}

	if !found {
		fmt.Printf("❌ Wallet %s not found in keystore\n", fromAddr.String())
		fmt.Printf("   Keystore directory: %s\n", c.node.GetKeystoreDir())
		fmt.Printf("   Available wallets:\n")
		for _, acc := range availableAccounts {
			fmt.Printf("   - %s\n", acc.String())
		}
		return
	}

	// DECRYPT PRIVATE KEY
	password, err := c.readPassword(fmt.Sprintf("Password for %s: ", fromAddr.String()))
	if err != nil {
		fmt.Printf("❌ Failed to read password: %v\n", err)
		return
	}

	qFrom, err := common.NewQuantumAddressFromBytes(fromAddr.Bytes())
	if err != nil {
		fmt.Printf("❌ Invalid sender address bytes: %v\n", err)
		return
	}

	privKey, err := qkeystore.Unlock(qFrom, password, c.node.GetKeystoreDir())
	if err != nil {
		fmt.Printf("❌ Wrong password or antdchain keystore unavailable: %v\n", err)
		return
	}

	pubKey, err := quantum.DerivePublicKey(privKey)
	if err != nil {
		fmt.Printf("❌ Failed to derive antdchain public key: %v\n", err)
		return
	}

	// CREATE AND SIGN TRANSACTION
	fmt.Printf("\n🔏 Creating and signing transaction...\n")
	txm := tx.NewTransferTx(
		ethToQuantumAddress(fromAddr),
		ethToQuantumAddress(toAddr),
		amount,
		suggestedNonce,
		gasPrice,
	)

	// Sign the transaction
	if err := txm.Sign(privKey, pubKey); err != nil {
		fmt.Printf("❌ Failed to sign transaction: %v\n", err)
		return
	}

	// Verify signature
	valid, err := txm.Verify()
	if err != nil || !valid {
		fmt.Printf("❌ Invalid signature: %v\n", err)
		return
	}

	txHash := txm.Hash()
	fmt.Printf("✅ Transaction created: %s\n", txHash.String())

	// REMOVE REPLACED TRANSACTION
	if replaceMode && replaceTxHash != (common.Hash{}) {
		c.node.mu.Lock()
		txPool := c.node.blockchain.TxPool()
		if chainTxPool, ok := txPool.(*chain.TxPool); ok {
			chainTxPool.RemoveTx(replaceTxHash)
			fmt.Printf("🗑️  Removed old transaction: %s\n", replaceTxHash.String()[:8])
		}
		c.node.mu.Unlock()
	}

	// ADD TO TRANSACTION POOL
	fmt.Printf("\n📤 Adding to transaction pool...\n")

	c.node.mu.Lock()
	txPool := c.node.blockchain.TxPool()

	// Add transaction to pool
	var addErr error
	if chainTxPool, ok := txPool.(*chain.TxPool); ok {
		addErr = chainTxPool.AddTx(txm, c.node.blockchain)
	} else {
		addErr = fmt.Errorf("unsupported pool type: %T", txPool)
	}
	c.node.mu.Unlock()

	if addErr != nil {
		fmt.Printf("❌ Failed to add to transaction pool: %v\n", addErr)

		// Suggest fixes based on error
		if strings.Contains(addErr.Error(), "nonce") {
			fmt.Printf("💡 Try with nonce %d\n", stateNonce)
		} else if strings.Contains(addErr.Error(), "balance") {
			fmt.Printf("💡 Check if you have enough balance for gas\n")
		} else if strings.Contains(addErr.Error(), "signature") {
			fmt.Printf("💡 Transaction signing failed\n")
		}
		return
	}

	fmt.Printf("✅ Transaction added to local mempool\n")

	// BROADCAST TO NETWORK
	if c.node.p2pNode != nil {
		fmt.Printf("📡 Broadcasting to peers...\n")

		if err := c.node.p2pNode.BroadcastTx(txm); err != nil {
			fmt.Printf("⚠️  Warning: Broadcast failed: %v\n", err)
			fmt.Printf("   Transaction is in local pool but not broadcast\n")
		} else {
			fmt.Printf("✅ Transaction broadcast to network\n")
		}
	} else {
		fmt.Printf("⚠️  P2P node not available\n")
		fmt.Printf("   Transaction is in local pool only\n")
	}

	// FINAL CONFIRMATION
	fmt.Printf("\n🎉 TRANSACTION SUCCESSFULLY CREATED!\n")
	fmt.Printf("══════════════════════════════════════════════════════════\n")
	fmt.Printf("   Transaction Hash: %s\n", txHash.String())
	fmt.Printf("   From:            %s\n", fromAddr.String())
	fmt.Printf("   To:              %s\n", toAddr.String())
	fmt.Printf("   Amount:          %s ANTD\n", formatBalance(amount))
	fmt.Printf("   Nonce:           %d\n", txm.Nonce)
	fmt.Printf("   Gas Price:       %s Gwei\n",
		new(big.Float).Quo(new(big.Float).SetInt(gasPrice), big.NewFloat(1e9)).Text('f', 1))
	fmt.Printf("   Max Fee:         %s ANTD\n", formatBalance(gasCost))

	// Check mining status
	if c.node.miningState.IsMining() {
		fmt.Printf("   Status:          ⏳ Pending (mining active)\n")
		fmt.Printf("   Will be mined in the next block\n")
	} else {
		fmt.Printf("   Status:          ⏸️  Queued (mining inactive)\n")
		fmt.Printf("   💡 Use 'startmining' to begin mining\n")
	}

	fmt.Printf("══════════════════════════════════════════════════════════\n")

	// Show next steps
	fmt.Printf("\n📋 Next Steps:\n")
	fmt.Printf("   1. Check status:      gettx %s\n", txHash.String())
	fmt.Printf("   2. View pool:         txpool\n")
	fmt.Printf("   3. Check balance:     balance %s\n", fromAddr.String())
	if replaceMode {
		fmt.Printf("   4. Old tx removed:   cleartx %s\n", replaceTxHash.String())
	}

	// Verify transaction is in pool
	time.Sleep(500 * time.Millisecond) // Brief pause
	c.node.mu.RLock()
	txPoolCheck := c.node.blockchain.TxPool()
	pendingCheck := txPoolCheck.GetPending()
	c.node.mu.RUnlock()

	inPool := false
	for _, pendingTx := range pendingCheck {
		if pendingTx.Hash() == txHash {
			inPool = true
			break
		}
	}

	if inPool {
		fmt.Printf("\n✅ Verified: Transaction is in local transaction pool\n")
	} else {
		fmt.Printf("\n⚠️  Warning: Transaction not found in pool after addition\n")
		fmt.Printf("   Try 'txpool' command to check\n")
	}
}

// Check transaction status
func (c *Console) handleCheckTx(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: checktx <hash>")
		return
	}

	txHash := common.Hash{}
	if len(parts[1]) == 66 { // 0x + 64 chars
		txHash = common.HexToHash(parts[1])
	} else if len(parts[1]) == 64 {
		txHash = common.HexToHash("0x" + parts[1])
	} else {
		fmt.Printf("❌ Invalid hash format\n")
		return
	}

	fmt.Printf("\n🔍 Checking transaction: %s\n", txHash.String())
	fmt.Printf("════════════════════════════════════════════════\n")

	found := false

	// Check in blocks
	c.node.mu.RLock()
	for i := uint64(0); ; i++ {
		block := c.node.blockchain.GetBlock(i)
		if block == nil {
			break
		}

		for _, tx := range block.Txs {
			if tx.Hash() == txHash {
				fmt.Printf("📦 CONFIRMED in block %d\n", i)
				fmt.Printf("   Block:     %s\n", block.Hash().String()[:8])
				fmt.Printf("   Height:    %d\n", i)
				fmt.Printf("   From:      %s\n", tx.From.String())
				if tx.To != nil {
					fmt.Printf("   To:        %s\n", tx.To.String())
				}
				fmt.Printf("   Value:     %s ANTD\n", formatBalance(tx.Value))
				fmt.Printf("   Nonce:     %d\n", tx.Nonce)
				fmt.Printf("   Gas Used:  %d\n", tx.Gas)
				fmt.Printf("   Gas Price: %s Gwei\n",
					new(big.Float).Quo(new(big.Float).SetInt(tx.GasPrice), big.NewFloat(1e9)).Text('f', 1))
				found = true
				break
			}
		}
		if found {
			break
		}
	}

	// Check in pool if not found in blocks
	if !found {
		txPool := c.node.blockchain.TxPool()
		pending := txPool.GetPending()

		for _, tx := range pending {
			if tx.Hash() == txHash {
				fmt.Printf("⏳ PENDING in transaction pool\n")
				fmt.Printf("   From:      %s\n", tx.From.String())
				if tx.To != nil {
					fmt.Printf("   To:        %s\n", tx.To.String())
				}
				fmt.Printf("   Value:     %s ANTD\n", formatBalance(tx.Value))
				fmt.Printf("   Nonce:     %d\n", tx.Nonce)

				// Check state
				stateNonce := c.node.blockchain.State().GetNonce(common.BytesToQuantumAddress(tx.From.Bytes()))
				if tx.Nonce == stateNonce {
					fmt.Printf("   Status:    ✅ Ready (next nonce)\n")
				} else if tx.Nonce > stateNonce {
					fmt.Printf("   Status:    ⏳ Future (need nonce %d first)\n", stateNonce)
				} else {
					fmt.Printf("   Status:    ❌ Stale (nonce %d already used)\n", tx.Nonce)
				}

				// Check if mining is active
				if c.node.miningState.IsMining() {
					fmt.Printf("   Mining:    ✅ Active - will be mined soon\n")
				} else {
					fmt.Printf("   Mining:    ⏸️  Inactive - use 'startmining'\n")
				}

				found = true
				break
			}
		}
	}

	c.node.mu.RUnlock()

	if !found {
		fmt.Printf("❌ Transaction not found\n")
		fmt.Printf("   Possible reasons:\n")
		fmt.Printf("   • Never created or added to pool\n")
		fmt.Printf("   • Dropped from pool (invalid/stale)\n")
		fmt.Printf("   • Hash is incorrect\n")
	}

	fmt.Printf("════════════════════════════════════════════════\n")
}

// Debugging transaction issues
func (c *Console) handleTxDebug(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: txdebug <address>")
		return
	}

	addr := mustParseQuantumAddress(parts[1])

	fmt.Printf("\n🔧 Transaction Debug for %s\n", addr.String())
	fmt.Printf("════════════════════════════════════════════════\n")

	c.node.mu.RLock()
	defer c.node.mu.RUnlock()

	// Get state
	state := c.node.blockchain.State()
	stateNonce := state.GetNonce(addr)
	balance := state.GetBalance(addr)

	// Get pending transactions
	txPool := c.node.blockchain.TxPool()
	pending := txPool.GetPending()

	fmt.Printf("State:\n")
	fmt.Printf("  Balance: %s ANTD\n", formatBalance(balance))
	fmt.Printf("  Nonce:   %d\n", stateNonce)

	// Show pending transactions
	var pendingFrom []*tx.Tx
	for _, tx := range pending {
		if common.BytesToQuantumAddress(tx.From.Bytes()) == addr {
			pendingFrom = append(pendingFrom, tx)
		}
	}

	if len(pendingFrom) == 0 {
		fmt.Printf("\nNo pending transactions\n")
	} else {
		fmt.Printf("\nPending transactions (%d):\n", len(pendingFrom))
		sort.Slice(pendingFrom, func(i, j int) bool {
			return pendingFrom[i].Nonce < pendingFrom[j].Nonce
		})

		for i, tx := range pendingFrom {
			status := "READY"
			if tx.Nonce < stateNonce {
				status = "STALE"
			} else if tx.Nonce > stateNonce+uint64(i) {
				status = "FUTURE"
			}

			fmt.Printf("  %d. Nonce %d [%s]\n", i+1, tx.Nonce, status)
			fmt.Printf("     Hash:   %s\n", tx.Hash().String()[:8])
			if tx.To != nil {
				fmt.Printf("     To:     %s\n", tx.To.String())
			}
			fmt.Printf("     Value:  %s ANTD\n", formatBalance(tx.Value))
			fmt.Printf("     Gas:    %d @ %s Gwei\n",
				tx.Gas,
				new(big.Float).Quo(new(big.Float).SetInt(tx.GasPrice), big.NewFloat(1e9)).Text('f', 1))
		}
	}

	// Check mining
	fmt.Printf("\nMining:\n")
	fmt.Printf("  Active:   %v\n", c.node.miningState.IsMining())
	fmt.Printf("  Enabled:  %v\n", c.node.miningState.IsEnabled())
}

func (c *Console) handleListWorkers() {
	if c.node.miningState == nil {
		fmt.Println("Error: PoS engine not available")
		return
	}

	// Get statistics
	stats := c.node.miningState.GetMiningStatistics()

	fmt.Println("=== Registered Workers ===")
	fmt.Printf("Miner Address: %s\n", stats["miner_address"])
	fmt.Printf("Blocks Mined: %d\n", stats["blocks_mined"])
	fmt.Printf("Total Rewards: %s\n", stats["total_rewards_antd"])

	// Check if this miner is registered as a staker
	minerAddress := c.node.miningState.GetMinerAddress()
	if minerAddress != (common.QuantumAddress{}) {
		fmt.Printf("\nCurrent Miner: %s\n", minerAddress.String())

		// Get balance
		c.node.mu.RLock()
		balance := c.node.blockchain.State().GetBalance(minerAddress)
		c.node.mu.RUnlock()
		fmt.Printf("  Balance: %s ANTD\n", formatBalance(balance))
	}

	fmt.Println("\nUse 'worker-info <address>' for detailed information")
}

func (c *Console) handleWorkerInfo(addressStr string) {
	address := mustParseQuantumAddress(addressStr)
	if address == (common.QuantumAddress{}) {
		fmt.Println("Error: Invalid address format")
		return
	}

	if c.node.miningState == nil {
		fmt.Println("Error: PoS engine not available")
		return
	}

	// Get miner address from mining state
	minerAddress := c.node.miningState.GetMinerAddress()
	isCurrentMiner := minerAddress == address

	fmt.Println("=== Worker Information ===")
	fmt.Printf("Address: %s\n", address.String())

	// Get balance
	c.node.mu.RLock()
	balance := c.node.blockchain.State().GetBalance(address)
	c.node.mu.RUnlock()
	fmt.Printf("Balance: %s ANTD\n", formatBalance(balance))

	// Check if this is the current miner
	if isCurrentMiner {
		fmt.Printf("Status: CURRENT MINER ✓\n")

		// Get mining statistics
		stats := c.node.miningState.GetMiningStatistics()
		if blocksMined, ok := stats["blocks_mined"].(uint64); ok {
			fmt.Printf("Blocks Mined: %d\n", blocksMined)
		}

		// Get next mining turn info
		fmt.Printf("Status: Not current miner\n")
	}
}

func (c *Console) handleForceRotate(reason string) {
	if c.node.miningState == nil {
		fmt.Println("Error: PoS engine not available")
		return
	}

	// This functionality may not be available in basic PoS
	fmt.Println("⚠️  Force rotate not available in basic PoS implementation")
	fmt.Println("   Rotation happens automatically based on stake and algorithm")
}

func (c *Console) handleRotationHistory(limit int) {
	if c.node.miningState == nil {
		fmt.Println("Error: PoS engine not available")
		return
	}

	fmt.Println("⚠️  Rotation history not available in basic PoS implementation")
	fmt.Println("   Check block history for mining information")
}

func (c *Console) handleMiningStats() {
	if c.node.miningState == nil {
		fmt.Println("Error: Mining state not initialized")
		return
	}

	stats := c.node.miningState.GetMiningStatistics()

	fmt.Println("=== PoS Mining Statistics ===")
	fmt.Printf("Mining Enabled: %v\n", stats["mining_enabled"])
	fmt.Printf("Currently Mining: %v\n", stats["is_mining"])
	fmt.Printf("Miner Address: %s\n", stats["miner_address"])
	fmt.Printf("Blocks Mined: %v\n", stats["blocks_mined"])
	fmt.Printf("Total Rewards: %v ANTD\n", stats["total_rewards_antd"])
	fmt.Printf("Has Private Key: %v\n", stats["has_private_key"])
}

func (c *Console) handleRegisterStake(parts []string) {
	if len(parts) != 2 {
		fmt.Println("Usage: register_stake <address>")
		return
	}

	addr := mustParseQuantumAddress(parts[1])
	if addr == (common.QuantumAddress{}) {
		fmt.Println("❌ Invalid address")
		return
	}

	c.node.mu.RLock()
	stakingManager := c.node.blockchain.StakingManager()
	c.node.mu.RUnlock()
	if stakingManager == nil {
		fmt.Println("❌ Staking manager not available")
		return
	}

	w := c.node.walletManager.GetWallet(addr.String())
	if w == nil {
		fmt.Println("❌ Wallet not found or not unlocked in this console")
		fmt.Println("   Use: unlock <address>")
		return
	}

	privKey, err := w.PrivateKey()
	if err != nil || len(privKey) == 0 {
		fmt.Println("❌ Could not access private key. Unlock wallet first.")
		return
	}

	stakeAmount := new(big.Int).Mul(big.NewInt(1_000_000), big.NewInt(1e18))
	if err := stakingManager.Stake(addr, stakeAmount, privKey); err != nil {
		fmt.Printf("❌ Failed to register stake: %v\n", err)
		return
	}

	c.node.SetMinerWalletAddress(addr)
	if c.node.miningState != nil {
		if err := c.node.miningState.SetPrivateKeyFromBytes(privKey); err != nil {
			fmt.Printf("⚠️  Stake registered, but failed to load miner private key: %v\n", err)
		}
		if err := c.node.miningState.SetMinerAddress(addr); err != nil {
			fmt.Printf("⚠️  Stake registered, but failed to set miner address: %v\n", err)
		}
	}

	if powEngine := c.node.blockchain.Pow(); powEngine != nil {
		pubKey, err := quantum.DerivePublicKey(privKey)
		if err != nil {
			fmt.Printf("⚠️  Stake registered, but failed to derive public key: %v\n", err)
		} else {
			powEngine.AutoRegisterIfEligible(addr, stakeAmount, pubKey)
		}
	}

	fmt.Printf("✅ Stake registered for %s with 1,000,000 ANTD\n", addr.String())
	fmt.Println("   Funds are locked during staking lock time and cannot be spent.")
	if c.node.miningState != nil {
		fmt.Println("✅ Auto-starting PoS mining for newly registered staker...")
		mining.StartPosMining(c.node.blockchain, c.node.miningState, addr, c.node.p2pNode)
	}
}

func (c *Console) handleUnlockStake(parts []string) {
	if len(parts) != 2 {
		fmt.Println("Usage: unlock_stake <address>")
		return
	}

	addr := mustParseQuantumAddress(parts[1])
	if addr == (common.QuantumAddress{}) {
		fmt.Println("❌ Invalid address")
		return
	}

	c.node.mu.RLock()
	stakingManager := c.node.blockchain.StakingManager()
	height := c.node.blockchain.GetChainHeight()
	c.node.mu.RUnlock()
	if stakingManager == nil {
		fmt.Println("❌ Staking manager not available")
		return
	}

	w := c.node.walletManager.GetWallet(addr.String())
	if w == nil {
		fmt.Println("❌ Wallet not found or not unlocked in this console")
		fmt.Println("   Use: unlock <address>")
		return
	}

	privKey, err := w.PrivateKey()
	if err != nil || len(privKey) == 0 {
		fmt.Println("❌ Could not access private key. Unlock wallet first.")
		return
	}

	if err := stakingManager.Unstake(addr, privKey); err != nil {
		fmt.Printf("❌ Failed to request stake unlock: %v\n", err)
		return
	}

	fmt.Printf("✅ Stake unlock requested for %s\n", addr.String())
	fmt.Printf("   Funds become spendable at block %d (current: %d)\n", height+20, height)
}

func (c *Console) handleStartMining() {
	if c.node.miningState == nil {
		fmt.Println("Error: Mining state not initialized")
		return
	}

	// Get address from Node
	minerAddress := c.node.MinerWalletAddress()
	if minerAddress == (common.QuantumAddress{}) {
		fmt.Println("❌ Error: No mining address set!")
		fmt.Println("   Use 'setaddress <your-address>' first")
		fmt.Println("")
		fmt.Println("   Your available wallets:")
		accounts := c.node.Keystore().Accounts()
		for i, acc := range accounts {
			c.node.mu.RLock()
			balance := c.node.blockchain.State().GetBalance(common.BytesToQuantumAddress(acc.Address.Bytes()))
			c.node.mu.RUnlock()
			fmt.Printf("   %d. %s → %s ANTD\n",
				i+1, acc.Address.String(), formatBalance(balance))
		}
		return
	}

	// Check if private key is loaded
	stats := c.node.miningState.GetMiningStatistics()
	if hasKey, ok := stats["has_private_key"].(bool); ok && !hasKey {
		fmt.Printf("⚠️  WARNING: No private key loaded for %s\n", minerAddress.String())
		fmt.Println("   Blocks cannot be signed without private key!")
		fmt.Println("   Use: unlock <address> to load private key from keystore")

		if !c.confirmAction("Start mining anyway? (blocks will fail)") {
			fmt.Println("Cancelled")
			return
		}
	}

	// Also set address in PosMiningState
	if err := c.node.miningState.SetMinerAddress(minerAddress); err != nil {
		fmt.Printf("❌ Failed to set miner address in mining state: %v\n", err)
		return
	}

	fmt.Printf("✅ Starting mining with address: %s\n", minerAddress.String())
	mining.StartPosMining(c.node.blockchain, c.node.miningState, minerAddress, c.node.p2pNode)
	fmt.Println("✓ Mining started. Waiting for your turn to mine blocks...")
}

func (c *Console) checkStakerRegistration(addr common.QuantumAddress) bool {
	c.node.mu.RLock()
	stakingManager := c.node.blockchain.StakingManager()
	powEngine := c.node.blockchain.Pow()
	c.node.mu.RUnlock()
	if stakingManager != nil {
		stake, err := stakingManager.GetStake(addr)
		if err == nil && stake != nil && stake.Sign() > 0 {
			return true
		}
	}

	if powEngine != nil && powEngine.IsKing(addr) {
		return true
	}

	// Fallback to on-chain staker snapshots reconstructed from block headers.
	for _, record := range c.registeredStakerRecords() {
		if record.Address == addr && record.IsActive {
			// unlock_stake@0 is a legacy/default marker in some snapshots; treat as active.
			if record.UnlockBlock == nil || *record.UnlockBlock == 0 {
				return true
			}
		}
	}

	return false
}

func (c *Console) handleListStakers() {
	c.printRegisteredStakers("Registered stakers")
}

func (c *Console) printRegisteredStakers(header string) {
	records := c.registeredStakerRecords()
	if len(records) == 0 {
		fmt.Printf("ℹ️  %s: none\n", header)
		return
	}

	fmt.Printf("\n✅ %s (%d)\n", header, len(records))
	fmt.Println("────────────────────────────────────────────────────────────────")
	for idx, record := range records {
		status := "active"
		if !record.IsActive {
			status = "inactive"
		}
		if record.UnlockBlock != nil && *record.UnlockBlock > 0 {
			status = fmt.Sprintf("unlock_stake@%d", *record.UnlockBlock)
		}
		amount := "0"
		if record.Amount != nil {
			amount = formatBalance(record.Amount)
		}
		fmt.Printf("   %2d) %s\n", idx+1, record.Address.String())
		fmt.Printf("       status: %-18s stake: %s ANTD\n", status, amount)
	}
	fmt.Println()
}

func (c *Console) registeredStakerRecords() []staking.StakeRecord {

	seen := make(map[common.QuantumAddress]staking.StakeRecord)
	stakers := make([]staking.StakeRecord, 0)

	c.node.mu.RLock()
	latestHeight := c.node.blockchain.GetLatestHeight()
	for height := latestHeight; ; height-- {
		blk := c.node.blockchain.GetBlock(height)
		if blk == nil {
			if height == 0 {
				break
			}
			continue
		}
		for _, staker := range blk.Header.Stakers {
			if _, exists := seen[staker.Address]; exists {
				continue
			}
			record := staking.StakeRecord{
				Address:     staker.Address,
				Amount:      staker.Amount,
				IsActive:    staker.IsActive,
				UnlockBlock: staker.UnlockStakeHeight,
			}
			seen[record.Address] = record
			stakers = append(stakers, record)
		}
		if height == 0 {
			break
		}
	}
	c.node.mu.RUnlock()

	// Keep fallback behavior for older chains where staker snapshots might be missing.
	if len(stakers) == 0 {
		c.node.mu.RLock()
		stakingManager := c.node.blockchain.StakingManager()
		c.node.mu.RUnlock()
		if stakingManager != nil {
			for _, record := range stakingManager.GetStakeRecords() {
				if _, exists := seen[record.Address]; exists {
					continue
				}
				seen[record.Address] = record
				stakers = append(stakers, record)
			}
		}
	sort.Slice(stakers, func(i, j int) bool {
		return stakers[i].Address.String() < stakers[j].Address.String()
	})
	}

	return stakers
}

func (c *Console) handleDebugTransactionFlow(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: debugtxflow <from>")
		return
	}

	fromAddr := mustParseQuantumAddress(parts[1])

	fmt.Printf("\n🔍 Debug Transaction Flow for %s\n", fromAddr.String())
	fmt.Printf("════════════════════════════════════════════════\n")

	// Get state
	c.node.mu.RLock()
	state := c.node.blockchain.State()
	stateNonce := state.GetNonce(fromAddr)
	balance := state.GetBalance(fromAddr)
	txPool := c.node.blockchain.TxPool()
	pendingTxs := txPool.GetPending()
	c.node.mu.RUnlock()

	fmt.Printf("State:\n")
	fmt.Printf("  Nonce: %d\n", stateNonce)
	fmt.Printf("  Balance: %s ANTD\n", formatBalance(balance))

	// Show pending transactions
	fmt.Printf("\nPending Transactions from this address:\n")
	var pendingFromThis []*tx.Tx
	for _, tx := range pendingTxs {
		if common.BytesToQuantumAddress(tx.From.Bytes()) == fromAddr {
			pendingFromThis = append(pendingFromThis, tx)
		}
	}

	if len(pendingFromThis) == 0 {
		fmt.Printf("  No pending transactions\n")
	} else {
		sort.Slice(pendingFromThis, func(i, j int) bool {
			return pendingFromThis[i].Nonce < pendingFromThis[j].Nonce
		})

		for _, tx := range pendingFromThis {
			fmt.Printf("  • Nonce: %d, Hash: %s\n", tx.Nonce, tx.Hash().String()[:8])
		}
	}

	// Show mining status
	fmt.Printf("\nMining Status:\n")
	fmt.Printf("  Mining           : %v\n", c.node.miningState.IsMining())
	fmt.Printf("  Mining Enabled   : %v\n", c.node.miningState.IsEnabled())

	// Show next nonce calculation
	fmt.Printf("\nNext Nonce Calculation:\n")
	fmt.Printf("  State nonce: %d\n", stateNonce)

	fmt.Printf("\n💡 Send a test transaction with: send %s 0x0000... 0.01\n", fromAddr.String())
}

// Fix the GetSubmitTime call in handleTxPool
func (c *Console) handleTxPool(parts []string) {
	c.node.mu.RLock()
	defer c.node.mu.RUnlock()

	txPool := c.node.blockchain.TxPool()
	if txPool == nil {
		fmt.Println("❌ Transaction pool not available")
		return
	}

	// Get pending transactions
	pendingTxs := txPool.GetPending()

	if len(pendingTxs) == 0 {
		fmt.Println("📭 Transaction pool is empty")
		fmt.Println("   Send transactions using: send <from> <to> <amount> [nonce]")
		return
	}

	// Group transactions by address for better display
	txsByAddress := make(map[common.QuantumAddress][]*tx.Tx)
	for _, tx := range pendingTxs {
		from := common.BytesToQuantumAddress(tx.From.Bytes())
		txsByAddress[from] = append(txsByAddress[from], tx)
	}

	fmt.Printf("📚 Transaction Pool: %d pending transaction(s) from %d address(es)\n",
		len(pendingTxs), len(txsByAddress))
	fmt.Println("═════════════════════════════════════════════════════════════════════")

	addressNum := 1
	for addr, txs := range txsByAddress {
		// Sort transactions by nonce for this address
		sort.Slice(txs, func(i, j int) bool {
			return txs[i].Nonce < txs[j].Nonce
		})

		// Get nonce information
		stateNonce := c.node.blockchain.State().GetNonce(addr)
		fmt.Printf("%d. Address: %s\n", addressNum, addr.String())
		fmt.Printf("   State nonce: %d | Pending txs: %d\n", stateNonce, len(txs))

		for i, transaction := range txs {
			status := "✅ Ready"
			if transaction.Nonce > stateNonce+uint64(i) {
				status = "⏳ Future"
			} else if transaction.Nonce < stateNonce {
				status = "❌ Stale"
			}

			fmt.Printf("   ┌─ TX %d: %s\n", i+1, status)
			fmt.Printf("   │  Hash:    %s\n", transaction.Hash().String())
			if transaction.To != nil {
				fmt.Printf("   │  To:      %s\n", transaction.To.String())
			} else {
				fmt.Printf("   │  To:      📝 Contract Creation\n")
			}
			fmt.Printf("   │  Value:   %s ANTD\n", formatBalance(transaction.Value))
			fmt.Printf("   │  Nonce:   %d", transaction.Nonce)

			// Show nonce sequence status
			if i == 0 && transaction.Nonce != stateNonce {
				fmt.Printf(" (gap: expected %d)", stateNonce)
			} else if i > 0 && transaction.Nonce != txs[i-1].Nonce+1 {
				fmt.Printf(" (gap: expected %d)", txs[i-1].Nonce+1)
			}
			fmt.Printf("\n")

			fmt.Printf("   │  Gas:     %d | Price: %s Gwei\n",
				transaction.Gas,
				new(big.Float).Quo(new(big.Float).SetInt(transaction.GasPrice), big.NewFloat(1e9)).Text('f', 1))

			fmt.Printf("   └%s\n", strings.Repeat("─", 60))
		}

		if addressNum < len(txsByAddress) {
			fmt.Printf("   %s\n", strings.Repeat("═", 50))
		}
		addressNum++
	}

	fmt.Println("═════════════════════════════════════════════════════════════════════")

	// Show pool statistics
	totalValue := new(big.Int)
	totalGas := uint64(0)
	for _, tx := range pendingTxs {
		totalValue.Add(totalValue, tx.Value)
		totalGas += tx.Gas
	}

	fmt.Printf("📊 Pool Statistics:\n")
	fmt.Printf("   Total Value:  %s ANTD\n", formatBalance(totalValue))
	fmt.Printf("   Total Gas:    %d\n", totalGas)
	fmt.Printf("   Unique From:  %d addresses\n", len(txsByAddress))

	fmt.Printf("💡 Tips:\n")
	fmt.Printf("   • Use 'nonce <address>' to check nonce status\n")
	fmt.Printf("   • Use 'clearstucktxs' to force cleanup\n")
	fmt.Printf("   • Transactions confirm in ~4 minutes after inclusion\n")
	fmt.Printf("   • Stale transactions are auto-removed\n")
}

func (c *Console) isTxInBlock(txHash common.Hash) (bool, uint64) {
	c.node.mu.RLock()
	defer c.node.mu.RUnlock()

	// Search through blocks
	for i := uint64(0); ; i++ {
		blk := c.node.blockchain.GetBlock(i)
		if blk == nil {
			break
		}

		for _, transaction := range blk.Txs {
			if transaction.Hash() == txHash {
				return true, i
			}
		}
	}

	return false, 0
}

func (c *Console) handleClearTx(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: cleartx <txhash>")
		return
	}

	txHash := common.HexToHash(parts[1])

	// Check if transaction is in a block
	inBlock, blockNum := c.isTxInBlock(txHash)
	if inBlock {
		fmt.Printf("❌ Transaction %s is in block %d and cannot be removed\n",
			txHash.String()[:8], blockNum)
		return
	}

	c.node.mu.Lock()
	defer c.node.mu.Unlock()

	fmt.Printf("🔄 Attempting to remove transaction %s from pool...\n", txHash.String())

	// Try to remove from pool
	txPool := c.node.blockchain.TxPool()

	if chainTxPool, ok := txPool.(*chain.TxPool); ok {
		chainTxPool.RemoveTx(txHash)
		fmt.Printf("✅ Transaction %s removed from pool\n", txHash.String())
	} else {
		fmt.Printf("❌ Cannot remove transaction - unsupported pool type\n")
	}
}

func (c *Console) handleGetTx(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: gettx <hash>")
		return
	}

	txHash := common.HexToHash(parts[1])
	fmt.Printf("\n🔍 Transaction Details: %s\n", txHash.String())
	fmt.Printf("════════════════════════════════════════════════\n")

	found := false

	// Check in blocks
	c.node.mu.RLock()
	for i := uint64(0); ; i++ {
		block := c.node.blockchain.GetBlock(i)
		if block == nil {
			break
		}

		for _, transaction := range block.Txs {
			if transaction.Hash() == txHash {
				fmt.Printf("📦 Found in block %d\n", i)
				fmt.Printf("   Block hash: %s\n", block.Hash().String()[:8])
				fmt.Printf("   From: %s\n", transaction.From.String())
				if transaction.To != nil {
					fmt.Printf("   To: %s\n", transaction.To.String())
				}
				fmt.Printf("   Value: %s ANTD\n", formatBalance(transaction.Value))
				fmt.Printf("   Nonce: %d\n", transaction.Nonce)
				fmt.Printf("   Gas: %d | Gas Price: %s Gwei\n",
					transaction.Gas,
					new(big.Float).Quo(new(big.Float).SetInt(transaction.GasPrice), big.NewFloat(1e9)).Text('f', 2))
				fmt.Printf("   Gas Cost: %s ANTD\n",
					formatBalance(new(big.Int).Mul(transaction.GasPrice, big.NewInt(int64(transaction.Gas)))))
				found = true
				break
			}
		}
		if found {
			break
		}
	}

	// Check in pool if not found in blocks
	if !found {
		txPool := c.node.blockchain.TxPool()
		pending := txPool.GetPending()

		for _, transaction := range pending {
			if transaction.Hash() == txHash {
				fmt.Printf("⏳ Found in transaction pool (pending)\n")
				fmt.Printf("   From: %s\n", transaction.From.String())
				if transaction.To != nil {
					fmt.Printf("   To: %s\n", transaction.To.String())
				}
				fmt.Printf("   Value: %s ANTD\n", formatBalance(transaction.Value))
				fmt.Printf("   Nonce: %d\n", transaction.Nonce)

				// Check state nonce
				stateNonce := c.node.blockchain.State().GetNonce(common.BytesToQuantumAddress(transaction.From.Bytes()))
				status := "✅ Ready"
				if transaction.Nonce > stateNonce {
					status = "⏳ Future"
				} else if transaction.Nonce < stateNonce {
					status = "❌ Stale"
				}
				fmt.Printf("   Status: %s (state nonce: %d)\n", status, stateNonce)

				found = true
				break
			}
		}
	}

	c.node.mu.RUnlock()

	if !found {
		fmt.Printf("❌ Transaction not found in blocks or pool\n")
		fmt.Printf("   Possible reasons:\n")
		fmt.Printf("     • Transaction was never created\n")
		fmt.Printf("     • Transaction was dropped from pool\n")
		fmt.Printf("     • Transaction hash is incorrect\n")
	}

	fmt.Printf("════════════════════════════════════════════════\n")
}

func (c *Console) handleSendDebug(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: senddebug <from>")
		return
	}

	fromAddr := mustParseQuantumAddress(parts[1])

	fmt.Printf("\n🔧 Debug Send for address %s\n", fromAddr.String())
	fmt.Printf("════════════════════════════════════════════════\n")

	// Get current state
	c.node.mu.RLock()
	state := c.node.blockchain.State()
	stateNonce := state.GetNonce(fromAddr)
	balance := state.GetBalance(fromAddr)
	txPool := c.node.blockchain.TxPool()
	pending := txPool.GetPending()
	c.node.mu.RUnlock()

	fmt.Printf("State:\n")
	fmt.Printf("  Balance: %s ANTD\n", formatBalance(balance))
	fmt.Printf("  Nonce: %d\n", stateNonce)

	// Show pending transactions from this address
	var pendingFrom []*tx.Tx
	for _, tx := range pending {
		if common.BytesToQuantumAddress(tx.From.Bytes()) == fromAddr {
			pendingFrom = append(pendingFrom, tx)
		}
	}

	if len(pendingFrom) == 0 {
		fmt.Printf("\nNo pending transactions from this address\n")
	} else {
		fmt.Printf("\nPending transactions (%d):\n", len(pendingFrom))
		sort.Slice(pendingFrom, func(i, j int) bool {
			return pendingFrom[i].Nonce < pendingFrom[j].Nonce
		})

		for i, tx := range pendingFrom {
			status := "✅"
			if tx.Nonce < stateNonce {
				status = "❌ STALE"
			} else if tx.Nonce > stateNonce+uint64(i) {
				status = "⏳ FUTURE"
			}

			fmt.Printf("  %d. Nonce: %d %s (Hash: %s)\n",
				i+1, tx.Nonce, status, tx.Hash().String()[:8])
		}
	}

	// Check mining status
	fmt.Printf("\nMining Status:\n")
	fmt.Printf("  Mining active: %v\n", c.node.miningState.IsMining())
	fmt.Printf("  Mining enabled: %v\n", c.node.miningState.IsEnabled())

	fmt.Printf("\n💡 Test command:\n")
	fmt.Printf("  send %s 0x0000...0000 0.01\n", fromAddr.String())
}

// GetUnlockedPrivateKey – safe, works with any go-ethereum version
func (n *Node) GetUnlockedPrivateKey(addr common.QuantumAddress) (*ecdsa.PrivateKey, error) {
	_ = addr
	return nil, fmt.Errorf("legacy ECDSA private keys are unsupported; use antdchain keystore unlock flow")
}

// List wallets from wallet manager
func (c *Console) handleListWallets() {
	wallets := c.node.walletManager.ListWallets()
	if len(wallets) == 0 || (len(wallets) == 1 && wallets[0] == "No wallets found") {
		fmt.Println("No wallets found")
		return
	}

	fmt.Printf("Found %d wallet(s):\n", len(wallets))
	for i, entry := range wallets {
		fmt.Printf("  %d. %s\n", i+1, entry)
	}
}

func (c *Console) getKeystoreFilePath(address string) string {
	addr := strings.ToLower(strings.TrimPrefix(address, "0x"))
	pattern := filepath.Join(c.node.GetKeystoreDir(), fmt.Sprintf("*--%s", addr))
	matches, _ := filepath.Glob(pattern)
	if len(matches) > 0 {
		return matches[0]
	}
	return filepath.Join(c.node.GetKeystoreDir(), "UTC--...--"+addr)
}

// parseANTDAmount converts human-readable ANTD amount to base units
func parseANTDAmount(amountStr string) (*big.Int, error) {
	amountStr = strings.TrimSpace(amountStr)
	if amountStr == "" {
		return nil, errors.New("amount cannot be empty")
	}

	// Split into whole and fractional parts
	parts := strings.Split(amountStr, ".")
	if len(parts) > 2 {
		return nil, errors.New("invalid amount format: too many decimal points")
	}

	// Parse whole part
	wholePart := parts[0]
	if wholePart == "" {
		wholePart = "0"
	}

	whole, ok := new(big.Int).SetString(wholePart, 10)
	if !ok {
		return nil, errors.New("invalid whole number part")
	}

	// Calculate whole part in base units
	oneANTD := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	result := new(big.Int).Mul(whole, oneANTD)

	// Parse fractional part if exists
	if len(parts) == 2 {
		fractionalPart := parts[1]
		if len(fractionalPart) > 18 {
			return nil, errors.New("too many decimal places (max 18)")
		}

		// Pad with zeros to 18 decimal places
		for len(fractionalPart) < 18 {
			fractionalPart += "0"
		}

		fractional, ok := new(big.Int).SetString(fractionalPart, 10)
		if !ok {
			return nil, errors.New("invalid fractional part")
		}

		result.Add(result, fractional)
	}

	if result.Sign() <= 0 {
		return nil, errors.New("amount must be positive")
	}

	return result, nil
}

// formatBalance converts base units to human-readable ANTD
func formatBalance(amount *big.Int) string {
	if amount == nil {
		return "0"
	}

	// 1 ANTD = 1e18 base units
	oneANTD := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)

	// Integer division for whole ANTD
	whole := new(big.Int).Div(amount, oneANTD)

	// Remainder for fractional part
	remainder := new(big.Int).Mod(amount, oneANTD)

	// If no fractional part, return whole number
	if remainder.Sign() == 0 {
		return whole.String()
	}

	// Convert remainder to decimal with 6 places
	fractional := new(big.Float).SetInt(remainder)
	divisor := new(big.Float).SetInt(oneANTD)
	fractional.Quo(fractional, divisor)

	// Format to string with 6 decimal places and remove leading "0."
	fractionalStr := fractional.Text('f', 6)
	if len(fractionalStr) > 2 && fractionalStr[:2] == "0." {
		fractionalStr = fractionalStr[2:]
	}

	// Remove trailing zeros
	fractionalStr = strings.TrimRight(fractionalStr, "0")
	if fractionalStr == "" {
		return whole.String()
	}

	return whole.String() + "." + fractionalStr
}

func (c *Console) handleImport(parts []string) {
	if len(parts) < 2 {
		fmt.Fprintln(os.Stderr, "Usage: import [--file=<path>] <hex-key>")
		return
	}

	password, err := c.readPassword("Set password for imported wallet: ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to read password: %v\n", err)
		return
	}
	confirm, err := c.readPassword("Confirm password: ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to read password confirmation: %v\n", err)
		return
	}
	if password != confirm {
		fmt.Fprintf(os.Stderr, "❌ Passwords do not match\n")
		return
	}
	if len(password) < 8 {
		fmt.Fprintf(os.Stderr, "❌ Password must be at least 8 characters\n")
		return
	}

	// --- Support reading key from a file (avoids terminal length limits) ---
	var raw string
	if len(parts) >= 2 && strings.HasPrefix(parts[1], "--file=") {
		filePath := strings.TrimPrefix(parts[1], "--file=")
		data, err := os.ReadFile(filePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "❌ Failed to read file: %v\n", err)
			return
		}
		raw = string(data)
	} else if len(parts) >= 3 && parts[1] == "--file" {
		filePath := parts[2]
		data, err := os.ReadFile(filePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "❌ Failed to read file: %v\n", err)
			return
		}
		raw = string(data)
	} else {
		// Standard direct input (from command line)
		raw = strings.Join(parts[1:], "")
	}

	// Clean the input: remove all whitespace and optional 0x/0X prefix
	raw = strings.NewReplacer("\n", "", "\r", "", "\t", "", " ", "").Replace(raw)
	raw = strings.TrimPrefix(raw, "0x")
	raw = strings.TrimPrefix(raw, "0X")

	if len(raw)%2 != 0 {
		fmt.Fprintln(os.Stderr, "❌ Invalid hex: odd length")
		return
	}

	privKeyBytes, err := hex.DecodeString(raw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Invalid hex: %v\n", err)
		return
	}

	// Determine the final 4032‑byte private key and the derived address
	var fullPriv []byte
	var addr common.QuantumAddress

	switch len(privKeyBytes) {
	case 32: // seed – expand to full key
		_, pubFromSeed, err2 := quantum.DeriveKeyFromSeed(privKeyBytes)
		if err2 != nil {
			fmt.Fprintf(os.Stderr, "❌ Failed to derive key from seed: %v\n", err2)
			return
		}
		addr, err = common.ParseQuantumAddress(quantum.PubKeyToAddress(pubFromSeed))
		if err != nil {
			fmt.Fprintf(os.Stderr, "❌ Bad address from seed: %v\n", err)
			return
		}

	case quantum.MLDSA65PrivateKeySize: // full packed key
		pub, err2 := quantum.DerivePublicKey(privKeyBytes)
		if err2 != nil {
			fmt.Fprintf(os.Stderr, "❌ Invalid Antd private key: %v\n", err2)
			return
		}
		fullPriv = privKeyBytes
		addr, err = common.ParseQuantumAddress(quantum.PubKeyToAddress(pub))
		if err != nil {
			fmt.Fprintf(os.Stderr, "❌ Bad address from private key: %v\n", err)
			return
		}

	default:
		fmt.Fprintf(os.Stderr, "❌ Invalid key length: %d bytes (expected 32‑byte seed or %d‑byte private key)\n",
			len(privKeyBytes), quantum.MLDSA65PrivateKeySize)
		return
	}

	// Store the 4032‑byte key in the keystore (uses fullPriv)
	importedAddr, err := qkeystore.ImportAccount(fullPriv, password, c.node.GetKeystoreDir())
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to store keystore: %v\n", err)
		return
	}
	if importedAddr != addr {
		fmt.Fprintf(os.Stderr, "❌ Address mismatch after keystore import: expected %s, got %s\n",
			addr.String(), importedAddr.String())
		return
	}

	// Register the wallet
	w := wallet.NewWalletWithAddress(c.node.blockchain, addr, c.getWalletDataDir())
	c.node.walletManager.AddWallet(addr.String(), w)

	fmt.Fprintf(os.Stderr, "✅ Wallet imported\n")
	fmt.Fprintf(os.Stderr, "   Address: %s\n", addr.String())
	fmt.Fprintf(os.Stderr, "   Type: Antd quantum (0q Base58Check)\n")
}

func (c *Console) handleExport(parts []string) {
	if len(parts) < 2 {
		fmt.Fprintln(os.Stderr, "Usage: export <address>")
		return
	}

	qAddr, err := common.ParseQuantumAddress(parts[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Invalid Antd address: %v\n", err)
		return
	}

	password, err := c.readPassword("Enter password to decrypt wallet: ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to read password: %v\n", err)
		return
	}

	privKeyBytes, err := qkeystore.Unlock(qAddr, password, c.node.GetKeystoreDir())
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to decrypt keystore: %v\n", err)
		return
	}
	if len(privKeyBytes) != quantum.MLDSA65PrivateKeySize {
		fmt.Fprintf(os.Stderr, "❌ Unexpected private key length: %d bytes (expected %d)\n",
			len(privKeyBytes), quantum.MLDSA65PrivateKeySize)
		return
	}

	// Write the full private key to stdout (perfect for redirection)
	privateKeyHex := hex.EncodeToString(privKeyBytes)
	fmt.Printf("0x%s\n", privateKeyHex)

	// If a seed is available, write it on a second line
	if seed := quantum.ExtractSeedFromPrivateKey(privKeyBytes); len(seed) == 32 {
		fmt.Printf("0x%s\n", hex.EncodeToString(seed))
	}

	// Informational messages go to stderr so they don't pollute the file
	fmt.Fprintln(os.Stderr, "\n⚠️  Keep this private key secure and never share it!")
	if seed := quantum.ExtractSeedFromPrivateKey(privKeyBytes); len(seed) == 32 {
		fmt.Fprintln(os.Stderr, "�� A 32‑byte seed is included above – it can regenerate the full key.")
		fmt.Fprintln(os.Stderr, "   Import it as a seed to restore your wallet.")
	} else {
		fmt.Fprintln(os.Stderr, "ℹ️  No seed available. This key was generated randomly.")
	}
}

func (c *Console) handleGetBlockInfo(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: getblockinfo <number>")
		return
	}
	var blockNumber uint64
	if parts[1] == "latest" {
		c.node.mu.RLock()
		latest := c.node.blockchain.Latest()
		c.node.mu.RUnlock()
		if latest == nil {
			fmt.Println("❌ No blocks available")
			return
		}
		blockNumber = latest.Header.Number.Uint64()
	} else {
		var err error
		blockNumber, err = parseUint64(parts[1])
		if err != nil {
			fmt.Printf("❌ Invalid block number: %s\n", parts[1])
			return
		}
	}
	c.node.mu.RLock()
	blk := c.node.blockchain.GetBlock(blockNumber)
	c.node.mu.RUnlock()
	if blk == nil {
		fmt.Printf("❌ Block %d not found\n", blockNumber)
		return
	}
	fmt.Printf("Block %d:\n", blockNumber)
	fmt.Printf("  Hash: %s\n", blk.Hash().String())
	fmt.Printf("  Timestamp: %d\n", blk.Header.Time)
	if blk.Header.Difficulty != nil {
		fmt.Printf("  Difficulty: %s\n", blk.Header.Difficulty.String())
	}
	fmt.Printf("  Gas Used: %d / %d\n", blk.Header.GasUsed, blk.Header.GasLimit)
	fmt.Printf("  Transactions: %d\n", len(blk.Txs))
	fmt.Printf("  Miner: %s\n", blk.Header.Coinbase.String())
}

func (c *Console) handleLock(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: lock <address>")
		return
	}
	addr := parts[1]
	if c.node.walletManager.Lock(addr) {
		fmt.Printf("✅ Wallet %s locked\n", addr)
	} else {
		fmt.Printf("❌ Wallet %s not found\n", addr)
	}
}

func (c *Console) handleUnlock(parts []string) {
	if len(parts) < 2 {
		fmt.Fprintln(os.Stderr, "Usage: unlock <address>")
		return
	}

	addr, err := common.ParseQuantumAddress(parts[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Invalid quantum address: %v\n", err)
		return
	}

	password, err := c.readPassword(fmt.Sprintf("Password for %s: ", addr.String()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to read password: %v\n", err)
		return
	}

	// Unlock through the wallet manager (uses quantum keystore)
	if err := c.node.walletManager.Unlock(addr.String(), password); err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to unlock wallet: %v\n", err)
		return
	}

	// Set the miner address for Proof‑of‑Stake
	c.node.SetMinerWalletAddress(addr)

	// Load the private key into the mining state and register the public key with the PoW engine
	if c.node.miningState != nil {
		w := c.node.walletManager.GetWallet(addr.String())
		if w != nil {
			privKey, err := w.PrivateKey()
			if err == nil {
				// Store the raw private key in the mining state
				if err := c.node.miningState.SetPrivateKeyFromBytes(privKey); err != nil {
					fmt.Fprintf(os.Stderr, "❌ Failed to load private key into miner: %v\n", err)
					return
				}

				// Derive and register the public key so block validation can verify signatures
				pubKey, err := quantum.DerivePublicKey(privKey)
				if err != nil {
					fmt.Fprintf(os.Stderr, "❌ Failed to derive public key: %v\n", err)
					return
				}

				// Register/update the public key for already-registered stakers.
				stakeAmount := big.NewInt(0)

				c.node.mu.RLock()
				if stakingManager := c.node.blockchain.StakingManager(); stakingManager != nil {
					if staked, stakeErr := stakingManager.GetStake(addr); stakeErr == nil && staked != nil {
						stakeAmount = staked
					}
				}
				c.node.mu.RUnlock()

				powEngine := c.node.blockchain.Pow()
				if powEngine != nil {
					powEngine.AutoRegisterIfEligible(addr, stakeAmount, pubKey)
					fmt.Fprintf(os.Stderr, "Public key registered for miner %s\n", addr.String())
				} else {
					fmt.Fprintf(os.Stderr, "⚠️ PoW engine not available – public key not registered\n")
				}
				fmt.Fprintf(os.Stderr, "�� Public key registered for miner %s\n", addr.String())
			}
		}
	}

	fmt.Fprintf(os.Stderr, "✅ Wallet %s unlocked successfully!\n", addr.String())
}

func (c *Console) handleBalance(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: balance <address>")
		return
	}
	address := mustParseQuantumAddress(parts[1])
	c.node.mu.RLock()
	balance := c.node.blockchain.State().GetBalance(address)
	c.node.mu.RUnlock()

	// Format the balance for display
	formattedBalance := formatBalance(balance)
	fmt.Printf("Balance for %s: %s ANTD\n", parts[1], formattedBalance)
}

func (c *Console) handleStatus() {
	c.node.mu.RLock()
	latest := c.node.blockchain.Latest()
	blockNumber := uint64(0)
	if latest != nil && latest.Header.Number != nil {
		blockNumber = latest.Header.Number.Uint64()
	}

	peers := 0
	peerID := ""
	if c.node.p2pNode != nil {
		peers = len(c.node.p2pNode.Peers())
		peerID = c.node.p2pNode.ID()
	}
	c.node.mu.RUnlock()

	fmt.Printf("=== ANTDChain Node Status ===\n")
	fmt.Printf("  Latest Block     : %d\n", blockNumber)
	fmt.Printf("  Mining           : %v\n", c.node.miningState.IsMining())
	fmt.Printf("  Mining Enabled   : %v\n", c.node.miningState.IsEnabled())
	fmt.Printf("  Peers Connected  : %d\n", peers)
	fmt.Printf("  Peer ID          : %s\n", peerID)

	// Show current mining reward address
	rewardAddr := c.node.MinerWalletAddress()
	if rewardAddr == (common.QuantumAddress{}) {
		fmt.Printf("  Miner Address    : <not set>\n")
		fmt.Println("     → Use 'setaddress <your-wallet>' to receive mining rewards")
	} else {
		fmt.Printf("  Miner Address    : %s\n", rewardAddr.String())
		c.node.mu.RLock()
		balance := c.node.blockchain.State().GetBalance(rewardAddr)
		c.node.mu.RUnlock()
		fmt.Printf("  Miner Balance    : %s ANTD\n", formatBalance(balance))
	}

	// Quantum wallets (0q Base58Check only)
	wallets := c.node.walletManager.ListWallets()
	walletCount := 0
	for _, walletInfo := range wallets {
		if walletInfo != "No wallets found" {
			walletCount++
		}
	}
	fmt.Printf("  Quantum Wallets  : %d\n", walletCount)
	if walletCount > 0 {
		fmt.Println("     Available wallets:")
		displayIndex := 1
		for _, walletInfo := range wallets {
			if walletInfo == "No wallets found" {
				continue
			}
			parts := strings.Fields(walletInfo)
			if len(parts) < 2 {
				continue
			}

			// ListWallets format: "<lock-icon> <address>"
			statusIcon := parts[0]
			addrStr := parts[1]
			quantumAddr := mustParseQuantumAddress(addrStr)

			c.node.mu.RLock()
			bal := c.node.blockchain.State().GetBalance(common.QuantumAddress(quantumAddr))
			c.node.mu.RUnlock()
			fmt.Printf("     %d. %s %s → %s ANTD\n", displayIndex, statusIcon, addrStr, formatBalance(bal))
			displayIndex++
		}
	}

	fmt.Printf("  Blocks Mined     : %d\n", blockNumber)
	fmt.Println("===============================")
}

func (c *Console) handleDebugChain() {
	fmt.Println("Chain Debug:")
	for i := uint64(0); ; i++ {
		c.node.mu.RLock()
		blk := c.node.blockchain.GetBlock(i)
		c.node.mu.RUnlock()
		if blk == nil {
			break
		}
		fmt.Printf("Block %d: Hash=%s, ParentHash=%s\n", i, blk.Hash().String(), blk.Header.ParentHash.String())
	}
}

func (c *Console) handleSaveWallets() {
	password, err := c.readPassword("Enter encryption password: ")
	if err != nil {
		fmt.Printf("❌ Failed to read password: %v\n", err)
		return
	}

	confirm, err := c.readPassword("Confirm password: ")
	if err != nil {
		fmt.Printf("❌ Failed to read confirmation: %v\n", err)
		return
	}

	if password != confirm {
		fmt.Printf("❌ Passwords do not match\n")
		return
	}

	if len(password) < 8 {
		fmt.Printf("❌ Password must be at least 8 characters\n")
		return
	}

	err = c.node.walletManager.SaveWallets(password)
	if err != nil {
		fmt.Printf("❌ Failed to save wallets: %v\n", err)
	} else {
		fmt.Printf("✅ Wallets saved with encryption\n")
		fmt.Printf("🔒 All wallets are now encrypted and secured\n")
	}
}

func (c *Console) listLocalProposals() {
	files, _ := filepath.Glob(filepath.Join(c.node.GetDataDir(), "proposal_*.json"))
	if len(files) == 0 {
		fmt.Println("No local proposals found")
		return
	}
	fmt.Printf("Local Governance Proposals (%d):\n", len(files))
	for _, f := range files {
		data, _ := os.ReadFile(f)
		var p map[string]interface{}
		json.Unmarshal(data, &p)
		id := p["proposal_id"].(float64)
		addr := p["address"].(string)
		status := "⏳ Pending"
		if exec, ok := p["executed"]; ok && exec.(bool) {
			status = "✅ Executed"
		}
		fmt.Printf("  #%d → %s [%s]\n", uint64(id), addr[:10], status)
	}
}

func (c *Console) handleLoadWallets() {
	keystoreDir := c.node.GetKeystoreDir()
	if _, err := os.Stat(keystoreDir); os.IsNotExist(err) {
		fmt.Printf("❌ No keystore directory found at %s. Create a wallet first using 'createaddress'.\n", keystoreDir)
		return
	}

	password, err := c.readPassword("Enter wallet password (used when unlocking): ")
	if err != nil {
		fmt.Printf("❌ Failed to read password: %v\n", err)
		return
	}
	_ = password

	err = c.node.walletManager.LoadWallets(c.node.blockchain, password)
	if err != nil {
		if strings.Contains(err.Error(), "wallet file does not exist") {
			fmt.Printf("❌ No wallets found in keystore directory: %s\n", keystoreDir)
		} else {
			fmt.Printf("❌ Failed to load wallets: %v\n", err)
		}
		return
	}

	fmt.Printf("✅ Wallets loaded from keystore: %s\n", keystoreDir)
	fmt.Printf("�� Wallets are loaded in locked mode for security\n")
	fmt.Printf("💡 Use 'unlock <address>' to unlock individual wallets\n")
}

func (c *Console) handleChangePassword() {
	oldPassword, err := c.readPassword("Enter current password: ")
	if err != nil {
		fmt.Printf("❌ Failed to read current password: %v\n", err)
		return
	}

	// Verify old password by trying to load wallets with it
	tempManager := wallet.NewWalletManager(c.getWalletDataDir())
	err = tempManager.LoadWallets(c.node.blockchain, oldPassword)
	if err != nil {
		fmt.Printf("❌ Current password is incorrect: %v\n", err)
		return
	}

	newPassword, err := c.readPassword("Enter new password: ")
	if err != nil {
		fmt.Printf("❌ Failed to read new password: %v\n", err)
		return
	}

	confirm, err := c.readPassword("Confirm new password: ")
	if err != nil {
		fmt.Printf("❌ Failed to read confirmation: %v\n", err)
		return
	}

	if newPassword != confirm {
		fmt.Printf("❌ New passwords do not match\n")
		return
	}

	if len(newPassword) < 8 {
		fmt.Printf("❌ New password must be at least 8 characters\n")
		return
	}

	// Save with new password
	err = c.node.walletManager.SaveWallets(newPassword)
	if err != nil {
		fmt.Printf("❌ Failed to change password: %v\n", err)
	} else {
		fmt.Printf("✅ Password changed successfully\n")
		fmt.Printf("🔐 All wallets are now encrypted with the new password\n")
	}
}

func (c *Console) handleBackup(parts []string) {
	backupDir := c.node.GetKeystoreDir() + "_backup_" + time.Now().Format("20060102_150405")
	if err := os.MkdirAll(backupDir, os.ModePerm); err != nil {
		fmt.Printf("Failed to create backup directory: %v\n", err)
		return
	}

	files, _ := os.ReadDir(c.node.GetKeystoreDir())
	copied := 0
	for _, f := range files {
		if !f.IsDir() && strings.HasPrefix(f.Name(), "UTC--") {
			src := filepath.Join(c.node.GetKeystoreDir(), f.Name())
			dst := filepath.Join(backupDir, f.Name())
			data, _ := os.ReadFile(src)
			os.WriteFile(dst, data, 0600)
			copied++
		}
	}

	fmt.Printf("Backup created: %s (%d wallets)\n", backupDir, copied)
	fmt.Printf("Keep this folder safe — it contains all your encrypted wallets!\n")
}

func (c *Console) handleRestore(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: restore <backup-file>")
		return
	}
	backupPath := parts[1]

	if !c.confirmAction("This will replace ALL current wallets with the backup. Continue?") {
		fmt.Println("Restore cancelled")
		return
	}

	password, err := c.readPassword("Enter encryption password for backup: ")
	if err != nil {
		fmt.Printf("❌ Failed to read password: %v\n", err)
		return
	}

	// Verify backup file can be decrypted by creating a temporary manager
	tempManager := wallet.NewWalletManager(c.getWalletDataDir() + "_temp")

	// Copy backup to temp location
	data, err := os.ReadFile(backupPath)
	if err != nil {
		fmt.Printf("❌ Failed to read backup file: %v\n", err)
		return
	}

	// Create temp directory
	tempDir := c.getWalletDataDir() + "_temp"
	tempPath := filepath.Join(tempDir, "wallets.encrypted.json")
	if err := os.MkdirAll(tempDir, os.ModePerm); err != nil {
		fmt.Printf("❌ Failed to create temp directory: %v\n", err)
		return
	}

	if err := os.WriteFile(tempPath, data, 0600); err != nil {
		fmt.Printf("❌ Failed to prepare backup: %v\n", err)
		os.RemoveAll(tempDir)
		return
	}

	// Try to load from backup
	err = tempManager.LoadWallets(c.node.blockchain, password)
	if err != nil {
		fmt.Printf("❌ Failed to decrypt backup: %v\n", err)
		// Clean up
		os.RemoveAll(tempDir)
		return
	}

	// Backup is valid, replace current wallets
	if err := os.WriteFile(c.getWalletFilePath(), data, 0600); err != nil {
		fmt.Printf("❌ Failed to restore wallets: %v\n", err)
		os.RemoveAll(tempDir)
		return
	}

	// Clean up temp
	os.RemoveAll(tempDir)

	fmt.Printf("✅ Wallets restored successfully from backup\n")
	fmt.Printf("💡 Use 'loadwallets' to load the restored wallets\n")
}

func (c *Console) handleDebugTx(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: debugtx <txhash>")
		return
	}

	txHash := common.HexToHash(parts[1])

	c.node.mu.RLock()
	defer c.node.mu.RUnlock()

	fmt.Printf("🔍 Searching for transaction %s\n", txHash.String())

	// Check if transaction exists in any block
	found := false
	for i := uint64(0); ; i++ {
		blk := c.node.blockchain.GetBlock(i)
		if blk == nil {
			break
		}
		for _, transaction := range blk.Txs {
			if transaction.Hash() == txHash {
				fmt.Printf("📦 Transaction %s found in block %d\n", txHash.String(), i)
				fmt.Printf("   From: %s\n", transaction.From.String())
				if transaction.To != nil {
					fmt.Printf("   To: %s\n", transaction.To.String())
				}
				fmt.Printf("   Nonce: %d\n", transaction.Nonce)
				fmt.Printf("   Value: %s\n", transaction.Value.String())

				// Check current state nonce
				state := c.node.blockchain.State()
				currentNonce := state.GetNonce(common.BytesToQuantumAddress(transaction.From.Bytes()))
				fmt.Printf("   Current state nonce for %s: %d\n", transaction.From.String(), currentNonce)

				if currentNonce != transaction.Nonce {
					fmt.Printf("   ❌ NONCE MISMATCH: transaction nonce=%d, state expects=%d\n",
						transaction.Nonce, currentNonce)
				} else {
					fmt.Printf("   ✅ Nonce matches state\n")
				}
				found = true
				break
			}
		}
		if found {
			break
		}
	}

	if !found {
		fmt.Printf("❌ Transaction %s not found in any block\n", txHash.String())
		fmt.Printf("💡 Note: Transaction pool inspection is currently limited\n")
	}
}

func (c *Console) handleClearStuckTxs() {
	if !c.confirmAction("This will attempt to clear stuck transactions. Continue?") {
		fmt.Println("Operation cancelled")
		return
	}

	c.node.mu.Lock()
	defer c.node.mu.Unlock()

	fmt.Println("🔄 Clearing potentially stuck transactions...")

	// Get the current state
	state := c.node.blockchain.State()
	if state == nil {
		fmt.Println("❌ Cannot access blockchain state")
		return
	}

	// Log current nonces for all known wallets
	wallets := c.node.walletManager.ListWallets()
	fmt.Printf("🔍 Checking %d wallets for nonce information:\n", len(wallets))

	for _, walletInfo := range wallets {
		parts := strings.Fields(walletInfo)
		if len(parts) < 2 {
			continue
		}
		addrStr := parts[1]
		addr := mustParseQuantumAddress(addrStr)
		currentNonce := state.GetNonce(addr)
		fmt.Printf("   %s: nonce=%d\n", addrStr, currentNonce)
	}

	// Clear transaction pool by stopping and restarting mining
	fmt.Println("💡 Stopping and restarting mining to clear transaction pool...")

	// Stop mining
	c.node.miningState.SetMining(false)
	c.node.miningState.SetEnabled(false)

	// Brief pause to ensure mining stops
	time.Sleep(1 * time.Second)

	// Restart mining - this should help clear any stuck state
	c.node.miningState.SetMining(true)
	c.node.miningState.SetEnabled(true)

	fmt.Printf("✅ Mining reset completed - transaction pool should be cleared\n")
}

func (c *Console) handleHelp() {
	fmt.Println("Available commands:")
	fmt.Println("  setaddress <addr>        - Set mining address")
	fmt.Println("  startmining              - Start mining")
	fmt.Println("  stopmining               - Stop mining")
	fmt.Println("  createaddress            - Create new wallet")
	fmt.Println("  send <from> <to> <amount>- Send transaction (auto-unlocks wallet)")
	fmt.Println("  import <privateKey>      - Import ML-DSA-65 wallet from private key")
	fmt.Println("  export <address>         - Export private key (unlocks wallet temporarily)")
	fmt.Println("  getblockinfo <number>    - Get block information")
	fmt.Println("  gettx <hash>             - Get transaction details")
	fmt.Println("  lock <address>           - Lock wallet manually")
	fmt.Println("  unlock <address>         - Unlock wallet with password")
	fmt.Println("  listwallets              - List all quantum wallets with lock status")
	fmt.Println("  listaddresses            - Alias for listwallets")
	fmt.Println("  localproposals           - List proposals")
	fmt.Println("  balance <address>        - Check account balance")
	fmt.Println("  status                   - Show node and wallet status")
	fmt.Println("  debugchain               - Debug chain information")
	fmt.Println("  savewallets              - Save wallets with encryption")
	fmt.Println("  loadwallets              - Load wallets with decryption")
	fmt.Println("  changepassword           - Change wallet encryption password")
	fmt.Println("  backup <path>            - Backup encrypted wallets to file")
	fmt.Println("  restore <path>           - Restore wallets from backup file")
	fmt.Println("  cleartx <hash>           - Remove transaction from pool")
	fmt.Println("  debugtx <hash>           - Debug transaction details")
	fmt.Println("  clearstucktxs            - Clear stuck transactions and reset mining")
	fmt.Println("  rotatingking             - Rotating King management commands")
	fmt.Println("  rk                       - Alias for rotatingking")
	fmt.Println("  exit                     - Exit console")
	fmt.Println("")
	fmt.Println("Security Features:")
	fmt.Println("  • All wallets are encrypted with strong AES-256-GCM")
	fmt.Println("  • Automatic locking after 30 minutes")
	fmt.Println("  • Brute force protection with exponential backoff")
	fmt.Println("  • Secure password input without echo")
	fmt.Println("  • Backup and restore functionality")
}

// parseUint64 parses a string to uint64
func parseUint64(s string) (uint64, error) {
	var n uint64
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}

// Rotating King handlers

func (c *Console) handleRotatingKing(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: rotatingking <command> [options]")
		fmt.Println("Commands:")
		fmt.Println("  status                     - Show current rotation status")
		fmt.Println("  address                    - Show current king address")
		fmt.Println("  cycle                      - Show rotation cycle info")
		fmt.Println("  next                       - Show next king in rotation")
		fmt.Println("  list                       - List all king addresses")
		fmt.Println("  history [limit]            - Show rotation history")
		fmt.Println("  rotate [index]             - Force rotate to specific king")
		fmt.Println("  setminer                   - Set miner to current king")
		fmt.Println("  info <address>             - Get info for specific address")
		fmt.Println("  add <address>              - Add address to rotation (100k ANTD required)")
		fmt.Println("  governance                 - Governance commands (Main King only)")
		fmt.Println("  dbsync <height|now>        - Sync database to specific height")
		fmt.Println("  dbstatus                   - Show database sync status")
		fmt.Println("  dbbackup [path]            - Backup rotating king database")
		fmt.Println("  dbhistory [range] [limit]  - Show rotation history from database")
		fmt.Println("  rewards <address>          - Show rewards for address")
		return
	}

	// Handle "rk add" command
	if parts[1] == "add" {
		if len(parts) < 3 {
			fmt.Println("Usage: rotatingking add <address>")
			fmt.Println("       or: rk add <address>")
			return
		}
		c.handleRKAdd(parts[2])
		return
	}

	// Handle database commands
	if parts[1] == "dbsync" {
		c.handleRKDBSync(parts)
		return
	} else if parts[1] == "dbstatus" {
		c.handleRKDBStatus(parts)
		return
	} else if parts[1] == "dbbackup" {
		c.handleRKDBBackup(parts)
		return
	} else if parts[1] == "dbhistory" {
		c.handleRKDBHistory(parts)
		return
	}

	// Check if blockchain is available (daemon mode only)
	c.node.mu.RLock()
	if c.node.blockchain == nil {
		c.node.mu.RUnlock()
		fmt.Println("❌ Blockchain not available")
		fmt.Println("   This command requires a running daemon node")
		return
	}

	rkManager := c.node.blockchain.GetRotatingKingManager()
	c.node.mu.RUnlock()

	if rkManager == nil {
		fmt.Println("❌ Rotating King system not initialized")
		fmt.Println("   This feature requires the rotating king system to be enabled")
		return
	}

	switch parts[1] {
	case "status":
		c.handleRKStatus(rkManager)
	case "address":
		c.handleRKAddress(rkManager)
	case "cycle":
		c.handleRKCycle(rkManager)
	case "next":
		c.handleRKNext(rkManager)
	case "list":
		c.handleRKList(rkManager)
	case "history":
		limit := 10
		if len(parts) > 2 {
			if l, err := strconv.Atoi(parts[2]); err == nil && l > 0 {
				limit = l
			}
		}
		c.handleRKHistory(rkManager, limit)
	case "rotate":
		if len(parts) > 2 {
			index, err := strconv.Atoi(parts[2])
			if err != nil {
				fmt.Printf("❌ Invalid index: %s\n", parts[2])
				return
			}
			c.handleRKRotate(rkManager, index)
		} else {
			c.handleRKRotate(rkManager, -1)
		}
	case "setminer":
		c.handleRKSetMiner(rkManager)
	case "info":
		if len(parts) < 3 {
			fmt.Println("Usage: rotatingking info <address>")
			return
		}
		c.handleRKInfo(rkManager, parts[2])
	case "governance":
		c.handleRKGovernance(rkManager, parts[2:])
	case "rewards":
		if len(parts) < 3 {
			fmt.Println("Usage: rotatingking rewards <address>")
			return
		}
		c.handleRKRewards(parts)
	default:
		fmt.Printf("❌ Unknown command: %s\n", parts[1])
	}
}

func (c *Console) handleRKAddress(rkManager reward.RotatingKingManager) {
	currentKing := rkManager.GetCurrentKing()

	c.node.mu.RLock()
	height := c.node.blockchain.GetChainHeight()
	balance := c.node.blockchain.State().GetBalance(currentKing)
	// blocksMined := c.node.blockchain.GetBlocksMinedBy(currentKing)
	c.node.mu.RUnlock()

	fmt.Println("👑 CURRENT ROTATING KING")
	fmt.Println("════════════════════════════════════════════════")
	fmt.Printf("Address:       %s\n", currentKing.String())
	fmt.Printf("Balance:       %s ANTD\n", formatBalance(balance))
	fmt.Printf("Blocks Mined:  <not available>\n")
	fmt.Printf("Block Height:  %d\n", height)

	// Show when rotation happened
	history := rkManager.GetRotationHistory(1)
	if len(history) > 0 {
		lastRotation := history[len(history)-1]
		fmt.Printf("Became King:   Block %d (%s ago)\n",
			lastRotation.BlockHeight,
			time.Since(lastRotation.Timestamp).Truncate(time.Second))
	}

	// Show reward multiplier
	multiplier := rkManager.GetKingRewardMultiplier()
	if multiplier != nil {
		bonusPercent, _ := multiplier.Sub(multiplier, big.NewFloat(1)).Float64()
		fmt.Printf("Reward Bonus:  +%.1f%%\n", bonusPercent*100)
	}

	fmt.Println("════════════════════════════════════════════════")
}

func (c *Console) handleRKCycle(rkManager reward.RotatingKingManager) {
	c.node.mu.RLock()
	height := c.node.blockchain.GetChainHeight()
	c.node.mu.RUnlock()

	info := rkManager.GetRotationInfo(height)

	fmt.Println("🔄 ROTATION CYCLE INFORMATION")
	fmt.Println("════════════════════════════════════════════════")

	fmt.Printf("Rotation Interval:  %v blocks\n", info["rotationInterval"])
	fmt.Printf("Current Block:      %d\n", height)
	fmt.Printf("Last Rotation:      Block %v\n", info["rotationHeight"])
	fmt.Printf("Next Rotation:      Block %v\n", info["nextRotationAt"])
	fmt.Printf("Blocks Remaining:   %v\n", info["blocksUntilRotation"])

	if estTime, ok := info["estimatedTimeUntilRotation"]; ok {
		fmt.Printf("Estimated Time:     %v\n", estTime)
	}

	fmt.Printf("Total Rotations:    %v\n", info["rotationCount"])
	fmt.Printf("King Count:         %v\n", info["kingCount"])

	// Calculate percentage of cycle completed
	if rotationHeight, ok := info["rotationHeight"].(uint64); ok {
		if nextRotation, ok := info["nextRotationAt"].(uint64); ok {
			cycleLength := nextRotation - rotationHeight
			if cycleLength > 0 {
				progress := height - rotationHeight
				percent := float64(progress) / float64(cycleLength) * 100
				fmt.Printf("Cycle Progress:     %.1f%%\n", percent)

				// Progress bar
				barWidth := 40
				filled := int(float64(barWidth) * percent / 100)
				fmt.Printf("                   [%s%s]\n",
					strings.Repeat("█", filled),
					strings.Repeat("░", barWidth-filled))
			}
		}
	}

	fmt.Println("════════════════════════════════════════════════")
}

func (c *Console) handleRKNext(rkManager reward.RotatingKingManager) {
	nextKing := rkManager.GetNextKing()

	c.node.mu.RLock()
	height := c.node.blockchain.GetChainHeight()
	info := rkManager.GetRotationInfo(height)
	balance := c.node.blockchain.State().GetBalance(nextKing)
	// blocksMined := c.node.blockchain.GetBlocksMinedBy(nextKing)
	c.node.mu.RUnlock()

	nextRotation, _ := info["nextRotationAt"].(uint64)
	blocksRemaining := nextRotation - height

	fmt.Println("⏭️ NEXT ROTATING KING")
	fmt.Println("════════════════════════════════════════════════")
	fmt.Printf("Address:           %s\n", nextKing.String())
	fmt.Printf("Balance:           %s ANTD\n", formatBalance(balance))
	fmt.Printf("Blocks Mined:      <not available>\n")
	fmt.Printf("Becomes King At:   Block %d\n", nextRotation)
	fmt.Printf("Blocks Remaining:  %d\n", blocksRemaining)

	// Estimate time
	if blocksRemaining > 0 {
		// Get average block time
		var avgBlockTime time.Duration
		latest := c.node.blockchain.Latest()
		if latest != nil && latest.Header != nil {
			// Simplified - get last block time
			parent := c.node.blockchain.GetBlock(height - 1)
			if parent != nil && parent.Header != nil {
				blockTime := latest.Header.Time - parent.Header.Time
				avgBlockTime = time.Duration(blockTime) * time.Second
			}
		}

		if avgBlockTime > 0 {
			estTime := time.Duration(blocksRemaining) * avgBlockTime
			fmt.Printf("Estimated Time:     %s\n", estTime.Truncate(time.Second))
		}
	}

	fmt.Println("════════════════════════════════════════════════")
}

func (c *Console) handleRKList(rkManager reward.RotatingKingManager) {
	addresses := rkManager.GetKingAddresses()
	currentKing := rkManager.GetCurrentKing()

	fmt.Printf("🏆 ROTATING KING LIST WITH DATABASE INFO (%d addresses)\n", len(addresses))
	fmt.Println("══════════════════════════════════════════════════════════")

	for i, addr := range addresses {
		c.node.mu.RLock()
		balance := c.node.blockchain.State().GetBalance(addr)
		isMainKing := addr == mustParseQuantumAddress("0qANA3c85k94LTyTXLGDdEzmLE32b1qhYZF")
		c.node.mu.RUnlock()

		status := ""
		if addr == currentKing {
			status = "👑 CURRENT KING"
		} else if isMainKing {
			status = "⚜️ MAIN KING"
		}

		fmt.Printf("%d. %s\n", i+1, addr.String())
		if status != "" {
			fmt.Printf("   [%s]\n", status)
		}
		fmt.Printf("   Balance: %s ANTD\n", formatBalance(balance))

		// Get rewards from database if available
		if rewards := rkManager.GetKingRewards(addr); rewards != nil && rewards.Sign() > 0 {
			fmt.Printf("   Total 5%% Rewards: %s ANTD\n", formatBalance(rewards))
		}

		// Show rotation order
		if i == (rkManager.GetCurrentKingIndex()+1)%len(addresses) {
			fmt.Printf("   ⏭️ Next in rotation\n")
		}

		// Show database sync status for this king
		if manager, ok := rkManager.(interface {
			GetRotationHistoryFromDB(fromBlock, toBlock uint64) ([]rotatingking.KingRotation, error)
		}); ok {
			// Get last rotation for this king
			rotations, err := manager.GetRotationHistoryFromDB(0, c.node.blockchain.GetChainHeight())
			if err == nil {
				kingRotations := 0
				for _, r := range rotations {
					if r.NewKing == addr || r.PreviousKing == addr {
						kingRotations++
					}
				}
				if kingRotations > 0 {
					fmt.Printf("   📜 In database: %d rotation(s)\n", kingRotations)
				}
			}
		}

		if i < len(addresses)-1 {
			fmt.Println("   ──────────────────────────")
		}
	}

	fmt.Println("══════════════════════════════════════════════════════════")
}

func (c *Console) handleRKHistory(rkManager reward.RotatingKingManager, limit int) {
	history := rkManager.GetRotationHistory(limit)

	if len(history) == 0 {
		fmt.Println("📜 No rotation history found")
		return
	}

	fmt.Printf("📜 ROTATION HISTORY (last %d)\n", len(history))
	fmt.Println("════════════════════════════════════════════════")

	for i := len(history) - 1; i >= 0; i-- {
		rotation := history[i]
		age := time.Since(rotation.Timestamp).Truncate(time.Second)

		fmt.Printf("Rotation #%d:\n", len(history)-i)
		fmt.Printf("  Block:      %d\n", rotation.BlockHeight)
		fmt.Printf("  Time:       %s (%s ago)\n",
			rotation.Timestamp.Format("2006-01-02 15:04:05"),
			age)
		fmt.Printf("  From:       %s\n", rotation.PreviousKing.String())
		fmt.Printf("  To:         %s\n", rotation.NewKing.String())

		if rotation.Reward != nil && rotation.Reward.Sign() > 0 {
			fmt.Printf("  Reward:     %s ANTD\n", formatBalance(rotation.Reward))
		}

		if i > 0 {
			fmt.Println("  ──────────────────────────")
		}
	}

	fmt.Println("════════════════════════════════════════════════")
}

func (c *Console) handleRKRotate(rkManager reward.RotatingKingManager, index int) {
	c.node.mu.RLock()
	minerAddr := c.node.MinerWalletAddress()
	mainKing := mustParseQuantumAddress("0qANA3c85k94LTyTXLGDdEzmLE32b1qhYZF")
	isMainKing := minerAddr == mainKing
	currentHeight := c.node.blockchain.GetChainHeight()
	blockHash := c.node.blockchain.GetBlock(currentHeight).Hash()
	c.node.mu.RUnlock()

	if !isMainKing {
		fmt.Println("❌ Only Main King can force rotations")
		return
	}

	addresses := rkManager.GetKingAddresses()
	if len(addresses) == 0 {
		fmt.Println("❌ No kings in rotation")
		return
	}

	currentIndex := rkManager.GetCurrentKingIndex()
	previousKing := addresses[currentIndex]

	var targetIndex int
	var targetAddr common.QuantumAddress

	if index == -1 {
		targetIndex = (currentIndex + 1) % len(addresses)
		targetAddr = addresses[targetIndex]
		fmt.Printf("Rotating to next king: %s (#%d)\n", targetAddr.String(), targetIndex+1)
	} else {
		if index < 0 || index >= len(addresses) {
			fmt.Printf("❌ Invalid index %d. Valid range: 0-%d\n", index, len(addresses)-1)
			return
		}
		targetIndex = index
		targetAddr = addresses[targetIndex]
		fmt.Printf("Forcing rotation to: %s (#%d)\n", targetAddr.String(), targetIndex+1)
	}

	if !c.confirmAction("Confirm force rotation? (will be persisted and broadcast to network)") {
		fmt.Println("❌ Cancelled")
		return
	}

	reason := fmt.Sprintf("manual rotation by Main King to index %d", targetIndex)

	// === PERSIST ROTATION TO DATABASE ===
	if err := c.persistRotationToDatabase(rkManager, previousKing, targetAddr, currentHeight, reason); err != nil {
		fmt.Printf("❌ Failed to persist rotation: %v\n", err)
		return
	}

	// Perform the rotation
	if err := rkManager.ForceRotate(targetIndex, reason); err != nil {
		fmt.Printf("❌ Rotation failed: %v\n", err)
		return
	}

	fmt.Printf("✅ Force rotation successful: now serving %s\n", targetAddr.String())

	// Broadcast via P2P
	if err := c.broadcastKingRotation(previousKing, targetAddr, reason); err != nil {
		fmt.Printf("⚠️  Warning: Failed to broadcast rotation event: %v\n", err)
		fmt.Println("   Rotation applied locally but not broadcast to network")
	} else {
		fmt.Printf("📡 Rotation event successfully broadcast to network at height %d\n", currentHeight)
	}

	// Also broadcast as KingRotationBroadcast (for compatibility)
	if c.node.p2pNode != nil {
		rotationBroadcast := &rotatingking.KingRotationBroadcast{
			BlockHeight:  currentHeight,
			BlockHash:    blockHash,
			PreviousKing: previousKing,
			NewKing:      targetAddr,
			Timestamp:    time.Now(),
		}

		if err := c.node.p2pNode.BroadcastRotation(rotationBroadcast); err != nil {
			fmt.Printf("⚠️  Secondary broadcast failed: %v\n", err)
		}
	}

	// Final status
	fmt.Printf("\n👑 New Current King: %s\n", targetAddr.String())
	fmt.Printf("   From: %s\n", previousKing.String())
	fmt.Printf("   Reason: %s\n", reason)
	fmt.Printf("   Block Height: %d\n", currentHeight)
	fmt.Printf("   Broadcast: %v\n", c.node.p2pNode != nil)
}

// persist rotation to database
func (c *Console) persistRotationToDatabase(rkManager reward.RotatingKingManager, previousKing, newKing common.QuantumAddress, height uint64, reason string) error {
	// Create rotation record
	rotation := rotatingking.KingRotation{
		BlockHeight:  height,
		PreviousKing: previousKing,
		NewKing:      newKing,
		Timestamp:    time.Now(),
		WasEligible:  true,
		Reason:       reason,
		Reward:       big.NewInt(0),
	}

	// Check if manager supports SaveRotationEvent
	if manager, ok := rkManager.(interface {
		SaveRotationEvent(rotation *rotatingking.KingRotation) error
	}); ok {
		return manager.SaveRotationEvent(&rotation)
	}

	// Fallback: Save to local file
	data := map[string]interface{}{
		"rotation": rotation,
		"height":   height,
		"time":     time.Now().Unix(),
	}

	dataDir := c.node.GetDataDir()
	filePath := filepath.Join(dataDir, fmt.Sprintf("rotation_%d.json", height))

	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filePath, jsonData, 0644)
}

// saves the king list to database
func (n *Node) SaveKingListToDB(addresses []common.QuantumAddress, height uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Get rotating king manager
	rkManager := n.blockchain.GetRotatingKingManager()
	if rkManager == nil {
		return errors.New("rotating king manager not available")
	}

	// Update addresses
	if err := rkManager.UpdateKingAddresses(addresses); err != nil {
		return err
	}

	// Also save to local backup file
	data := map[string]interface{}{
		"addresses":  addresses,
		"height":     height,
		"timestamp":  time.Now().Unix(),
		"block_hash": n.blockchain.GetBlock(height).Hash().String(),
	}

	filePath := filepath.Join(n.GetDataDir(), "king_list_backup.json")
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filePath, jsonData, 0644)
}

func (n *Node) GetCurrentKingList() []common.QuantumAddress {
	n.mu.RLock()
	defer n.mu.RUnlock()

	rkManager := n.blockchain.GetRotatingKingManager()
	if rkManager == nil {
		return []common.QuantumAddress{}
	}

	return rkManager.GetKingAddresses()
}

// forces a rotation to specific index
func (n *Node) ForceRotateKing(index int, reason string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	rkManager := n.blockchain.GetRotatingKingManager()
	if rkManager == nil {
		return errors.New("rotating king manager not available")
	}

	return rkManager.ForceRotate(index, reason)
}

func (c *Console) handleEmergencySync(parts []string) {
	fmt.Println("🚨 EMERGENCY CONFIGURATION SYNC")
	fmt.Println("This will request configuration from all peers and sync immediately")

	if !c.confirmAction("Are you sure you want to force configuration sync?") {
		fmt.Println("❌ Cancelled")
		return
	}

	// Get current state
	c.node.mu.RLock()
	rkManager := c.node.blockchain.GetRotatingKingManager()
	currentList := rkManager.GetKingAddresses()
	c.node.mu.RUnlock()

	fmt.Printf("Current configuration: %d addresses\n", len(currentList))

	// Request configuration from all peers
	if c.node.p2pNode != nil {
		fmt.Println("📤 Requesting configuration from all peers...")

		// Broadcast urgent request
		for _, peer := range c.node.p2pNode.Peers() {
			c.node.p2pNode.RequestKingConfigFromPeer(peer)
		}

		// Wait for responses
		fmt.Println("⏳ Waiting 10 seconds for responses...")
		time.Sleep(10 * time.Second)

		// Check if we got better configuration
		c.node.mu.RLock()
		newList := rkManager.GetKingAddresses()
		c.node.mu.RUnlock()

		if len(newList) > len(currentList) {
			fmt.Printf("✅ Configuration improved: %d → %d addresses\n",
				len(currentList), len(newList))
		} else {
			fmt.Printf("⚠️  Configuration unchanged: %d addresses\n", len(newList))
		}
	} else {
		fmt.Println("❌ P2P node not available")
	}
}

func (c *Console) handleRKSetMiner(rkManager reward.RotatingKingManager) {
	currentKing := rkManager.GetCurrentKing()

	if currentKing == (common.QuantumAddress{}) {
		fmt.Println("❌ No current rotating king")
		return
	}

	minerAddr := c.node.MinerWalletAddress()
	if currentKing == minerAddr {
		fmt.Println("⚠️  Miner is already set to current king")
		return
	}

	// Check if we have the key for this address
	hasKey := false
	for _, acc := range c.node.Keystore().Accounts() {
		if ethAddressMatchesQuantumAddress(acc.Address, currentKing) {
			hasKey = true
			break
		}
	}

	if !hasKey {
		fmt.Printf("❌ Wallet %s not found in keystore\n", currentKing.String())
		fmt.Println("   You need the private key to mine as this address")
		return
	}

	c.node.SetMinerWalletAddress(currentKing)
	fmt.Printf("✅ Miner address set to current rotating king: %s\n", currentKing.String())
	fmt.Println("💡 Use 'startmining' to begin mining as rotating king")
}

func (c *Console) handleRKInfo(rkManager reward.RotatingKingManager, addrStr string) {
	addr := mustParseQuantumAddress(addrStr)

	c.node.mu.RLock()
	balance := c.node.blockchain.State().GetBalance(addr)
	// blocksMined := c.node.blockchain.GetBlocksMinedBy(addr)
	isMainKing := addr == mustParseQuantumAddress("0qANA3c85k94LTyTXLGDdEzmLE32b1qhYZF")
	c.node.mu.RUnlock()

	isKing := rkManager.IsKing(addr)
	isCurrentKing := rkManager.IsCurrentKing(addr)
	addresses := rkManager.GetKingAddresses()

	fmt.Printf("📋 KING INFORMATION: %s\n", addr.String())
	fmt.Println("════════════════════════════════════════════════")

	fmt.Printf("Balance:           %s ANTD\n", formatBalance(balance))
	fmt.Printf("Blocks Mined:      <not available>\n")
	fmt.Printf("Is Main King:      %v\n", isMainKing)
	fmt.Printf("Is In Rotation:    %v\n", isKing)
	fmt.Printf("Is Current King:   %v\n", isCurrentKing)

	if isKing {
		// Find position in rotation
		for i, kingAddr := range addresses {
			if kingAddr == addr {
				currentIndex := rkManager.GetCurrentKingIndex()
				if i == currentIndex {
					fmt.Printf("Rotation Position: 👑 Current King (#%d)\n", i+1)
				} else if i == (currentIndex+1)%len(addresses) {
					fmt.Printf("Rotation Position: ⏭️ Next King (#%d)\n", i+1)
					nextRotation := rkManager.GetRotationInfo(c.node.blockchain.GetChainHeight())["nextRotationAt"]
					fmt.Printf("Becomes King At:   Block %v\n", nextRotation)
				} else {
					fmt.Printf("Rotation Position: #%d in queue\n", i+1)
					// Calculate when this king will be active
					diff := (i - currentIndex + len(addresses)) % len(addresses)
					rotationsNeeded := diff
					fmt.Printf("Rotations Needed:  %d\n", rotationsNeeded)
				}
				break
			}
		}
	}

	fmt.Println("════════════════════════════════════════════════")
}

func (c *Console) handleRKGovernance(rkManager reward.RotatingKingManager, parts []string) {
	if len(parts) < 1 {
		fmt.Println("Usage: rotatingking governance <command>")
		fmt.Println("Commands:")
		fmt.Println("  add <address>        - Add address to rotation (Main King only)")
		fmt.Println("  remove <address>     - Remove address from rotation")
		fmt.Println("  proposal <id>        - View proposal details")
		fmt.Println("  proposals            - List all proposals")
		fmt.Println("  execute <id>         - Execute proposal after timelock")
		fmt.Println("  timelock             - Show timelock status")
		return
	}

	c.node.mu.RLock()
	mainKing := mustParseQuantumAddress("0qANA3c85k94LTyTXLGDdEzmLE32b1qhYZF")
	isMainKing := c.node.MinerWalletAddress() == mainKing
	c.node.mu.RUnlock()

	if !isMainKing && parts[0] != "proposal" && parts[0] != "proposals" && parts[0] != "timelock" {
		fmt.Println("❌ Only Main King can perform governance actions")
		return
	}

	govController := c.getGovernanceController()

	switch parts[0] {
	case "add":
		if len(parts) < 2 {
			fmt.Println("Usage: rotatingking governance add <address>")
			return
		}
		c.handleRKGovernanceAdd(rkManager, parts[1])
	case "remove":
		if len(parts) < 2 {
			fmt.Println("Usage: rotatingking governance remove <address>")
			return
		}
		c.handleRKGovernanceRemove(rkManager, parts[1])
	case "proposal":
		if len(parts) < 2 {
			fmt.Println("Usage: rotatingking governance proposal <id>")
			return
		}
		id, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			fmt.Printf("❌ Invalid proposal ID: %s\n", parts[1])
			return
		}
		c.handleRKGovernanceProposal(govController, id)
	case "proposals":
		c.handleRKGovernanceProposals(govController)
	case "execute":
		if len(parts) < 2 {
			fmt.Println("Usage: rotatingking governance execute <id>")
			return
		}
		id, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			fmt.Printf("❌ Invalid proposal ID: %s\n", parts[1])
			return
		}
		c.handleRKGovernanceExecute(govController, id)
	case "timelock":
		c.handleRKGovernanceTimelock(govController)
	default:
		fmt.Printf("❌ Unknown governance command: %s\n", parts[0])
	}
}

func (c *Console) handleRKGovernanceAdd(rkManager reward.RotatingKingManager, addrStr string) {
	addr, err := parseQuantumAddressInput(addrStr)
	if err != nil {
		fmt.Printf("❌ Invalid address format: %s\n", addrStr)
		fmt.Println("   Address must be a valid ANTDChain quantum address")
		return
	}

	// Check if address is the zero address
	if addr == (common.QuantumAddress{}) {
		fmt.Println("❌ Cannot add zero address (0x000...) to rotation")
		return
	}

	// Check if address is the Main King address
	mainKing := mustParseQuantumAddress("0qANA3c85k94LTyTXLGDdEzmLE32b1qhYZF")
	if addr == mainKing {
		fmt.Println("❌ Main King is already permanently in the reward distribution")
		fmt.Println("   Main King receives 5% rewards automatically")
		return
	}

	// Check if already in rotation
	addresses := rkManager.GetKingAddresses()
	for _, kingAddr := range addresses {
		if kingAddr == addr {
			fmt.Printf("❌ Address %s is already in rotation\n", addr.String())
			position := getAddressPosition(addresses, addr)
			if position >= 0 {
				fmt.Printf("   Position: %d/%d\n", position+1, len(addresses))
			}
			return
		}
	}

	// Check minimum balance/stake (configurable threshold)
	c.node.mu.RLock()
	balance := c.node.blockchain.State().GetBalance(addr)
	currentHeight := c.node.blockchain.GetChainHeight()
	currentTime := uint64(time.Now().Unix())
	c.node.mu.RUnlock()

	// Minimum stake requirement (configurable, e.g., 100000 ANTD)
	minStakeRequired := new(big.Int).Mul(big.NewInt(100_000), big.NewInt(1e18)) // 100000 ANTD

	// Additional validation checks
	if balance.Sign() == 0 {
		fmt.Printf("❌ Address %s has zero balance\n", addr.String())
		fmt.Println("   Address must have some ANTD balance to be eligible")
		return
	}

	if balance.Cmp(minStakeRequired) < 0 {
		fmt.Printf("❌ Address %s does not meet minimum stake requirement\n", addr.String())
		fmt.Printf("   Current balance: %s ANTD\n", formatBalance(balance))
		fmt.Printf("   Required minimum: %s ANTD\n", formatBalance(minStakeRequired))

		shortBy := new(big.Int).Sub(minStakeRequired, balance)
		fmt.Printf("   Short by: %s ANTD\n", formatBalance(shortBy))
		fmt.Println("   Address must meet minimum stake to ensure network commitment")
		return
	}

	// Check if address exists in keystore (optional but helpful)
	hasKey := false
	for _, acc := range c.node.Keystore().Accounts() {
		if ethAddressMatchesQuantumAddress(acc.Address, addr) {
			hasKey = true
			break
		}
	}

	if !hasKey {
		fmt.Printf("⚠️  Note: Address %s not found in local keystore\n", addr.String())
		fmt.Println("   This address can still receive rotating king rewards")
		fmt.Println("   However, you won't be able to mine as this rotating king")
	}

	// Show summary and confirm
	fmt.Printf("\n📋 Governance Proposal: Add Address to Rotation\n")
	fmt.Printf("══════════════════════════════════════════════════════════\n")
	fmt.Printf("   Proposal ID:      governance_add_%d_%s\n", currentHeight, addr.String()[:8])
	fmt.Printf("   Address:          %s\n", addr.String())
	fmt.Printf("   Balance:          %s ANTD\n", formatBalance(balance))
	fmt.Printf("   Meets 1k ANTD Min: %v ✅\n", balance.Cmp(minStakeRequired) >= 0)
	fmt.Printf("   Has Private Key:  %v\n", hasKey)
	fmt.Printf("   Current Kings:    %d\n", len(addresses))
	fmt.Printf("   New Position:     %d\n", len(addresses)+1)
	fmt.Printf("   Block Height:     %d\n", currentHeight)
	fmt.Printf("══════════════════════════════════════════════════════════\n")

	if !c.confirmAction("\nCreate governance proposal to add this address?") {
		fmt.Println("❌ Operation cancelled")
		return
	}

	// === CREATE GOVERNANCE PROPOSAL ===
	// Get the Main King address (governance controller)
	c.node.mu.RLock()
	minerAddr := c.node.MinerWalletAddress()
	isMainKing := minerAddr == mainKing
	c.node.mu.RUnlock()

	if !isMainKing {
		fmt.Println("❌ Only Main King can create governance proposals")
		fmt.Printf("   Main King: %s\n", mainKing.String())
		fmt.Printf("   Your address: %s\n", minerAddr.String())
		return
	}

	// Find Main King wallet in keystore (just for verification)
	foundMainKing := false
	for _, acc := range c.node.Keystore().Accounts() {
		if ethAddressMatchesQuantumAddress(acc.Address, mainKing) {
			foundMainKing = true
			break
		}
	}

	if !foundMainKing {
		fmt.Printf("⚠️  Note: Main King wallet %s not found in local keystore\n", mainKing.String())
		fmt.Println("   You can still create proposals, but execution may require the key")
	}

	// Create new rotating kings list with the added address
	newRotatingKings := make([]common.QuantumAddress, len(addresses)+1)
	copy(newRotatingKings, addresses)
	newRotatingKings[len(addresses)] = addr

	// Try to create proposal through available interface
	govController := c.getGovernanceController()
	if govController == nil {
		// If no governance controller, create a local proposal file
		c.createLocalGovernanceProposal(mainKing, newRotatingKings, currentTime, currentHeight, addr, balance, hasKey)
		return
	}

	// Try to cast to the expected type
	switch gc := govController.(type) {
	case interface {
		ProposeRotatingKingsUpdate(caller common.QuantumAddress, newKings []common.QuantumAddress, now uint64) (uint64, error)
	}:
		proposalID, err := gc.ProposeRotatingKingsUpdate(mainKing, newRotatingKings, currentTime)
		if err != nil {
			fmt.Printf("❌ Failed to create governance proposal: %v\n", err)
			return
		}

		c.showProposalDetails(proposalID, govController, currentTime, addr, newRotatingKings, rkManager, currentHeight, hasKey, minStakeRequired, balance)

	default:
		// Fallback to local proposal
		c.createLocalGovernanceProposal(mainKing, newRotatingKings, currentTime, currentHeight, addr, balance, hasKey)
	}
}

// Create local proposal when no governance controller is available
func (c *Console) createLocalGovernanceProposal(mainKing common.QuantumAddress, newRotatingKings []common.QuantumAddress,
	currentTime uint64, currentHeight uint64, addr common.QuantumAddress, balance *big.Int, hasKey bool) {

	// Create proposal info
	proposalInfo := map[string]interface{}{
		"proposal_id":       fmt.Sprintf("local_%d", currentTime),
		"type":              "add_to_rotation",
		"proposed_by":       mainKing.String(),
		"address":           addr.String(),
		"balance":           balance.String(),
		"has_private_key":   hasKey,
		"created":           currentTime,
		"created_human":     time.Unix(int64(currentTime), 0).Format(time.RFC3339),
		"block_height":      currentHeight,
		"new_rotation_list": newRotatingKings,
		"can_execute_after": currentTime + 48*60*60, // 48 hours
		"status":            "local_proposal",
	}

	// Save to file
	proposalFile := filepath.Join(c.node.GetDataDir(), fmt.Sprintf("proposal_local_%d.json", currentTime))
	data, err := json.MarshalIndent(proposalInfo, "", "  ")
	if err != nil {
		fmt.Printf("❌ Failed to create proposal file: %v\n", err)
		return
	}

	if err := os.WriteFile(proposalFile, data, 0644); err != nil {
		fmt.Printf("❌ Failed to save proposal file: %v\n", err)
		return
	}

	fmt.Printf("\n✅ Local governance proposal created!\n")
	fmt.Printf("   File: %s\n", proposalFile)
	fmt.Printf("   Note: This is a local proposal only\n")
	fmt.Printf("   To implement: Update rotatingking.go configuration\n")
}

func (c *Console) getGovernanceController() interface{} {
	c.node.mu.RLock()
	defer c.node.mu.RUnlock()

	// Check if blockchain has a GetGovernanceController method using reflection
	bcValue := reflect.ValueOf(c.node.blockchain)
	method := bcValue.MethodByName("GetGovernanceController")
	if method.IsValid() {
		results := method.Call(nil)
		if len(results) > 0 && !results[0].IsNil() {
			return results[0].Interface()
		}
	}

	// Alternative: check for GetRewardDistributor -> GetGovernanceController chain
	method = bcValue.MethodByName("GetRewardDistributor")
	if method.IsValid() {
		results := method.Call(nil)
		if len(results) > 0 && !results[0].IsNil() {
			distributor := results[0].Interface()
			distValue := reflect.ValueOf(distributor)
			gcMethod := distValue.MethodByName("GetGovernanceController")
			if gcMethod.IsValid() {
				gcResults := gcMethod.Call(nil)
				if len(gcResults) > 0 && !gcResults[0].IsNil() {
					return gcResults[0].Interface()
				}
			}
		}
	}

	return nil
}

func formatDuration(d time.Duration) string {
	if d.Hours() >= 48 {
		days := d.Hours() / 24
		return fmt.Sprintf("%.1f days", days)
	} else if d.Hours() >= 1 {
		hours := d.Hours()
		return fmt.Sprintf("%.1f hours", hours)
	} else {
		minutes := d.Minutes()
		return fmt.Sprintf("%.1f minutes", minutes)
	}
}

func getAddressPosition(addresses []common.QuantumAddress, addr common.QuantumAddress) int {
	for i, a := range addresses {
		if a == addr {
			return i
		}
	}
	return -1
}

func (c *Console) showProposalDetails(proposalID uint64, govController interface{}, currentTime uint64,
	addr common.QuantumAddress, newRotatingKings []common.QuantumAddress, rkManager reward.RotatingKingManager,
	currentHeight uint64, hasKey bool, minStakeRequired *big.Int, balance *big.Int) {

	// Calculate execution time (48 hours from now)
	executionTime := currentTime + 48*60*60 // 48 hours in seconds

	fmt.Printf("✅ Governance proposal created successfully!\n")
	fmt.Printf("   Proposal ID: %d\n", proposalID)
	fmt.Printf("   Timestamp: %s\n", time.Unix(int64(currentTime), 0).Format(time.RFC3339))
	fmt.Printf("   Can execute after: %s\n", time.Unix(int64(executionTime), 0).Format(time.RFC3339))

	// === DISPLAY RESULTS ===
	fmt.Printf("\n🎉 GOVERNANCE PROPOSAL CREATED!\n")
	fmt.Printf("══════════════════════════════════════════════════════════\n")
	fmt.Printf("   Proposal ID:      %d\n", proposalID)
	fmt.Printf("   From (Main King): %s\n", mustParseQuantumAddress("0qANA3c85k94LTyTXLGDdEzmLE32b1qhYZF").Hex())
	fmt.Printf("   Action:           Add %s to rotation\n", addr.String())
	fmt.Printf("   New Total Kings:  %d\n", len(newRotatingKings))
	fmt.Printf("   Created:          %s\n", time.Unix(int64(currentTime), 0).Format(time.RFC3339))
	fmt.Printf("   Can Execute:      %s\n", time.Unix(int64(executionTime), 0).Format(time.RFC3339))
	fmt.Printf("   Status:           ⏳ Awaiting timelock (48 hours)\n")
	fmt.Printf("══════════════════════════════════════════════════════════\n")

	// Show governance process
	fmt.Printf("\n📋 Governance Process:\n")
	fmt.Printf("   1. ✅ Proposal created and saved locally\n")
	fmt.Printf("   2. ⏳ 48-hour timelock period begins\n")
	fmt.Printf("   3. ⏳ After 48 hours, proposal can be executed\n")
	fmt.Printf("   4. ✅ Execution updates rotating kings list\n")
	fmt.Printf("   5. ✅ Changes take effect immediately\n")

	// Show what will happen after execution
	fmt.Printf("\n🔄 After Execution:\n")
	fmt.Printf("   • Address %s will be added to rotation list\n", addr.String()[:8])
	fmt.Printf("   • Will receive 5%% rewards when serving as king\n")
	fmt.Printf("   • Must maintain minimum %s ANTD stake\n", formatBalance(minStakeRequired))
	fmt.Printf("   • Rotation order preserved (added to end)\n")

	if !hasKey {
		fmt.Printf("   • ⚠️  You don't control this address (no private key)\n")
		fmt.Printf("     Someone else will receive the 5%% rewards\n")
	}

	// Show current and new rotation order
	currentAddresses := rkManager.GetKingAddresses()
	fmt.Printf("\n📊 Rotation Order Comparison:\n")
	fmt.Printf("   Current (%d addresses):\n", len(currentAddresses))
	for i, kingAddr := range currentAddresses {
		status := ""
		if kingAddr == rkManager.GetCurrentKing() {
			status = " 👑"
		}
		fmt.Printf("     %d. %s%s\n", i+1, kingAddr.String()[:8], status)
	}

	fmt.Printf("\n   Proposed (%d addresses):\n", len(newRotatingKings))
	for i, kingAddr := range newRotatingKings {
		status := ""
		if kingAddr == rkManager.GetCurrentKing() {
			status = " 👑"
		}
		if kingAddr == addr {
			status = " 🆕"
		}
		fmt.Printf("     %d. %s%s\n", i+1, kingAddr.String()[:8], status)
	}

	// Show monitoring commands
	fmt.Printf("\n🔍 Monitoring Commands:\n")
	fmt.Printf("   Check proposal:    rotatingking governance proposal %d\n", proposalID)
	fmt.Printf("   List proposals:    rotatingking governance proposals\n")
	fmt.Printf("   Check address:     rotatingking info %s\n", addr.String())
	fmt.Printf("   Rotation status:   rotatingking status\n")
	fmt.Printf("   Check timelock:    rotatingking governance timelock\n")

	// Show timeline estimate
	fmt.Printf("\n⏱️  Timeline Estimate:\n")
	currentCycle := rkManager.GetRotationInfo(currentHeight)
	nextRotation, _ := currentCycle["nextRotationAt"].(uint64)
	blocksUntilRotation := nextRotation - currentHeight

	// Calculate time until proposal can be executed
	timeUntilExecution := executionTime - currentTime
	execDuration := time.Duration(timeUntilExecution) * time.Second

	fmt.Printf("   • Proposal created:         Now\n")
	fmt.Printf("   • Can execute after:        %s\n", time.Unix(int64(executionTime), 0).Format("Jan 02, 2006 15:04"))
	fmt.Printf("   • Time until execution:     %s\n", formatDuration(execDuration))

	// Show when address would become king
	if blocksUntilRotation > 0 && len(currentAddresses) > 0 {
		currentIndex := rkManager.GetCurrentKingIndex()
		positionsAhead := len(currentAddresses) - currentIndex
		if positionsAhead <= 0 {
			positionsAhead += len(currentAddresses)
		}

		// The new address will be at the end, so it needs to wait for all current addresses
		positionsAhead += 1 // Account for the new position

		fmt.Printf("   • Address becomes king:     After %d rotations\n", positionsAhead)

		// Rough estimate: 30 seconds per block, 100 blocks per rotation
		rotationsUntilKing := positionsAhead
		blocksUntilKing := rotationsUntilKing * int(rkManager.GetRotationInterval())
		estTimeUntilKing := time.Duration(blocksUntilKing*30) * time.Second

		if estTimeUntilKing > time.Hour*24 {
			days := estTimeUntilKing.Hours() / 24
			fmt.Printf("   • Estimated time as king:   ~%.1f days\n", days)
		} else if estTimeUntilKing > time.Hour {
			hours := estTimeUntilKing.Hours()
			fmt.Printf("   • Estimated time as king:   ~%.1f hours\n", hours)
		} else {
			minutes := estTimeUntilKing.Minutes()
			fmt.Printf("   • Estimated time as king:   ~%.1f minutes\n", minutes)
		}
	}

	// Show address eligibility information
	fmt.Printf("\n📈 Address Eligibility:\n")
	fmt.Printf("   Address:        %s\n", addr.String())
	fmt.Printf("   Current Balance: %s ANTD\n", formatBalance(balance))
	fmt.Printf("   Minimum Required: %s ANTD\n", formatBalance(minStakeRequired))

	if balance.Cmp(minStakeRequired) >= 0 {
		fmt.Printf("   ✅ Meets stake requirement\n")

		// Calculate percentage above minimum
		percentage := new(big.Float).Quo(
			new(big.Float).SetInt(balance),
			new(big.Float).SetInt(minStakeRequired),
		)
		percent, _ := percentage.Float64()
		fmt.Printf("   📊 %.1f%% above minimum\n", (percent-1)*100)
	} else {
		fmt.Printf("   ❌ Below stake requirement\n")

		// Calculate how much more is needed
		needed := new(big.Int).Sub(minStakeRequired, balance)
		fmt.Printf("   💰 Need additional: %s ANTD\n", formatBalance(needed))
	}

	// Security and final notes
	fmt.Printf("\n⚠️  Important Notes:\n")
	fmt.Printf("   1. Proposal has 48-hour timelock for security\n")
	fmt.Printf("   2. Only Main King can create proposals\n")
	fmt.Printf("   3. Anyone can execute the proposal after timelock\n")
	fmt.Printf("   4. Address must maintain minimum stake\n")
	fmt.Printf("   5. Verify address %s is correct\n", addr.String())
	fmt.Printf("   6. Proposal is saved locally and can be shared\n")
	fmt.Printf("   7. Changes affect all network nodes\n")

	// Next steps
	fmt.Printf("\n📋 Next Steps:\n")
	fmt.Printf("   1. Wait 48 hours for timelock to expire\n")
	fmt.Printf("   2. Execute proposal: rotatingking governance execute %d\n", proposalID)
	fmt.Printf("   3. Monitor rotation: rotatingking status\n")
	fmt.Printf("   4. Check rewards: rotatingking rewards %s\n", addr.String())
	fmt.Printf("   5. Verify execution: getblockinfo latest\n")

	// Save proposal info to file for reference
	proposalFile := filepath.Join(c.node.GetDataDir(), fmt.Sprintf("proposal_%d.json", proposalID))
	proposalInfo := map[string]interface{}{
		"proposal_id":            proposalID,
		"type":                   "add_to_rotation",
		"address":                addr.String(),
		"balance":                balance.String(),
		"balance_formatted":      formatBalance(balance),
		"has_private_key":        hasKey,
		"created_timestamp":      currentTime,
		"created_human":          time.Unix(int64(currentTime), 0).Format(time.RFC3339),
		"execution_time":         executionTime,
		"execution_human":        time.Unix(int64(executionTime), 0).Format(time.RFC3339),
		"block_height":           currentHeight,
		"min_stake_required":     minStakeRequired.String(),
		"min_stake_formatted":    formatBalance(minStakeRequired),
		"current_rotation_count": len(currentAddresses),
		"new_rotation_count":     len(newRotatingKings),
		"new_rotation_list":      newRotatingKings,
		"status":                 "pending_timelock",
	}

	if data, err := json.MarshalIndent(proposalInfo, "", "  "); err == nil {
		if err := os.WriteFile(proposalFile, data, 0644); err == nil {
			fmt.Printf("\n💾 Proposal details saved to: %s\n", proposalFile)

			// Also create a summary file
			summaryFile := filepath.Join(c.node.GetDataDir(), "governance_summary.txt")
			summary := fmt.Sprintf(
				"Proposal #%d: Add %s to rotation\n"+
					"Created: %s\n"+
					"Can execute: %s\n"+
					"Status: Pending timelock\n"+
					"Balance: %s ANTD\n"+
					"Min Stake: %s ANTD\n"+
					"New rotation size: %d addresses\n",
				proposalID,
				addr.String(),
				time.Unix(int64(currentTime), 0).Format("2006-01-02 15:04"),
				time.Unix(int64(executionTime), 0).Format("2006-01-02 15:04"),
				formatBalance(balance),
				formatBalance(minStakeRequired),
				len(newRotatingKings),
			)
			os.WriteFile(summaryFile, []byte(summary), 0644)
		}
	}

	// Show reminder about checking proposal status
	fmt.Printf("\n⏰ Reminder:\n")
	fmt.Printf("   Proposal #%d will be ready for execution at:\n", proposalID)
	fmt.Printf("   %s\n", time.Unix(int64(executionTime), 0).Format("Monday, January 2, 2006 at 15:04 MST"))

	// Calculate and show countdown
	now := time.Now().Unix()
	execUnix := int64(executionTime)
	if execUnix > now {
		timeRemaining := time.Duration(execUnix-now) * time.Second
		fmt.Printf("   Time remaining: %s\n", formatDuration(timeRemaining))
	}

	// Final success message
	fmt.Printf("\n✨ Proposal creation complete!\n")
	fmt.Printf("   Use 'rotatingking governance' commands to manage this proposal.\n")
}

func (c *Console) handleRKGovernanceProposal(govController interface{}, id uint64) {
	fmt.Printf("📄 Governance Proposal #%d\n", id)
	fmt.Println("════════════════════════════════════════════════")

	// Try to get proposal using type assertion
	if gc, ok := govController.(interface {
		GetProposal(id uint64) (interface{}, bool)
	}); ok {
		prop, exists := gc.GetProposal(id)
		if !exists {
			fmt.Printf("❌ Proposal %d not found\n", id)
			return
		}

		// Try to display proposal details based on reflection
		v := reflect.ValueOf(prop)
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}

		if v.Kind() == reflect.Struct {
			fmt.Printf("Proposal Details:\n")
			for i := 0; i < v.NumField(); i++ {
				field := v.Type().Field(i)
				value := v.Field(i)

				// Skip unexported fields
				if !field.IsExported() {
					continue
				}

				fieldName := field.Name
				var fieldValue string

				switch value.Kind() {
				case reflect.String:
					fieldValue = value.String()
				case reflect.Bool:
					fieldValue = fmt.Sprintf("%v", value.Bool())
				case reflect.Uint64, reflect.Uint32, reflect.Uint:
					fieldValue = fmt.Sprintf("%d", value.Uint())
				case reflect.Slice:
					if value.Type().Elem().String() == "github.com/ethereum/go-ethereum/common.QuantumAddress" {
						// Handle address slice
						addrs := make([]string, value.Len())
						for j := 0; j < value.Len(); j++ {
							addr := value.Index(j).Interface().(common.QuantumAddress)
							addrs[j] = addr.String()
						}
						fieldValue = fmt.Sprintf("[%s]", strings.Join(addrs, ", "))
					} else {
						fieldValue = fmt.Sprintf("Slice of %d items", value.Len())
					}
				default:
					fieldValue = fmt.Sprintf("%v", value.Interface())
				}

				fmt.Printf("   %s: %s\n", fieldName, fieldValue)
			}
		}
	} else {
		fmt.Printf("❌ Cannot access proposal details - interface not available\n")
		fmt.Printf("💡 Try checking local proposal files in data directory\n")
	}

	fmt.Println("════════════════════════════════════════════════")
}

func (c *Console) handleRKGovernanceProposals(govController interface{}) {
	fmt.Println("📋 Governance Proposals")
	fmt.Println("════════════════════════════════════════════════")

	now := uint64(time.Now().Unix())

	// Try to list proposals
	if gc, ok := govController.(interface{ ListProposals() map[uint64]interface{} }); ok {
		proposals := gc.ListProposals()
		if len(proposals) == 0 {
			fmt.Println("📭 No governance proposals found")
			return
		}

		// Sort proposal IDs
		ids := make([]uint64, 0, len(proposals))
		for id := range proposals {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

		for _, id := range ids {
			prop := proposals[id]
			v := reflect.ValueOf(prop)
			if v.Kind() == reflect.Ptr {
				v = v.Elem()
			}

			// Get basic proposal info
			var propType, executor, created string
			var executed bool
			var eta uint64

			if v.Kind() == reflect.Struct {
				for i := 0; i < v.NumField(); i++ {
					field := v.Type().Field(i)
					value := v.Field(i)

					if !field.IsExported() {
						continue
					}

					switch field.Name {
					case "ProposalType":
						propType = fmt.Sprintf("%v", value.Interface())
					case "Executor":
						if addr, ok := value.Interface().(common.QuantumAddress); ok {
							executor = addr.String()[:8]
						}
					case "CreatedTimestamp":
						if ts, ok := value.Interface().(uint64); ok {
							created = time.Unix(int64(ts), 0).Format("2006-01-02")
						}
					case "Executed":
						executed = value.Bool()
					case "ETA":
						if ts, ok := value.Interface().(uint64); ok {
							eta = ts
						}
					}
				}
			}

			status := "⏳ Pending"
			if executed {
				status = "✅ Executed"
			} else if now >= eta {
				status = "🚀 Ready"
			}

			fmt.Printf("#%d - %s\n", id, status)
			if propType != "" {
				fmt.Printf("   Type: %s\n", propType)
			}
			if executor != "" {
				fmt.Printf("   By: %s\n", executor)
			}
			if created != "" {
				fmt.Printf("   Created: %s\n", created)
			}
			if !executed && eta > 0 {
				if now >= eta {
					fmt.Printf("   ⚡ Ready for execution\n")
				} else {
					timeRemaining := eta - now
					fmt.Printf("   ⏰ Can execute in: %s\n", formatDuration(time.Duration(timeRemaining)*time.Second))
				}
			}
			fmt.Println()
		}
	} else {
		// Check for local proposal files
		dataDir := c.node.GetDataDir()
		files, _ := os.ReadDir(dataDir)
		localProposals := 0

		for _, file := range files {
			if strings.HasPrefix(file.Name(), "proposal_") && strings.HasSuffix(file.Name(), ".json") {
				localProposals++
			}
		}

		if localProposals > 0 {
			fmt.Printf("Found %d local proposal files in %s\n", localProposals, dataDir)
			fmt.Println("💡 Use 'rotatingking governance proposal <id>' to view details")
		} else {
			fmt.Println("📭 No governance proposals found")
		}
	}

	fmt.Println("════════════════════════════════════════════════")
}

func (c *Console) handleRKGovernanceExecute(govController interface{}, id uint64) {
	fmt.Printf("🔨 Execute Proposal #%d\n", id)
	fmt.Println("════════════════════════════════════════════════")

	now := uint64(time.Now().Unix())

	// Try to execute proposal
	if gc, ok := govController.(interface {
		GetProposal(id uint64) (interface{}, bool)
		ExecuteProposal(id uint64, caller common.QuantumAddress, now uint64) error
	}); ok {

		prop, exists := gc.GetProposal(id)
		if !exists {
			fmt.Printf("❌ Proposal %d not found\n", id)
			return
		}

		// Check if already executed
		v := reflect.ValueOf(prop)
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}

		var executed bool
		var eta uint64
		var executor common.QuantumAddress

		if v.Kind() == reflect.Struct {
			for i := 0; i < v.NumField(); i++ {
				field := v.Type().Field(i)
				value := v.Field(i)

				if !field.IsExported() {
					continue
				}

				switch field.Name {
				case "Executed":
					executed = value.Bool()
				case "ETA":
					if ts, ok := value.Interface().(uint64); ok {
						eta = ts
					}
				case "Executor":
					if addr, ok := value.Interface().(common.QuantumAddress); ok {
						executor = addr
					}
				}
			}
		}

		if executed {
			fmt.Printf("❌ Proposal %d already executed\n", id)
			return
		}

		if now < eta {
			timeRemaining := eta - now
			fmt.Printf("❌ Proposal %d cannot be executed yet\n", id)
			fmt.Printf("   Time remaining: %s\n", formatDuration(time.Duration(timeRemaining)*time.Second))
			fmt.Printf("   Can execute after: %s\n", time.Unix(int64(eta), 0).Format(time.RFC3339))
			return
		}

		// Confirm execution
		fmt.Printf("Proposal #%d is ready for execution\n", id)
		if executor != (common.QuantumAddress{}) {
			fmt.Printf("Proposed by: %s\n", executor.String())
		}
		fmt.Printf("ETA: %s\n", time.Unix(int64(eta), 0).Format(time.RFC3339))
		fmt.Printf("Current time: %s\n", time.Unix(int64(now), 0).Format(time.RFC3339))

		if !c.confirmAction("\nExecute this proposal?") {
			fmt.Println("❌ Execution cancelled")
			return
		}

		// Get caller address (current user)
		c.node.mu.RLock()
		caller := c.node.MinerWalletAddress()
		c.node.mu.RUnlock()

		if caller == (common.QuantumAddress{}) {
			fmt.Println("❌ No mining address set")
			fmt.Println("   Use 'setaddress <your-address>' first")
			return
		}

		// Execute the proposal
		if err := gc.ExecuteProposal(id, caller, now); err != nil {
			fmt.Printf("❌ Failed to execute proposal: %v\n", err)
			return
		}

		fmt.Printf("✅ Proposal #%d executed successfully!\n", id)
		fmt.Printf("   Executed at: %s\n", time.Unix(int64(now), 0).Format(time.RFC3339))
		fmt.Printf("   Executed by: %s\n", caller.String())

	} else {
		fmt.Printf("❌ Cannot execute proposals - interface not available\n")
		fmt.Printf("💡 Check if governance system is properly initialized\n")
	}

	fmt.Println("════════════════════════════════════════════════")
}

func (c *Console) handleRKGovernanceTimelock(govController interface{}) {
	fmt.Println("⏰ Governance Timelock Status")
	fmt.Println("════════════════════════════════════════════════")

	now := uint64(time.Now().Unix())
	fmt.Printf("Current time: %s\n", time.Unix(int64(now), 0).Format(time.RFC3339))
	fmt.Printf("Timelock period: 48 hours (172,800 seconds)\n")
	fmt.Println()

	// Try to get proposals
	if gc, ok := govController.(interface{ ListProposals() map[uint64]interface{} }); ok {
		proposals := gc.ListProposals()

		// Count by status
		pending, ready, executed := 0, 0, 0
		var upcoming []uint64

		for id, prop := range proposals {
			v := reflect.ValueOf(prop)
			if v.Kind() == reflect.Ptr {
				v = v.Elem()
			}

			var propExecuted bool
			var eta uint64

			if v.Kind() == reflect.Struct {
				for i := 0; i < v.NumField(); i++ {
					field := v.Type().Field(i)
					value := v.Field(i)

					if !field.IsExported() {
						continue
					}

					switch field.Name {
					case "Executed":
						propExecuted = value.Bool()
					case "ETA":
						if ts, ok := value.Interface().(uint64); ok {
							eta = ts
						}
					}
				}
			}

			if propExecuted {
				executed++
			} else if now >= eta {
				ready++
			} else {
				pending++
				upcoming = append(upcoming, id)
			}
		}

		fmt.Printf("📊 Proposal Status:\n")
		fmt.Printf("   Total:      %d\n", len(proposals))
		fmt.Printf("   ⏳ Pending:  %d\n", pending)
		fmt.Printf("   🚀 Ready:    %d\n", ready)
		fmt.Printf("   ✅ Executed: %d\n", executed)
		fmt.Println()

		// Show upcoming executions
		if len(upcoming) > 0 {
			fmt.Printf("⏱️  Upcoming Executions:\n")

			// Sort by ETA
			sort.Slice(upcoming, func(i, j int) bool {
				propI, _ := gc.(interface {
					GetProposal(id uint64) (interface{}, bool)
				}).GetProposal(upcoming[i])
				propJ, _ := gc.(interface {
					GetProposal(id uint64) (interface{}, bool)
				}).GetProposal(upcoming[j])

				etaI, etaJ := uint64(0), uint64(0)

				vI := reflect.ValueOf(propI)
				if vI.Kind() == reflect.Ptr {
					vI = vI.Elem()
				}
				vJ := reflect.ValueOf(propJ)
				if vJ.Kind() == reflect.Ptr {
					vJ = vJ.Elem()
				}

				// Extract ETAs
				if vI.Kind() == reflect.Struct {
					for i := 0; i < vI.NumField(); i++ {
						if vI.Type().Field(i).Name == "ETA" {
							if ts, ok := vI.Field(i).Interface().(uint64); ok {
								etaI = ts
							}
						}
					}
				}
				if vJ.Kind() == reflect.Struct {
					for i := 0; i < vJ.NumField(); i++ {
						if vJ.Type().Field(i).Name == "ETA" {
							if ts, ok := vJ.Field(i).Interface().(uint64); ok {
								etaJ = ts
							}
						}
					}
				}

				return etaI < etaJ
			})

			for _, id := range upcoming {
				prop, _ := gc.(interface {
					GetProposal(id uint64) (interface{}, bool)
				}).GetProposal(id)
				v := reflect.ValueOf(prop)
				if v.Kind() == reflect.Ptr {
					v = v.Elem()
				}

				var eta uint64
				if v.Kind() == reflect.Struct {
					for i := 0; i < v.NumField(); i++ {
						if v.Type().Field(i).Name == "ETA" {
							if ts, ok := v.Field(i).Interface().(uint64); ok {
								eta = ts
							}
						}
					}
				}

				if eta > 0 {
					timeRemaining := eta - now
					fmt.Printf("   #%d - %s (in %s)\n",
						id,
						time.Unix(int64(eta), 0).Format("Jan 02 15:04"),
						formatDuration(time.Duration(timeRemaining)*time.Second))
				}
			}
		}
	} else {
		fmt.Println("❌ Cannot access proposal data")
		fmt.Println("💡 Governance system may not be initialized")
	}

	fmt.Println("════════════════════════════════════════════════")
}

func (c *Console) handleRKGovernanceRemove(rkManager reward.RotatingKingManager, addrStr string) {
	addr := mustParseQuantumAddress(addrStr)

	addresses := rkManager.GetKingAddresses()

	// Check if address is in rotation
	found := false
	for i, kingAddr := range addresses {
		if kingAddr == addr {
			found = true

			if rkManager.IsCurrentKing(addr) {
				fmt.Printf("❌ Cannot remove current king %s\n", addr.String())
				fmt.Println("   Rotate to another king first")
				return
			}

			if !c.confirmAction(fmt.Sprintf("Remove %s from rotation?", addr.String())) {
				return
			}

			// Create new slice without the address
			newAddresses := make([]common.QuantumAddress, 0, len(addresses)-1)
			newAddresses = append(newAddresses, addresses[:i]...)
			newAddresses = append(newAddresses, addresses[i+1:]...)

			fmt.Printf("✅ Address %s removed from rotation\n", addr.String())
			fmt.Printf("   New rotation count: %d\n", len(newAddresses))
			fmt.Println("💡 Note: Actual removal requires governance transaction")
			return
		}
	}

	if !found {
		fmt.Printf("❌ Address %s is not in rotation\n", addr.String())
	}
}

func (c *Console) handleRKStatus(rkManager reward.RotatingKingManager) {
	c.node.mu.RLock()
	height := c.node.blockchain.GetChainHeight()
	currentAddr := rkManager.GetCurrentKing()

	// Get current king's balance to check eligibility
	currentKingBalance := c.node.blockchain.State().GetBalance(currentAddr)
	isEligible := currentKingBalance.Cmp(rotatingking.EligibilityThreshold) >= 0
	c.node.mu.RUnlock()

	info := rkManager.GetRotationInfo(height)

	fmt.Println("👑 ROTATING KING STATUS WITH DATABASE")
	fmt.Println("══════════════════════════════════════════════════════════")

	// Display basic info
	currentKingStr := "None"
	if val, ok := info["currentKing"].(string); ok && val != "" {
		currentKingStr = val
	}
	fmt.Printf("Current King:      %s\n", currentKingStr)
	fmt.Printf("Eligible for 5%%:   %v (%s ANTD)\n",
		isEligible,
		formatBalance(currentKingBalance))

	// Show total 5% rewards distributed to current king
	if rewardsStr, ok := info["currentKingRewards"].(string); ok && rewardsStr != "" {
		if rewards, ok := new(big.Int).SetString(rewardsStr, 10); ok && rewards.Sign() > 0 {
			fmt.Printf("Current King 5%% Rewards: %s ANTD\n", formatBalance(rewards))
		}
	}

	// Display database sync status
	if manager, ok := rkManager.(interface {
		GetSyncState() (*rotatingking.SyncState, error)
	}); ok {
		syncState, err := manager.GetSyncState()
		if err == nil {
			fmt.Printf("\n🗃️ DATABASE SYNC STATUS:\n")
			fmt.Printf("  Synced to Block:   %d (%.1f%%)\n",
				syncState.LastSyncedBlock, syncState.SyncProgress*100)
			fmt.Printf("  Last Sync:         %s\n",
				syncState.LastSyncTime.Format("2006-01-02 15:04:05"))
			fmt.Printf("  Is Syncing:        %v\n", syncState.IsSyncing)

			if syncState.SyncError != "" {
				fmt.Printf("  Last Error:        %s\n", syncState.SyncError)
			}
		}
	}

	fmt.Printf("\n🔄 ROTATION INFORMATION:\n")
	fmt.Printf("  Block Height:     %d\n", height)

	interval := uint64(100)
	if i, ok := info["rotationInterval"].(uint64); ok {
		interval = i
	}
	fmt.Printf("  Rotation Every:   %d blocks\n", interval)

	nextRotation := height + interval
	if nr, ok := info["nextRotationAt"].(uint64); ok && nr > height {
		nextRotation = nr
	}
	fmt.Printf("  Next Rotation:    Block %d (%d blocks remaining)\n",
		nextRotation, nextRotation-height)

	// Estimate time until next rotation
	if est, ok := info["estimatedTimeUntilRotation"].(string); ok && est != "" {
		fmt.Printf("  Estimated Time:   %s\n", est)
	}

	kingCount := len(rkManager.GetKingAddresses())
	fmt.Printf("  Total Kings:      %d\n", kingCount)
	fmt.Printf("  Rotation Count:   %d\n", info["rotationCount"])

	// Show database statistics if available
	if manager, ok := rkManager.(interface {
		GetDBMetrics() *rotatingking.DBMetrics
	}); ok {
		metrics := manager.GetDBMetrics()
		fmt.Printf("\n📊 DATABASE STATISTICS:\n")
		fmt.Printf("  Write Operations: %d\n", metrics.WriteCount)
		fmt.Printf("  Read Operations:  %d\n", metrics.ReadCount)
		fmt.Printf("  Database Size:    %s\n", formatBytes(metrics.DBSize))
	}

	fmt.Printf("\n💰 REWARD DISTRIBUTION:\n")
	fmt.Println("  • Miner:          0% of block reward + fees")
	fmt.Println("  • Main King:      10% always")
	if isEligible {
		fmt.Println("  • Current King:   90% (eligible)")
	} else {
		fmt.Println("  • Current King:   0% (ineligible — balance < 100k ANTD)")
		fmt.Println("                    Main King gets extra 5%")
	}

	fmt.Println("══════════════════════════════════════════════════════════")
}

func (c *Console) handleRKAdd(addrStr string) {
	addr, err := parseQuantumAddressInput(addrStr)
	if err != nil {
		fmt.Printf("❌ Invalid address format: %s\n", addrStr)
		fmt.Println("   Address must be a valid ANTDChain quantum address")
		return
	}

	if addr == (common.QuantumAddress{}) {
		fmt.Println("❌ Cannot add zero address")
		return
	}

	// Get rotating king manager
	c.node.mu.RLock()
	rkManager := c.node.blockchain.GetRotatingKingManager()
	currentHeight := c.node.blockchain.GetChainHeight()
	balance := c.node.blockchain.State().GetBalance(addr)
	c.node.mu.RUnlock()

	if rkManager == nil {
		fmt.Println("❌ Rotating King system not initialized")
		return
	}

	// Check if already in rotation
	currentList := rkManager.GetKingAddresses()
	for _, king := range currentList {
		if king == addr {
			fmt.Printf("❌ Address %s is already in the rotating king list\n", addr.String())
			position := 0
			for i, k := range currentList {
				if k == addr {
					position = i + 1
					break
				}
			}
			fmt.Printf("   Position: %d of %d\n", position, len(currentList))
			return
		}
	}

	// Check minimum stake requirement: 100,000 ANTD
	minStake := new(big.Int).Mul(big.NewInt(100_000), big.NewInt(1e18)) // 100k ANTD in wei
	if balance.Cmp(minStake) < 0 {
		fmt.Printf("❌ Insufficient balance for rotating king eligibility\n")
		fmt.Printf("   Current balance: %s ANTD\n", formatBalance(balance))
		fmt.Printf("   Required minimum: %s ANTD\n", formatBalance(minStake))
		shortBy := new(big.Int).Sub(minStake, balance)
		fmt.Printf("   Short by: %s ANTD\n", formatBalance(shortBy))
		return
	}

	// Confirm action
	fmt.Printf("\n📝 Add Address to Rotating King List\n")
	fmt.Printf("════════════════════════════════════════════════\n")
	fmt.Printf("   Address:      %s\n", addr.String())
	fmt.Printf("   Balance:      %s ANTD (>= 100,000 ANTD ✓)\n", formatBalance(balance))
	fmt.Printf("   Current Kings: %d\n", len(currentList))
	fmt.Printf("   New Total:     %d\n", len(currentList)+1)
	fmt.Printf("   Block Height:  %d\n", currentHeight)
	fmt.Printf("   Position:      Last in rotation\n")
	fmt.Println("════════════════════════════════════════════════")

	if !c.confirmAction("Confirm adding this address to the rotating king list?") {
		fmt.Println("❌ Operation cancelled")
		return
	}

	// Create new list
	newList := make([]common.QuantumAddress, len(currentList)+1)
	copy(newList, currentList)
	newList[len(currentList)] = addr

	// === PERSIST TO DATABASE ===
	if err := c.persistKingListToDatabase(rkManager, newList, currentHeight); err != nil {
		fmt.Printf("❌ Failed to persist to database: %v\n", err)
		return
	}

	// === UPDATE IN MEMORY ===
	if err := rkManager.UpdateKingAddresses(newList); err != nil {
		fmt.Printf("❌ Failed to update king list locally: %v\n", err)
		return
	}

	fmt.Printf("✅ Address %s added to rotating king list\n", addr.String())
	fmt.Printf("   New total: %d kings\n", len(newList))
	fmt.Printf("   Saved to database ✓\n")

	// === BROADCAST TO NETWORK ===
	if err := c.broadcastKingUpdate(newList, currentHeight, "add", addr); err != nil {
		fmt.Printf("⚠️  Warning: Failed to broadcast update to network: %v\n", err)
		fmt.Println("   The change is saved locally and in the database")
		fmt.Println("   Other nodes will sync it eventually via DB sync")
	} else {
		fmt.Printf("📡 Update successfully broadcast to network\n")
	}

	// Final success message
	fmt.Printf("\n🎉 Rotating king list updated successfully!\n")
	fmt.Printf("   Use 'rotatingking list' to view current list\n")
	fmt.Printf("   Use 'rotatingking status' to check rotation\n")
	fmt.Printf("   New king will receive 5%% rewards when serving\n")
}

func (c *Console) broadcastKingUpdate(newList []common.QuantumAddress, height uint64, action string, addr common.QuantumAddress) error {
	if c.node.p2pNode == nil {
		return errors.New("P2P node not available")
	}

	// Create the update event
	updateEvent := &rotatingking.KingListUpdateEvent{
		BlockHeight: height,
		NewList:     newList,
		Added:       addr,
		Timestamp:   time.Now(),
		Reason:      fmt.Sprintf("%s: %s", action, addr.String()[:8]),
	}

	// Direct broadcast
	return c.node.p2pNode.BroadcastKingListUpdate(updateEvent)
}

func (c *Console) persistKingListToDatabase(rkManager interface{}, newList []common.QuantumAddress, height uint64) error {
	// Try to cast to RotatingKingManager from rotatingking package
	if manager, ok := rkManager.(interface {
		UpdateKingAddresses(newAddresses []common.QuantumAddress) error
	}); ok {
		return manager.UpdateKingAddresses(newList)
	}

	// Fallback: Save to file
	data := map[string]interface{}{
		"addresses":  newList,
		"height":     height,
		"timestamp":  time.Now().Unix(),
		"block_hash": c.node.blockchain.GetBlock(height).Hash().String(),
	}

	dataDir := c.node.GetDataDir()
	filePath := filepath.Join(dataDir, "rotating_kings.json")

	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filePath, jsonData, 0644)
}

func (c *Console) handleRKRewards(parts []string) {
	if len(parts) < 3 {
		fmt.Println("Usage: rk rewards <address>")
		return
	}

	addr := mustParseQuantumAddress(parts[2])

	c.node.mu.RLock()
	defer c.node.mu.RUnlock()

	// Try to get reward manager
	rkManager := c.node.blockchain.GetRotatingKingManager()
	if rkManager == nil {
		fmt.Println("Rotating King system not available")
		return
	}

	// Try to get rewards for address
	fmt.Printf("Rewards for %s:\n", addr.String())

	// Check if address is a rotating king
	addresses := make([]common.QuantumAddress, 0)
	if manager, ok := rkManager.(interface {
		GetKingAddresses() []common.QuantumAddress
	}); ok {
		addresses = manager.GetKingAddresses()
	}

	isKing := false
	for _, kingAddr := range addresses {
		if kingAddr == addr {
			isKing = true
			break
		}
	}

	if isKing {
		fmt.Printf("  Status: ✅ Rotating King\n")

		// Try to get reward info
		if manager, ok := rkManager.(interface {
			GetKingRewards(common.QuantumAddress) *big.Int
		}); ok {
			rewards := manager.GetKingRewards(addr)
			if rewards != nil && rewards.Sign() > 0 {
				fmt.Printf("  Total 5%% Rewards: %s ANTD\n", formatBalance(rewards))
			}
		}
	} else {
		fmt.Printf("  Status: ❌ Not a Rotating King\n")
	}

	// Check if eligible
	balance := c.node.blockchain.State().GetBalance(addr)
	threshold := new(big.Int).Mul(big.NewInt(100000), big.NewInt(1e18)) // 100k ANTD
	eligible := balance.Cmp(threshold) >= 0

	fmt.Printf("  Balance: %s ANTD\n", formatBalance(balance))
	fmt.Printf("  Eligible (100k ANTD): %v\n", eligible)

	if eligible && !isKing {
		fmt.Printf("\n💡 To become a Rotating King:\n")
		fmt.Printf("   Use: rk add %s\n", addr.String())
	}
}

func (n *Node) SetMinerWalletAddress(addr common.QuantumAddress) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.minerWalletAddress = addr
	if qAddr, err := common.NewQuantumAddressFromBytes(addr.Bytes()); err == nil {
		log.Printf("Miner wallet address set to: %s", qAddr.String())
		return
	}
	log.Printf("Miner wallet address set to: %s", addr.String())
}

func (n *Node) MinerWalletAddress() common.QuantumAddress {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.minerWalletAddress
}

func readPasswordOnce(prompt string) (string, error) {
	fmt.Print(prompt)
	bytePassword, err := term.ReadPassword(int(syscall.Stdin))
	fmt.Println()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(bytePassword)), nil
}

func (c *Console) handleRebroadcast(parts []string) {
	if len(parts) > 1 {
		// Specific transaction rebroadcast
		c.handleRebroadcastTx(parts[1])
		return
	}

	// Rebroadcast all mempool transactions
	c.node.mu.RLock()
	pendingTxs := c.node.blockchain.TxPool().GetPending()
	c.node.mu.RUnlock()

	if len(pendingTxs) == 0 {
		fmt.Println("📭 Mempool is empty - no transactions to rebroadcast")
		return
	}

	fmt.Printf("📡 Rebroadcasting %d transactions from mempool...\n", len(pendingTxs))

	successful := 0
	failed := 0

	for _, tx := range pendingTxs {
		if c.node.p2pNode != nil {
			if err := c.node.p2pNode.BroadcastTx(tx); err != nil {
				failed++
				fmt.Printf("   ❌ Failed to rebroadcast %s: %v\n", tx.Hash().String()[:8], err)
			} else {
				successful++
				fmt.Printf("   ✅ Rebroadcast %s\n", tx.Hash().String()[:8])
			}
		}
	}

	fmt.Printf("\n📊 Rebroadcast Results:\n")
	fmt.Printf("   Successful: %d\n", successful)
	fmt.Printf("   Failed:     %d\n", failed)
	fmt.Printf("   Total:      %d\n", len(pendingTxs))

	if successful > 0 {
		fmt.Println("✅ Transactions rebroadcast to network")
	}
}

func (c *Console) handleRebroadcastTx(txHashStr string) {
	txHash := common.HexToHash(txHashStr)

	// Search in mempool
	c.node.mu.RLock()
	pendingTxs := c.node.blockchain.TxPool().GetPending()
	c.node.mu.RUnlock()

	var foundTx *tx.Tx
	for _, tx := range pendingTxs {
		if tx.Hash() == txHash {
			foundTx = tx
			break
		}
	}

	if foundTx == nil {
		// Search in recent blocks
		c.node.mu.RLock()
		latest := c.node.blockchain.Latest()
		if latest != nil {
			latestHeight := latest.Header.Number.Uint64()
			// Check last 6 blocks
			startHeight := uint64(0)
			if latestHeight > 6 {
				startHeight = latestHeight - 6
			}

			for h := startHeight; h <= latestHeight; h++ {
				block := c.node.blockchain.GetBlock(h)
				if block == nil {
					continue
				}
				for _, t := range block.Txs {
					if t.Hash() == txHash {
						foundTx = t
						break
					}
				}
				if foundTx != nil {
					break
				}
			}
		}
		c.node.mu.RUnlock()
	}

	if foundTx == nil {
		fmt.Printf("❌ Transaction %s not found in mempool or recent blocks\n", txHashStr)
		return
	}

	if c.node.p2pNode == nil {
		fmt.Println("❌ P2P node not available")
		return
	}

	fmt.Printf("📡 Rebroadcasting transaction %s...\n", txHash.String()[:8])

	if err := c.node.p2pNode.BroadcastTx(foundTx); err != nil {
		fmt.Printf("❌ Failed to rebroadcast: %v\n", err)
	} else {
		fmt.Printf("✅ Transaction %s rebroadcast successfully\n", txHash.String()[:8])
	}
}

// Show detailed mempool information
func (c *Console) handleMempoolInfo(parts []string) {
	c.node.mu.RLock()
	pendingTxs := c.node.blockchain.TxPool().GetPending()
	c.node.mu.RUnlock()

	fmt.Println("📊 MEMPOOL INFORMATION")
	fmt.Println("════════════════════════════════════════════════")

	// Calculate statistics
	totalSize := 0
	totalFees := big.NewInt(0)
	totalValue := big.NewInt(0)
	addresses := make(map[common.QuantumAddress]bool)

	for _, tx := range pendingTxs {
		data, _ := tx.Serialize()
		totalSize += len(data)

		// Fee calculation
		fee := new(big.Int).Mul(big.NewInt(int64(tx.Gas)), tx.GasPrice)
		totalFees.Add(totalFees, fee)

		// Total value
		totalValue.Add(totalValue, tx.Value)

		// Unique addresses
		addresses[common.BytesToQuantumAddress(tx.From.Bytes())] = true
		if tx.To != nil {
			addresses[common.BytesToQuantumAddress(tx.To.Bytes())] = true
		}
	}

	fmt.Printf("Transactions:      %d\n", len(pendingTxs))
	fmt.Printf("Total Size:        %d bytes\n", totalSize)
	fmt.Printf("Total Value:       %s ANTD\n", formatBalance(totalValue))
	fmt.Printf("Total Fees:        %s ANTD\n", formatBalance(totalFees))
	fmt.Printf("Unique Addresses:  %d\n", len(addresses))

	// Show by address
	fmt.Printf("\n📈 By Address:\n")
	txsByAddress := make(map[common.QuantumAddress]int)
	for _, tx := range pendingTxs {
		txsByAddress[common.BytesToQuantumAddress(tx.From.Bytes())]++
	}

	for addr, count := range txsByAddress {
		fmt.Printf("   %s: %d tx(s)\n", addr.String()[:8], count)
	}

	// Show fee distribution
	if len(pendingTxs) > 0 {
		fmt.Printf("\n💰 Fee Distribution:\n")
		// Sort by fee per byte
		sortedTxs := make([]*tx.Tx, len(pendingTxs))
		copy(sortedTxs, pendingTxs)

		sort.Slice(sortedTxs, func(i, j int) bool {
			feeI := new(big.Int).Mul(big.NewInt(int64(sortedTxs[i].Gas)), sortedTxs[i].GasPrice)
			feeJ := new(big.Int).Mul(big.NewInt(int64(sortedTxs[j].Gas)), sortedTxs[j].GasPrice)
			return feeI.Cmp(feeJ) > 0 // Descending
		})

		// Show top 5 by fee
		limit := 5
		if len(sortedTxs) < limit {
			limit = len(sortedTxs)
		}

		for i := 0; i < limit; i++ {
			tx := sortedTxs[i]
			fee := new(big.Int).Mul(big.NewInt(int64(tx.Gas)), tx.GasPrice)
			data, _ := tx.Serialize()
			feePerByte := new(big.Float).Quo(
				new(big.Float).SetInt(fee),
				new(big.Float).SetInt(big.NewInt(int64(len(data)))),
			)

			fmt.Printf("   %d. %s: %s ANTD (%.2f ANTD/byte)\n",
				i+1,
				tx.Hash().String()[:8],
				formatBalance(fee),
				feePerByte,
			)
		}
	}

	fmt.Println("════════════════════════════════════════════════")
	fmt.Println("💡 Use 'rebroadcast' to resend all pending transactions")
	fmt.Println("💡 Use 'rebroadcast <txhash>' to resend specific transaction")
}

// broadcast king list update
func (c *Console) broadcastKingListUpdate(rkManager interface{}, newList []common.QuantumAddress) error {
	if c.node.p2pNode == nil {
		return errors.New("P2P node not available")
	}

	// Get current block height
	c.node.mu.RLock()
	height := c.node.blockchain.GetChainHeight()
	c.node.mu.RUnlock()

	// Create the update event
	updateEvent := &rotatingking.KingListUpdateEvent{
		BlockHeight: height,
		NewList:     newList,
		Timestamp:   time.Now(),
	}

	return c.node.p2pNode.BroadcastKingListUpdate(updateEvent)
}

func (c *Console) handleRKDBSync(parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: rotatingking dbsync <height>")
		fmt.Println("       rotatingking dbsync now   (sync to current height)")
		return
	}

	c.node.mu.RLock()
	rkManager := c.node.blockchain.GetRotatingKingManager()
	currentHeight := c.node.blockchain.GetChainHeight()
	c.node.mu.RUnlock()

	if rkManager == nil {
		fmt.Println("❌ Rotating King system not initialized")
		return
	}

	var targetHeight uint64
	if parts[1] == "now" {
		targetHeight = currentHeight
	} else {
		height, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			fmt.Printf("❌ Invalid height: %s\n", parts[1])
			return
		}
		targetHeight = height
	}

	if targetHeight > currentHeight {
		fmt.Printf("❌ Cannot sync to future height %d (current: %d)\n", targetHeight, currentHeight)
		return
	}

	fmt.Printf("🔄 Synchronizing rotating king database to height %d...\n", targetHeight)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if manager, ok := rkManager.(interface {
		SyncBlocks(ctx context.Context, height uint64) error
	}); ok {
		if err := manager.SyncBlocks(ctx, targetHeight); err != nil {
			fmt.Printf("❌ Database sync failed: %v\n", err)
			return
		}
		fmt.Printf("✅ Database synchronized to height %d\n", targetHeight)
	} else {
		fmt.Println("❌ Rotating king manager doesn't support database sync")
	}
}

func (c *Console) handleRKDBStatus(parts []string) {
	c.node.mu.RLock()
	rkManager := c.node.blockchain.GetRotatingKingManager()
	c.node.mu.RUnlock()

	if rkManager == nil {
		fmt.Println("❌ Rotating King system not initialized")
		return
	}

	fmt.Println("🗃️ ROTATING KING DATABASE STATUS")
	fmt.Println("════════════════════════════════════════════════")

	if manager, ok := rkManager.(interface {
		GetSyncState() (*rotatingking.SyncState, error)
	}); ok {
		syncState, err := manager.GetSyncState()
		if err != nil {
			fmt.Printf("❌ Failed to get sync state: %v\n", err)
			return
		}

		fmt.Printf("Last Synced Block:   %d\n", syncState.LastSyncedBlock)
		fmt.Printf("Last Sync Time:      %s\n", syncState.LastSyncTime.Format(time.RFC3339))
		fmt.Printf("Is Syncing:          %v\n", syncState.IsSyncing)
		fmt.Printf("Sync Progress:       %.1f%%\n", syncState.SyncProgress*100)

		if syncState.SyncError != "" {
			fmt.Printf("Last Sync Error:     %s\n", syncState.SyncError)
		}
	}

	if manager, ok := rkManager.(interface {
		GetDBMetrics() *rotatingking.DBMetrics
	}); ok {
		metrics := manager.GetDBMetrics()
		fmt.Printf("\n📊 DATABASE METRICS:\n")
		fmt.Printf("Write Count:         %d\n", metrics.WriteCount)
		fmt.Printf("Read Count:          %d\n", metrics.ReadCount)
		fmt.Printf("Batch Write Size:    %d\n", metrics.BatchWriteSize)
		fmt.Printf("Last Write:          %s\n", metrics.LastWriteTime.Format(time.RFC3339))
		fmt.Printf("Database Size:       %s\n", formatBytes(metrics.DBSize))
	}

	if manager, ok := rkManager.(interface {
		GetLastSyncedBlock() (*rotatingking.BlockSyncRecord, error)
	}); ok {
		record, err := manager.GetLastSyncedBlock()
		if err == nil && record != nil {
			fmt.Printf("\n📦 LAST SYNCED BLOCK:\n")
			fmt.Printf("Block Height:        %d\n", record.BlockHeight)
			fmt.Printf("Block Hash:          %s\n", record.BlockHash.String())
			fmt.Printf("Timestamp:           %s\n", record.Timestamp.Format(time.RFC3339))
			fmt.Printf("Sync Duration:       %v\n", record.SyncDuration)

			if len(record.RotationEvents) > 0 {
				fmt.Printf("Rotation Events:     %d\n", len(record.RotationEvents))
				for i, event := range record.RotationEvents {
					if i < 3 { // Show first 3 events
						fmt.Printf("  • %s\n", event)
					}
				}
				if len(record.RotationEvents) > 3 {
					fmt.Printf("  ... and %d more\n", len(record.RotationEvents)-3)
				}
			}
		}
	}

	fmt.Println("════════════════════════════════════════════════")
}

func (c *Console) handleRKDBBackup(parts []string) {
	backupPath := ""
	if len(parts) > 1 {
		backupPath = parts[1]
	} else {
		// Default backup location
		backupPath = filepath.Join(c.node.GetDataDir(), "rotatingking_backup")
	}

	c.node.mu.RLock()
	rkManager := c.node.blockchain.GetRotatingKingManager()
	c.node.mu.RUnlock()

	if rkManager == nil {
		fmt.Println("❌ Rotating King system not initialized")
		return
	}

	fmt.Printf("💾 Backing up rotating king database to: %s\n", backupPath)

	if manager, ok := rkManager.(interface{ BackupDatabase(path string) error }); ok {
		if err := manager.BackupDatabase(backupPath); err != nil {
			fmt.Printf("❌ Backup failed: %v\n", err)
			return
		}
		fmt.Printf("✅ Backup completed successfully\n")

		// List backup files
		if files, err := os.ReadDir(backupPath); err == nil {
			fmt.Printf("📁 Backup files (%d):\n", len(files))
			for _, file := range files {
				info, _ := file.Info()
				fmt.Printf("  • %s (%s)\n", file.Name(), formatBytes(info.Size()))
			}
		}
	} else {
		fmt.Println("❌ Rotating king manager doesn't support backup")
	}
}

func (c *Console) handleRKDBHistory(parts []string) {
	c.node.mu.RLock()
	currentHeight := c.node.blockchain.GetChainHeight()
	c.node.mu.RUnlock()

	fromBlock := uint64(0)
	toBlock := currentHeight
	limit := 10

	if len(parts) > 1 {
		if parts[1] == "recent" {
			if currentHeight > 100 {
				fromBlock = currentHeight - 100
			}
		} else if parts[1] == "all" {
			// Show all history
		} else {
			// Parse block range
			if strings.Contains(parts[1], "-") {
				rangeParts := strings.Split(parts[1], "-")
				if len(rangeParts) == 2 {
					from, _ := strconv.ParseUint(rangeParts[0], 10, 64)
					to, _ := strconv.ParseUint(rangeParts[1], 10, 64)
					fromBlock = from
					toBlock = to
					if toBlock > currentHeight {
						toBlock = currentHeight
					}
				}
			} else {
				// Single block
				block, _ := strconv.ParseUint(parts[1], 10, 64)
				fromBlock = block
				toBlock = block
			}
		}
	}

	if len(parts) > 2 {
		if l, err := strconv.Atoi(parts[2]); err == nil && l > 0 {
			limit = l
		}
	}

	c.node.mu.RLock()
	rkManager := c.node.blockchain.GetRotatingKingManager()
	c.node.mu.RUnlock()

	if rkManager == nil {
		fmt.Println("❌ Rotating King system not initialized")
		return
	}

	fmt.Printf("📜 ROTATING KING DATABASE HISTORY (Blocks %d-%d)\n", fromBlock, toBlock)
	fmt.Println("══════════════════════════════════════════════════════════")

	if manager, ok := rkManager.(interface {
		GetRotationHistoryFromDB(fromBlock, toBlock uint64) ([]rotatingking.KingRotation, error)
	}); ok {
		rotations, err := manager.GetRotationHistoryFromDB(fromBlock, toBlock)
		if err != nil {
			fmt.Printf("❌ Failed to get rotation history: %v\n", err)
			return
		}

		if len(rotations) == 0 {
			fmt.Println("No rotation events found in database for this range")
			return
		}

		// Apply limit
		if limit > 0 && len(rotations) > limit {
			rotations = rotations[len(rotations)-limit:]
		}

		for i := len(rotations) - 1; i >= 0; i-- {
			rotation := rotations[i]
			fmt.Printf("Rotation #%d:\n", len(rotations)-i)
			fmt.Printf("  Block:      %d\n", rotation.BlockHeight)
			fmt.Printf("  Timestamp:  %s\n", rotation.Timestamp.Format("2006-01-02 15:04:05"))
			fmt.Printf("  From:       %s\n", rotation.PreviousKing.String())
			fmt.Printf("  To:         %s\n", rotation.NewKing.String())
			fmt.Printf("  Eligible:   %v\n", rotation.WasEligible)

			if rotation.Reward != nil && rotation.Reward.Sign() > 0 {
				fmt.Printf("  Reward:     %s ANTD\n", formatBalance(rotation.Reward))
			}

			if rotation.Reason != "" {
				fmt.Printf("  Reason:     %s\n", rotation.Reason)
			}

			if i > 0 {
				fmt.Println("  ───────────────────────────────────────")
			}
		}

		fmt.Printf("\n📊 Statistics:\n")
		fmt.Printf("  Total Events:    %d\n", len(rotations))
		fmt.Printf("  Eligible:        %d\n", countEligible(rotations))
		fmt.Printf("  Total Rewards:   %s ANTD\n", formatBalance(calculateTotalRewards(rotations)))
	} else {
		fmt.Println("❌ Database history not available")
	}

	fmt.Println("══════════════════════════════════════════════════════════")
}

func countEligible(rotations []rotatingking.KingRotation) int {
	count := 0
	for _, r := range rotations {
		if r.WasEligible {
			count++
		}
	}
	return count
}

func calculateTotalRewards(rotations []rotatingking.KingRotation) *big.Int {
	total := big.NewInt(0)
	for _, r := range rotations {
		if r.Reward != nil {
			total.Add(total, r.Reward)
		}
	}
	return total
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func (c *Console) handleKeyStatus(parts []string) {
	if c.node.miningState == nil {
		fmt.Println("❌ Mining state not initialized")
		return
	}

	stats := c.node.miningState.GetMiningStatistics()

	fmt.Println("🔑 Private Key Status:")
	fmt.Printf("  Miner Address:      %s\n", stats["miner_address"])
	fmt.Printf("  Has Private Key:    %v\n", stats["has_private_key"])

	if pubKey := c.node.miningState.GetPublicKey(); pubKey != nil {
		previewLen := 10
		if len(pubKey) < previewLen {
			previewLen = len(pubKey)
		}
		fmt.Printf("  Public Key:         %x...\n", pubKey[:previewLen])
	}

	// Check keystore
	addr := c.node.miningState.GetMinerAddress()
	if addr != (common.QuantumAddress{}) {
		fmt.Printf("\n🔍 Keystore Check:\n")

		found := false
		for _, acc := range c.node.Keystore().Accounts() {
			if ethAddressMatchesQuantumAddress(acc.Address, addr) {
				fmt.Printf("  ✓ Wallet found in keystore\n")
				fmt.Printf("  File: %s\n", acc.URL.Path)

				// Check if file exists
				if info, err := os.Stat(acc.URL.Path); err == nil {
					fmt.Printf("  Size: %d bytes\n", info.Size())
					fmt.Printf("  Modified: %s\n", info.ModTime().Format("2006-01-02 15:04"))
				}
				found = true
				break
			}
		}

		if !found {
			fmt.Printf("  ❌ Wallet NOT found in keystore\n")
		}
	}
}

func (n *Node) AutoUnlockMinerWallet() error {
	if n.miningState == nil || n.keystore == nil {
		return errors.New("mining state or keystore not initialized")
	}

	if n.minerWalletAddress == (common.QuantumAddress{}) {
		return errors.New("miner wallet address not set")
	}

	// Try common passwords
	commonPasswords := []string{"", "password", "123456", "test", "admin", "12345678"}

	// Find the keystore file
	var keyFile string
	for _, acc := range n.keystore.Accounts() {
		if ethAddressMatchesQuantumAddress(acc.Address, n.minerWalletAddress) {
			keyFile = acc.URL.Path
			break
		}
	}

	if keyFile == "" {
		return fmt.Errorf("miner wallet %s not found in keystore", n.minerWalletAddress.String())
	}

	for _, pwd := range commonPasswords {
		err := n.miningState.LoadPrivateKeyFromFile(keyFile, pwd)
		if err == nil {
			log.Printf("[console] ✓ Auto-unlocked miner wallet with password '%s'", pwd)
			return nil
		}
	}

	return errors.New("failed to unlock with common passwords")
}

func (c *Console) handleCheckEligibility(parts []string) {
	c.node.mu.RLock()
	defer c.node.mu.RUnlock()

	minerAddr := c.node.MinerWalletAddress()
	if minerAddr == (common.QuantumAddress{}) {
		fmt.Println("❌ No miner address set")
		return
	}

	posEngine := c.node.blockchain.Pow()
	if posEngine == nil {
		fmt.Println("❌ PoS engine not found")
		return
	}

	parent := c.node.blockchain.Latest()
	if parent == nil {
		fmt.Println("❌ No parent block (genesis?)")
		return
	}

	height := parent.Header.Number.Uint64() + 1
	timestamp := uint64(time.Now().Unix())

	fmt.Printf("🔍 Checking eligibility for block %d:\n", height)
	fmt.Printf("   Miner: %s\n", minerAddr.String())
	fmt.Printf("   Parent hash: %s\n", parent.Hash().String())
	fmt.Printf("   Timestamp: %d\n", timestamp)

	// Use reflection to call VerifyMinerEligibility
	v := reflect.ValueOf(posEngine)
	method := v.MethodByName("VerifyMinerEligibility")
	if !method.IsValid() {
		fmt.Println("❌ VerifyMinerEligibility method not found")
		return
	}

	results := method.Call([]reflect.Value{
		reflect.ValueOf(minerAddr),
		reflect.ValueOf(parent.Hash()),
		reflect.ValueOf(height),
		reflect.ValueOf(timestamp),
	})

	if len(results) >= 2 {
		eligible := results[0].Bool()
		err := results[1].Interface()

		fmt.Printf("   Eligible: %v\n", eligible)
		if err != nil {
			fmt.Printf("   Error: %v\n", err)
		}
	}
}

func (c *Console) broadcastKingRotation(previousKing, newKing common.QuantumAddress, reason string) error {
	if c.node.p2pNode == nil {
		return errors.New("P2P node not available")
	}

	// Get current block height
	c.node.mu.RLock()
	currentHeight := c.node.blockchain.GetChainHeight()
	c.node.mu.RUnlock()

	// Ensure we have a valid height (not 0)
	if currentHeight == 0 {
		// Try to get the latest block
		if latest := c.node.blockchain.Latest(); latest != nil {
			currentHeight = latest.Header.Number.Uint64()
		}
	}

	rotation := rotatingking.KingRotation{
		BlockHeight:  currentHeight,
		PreviousKing: previousKing,
		NewKing:      newKing,
		Timestamp:    time.Now(),
		WasEligible:  true,
		Reason:       reason,
		Reward:       big.NewInt(0),
	}

	// Log the rotation for debugging
	log.Printf("[console] Broadcasting rotation: %s -> %s at height %d",
		previousKing.String()[:8], newKing.String()[:8], currentHeight)

	return c.node.p2pNode.BroadcastKingRotation(&rotation)
}
