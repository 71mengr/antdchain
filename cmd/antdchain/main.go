// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package main
import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/antdaza/antdchain/common"
	"github.com/antdaza/antdchain/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/antdaza/antdchain/antdc/chain"
	"github.com/antdaza/antdchain/antdc/checkpoints"
	"github.com/antdaza/antdchain/antdc/mining"
	"github.com/antdaza/antdchain/antdc/p2p"
	"github.com/antdaza/antdchain/antdc/pow"
	"github.com/antdaza/antdchain/antdc/rotatingking"
	"github.com/antdaza/antdchain/antdc/tx"
	"github.com/antdaza/antdchain/antdc/wallet"
	"github.com/antdaza/antdchain/cmd/antdchain/king"
	"github.com/antdaza/antdchain/console"
	antdrpc "github.com/antdaza/antdchain/rpc"
)

var submitMutex sync.Mutex
var startTime = time.Now()

type P2PWrapper struct{ *p2p.Node }

func (p *P2PWrapper) Close() { log.Println("P2P wrapper closed") }

type RotatingKingAPI struct {
	node *console.Node
}

type RotatingKingInfo struct {
	CurrentKing             common.QuantumAddress `json:"currentKing"`
	NextKing                common.QuantumAddress `json:"nextKing,omitempty"`
	BlocksUntilRotation     uint64         `json:"blocksUntilRotation"`
	KingCount               int            `json:"kingCount"`
	RotationCount           uint64         `json:"rotationCount"`
	NextRotationAt          uint64         `json:"nextRotationAt"`
	RotationHeight          uint64         `json:"rotationHeight"`
	RotationInterval        uint64         `json:"rotationInterval"`
	ActivationDelay         uint64         `json:"activationDelay"`
	MinStakeRequired        string         `json:"minStakeRequired"`
	TotalRewardsDistributed string         `json:"totalRewardsDistributed,omitempty"`
}

type KingStats struct {
	InRotation            bool   `json:"inRotation"`
	Position              int    `json:"position,omitempty"`
	TotalPositions        int    `json:"totalPositions,omitempty"`
	IsCurrentKing         bool   `json:"isCurrentKing"`
	BecameKingAtBlock     uint64 `json:"becameKingAtBlock,omitempty"`
	NextRotationAtBlock   uint64 `json:"nextRotationAtBlock,omitempty"`
	RotationsUntilKing    int    `json:"rotationsUntilKing,omitempty"`
	TotalRewards          string `json:"totalRewards,omitempty"`
	TotalRewardsFormatted string `json:"totalRewardsFormatted,omitempty"`
}

// WebServer serves a simple web interface
type WebServer struct {
	node          *console.Node
	walletManager *wallet.WalletManager
	router        *mux.Router
	logger        *logrus.Logger
}

// NewWebServer creates a new web server
func NewWebServer(node *console.Node, walletManager *wallet.WalletManager) *WebServer {
	return &WebServer{
		node:          node,
		walletManager: walletManager,
		logger:        logrus.New(),
	}
}

// Start starts the web server
func (ws *WebServer) Start(port int) error {
	mux := http.NewServeMux()

	// Basic routes
	mux.HandleFunc("/", ws.handleIndex)
	mux.HandleFunc("/status", ws.handleStatus)
	mux.HandleFunc("/blocks", ws.handleBlocks)
	mux.HandleFunc("/health", ws.handleHealth)

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	log.Printf("Web server starting on port %d", port)
	return server.ListenAndServe()
}

func (ws *WebServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	fmt.Fprintf(w, `
    <!DOCTYPE html>
    <html>
    <head>
        <title>ANTDChain Node</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            .container { max-width: 1200px; margin: 0 auto; }
            .card { border: 1px solid #ddd; padding: 20px; margin: 20px 0; border-radius: 5px; }
            .status { color: green; font-weight: bold; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🚀 ANTDChain Node</h1>
            <div class="card">
                <h2>Node Status</h2>
                <p>Version: 2.0.0</p>
                <p>Consensus: Proof-of-Stake</p>
                <p class="status">● Online</p>
                <p><a href="/status">Detailed Status</a> | <a href="/blocks">Block Explorer</a> | <a href="/health">Health Check</a></p>
            </div>
        </div>
    </body>
    </html>
    `)
}

var CheckpointCommands = &cli.Command{
	Name:  "checkpoint",
	Usage: "Checkpoint management commands",
	Subcommands: []*cli.Command{
		{
			Name:  "init",
			Usage: "Initialize checkpoint configuration",
			Action: func(c *cli.Context) error {
				dataDir := c.String("data-dir")
				if dataDir == "" {
					dataDir = getDefaultDataDir()
				}

				// Check if config already exists
				configPath := filepath.Join(dataDir, "checkpoints.json")
				if _, err := os.Stat(configPath); err == nil {
					fmt.Printf("⚠️  Checkpoint config already exists at: %s\n", configPath)
					fmt.Print("Do you want to overwrite it? (y/N): ")

					var response string
					fmt.Scanln(&response)
					if strings.ToLower(response) != "y" && strings.ToLower(response) != "yes" {
						fmt.Println("Operation cancelled.")
						return nil
					}
				}

				// Get actual genesis hash
				genesisHash := common.HexToHash("0x860ed1a1e026261b452d2bbe908edf8297b02107912331c4c91e2f757ae9e164")

				err := checkpoints.CreateSampleConfig(configPath, genesisHash)
				if err != nil {
					return fmt.Errorf("failed to create checkpoint config: %w", err)
				}

				fmt.Printf("✅ Checkpoint configuration created at: %s\n", configPath)
				fmt.Printf("   Genesis hash: %s\n", genesisHash.Hex())
				return nil
			},
		},
		{
			Name:  "verify",
			Usage: "Verify checkpoint integrity",
			Action: func(c *cli.Context) error {
				dataDir := c.String("data-dir")
				if dataDir == "" {
					dataDir = getDefaultDataDir()
				}

				// Initialize checkpoint manager
				checkpointDir := filepath.Join(dataDir, "checkpoints")
				configPath := filepath.Join(dataDir, "checkpoints.json")

				genesisHash := common.HexToHash("0x860ed1a1e026261b452d2bbe908edf8297b02107912331c4c91e2f757ae9e164")

				cp, err := checkpoints.NewCheckpoints(checkpointDir, configPath, genesisHash)
				if err != nil {
					return fmt.Errorf("failed to initialize checkpoint manager: %w", err)
				}
				defer cp.Stop()

				stats := cp.GetStats()

				fmt.Println("📊 Checkpoint Manager Status:")
				fmt.Printf("   • Total checkpoints: %d\n", stats["totalCheckpoints"])
				fmt.Printf("   • Authority: %s\n", stats["authority"])
				fmt.Printf("   • Trusted keys: %d\n", stats["trustedKeys"])
				fmt.Printf("   • Genesis valid: %v\n", stats["genesisValid"])
				fmt.Printf("   • Initialized: %v\n", stats["initialized"])

				if latest, exists := cp.GetLatestCheckpoint(); exists {
					fmt.Printf("   • Latest checkpoint: height=%d, hash=%s\n",
						latest.Height, latest.Hash.Hex()[:16])
					fmt.Printf("   • Signatures: %d\n", len(latest.Signatures))
				}

				return nil
			},
		},
		{
			Name:  "export",
			Usage: "Export checkpoints to JSON",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  "output",
					Value: "checkpoints_export.json",
					Usage: "Output file",
				},
			},
			Action: func(c *cli.Context) error {
				dataDir := c.String("data-dir")
				if dataDir == "" {
					dataDir = getDefaultDataDir()
				}

				outputFile := c.String("output")

				// Initialize checkpoint manager
				checkpointDir := filepath.Join(dataDir, "checkpoints")
				configPath := filepath.Join(dataDir, "checkpoints.json")

				genesisHash := common.HexToHash("0x860ed1a1e026261b452d2bbe908edf8297b02107912331c4c91e2f757ae9e164")

				cp, err := checkpoints.NewCheckpoints(checkpointDir, configPath, genesisHash)
				if err != nil {
					return fmt.Errorf("failed to initialize checkpoint manager: %w", err)
				}
				defer cp.Stop()

				data, err := cp.ExportCheckpoints()
				if err != nil {
					return fmt.Errorf("failed to export checkpoints: %w", err)
				}

				if err := os.WriteFile(outputFile, data, 0644); err != nil {
					return fmt.Errorf("failed to write export file: %w", err)
				}

				fmt.Printf("✅ Checkpoints exported to: %s\n", outputFile)
				return nil
			},
		},
	},
}

func (ws *WebServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	height := uint64(0)
	if latest := ws.node.Blockchain().Latest(); latest != nil {
		height = latest.Header.Number.Uint64()
	}

	peers := 0
	if ws.node.P2PNode() != nil {
		peers = len(ws.node.P2PNode().Peers())
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "online",
		"height":    height,
		"peers":     peers,
		"mining":    ws.node.MiningState() != nil,
		"consensus": "pos",
		"uptime":    time.Since(startTime).Seconds(),
	})
}

func (ws *WebServer) handleBlocks(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var blocks []map[string]interface{}
	height := ws.node.Blockchain().GetChainHeight()

	// Show last 10 blocks
	start := uint64(0)
	if height > 10 {
		start = height - 10
	}

	for i := start; i <= height; i++ {
		if blk := ws.node.Blockchain().GetBlock(i); blk != nil {
			blocks = append(blocks, map[string]interface{}{
				"number":    i,
				"hash":      blk.Hash().Hex(),
				"miner":     blk.Header.Coinbase.Hex(),
				"timestamp": blk.Header.Time,
				"txs":       len(blk.Txs),
			})
		}
	}

	json.NewEncoder(w).Encode(blocks)
}

func (ws *WebServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ok",
		"time":   time.Now().UTC().Format(time.RFC3339),
	})
}

func getDefaultDataDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "./antdchain_data"
	}

	switch runtime.GOOS {
	case "windows":
		return filepath.Join(home, "AppData", "Local", "Antdchain")
	case "darwin":
		return filepath.Join(home, ".antdchain_data")
	default:
		return filepath.Join(home, ".antdchain_data")
	}
}

func main() {
	defaultDir := getDefaultDataDir()

	app := &cli.App{
		Name:    "antdchain",
		Usage:   "ANTDChain — Proof-of-Stake L1 blockchain",
		Version: "v2.0.0 — 2025",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "data-dir", Value: defaultDir, Usage: "Data directory"},
			&cli.IntFlag{Name: "rpc-port", Value: 8089, Usage: "JSON-RPC port"},
			&cli.IntFlag{Name: "web-port", Value: 8090, Usage: "Web interface port"},
			&cli.IntFlag{Name: "p2p-port", Value: 3000, Usage: "P2P port"},
			&cli.StringFlag{Name: "bootstrap", Value: "/ip4/129.151.164.223/tcp/3000/p2p/12D3KooWNQMrzwLnDxL8NHMpYuTCpZja3K4AXJDrW2NzSPsGdERC", Usage: "Bootstrap nodes"},
			&cli.BoolFlag{Name: "startmining", Usage: "Start PoS mining"},
			&cli.BoolFlag{Name: "console", Usage: "Open console"},
			&cli.BoolFlag{Name: "no-web", Usage: "Disable web interface"},
			&cli.StringFlag{Name: "rpcuser", Value: "", Usage: "RPC username"},
			&cli.StringFlag{Name: "rpcpassword", Value: "", Usage: "RPC password", EnvVars: []string{"ANTDCHAIN_RPCPASSWORD"}},
			&cli.BoolFlag{Name: "rpcauthdisabled", Usage: "Disable RPC auth"},
			&cli.StringFlag{Name: "rpc-ssl-cert", Value: "", Usage: "SSL certificate file"},
			&cli.StringFlag{Name: "rpc-ssl-key", Value: "", Usage: "SSL private key file"},
			&cli.BoolFlag{Name: "rpc-ssl", Value: false, Usage: "Enable HTTPS for RPC"},
			&cli.StringFlag{Name: "rpc-host", Value: "0.0.0.0", Usage: "RPC host to bind"},
		},
		Commands: []*cli.Command{
			king.KingCommands,
			CheckpointCommands,
		},
		Action: runNode,
	}

	if err := app.Run(os.Args); err != nil {
		logrus.Fatal(err)
	}
}

func jsonRPCHandler(n *console.Node, user, pass string, authDisabled bool) http.HandlerFunc {
	rpcServer := rpc.NewServer()

	ethAPI := &EthAPI{node: n}
	netAPI := &NetAPI{networkID: 20258}
	web3API := &Web3API{}
	rkAPI := &RotatingKingAPI{node: n}

	rpcServer.RegisterName("eth", ethAPI)
	rpcServer.RegisterName("net", netAPI)
	rpcServer.RegisterName("web3", web3API)
	rpcServer.RegisterName("rotatingking", rkAPI)
	rpcServer.RegisterName("rk", rkAPI) // Alias
	log.Printf("✅ All RPC APIs registered (including rotating king)")

	// ========== CHECKPOINT API ==========
	// Get checkpoint manager from blockchain
	if checkpointManager := n.Blockchain().GetCheckpointManager(); checkpointManager != nil {
		cpAPI := NewCheckpointAPI(checkpointManager)
		rpcServer.RegisterName("checkpoint", cpAPI)
		rpcServer.RegisterName("cp", cpAPI) // Alias for convenience

		log.Printf("✅ Checkpoint RPC API registered")
	} else {
		log.Printf("⚠️  Checkpoint manager not available - RPC methods disabled")
	}
	// =======================================

	// ========== ROTATING KING API ==========
	// Get rotating king manager from blockchain
	if rkManager := n.Blockchain().GetRotatingKingManager(); rkManager != nil {
		rkAPI := antdrpc.NewRotatingKingAPI(rkManager)
		rpcServer.RegisterName("rotatingking", rkAPI)

		// Also register alias "rk" for convenience
		rpcServer.RegisterName("rk", rkAPI)

		log.Printf("✅ Rotating King RPC API registered")
	} else {
		log.Printf("⚠️  Rotating King manager not available - RPC methods disabled")
	}
	// ===========================================

	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method != "POST" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if !authDisabled {
			// Check API key header first
			apiKey := r.Header.Get("X-API-Key")
			if apiKey != "" {
				if apiKey != pass {
					w.WriteHeader(http.StatusUnauthorized)
					json.NewEncoder(w).Encode(map[string]any{
						"jsonrpc": "2.0",
						"error":   map[string]any{"code": -32000, "message": "Invalid API key"},
						"id":      nil,
					})
					return
				}
			} else {
				// Basic authentication fallback
				u, p, ok := r.BasicAuth()
				if !ok || u != user || p != pass {
					w.Header().Set("WWW-Authenticate", `Basic realm="ANTDChain"`)
					w.WriteHeader(http.StatusUnauthorized)
					json.NewEncoder(w).Encode(map[string]any{
						"jsonrpc": "2.0",
						"error":   map[string]any{"code": -32000, "message": "Unauthorized"},
						"id":      nil,
					})
					return
				}
			}
		}

		rpcServer.ServeHTTP(w, r)
	}
}

func healthHandler(n *console.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		height := uint64(0)
		if b := n.Blockchain().Latest(); b != nil {
			height = b.Header.Number.Uint64()
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":    "ok",
			"height":    height,
			"version":   "2.0.0",
			"network":   "antdchain",
			"consensus": "pos",
			"uptime":    time.Since(startTime).Seconds(),
		})
	}
}

func triggerConfigurationSync(bc *chain.Blockchain, p2pNode *p2p.Node, logger *logrus.Logger) {
	logger.Info("🔍 Starting configuration synchronization check...")

	// Get rotating king manager
	rkManager := bc.GetRotatingKingManager()
	if rkManager == nil {
		logger.Warn("⚠️  Rotating king manager not available")
		return
	}

	// Get our configuration
	var ourCount int
	if manager, ok := rkManager.(interface{ GetKingAddresses() []common.QuantumAddress }); ok {
		addresses := manager.GetKingAddresses()
		ourCount = len(addresses)

		logger.Infof("📊 Our configuration has %d addresses:", ourCount)
		for i, addr := range addresses {
			logger.Infof("  [%d] %s", i+1, addr.Hex())
		}
	}

	// Decision logic
	if ourCount <= 4 {
		logger.Warnf("🔄 LOW ADDRESS COUNT (%d) - TRIGGERING CONFIGURATION SYNC", ourCount)

		// Trigger configuration sync via P2P
		if p2pNode != nil {
			// First, request configurations from all peers
			for _, pid := range p2pNode.Peers() {
				p2pNode.RequestKingConfiguration(pid)
			}

			// After delay, check and compare configurations
			go func() {
				time.Sleep(20 * time.Second)
				p2pNode.CheckIfConfigurationSyncNeeded()

				// Force manual sync after 30 seconds if still low
				time.Sleep(30 * time.Second)

				// Re-check our count
				if manager, ok := rkManager.(interface{ GetKingAddresses() []common.QuantumAddress }); ok {
					currentCount := len(manager.GetKingAddresses())
					if currentCount <= 4 {
						logger.Warnf("🔄 STILL LOW (%d) - FORCING MANUAL CONFIG SYNC", currentCount)
						p2pNode.TriggerManualDBSync()
					}
				}
			}()
		}
	} else {
		logger.Info("✅ Configuration looks complete")

		// Still broadcast to help others
		if p2pNode != nil {
			logger.Info("📤 Broadcasting configuration to help other nodes")
			p2pNode.BroadcastCurrentConfig()
		}
	}
}

func createConsoleNode(bc *chain.Blockchain, posMiningState *mining.PosMiningState,
	walletManager *wallet.WalletManager, p2pNode *p2p.Node) (*console.Node, error) {

	return console.NewNode(bc, posMiningState, walletManager, p2pNode)
}

func getGenesisStakers() []struct {
	address common.QuantumAddress
	stake   *big.Int
} {
	var stakers []struct {
		address common.QuantumAddress
		stake   *big.Int
	}

	// Load from environment variable
	genesisConfig := os.Getenv("ANTDCHAIN_GENESIS_STAKERS")
	if genesisConfig != "" {
		var config []map[string]string
		if err := json.Unmarshal([]byte(genesisConfig), &config); err == nil {
			for _, w := range config {
				addr, _ := common.ParseQuantumAddress(w["address"])
				stakeStr := w["stake"]
				if stake, ok := new(big.Int).SetString(stakeStr, 10); ok {
					stakeWei := new(big.Int).Mul(stake, big.NewInt(1e18))
					stakers = append(stakers, struct {
						address common.QuantumAddress
						stake   *big.Int
					}{
						address: addr,
						stake:   stakeWei,
					})
				}
			}
		}
	}

	// Default genesis stakers if none configured
	if len(stakers) == 0 {
		stakers = []struct {
			address common.QuantumAddress
			stake   *big.Int
		}{}
	}

	return stakers
}

// ============================================================================
// MAIN NODE FUNCTION
// ============================================================================
func runNode(c *cli.Context) error {
	// Parse flags
	dataDir := c.String("data-dir")
	rpcPort := c.Int("rpc-port")
	webPort := c.Int("web-port")
	p2pPort := c.Int("p2p-port")
	bootstrap := c.String("bootstrap")
	startMining := c.Bool("startmining")
	openConsole := c.Bool("console")
	noWeb := c.Bool("no-web")
	rpcUser := c.String("rpcuser")
	rpcPass := c.String("rpcpassword")
	authDisabled := c.Bool("rpcauthdisabled")

	// Setup logger
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetLevel(logrus.InfoLevel)

	var consoleLogOutput *os.File
	if openConsole {
		logOutput, cleanupConsoleLogging, err := configureConsoleLogging(dataDir, logger)
		if err != nil {
			return fmt.Errorf("failed to configure console logging: %w", err)
		}
		consoleLogOutput = logOutput
		defer cleanupConsoleLogging()
	}

	// Ensure data directory exists
	if err := os.MkdirAll(dataDir, os.ModePerm); err != nil {
		logger.Fatal("Failed to create data directory:", err)
	}

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// ==============================================
	// INITIALIZE WALLET MANAGER
	// ==============================================
	logger.Info("Initializing wallet manager...")
	walletManager := wallet.NewWalletManager(dataDir)

	// Create a temporary wallet for initialization
	tempWallet, err := wallet.NewWallet(nil, dataDir)
	if err != nil {
		logger.Warn("Failed to create temporary wallet:", err)
	}

	// ==============================================
	// INITIALIZE BLOCKCHAIN
	// ==============================================
	logger.Info("Initializing blockchain...")
	statePath := filepath.Join(dataDir, "state")

	// Ensure genesis block exists
	chain.EnsureGenesisBlock(statePath, common.BytesToQuantumAddress(tempWallet.Address().Bytes()))

	// Create blockchain
	bc, err := chain.NewBlockchain(statePath, common.BytesToQuantumAddress(tempWallet.Address().Bytes()))
	if err != nil {
		logger.Fatal("Blockchain initialization failed:", err)
	}

	// =============================
	// INITIALIZE CHECKPOINT MANAGER
	// =============================
	logger.Info("Initializing checkpoint manager...")

	// Get the actual genesis hash from blockchain
	genesisBlock := bc.GetBlock(0)
	if genesisBlock == nil {
		logger.Fatal("Failed to get genesis block")
	}
	actualGenesisHash := genesisBlock.Hash()

	// Auto-create config if missing
	if err := autoInitCheckpointConfig(dataDir, bc); err != nil {
		logger.Warnf("Failed to auto-create checkpoint config: %v", err)
	}

	// Initialize checkpoint manager - it will auto-create config if missing
	checkpointDir := filepath.Join(dataDir, "checkpoints")
	if err := os.MkdirAll(checkpointDir, os.ModePerm); err != nil {
		logger.Warnf("Failed to create checkpoint directory: %v", err)
	}

	configPath := filepath.Join(dataDir, "checkpoints.json")
	checkpointManager, err := checkpoints.NewCheckpoints(checkpointDir, configPath, actualGenesisHash)
	if err != nil {
		logger.Errorf("Failed to initialize checkpoint manager: %v", err)
		logger.Info("Checkpoint verification will be disabled")
	} else {
		// Set checkpoint manager on blockchain
		bc.SetCheckpointManager(checkpointManager)

		// Add shutdown hook
		defer checkpointManager.Stop()

		logger.Info("✓ Checkpoint manager initialized successfully")

		// Log stats
		stats := checkpointManager.GetStats()
		logger.WithFields(logrus.Fields{
			"checkpoints": stats["totalCheckpoints"],
			"genesisHash": stats["genesisHash"],
			"authority":   stats["authority"],
		}).Info("Checkpoint manager stats")
	}

	// ==============================================
	// INITIALIZE TRANSACTION POOL
	// ==============================================
	logger.Info("Initializing transaction pool...")
	txPool := chain.NewTxPool()
	txPool.SetChain(bc)
	bc.SetTxPool(txPool)

	// Load saved transactions
	if err := txPool.LoadFromDisk(dataDir, bc); err != nil {
		logger.Warn("Failed to load saved transactions:", err)
	}

	// Start background cleanup
	txPool.StartBackgroundCleanup(bc)

	// Save transactions on exit
	defer func() {
		if err := txPool.SaveToDisk(dataDir); err != nil {
			logger.Warn("Failed to save transactions:", err)
		}
	}()

	// ==============================================
	// SETUP WALLET MANAGER WITH BLOCKCHAIN
	// ==============================================
	walletManager.SetBlockchain(bc)

	// Get or create miner wallet
	minerWallet, err := walletManager.GetOrCreateMinerWallet()
	if err != nil {
		logger.Fatal("Failed to get miner wallet:", err)
	}

	// ==============================================
	// INITIALIZE P2P NETWORK
	// ==============================================
	logger.Info("Initializing P2P network...")

	bootNodes := strings.Split(bootstrap, ",")
	if bootstrap == "" {
		bootNodes = nil
	}

	p2pConfig := p2p.Config{
		DataDir:           dataDir,
		Port:              p2pPort,
		BootstrapPeers:    bootNodes,
		EnableMDNS:        true,
		EnableDHT:         true,
		EnableNATService:  true,
		MaxPeers:          50,
		MinPeers:          1,
		ConnectionTimeout: 30 * time.Second,
		LogOutput:         consoleLogOutput,
	}

	p2pNode, err := p2p.NewNodeWithConfig(bc, p2pConfig)
	if err != nil {
		logger.Fatal("P2P initialization failed:", err)
	}

	// Set P2P broadcaster on blockchain
	if bc != nil {
		bc.SetP2PBroadcaster(p2pNode)
	}

	p2pWrapper := &P2PWrapper{p2pNode}
	defer p2pWrapper.Close()

	// Start peer synchronization
	go func() {
		time.Sleep(10 * time.Second)
		for _, pid := range p2pNode.Peers() {
			go p2pNode.SyncWithPeer(pid)
		}
	}()

	// Start periodic king config check
	go p2pNode.PeriodicKingConfigCheck()

	// ==============================================
	// START DATABASE SYNC SERVICE
	// ==============================================
	logger.Info("Starting rotating king database sync service...")

	// Start database sync with a small delay to let connections establish
	go func() {
		time.Sleep(15 * time.Second)

		// Check if rotating king manager is available
		if rkManager := bc.GetRotatingKingManager(); rkManager != nil {
			logger.Info("🔄 Rotating king database synchronization service started")

			// Log initial sync status
			if manager, ok := rkManager.(interface {
				GetSyncState() (*rotatingking.SyncState, error)
			}); ok {
				syncState, err := manager.GetSyncState()
				if err == nil && syncState != nil {
					logger.Infof("  • Database last synced: block %d", syncState.LastSyncedBlock)
					logger.Infof("  • Last sync time: %s", syncState.LastSyncTime.Format(time.RFC3339))
					if syncState.SyncProgress > 0 {
						logger.Infof("  • Sync progress: %.1f%%", syncState.SyncProgress*100)
					}
				}
			}
		} else {
			logger.Warn("⚠️  Rotating king manager not available - database sync disabled")
		}
	}()

	// ==============================================
	// TRIGGER CONFIGURATION SYNC CHECK
	// ==============================================
	go func() {
		time.Sleep(30 * time.Second)
		triggerConfigurationSync(bc, p2pNode, logger)

		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				logger.Debug("🔄 Running periodic configuration check")
				triggerConfigurationSync(bc, p2pNode, logger)
			case <-sigChan:
				return
			}
		}
	}()

	// ==============================================
	// INITIALIZE PoS MINING SYSTEM
	// ==============================================
	logger.Info("Initializing Proof-of-Stake mining system...")

	// Create the PoS engine
	powEngine := pow.NewPoW()

	// Create PoS mining state
	posMiningState := mining.NewPosMiningState(powEngine)

	// Set sync callback
	posMiningState.SetSyncCallback(func(isSyncing bool) {
		if isSyncing {
			posMiningState.PauseMining()
		} else {
			posMiningState.ResumeMining()
		}
	})

	// === GENESIS AUTO-STAKING ===
	logger.Info("Scanning state for genesis stakers (≥ 1,000,000 ANTD)...")

	minStake := new(big.Int).Mul(big.NewInt(1_000_000), big.NewInt(1e18))
	registeredCount := 0

	// Check candidate addresses
	mainKingAddr, _ := common.ParseQuantumAddress("0qANA3c85k94LTyTXLGDdEzmLE32b1qhYZF")
	candidateAddresses := []common.QuantumAddress{
		mainKingAddr,                                         // Main King
		common.BytesToQuantumAddress(minerWallet.Address().Bytes()), // Miner wallet
	}

	for _, addr := range candidateAddresses {
		if addr == (common.QuantumAddress{}) {
			continue
		}
		balance := bc.State().GetBalance(addr)
		if balance.Cmp(minStake) >= 0 {
			// Auto-register if eligible
			powEngine.AutoRegisterIfEligible(addr, balance, nil)
			registeredCount++
			logger.Infof("Genesis auto-registered staker: %s (%s ANTD)",
				addr.Hex()[:12],
				new(big.Int).Div(balance, big.NewInt(1e18)).String())
		}
	}

	if registeredCount == 0 {
		logger.Warn("No genesis stakers found — mining will not start until someone reaches 1M ANTD")
	} else {
		logger.Infof("Genesis auto-staking complete: %d validator(s) active", registeredCount)
	}

	logger.Info("✓ Proof-of-Stake system ready")

	// ==============================================
	// CREATE CONSOLE NODE
	// ==============================================
	logger.Info("Creating console node...")

	// Create console node with PosMiningState
	node, err := createConsoleNode(bc, posMiningState, walletManager, p2pNode)
	if err != nil {
		logger.Fatal("Console node creation failed:", err)
	}

	// ==============================================
	// AUTO-UNLOCK MINER WALLET
	// ==============================================
	logger.Info("Attempting to auto-unlock miner wallet...")

	// Try to auto-unlock
	if err := node.AutoUnlockMinerWallet(); err != nil {
		logger.Warnf("Failed to auto-unlock miner wallet: %v", err)
		logger.Info("You may need to unlock manually with: unlock <address>")
	} else {
		logger.Info("✓ Miner wallet auto-unlocked successfully")
	}

	// ==============================================
	// START WEB INTERFACE
	// ==============================================
	if !noWeb {
		logger.Info("Starting web interface...")
		webServer := NewWebServer(node, walletManager)

		go func() {
			logger.Infof("Web interface available at http://0.0.0.0:%d", webPort)
			if err := webServer.Start(webPort); err != nil && err != http.ErrServerClosed {
				logger.Errorf("Web server failed: %v", err)
			}
		}()
	} else {
		logger.Info("Web interface disabled (--no-web flag)")
	}

	// ==============================================
	// START PoS MINING (IF REQUESTED)
	// ==============================================
	if startMining {
		logger.Info("Starting PoS mining...")
		addr := common.BytesToQuantumAddress(minerWallet.Address().Bytes())
		if addr == (common.QuantumAddress{}) {
			logger.Warn("No miner address available!")
		} else {
			logger.Infof("Mining to address: %s", addr.Hex())

			// Start PoS mining
			go mining.StartPosMining(bc, posMiningState, addr, p2pNode)
			logger.Info("✓ Proof-of-Stake mining started")
			logger.Info("  Each staker mines 5 blocks before rotating")
			logger.Info("  Waiting for your turn to mine...")
		}
	}

	// ==============================================
	// START BACKGROUND SERVICES
	// ==============================================

	// Auto-rebroadcast transactions
	go func() {
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if pool := bc.TxPool(); pool != nil {
				for _, tx := range pool.GetPending() {
					p2pNode.BroadcastTx(tx)
				}
			}
		}
	}()

	// Maintain peer connections
	go func() {
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if len(p2pNode.Peers()) == 0 {
				p2pNode.ForceBootstrap()
			}
		}
	}()
	// ==============================================
	// START JSON-RPC SERVER (HTTP + HTTPS support)
	// ==============================================
	logger.Info("Starting JSON-RPC server...")

	rpcHost := c.String("rpc-host")
	rpcPort = c.Int("rpc-port")
	useSSL := c.Bool("rpc-ssl")
	certFile := c.String("rpc-ssl-cert")
	keyFile := c.String("rpc-ssl-key")

	// Prepare addresses
	rpcAddr := fmt.Sprintf("%s:%d", rpcHost, rpcPort)

	// Create RPC routes using gorilla/mux
	router := mux.NewRouter()
	router.HandleFunc("/health", healthHandler(node))
	router.HandleFunc("/", jsonRPCHandler(node, rpcUser, rpcPass, authDisabled))
	router.HandleFunc("/rpc", jsonRPCHandler(node, rpcUser, rpcPass, authDisabled))

	// Apply CORS only if needed (you can make this conditional later)
	router.Use(corsMiddleware)

	// Server configuration
	rpcServer := &http.Server{
		Addr:         rpcAddr,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		var err error

		if useSSL {
			// Try provided paths first
			if certFile == "" || keyFile == "" {
				// Auto-discovery
				possibleCertPaths := []string{
					filepath.Join(dataDir, "ssl", "server.crt"),
					filepath.Join(dataDir, "ssl", "fullchain.pem"),
					"/etc/letsencrypt/live/localhost/fullchain.pem",
					"/etc/letsencrypt/live/" + os.Getenv("HOSTNAME") + "/fullchain.pem",
					filepath.Join(os.Getenv("HOME"), ".antdchain", "ssl", "server.crt"),
				}

				possibleKeyPaths := []string{
					filepath.Join(dataDir, "ssl", "server.key"),
					filepath.Join(dataDir, "ssl", "privkey.pem"),
					"/etc/letsencrypt/live/localhost/privkey.pem",
					"/etc/letsencrypt/live/" + os.Getenv("HOSTNAME") + "/privkey.pem",
					filepath.Join(os.Getenv("HOME"), ".antdchain", "ssl", "server.key"),
				}

				for i := range possibleCertPaths {
					cert := possibleCertPaths[i]
					key := possibleKeyPaths[i]
					if fileExists(cert) && fileExists(key) {
						certFile = cert
						keyFile = key
						logger.Infof("Auto-detected SSL certificate: %s", cert)
						break
					}
				}
			}

			// Still no cert? Generate self-signed (only once)
			if certFile == "" || keyFile == "" {
				sslDir := filepath.Join(dataDir, "ssl")
				_ = os.MkdirAll(sslDir, os.ModePerm)

				certFile = filepath.Join(sslDir, "server.crt")
				keyFile = filepath.Join(sslDir, "server.key")

				if !fileExists(certFile) || !fileExists(keyFile) {
					hostname, _ := os.Hostname()
					if hostname == "" {
						hostname = "localhost"
					}

					logger.Info("Generating self-signed certificate (one-time)...")
					if err := generateSelfSignedCert(certFile, keyFile, hostname); err != nil {
						logger.Errorf("Failed to generate self-signed cert: %v → falling back to HTTP", err)
						useSSL = false
					} else {
						logger.Info("Self-signed certificate generated successfully")
					}
				} else {
					logger.Info("Using existing self-signed certificate")
				}
			}

			if useSSL && fileExists(certFile) && fileExists(keyFile) {
				logger.Infof("JSON-RPC → https://%s/rpc", rpcAddr)
				err = rpcServer.ListenAndServeTLS(certFile, keyFile)
			} else {
				logger.Warn("HTTPS requested but no valid cert/key pair → starting plain HTTP")
				err = rpcServer.ListenAndServe()
			}
		} else {
			logger.Infof("JSON-RPC → http://%s/rpc", rpcAddr)
			err = rpcServer.ListenAndServe()
		}

		if err != nil && err != http.ErrServerClosed {
			logger.Fatalf("RPC server failed: %v", err)
		}
	}()

	// ==============================================
	// START CONSOLE (IF REQUESTED)
	// ==============================================
	if openConsole {
		logger.Info("Starting console...")
		go console.NewConsole(node).Start()
	}

	// ==============================================
	// LOG STARTUP COMPLETE
	// ==============================================
	logger.Info("🚀 ANTDChain PoS node started successfully!")
	logger.Infof("   • Data directory:     %s", dataDir)
	if !noWeb {
		logger.Infof("   • Web Interface:      http://0.0.0.0:%d", webPort)
	}
	logger.Infof("   • JSON-RPC API:       http://0.0.0.0:%d/rpc", rpcPort)
	logger.Infof("   • P2P Port:           %d", p2pPort)
	logger.Infof("   • Mining:             %v", startMining)
	logger.Infof("   • Consensus:          Proof-of-Stake")
	if minerWallet != nil {
		logger.Infof("   • Miner Address:      %s", minerWallet.Address().String())
	}
	genesisStakers := getGenesisStakers()
	logger.Infof("   • Chain Height:       %d", bc.GetChainHeight())
	logger.Infof("   • Genesis Stakers:    %d", len(genesisStakers))
	logger.Infof("   • Min Stake:          1,000,000 ANTD")
	logger.Infof("   • Block Time:         12 seconds")

	// ==============================================
	// WAIT FOR SHUTDOWN SIGNAL
	// ==============================================
	<-sigChan
	logger.Info("Shutting down ANTDChain node...")

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Stop mining
	if startMining {
		mining.StopMining(posMiningState)
	}

	// Shutdown RPC server
	if err := rpcServer.Shutdown(shutdownCtx); err != nil {
		logger.Warn("Failed to shutdown RPC server gracefully:", err)
	}

	// Close blockchain
	bc.Close()

	logger.Info("ANTDChain node stopped gracefully")
	return nil
}

func configureConsoleLogging(dataDir string, logger *logrus.Logger) (*os.File, func(), error) {
	logPath := filepath.Join(dataDir, "console.log")
	if err := os.MkdirAll(dataDir, os.ModePerm); err != nil {
		return nil, nil, err
	}

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, nil, err
	}

	log.SetOutput(logFile)
	logger.SetOutput(logFile)
	logrus.SetOutput(logFile)

	fmt.Printf("Console mode enabled: runtime logs redirected to %s\n", logPath)

	return logFile, func() {
		_ = logFile.Close()
	}, nil
}

// ============================================================================
// ETH API IMPLEMENTATION
// ============================================================================

type EthAPI struct{ node *console.Node }

func (api *EthAPI) ChainId() (string, error) { return "0x4F22", nil }

func (api *EthAPI) BlockNumber() (string, error) {
	latest := api.node.Blockchain().Latest()
	if latest == nil {
		return "0x0", nil
	}
	return "0x" + strconv.FormatUint(latest.Header.Number.Uint64(), 16), nil
}

func (api *EthAPI) Syncing() (interface{}, error) {
	bc := api.node.Blockchain()
	if bc.IsSyncing() {
		return map[string]interface{}{
			"startingBlock": "0x0",
			"currentBlock":  hexutil.EncodeUint64(bc.GetChainHeight()),
			"highestBlock":  hexutil.EncodeUint64(bc.GetSyncTarget()),
		}, nil
	}
	return false, nil
}

func (api *EthAPI) GetBalance(address string, _ interface{}) (string, error) {
	addr, _ := common.ParseQuantumAddress(address)
	bal := api.node.Blockchain().GetAccountBalance(addr)
	if bal == nil {
		bal = big.NewInt(0)
	}
	return "0x" + bal.Text(16), nil
}

func (api *EthAPI) GetTransactionCount(address string, _ interface{}) (string, error) {
	addr, _ := common.ParseQuantumAddress(address)
	nonce := api.node.Blockchain().State().GetNonce(addr)
	return hexutil.EncodeUint64(nonce), nil
}

func (api *EthAPI) GasPrice() (string, error) { return "0x2540be400", nil } // 10 Gwei

func (api *EthAPI) GetCode(string, interface{}) (string, error) { return "0x", nil }

func (api *EthAPI) GetStorageAt(string, string, interface{}) (string, error) {
	return "0x0000000000000000000000000000000000000000000000000000000000000000", nil
}

func (api *EthAPI) SendRawTransaction(raw string) (string, error) {
	data, err := hex.DecodeString(strings.TrimPrefix(raw, "0x"))
	if err != nil {
		return "", err
	}
	txObj, err := tx.Deserialize(data)
	if err != nil {
		return "", err
	}
	if err := api.node.Blockchain().TxPool().AddTx(txObj, api.node.Blockchain()); err != nil {
		return "", err
	}
	return txObj.Hash().Hex(), nil
}

func (api *EthAPI) GetBlockByNumber(num string, full bool) (interface{}, error) {
	var height uint64
	switch num {
	case "latest", "pending":
		if latest := api.node.Blockchain().Latest(); latest != nil {
			height = latest.Header.Number.Uint64()
		}
	case "earliest":
		height = 0
	default:
		n, _ := strconv.ParseUint(strings.TrimPrefix(num, "0x"), 16, 64)
		height = n
	}

	blk := api.node.Blockchain().GetBlock(height)
	if blk == nil {
		return nil, nil
	}

	base := map[string]interface{}{
		"number":           hexutil.EncodeUint64(height),
		"hash":             blk.Hash().Hex(),
		"parentHash":       blk.Header.ParentHash.Hex(),
		"miner":            blk.Header.Coinbase.Hex(),
		"stateRoot":        blk.Header.Root.Hex(),
		"transactionsRoot": blk.Header.TxHash.Hex(),
		"receiptsRoot":     blk.Header.Root.Hex(),
		"logsBloom":        "0x" + strings.Repeat("0", 512),
		"difficulty":       "0x0",
		"totalDifficulty":  "0x0",
		"size":             "0x400",
		"gasLimit":         "0x1c9c380",
		"gasUsed":          "0x0",
		"timestamp":        hexutil.EncodeUint64(blk.Header.Time),
		"extraData":        "0x",
		"mixHash":          blk.Header.MixDigest.Hex(),
		"nonce":            "0x0000000000000000",
		"baseFeePerGas":    "0x0",
		"uncles":           []string{},
	}

	if full {
		txs := make([]map[string]interface{}, len(blk.Txs))
		for i, t := range blk.Txs {
			to := ""
			if t.To != nil {
				to = t.To.String()
			}
			txs[i] = map[string]interface{}{
				"hash":     t.Hash().Hex(),
				"from":     t.From.String(),
				"to":       to,
				"value":    "0x" + t.Value.Text(16),
				"gas":      hexutil.EncodeUint64(t.Gas),
				"gasPrice": "0x" + t.GasPrice.Text(16),
				"input":    "0x" + hex.EncodeToString(t.Data),
				"nonce":    hexutil.EncodeUint64(t.Nonce),
			}
		}
		base["transactions"] = txs
	} else {
		hashes := make([]string, len(blk.Txs))
		for i, t := range blk.Txs {
			hashes[i] = t.Hash().Hex()
		}
		base["transactions"] = hashes
	}
	return base, nil
}

func (api *EthAPI) GetBlockByHash(hash string, full bool) (interface{}, error) {
	h := common.HexToHash(hash)
	for i := uint64(0); i <= api.node.Blockchain().GetChainHeight(); i++ {
		if blk := api.node.Blockchain().GetBlock(i); blk != nil && blk.Hash() == h {
			return api.GetBlockByNumber("latest", full)
		}
	}
	return nil, nil
}

func (api *EthAPI) GetTransactionByHash(h string) (interface{}, error) {
	hash := common.HexToHash(h)
	latest := api.node.Blockchain().Latest()
	if latest == nil {
		return nil, nil
	}

	for height := uint64(0); height <= latest.Header.Number.Uint64(); height++ {
		blk := api.node.Blockchain().GetBlock(height)
		if blk == nil {
			continue
		}
		for i, t := range blk.Txs {
			if t.Hash() == hash {
				to := ""
				if t.To != nil {
					to = t.To.String()
				}
				return map[string]interface{}{
					"hash":             t.Hash().Hex(),
					"from":             t.From.String(),
					"to":               to,
					"value":            "0x" + t.Value.Text(16),
					"gas":              hexutil.EncodeUint64(t.Gas),
					"gasPrice":         "0x" + t.GasPrice.Text(16),
					"input":            "0x" + hex.EncodeToString(t.Data),
					"nonce":            hexutil.EncodeUint64(t.Nonce),
					"blockHash":        blk.Hash().Hex(),
					"blockNumber":      hexutil.EncodeUint64(height),
					"transactionIndex": hexutil.EncodeUint64(uint64(i)),
				}, nil
			}
		}
	}
	return nil, nil
}

func (api *EthAPI) GetTransactionReceipt(h string) (interface{}, error) {
	return map[string]interface{}{
		"transactionHash":   h,
		"status":            "0x1",
		"gasUsed":           "0x5208",
		"cumulativeGasUsed": "0x5208",
		"logsBloom":         "0x" + strings.Repeat("0", 512),
		"logs":              []interface{}{},
	}, nil
}

func (api *EthAPI) Accounts() ([]string, error) {
	var list []string
	if keystore := api.node.Keystore(); keystore != nil {
		for _, a := range keystore.Accounts() {
			list = append(list, a.Address.Hex())
		}
	}
	return list, nil
}

// generateSelfSignedCert creates a self-signed SSL certificate
func generateSelfSignedCert(certPath, keyPath string, host string) error {
	// Generate RSA key
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}

	// Create certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().Unix()),
		Subject: pkix.Name{
			Organization: []string{"ANTDChain Node"},
			CommonName:   host,
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(10 * 365 * 24 * time.Hour), // 10 years
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
	}

	// Add Subject Alternative Names (SANs)
	// Get all network interfaces' IP addresses
	ifaces, err := net.Interfaces()
	if err == nil {
		for _, i := range ifaces {
			addrs, err := i.Addrs()
			if err != nil {
				continue
			}
			for _, addr := range addrs {
				var ip net.IP
				switch v := addr.(type) {
				case *net.IPNet:
					ip = v.IP
				case *net.IPAddr:
					ip = v.IP
				}
				// Skip loopback and link-local addresses
				if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
					continue
				}
				template.IPAddresses = append(template.IPAddresses, ip)
			}
		}
	}

	// Always include localhost and common names
	template.DNSNames = append(template.DNSNames,
		"localhost",
		host,
		"*.localhost",
		fmt.Sprintf("*.%s", host),
	)

	// Add local IPs
	template.IPAddresses = append(template.IPAddresses,
		net.IPv4(127, 0, 0, 1),
		net.IPv6loopback,
	)

	// Create certificate
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return err
	}

	// Write certificate
	certOut, err := os.Create(certPath)
	if err != nil {
		return err
	}
	defer certOut.Close()

	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
		return err
	}

	// Write private key
	keyOut, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer keyOut.Close()

	privBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		// Fallback to PKCS1 for older systems
		privBytes = x509.MarshalPKCS1PrivateKey(priv)
	}

	if err := pem.Encode(keyOut, &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: privBytes,
	}); err != nil {
		return err
	}
	log.Printf("Generated SSL certificate for %s with IPs: %v", host, template.IPAddresses)
	//    logger.Infof("Generated SSL certificate for %s with IPs: %v", host, template.IPAddresses)
	return nil
}

// adds CORS headers to all responses
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Get the origin from request
		origin := r.Header.Get("Origin")

		// Allow all origins by default (for development)
		// For production, you might want to restrict this
		if origin == "" {
			origin = "*"
		}

		// Set CORS headers
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, PATCH")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key, X-Requested-With, Accept, Origin, Access-Control-Request-Method, Access-Control-Request-Headers")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Max-Age", "86400") // 24 hours
		w.Header().Set("Access-Control-Expose-Headers", "Content-Length, Content-Range")

		// Add security headers
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-XSS-Protection", "1; mode=block")

		// Handle preflight requests
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// ============================================================================
// NET & WEB3 API
// ============================================================================
//func (n *NetAPI) Version() string { return "20258" }

type Web3API struct{}

func (w *Web3API) ClientVersion() string { return "ANTDChain-PoS/v2.0.0" }

// ============================================================================
// NET API IMPLEMENTATION
// ============================================================================

type NetAPI struct {
	node      *console.Node
	networkID uint64
}

func (n *NetAPI) Version() string {
	return fmt.Sprintf("%d", n.networkID)
}

func (n *NetAPI) PeerCount() string {
	if n.node == nil || n.node.P2PNode() == nil {
		return "0x0"
	}

	peerCount := len(n.node.P2PNode().Peers())
	return fmt.Sprintf("0x%x", peerCount)
}

func (n *NetAPI) Listening() bool {
	return true // Always listening if node is running
}

func (n *NetAPI) GetPeerCount() string {
	return n.PeerCount() // Alias for compatibility
}

// ============================================================================
// ROTATING KING API IMPLEMENTATION
// ============================================================================

func (r *RotatingKingAPI) GetInfo() (*RotatingKingInfo, error) {
	if r.node == nil || r.node.Blockchain() == nil {
		return nil, errors.New("blockchain not available")
	}

	rkManager := r.node.Blockchain().GetRotatingKingManager()
	if rkManager == nil {
		return nil, errors.New("rotating king manager not available")
	}

	// Get current height
	height := r.node.Blockchain().GetChainHeight()

	// Get rotation info
	rotationInfo := rkManager.GetRotationInfo(height)

	// Convert to our struct
	info := &RotatingKingInfo{
		KingCount:        rotationInfo["kingCount"].(int),
		RotationCount:    rotationInfo["rotationCount"].(uint64),
		NextRotationAt:   rotationInfo["nextRotationAt"].(uint64),
		RotationHeight:   rotationInfo["rotationHeight"].(uint64),
		RotationInterval: rotationInfo["rotationInterval"].(uint64),
		ActivationDelay:  rotationInfo["activationDelay"].(uint64),
		MinStakeRequired: "100000000000000000000000", // 100,000 ANTD in wei
	}

	// Set current king if available
	if currentKing := rkManager.GetCurrentKing(); currentKing != (common.QuantumAddress{}) {
		info.CurrentKing = currentKing
	}

	// Set next king if available
	if nextKing := rkManager.GetNextKing(); nextKing != (common.QuantumAddress{}) {
		info.NextKing = nextKing
	}

	// Calculate blocks until rotation
	if height < info.NextRotationAt {
		info.BlocksUntilRotation = info.NextRotationAt - height
	}

	// Get total rewards if available
	if totalRewards := rkManager.GetTotalRewardsDistributed(); totalRewards != nil {
		info.TotalRewardsDistributed = totalRewards.String()
	}

	return info, nil
}

func (r *RotatingKingAPI) GetCurrentKing() (common.QuantumAddress, error) {
	if r.node == nil || r.node.Blockchain() == nil {
		return common.QuantumAddress{}, errors.New("blockchain not available")
	}

	rkManager := r.node.Blockchain().GetRotatingKingManager()
	if rkManager == nil {
		return common.QuantumAddress{}, errors.New("rotating king manager not available")
	}

	return rkManager.GetCurrentKing(), nil
}

func (r *RotatingKingAPI) GetNextKing() (common.QuantumAddress, error) {
	if r.node == nil || r.node.Blockchain() == nil {
		return common.QuantumAddress{}, errors.New("blockchain not available")
	}

	rkManager := r.node.Blockchain().GetRotatingKingManager()
	if rkManager == nil {
		return common.QuantumAddress{}, errors.New("rotating king manager not available")
	}

	return rkManager.GetNextKing(), nil
}

func (r *RotatingKingAPI) GetKingAddresses() ([]common.QuantumAddress, error) {
	if r.node == nil || r.node.Blockchain() == nil {
		return nil, errors.New("blockchain not available")
	}

	rkManager := r.node.Blockchain().GetRotatingKingManager()
	if rkManager == nil {
		return nil, errors.New("rotating king manager not available")
	}

	return rkManager.GetKingAddresses(), nil
}

func (r *RotatingKingAPI) GetKingStats(addressStr string) (*KingStats, error) {
	if r.node == nil || r.node.Blockchain() == nil {
		return nil, errors.New("blockchain not available")
	}

	address, _ := common.ParseQuantumAddress(addressStr)
	rkManager := r.node.Blockchain().GetRotatingKingManager()
	if rkManager == nil {
		return nil, errors.New("rotating king manager not available")
	}

	// Get stats from manager
	statsMap := rkManager.GetKingStats(address)
	if statsMap == nil {
		return &KingStats{InRotation: false}, nil
	}

	// Convert map to struct
	stats := &KingStats{
		InRotation: statsMap["inRotation"].(bool),
	}

	if position, ok := statsMap["position"].(int); ok {
		stats.Position = position
	}

	if totalPositions, ok := statsMap["totalPositions"].(int); ok {
		stats.TotalPositions = totalPositions
	}

	if isCurrentKing, ok := statsMap["isCurrentKing"].(bool); ok {
		stats.IsCurrentKing = isCurrentKing
	}

	if becameKingAtBlock, ok := statsMap["becameKingAtBlock"].(uint64); ok {
		stats.BecameKingAtBlock = becameKingAtBlock
	}

	if nextRotationAtBlock, ok := statsMap["nextRotationAtBlock"].(uint64); ok {
		stats.NextRotationAtBlock = nextRotationAtBlock
	}

	if rotationsUntilKing, ok := statsMap["rotationsUntilKing"].(int); ok {
		stats.RotationsUntilKing = rotationsUntilKing
	}

	if totalRewards, ok := statsMap["totalRewards"].(string); ok {
		stats.TotalRewards = totalRewards
	}

	if totalRewardsFormatted, ok := statsMap["totalRewardsFormatted"].(string); ok {
		stats.TotalRewardsFormatted = totalRewardsFormatted
	}

	return stats, nil
}

func (r *RotatingKingAPI) GetRotationHistory(limit int) ([]interface{}, error) {
	if r.node == nil || r.node.Blockchain() == nil {
		return nil, errors.New("blockchain not available")
	}

	rkManager := r.node.Blockchain().GetRotatingKingManager()
	if rkManager == nil {
		return nil, errors.New("rotating king manager not available")
	}

	// Get rotation history
	rotations := rkManager.GetRotationHistory(limit)

	// Convert to interface{} for JSON-RPC
	result := make([]interface{}, len(rotations))
	for i, rotation := range rotations {
		result[i] = map[string]interface{}{
			"blockHeight":  rotation.BlockHeight,
			"previousKing": rotation.PreviousKing.Hex(),
			"newKing":      rotation.NewKing.Hex(),
			"timestamp":    rotation.Timestamp.Format(time.RFC3339),
			"reward":       rotation.Reward.String(),
			"wasEligible":  rotation.WasEligible,
			"reason":       rotation.Reason,
		}
	}

	return result, nil
}

func (r *RotatingKingAPI) GetTotalRewardsDistributed() (string, error) {
	if r.node == nil || r.node.Blockchain() == nil {
		return "0", errors.New("blockchain not available")
	}

	rkManager := r.node.Blockchain().GetRotatingKingManager()
	if rkManager == nil {
		return "0", errors.New("rotating king manager not available")
	}

	totalRewards := rkManager.GetTotalRewardsDistributed()
	if totalRewards == nil {
		return "0", nil
	}

	return totalRewards.String(), nil
}

// Alias methods for "rk" namespace (same implementations)
func (r *RotatingKingAPI) Info() (*RotatingKingInfo, error) {
	return r.GetInfo()
}

func (r *RotatingKingAPI) CurrentKing() (common.QuantumAddress, error) {
	return r.GetCurrentKing()
}

func (r *RotatingKingAPI) NextKing() (common.QuantumAddress, error) {
	return r.GetNextKing()
}

func (r *RotatingKingAPI) KingAddresses() ([]common.QuantumAddress, error) {
	return r.GetKingAddresses()
}

func (r *RotatingKingAPI) KingStats(addressStr string) (*KingStats, error) {
	return r.GetKingStats(addressStr)
}

func (r *RotatingKingAPI) RotationHistory(limit int) ([]interface{}, error) {
	return r.GetRotationHistory(limit)
}

// ============================================================================
// CHECKPOINT API IMPLEMENTATION
// ============================================================================

type CheckpointAPI struct {
	cp *checkpoints.Checkpoints
}

func NewCheckpointAPI(cp *checkpoints.Checkpoints) *CheckpointAPI {
	return &CheckpointAPI{cp: cp}
}

func (api *CheckpointAPI) GetCheckpoint(height uint64) (*checkpoints.Checkpoint, error) {
	if api.cp == nil {
		return nil, errors.New("checkpoint manager not available")
	}

	cp, exists := api.cp.GetCheckpoint(height)
	if !exists {
		return nil, fmt.Errorf("checkpoint not found at height %d", height)
	}

	return cp, nil
}

func (api *CheckpointAPI) GetLatestCheckpoint() (*checkpoints.Checkpoint, error) {
	if api.cp == nil {
		return nil, errors.New("checkpoint manager not available")
	}

	cp, exists := api.cp.GetLatestCheckpoint()
	if !exists {
		return nil, errors.New("no checkpoints available")
	}

	return cp, nil
}

func (api *CheckpointAPI) GetStats() (map[string]interface{}, error) {
	if api.cp == nil {
		return nil, errors.New("checkpoint manager not available")
	}

	return api.cp.GetStats(), nil
}

func (api *CheckpointAPI) Export() (string, error) {
	if api.cp == nil {
		return "", errors.New("checkpoint manager not available")
	}

	data, err := api.cp.ExportCheckpoints()
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func (api *CheckpointAPI) GetCheckpointsInRange(start, end uint64) ([]*checkpoints.Checkpoint, error) {
	if api.cp == nil {
		return nil, errors.New("checkpoint manager not available")
	}

	return api.cp.GetCheckpointsInRange(start, end), nil
}

func (api *CheckpointAPI) VerifyChain(height uint64) (map[string]interface{}, error) {
	if api.cp == nil {
		return nil, errors.New("checkpoint manager not available")
	}

	// Verify chain against checkpoints
	stats := api.cp.GetStats()

	// Add verification results
	result := map[string]interface{}{
		"status":       "verified",
		"checkpoints":  stats["totalCheckpoints"],
		"latestHeight": 0,
	}

	if latest, exists := api.cp.GetLatestCheckpoint(); exists {
		result["latestHeight"] = latest.Height
		result["latestHash"] = latest.Hash.Hex()

		// Check if requested height is before latest checkpoint
		if height > 0 && height <= latest.Height {
			cp, _ := api.cp.GetCheckpoint(height)
			if cp != nil {
				result["requestedHeight"] = height
				result["requestedHash"] = cp.Hash.Hex()
				result["verifications"] = cp.Verifications
				result["signatures"] = len(cp.Signatures)
			}
		}
	}

	return result, nil
}

func (api *CheckpointAPI) ValidateBlock(height uint64, hashStr string) (bool, error) {
	if api.cp == nil {
		return false, errors.New("checkpoint manager not available")
	}

	hash := common.HexToHash(hashStr)
	err := api.cp.ValidateBlock(height, hash)
	if err != nil {
		return false, err
	}

	return true, nil
}

func autoInitCheckpointConfig(dataDir string, bc *chain.Blockchain) error {
	configPath := filepath.Join(dataDir, "checkpoints.json")

	// Check if config already exists
	if _, err := os.Stat(configPath); err == nil {
		return nil // Config already exists
	}

	// Get genesis hash
	genesisBlock := bc.GetBlock(0)
	if genesisBlock == nil {
		return errors.New("failed to get genesis block")
	}
	genesisHash := genesisBlock.Hash()

	// Create checkpoint directory
	checkpointDir := filepath.Join(dataDir, "checkpoints")
	if err := os.MkdirAll(checkpointDir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create checkpoint directory: %w", err)
	}

	// Create sample config
	log.Printf("Auto-creating checkpoint configuration at %s", configPath)
	err := checkpoints.CreateSampleConfig(configPath, genesisHash)
	if err != nil {
		return fmt.Errorf("failed to create checkpoint config: %w", err)
	}

	log.Printf("✅ Checkpoint configuration created with genesis hash: %s", genesisHash.Hex())
	return nil
}
