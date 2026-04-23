// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package main
import (
    "fmt"
    "log"
    "os"
    "path/filepath"

    "github.com/urfave/cli/v2"
    "github.com/antdaza/antdchain/common"
    "github.com/antdaza/antdchain/console"
    "github.com/antdaza/antdchain/antdc/chain"
    "github.com/antdaza/antdchain/antdc/wallet"
)

func main() {
    app := &cli.App{
        Name:    "antdchain-wallet",
        Usage:   "ANTDChain Wallet CLI - Connect to ANTDChain daemon",
        Version: "v2.0.0",
        Flags: []cli.Flag{
            &cli.StringFlag{
                Name:    "wallet-dir",
                Value:   getDefaultWalletDir(),
                Usage:   "Wallet data directory",
                EnvVars: []string{"ANTDCHAIN_WALLET_DIR"},
            },
            &cli.StringFlag{
                Name:    "daemon-host",
                Value:   "localhost",
                Usage:   "Daemon hostname/IP",
                EnvVars: []string{"ANTDCHAIN_DAEMON_HOST"},
            },
            &cli.IntFlag{
                Name:    "daemon-rpc-port",
                Value:   8089,
                Usage:   "Daemon RPC port",
                EnvVars: []string{"ANTDCHAIN_DAEMON_RPC_PORT"},
            },
            &cli.IntFlag{
                Name:    "daemon-p2p-port",
                Value:   3000,
                Usage:   "Daemon P2P port (for reference)",
                EnvVars: []string{"ANTDCHAIN_DAEMON_P2P_PORT"},
            },
            &cli.StringFlag{
                Name:    "rpc-user",
                Value:   "",
                Usage:   "RPC username",
                EnvVars: []string{"ANTDCHAIN_RPCUSER"},
            },
            &cli.StringFlag{
                Name:    "rpc-password",
                Value:   "",
                Usage:   "RPC password",
                EnvVars: []string{"ANTDCHAIN_RPCPASSWORD"},
            },
            &cli.StringFlag{
                Name:    "api-key",
                Value:   "",
                Usage:   "API key for authentication",
                EnvVars: []string{"ANTDCHAIN_API_KEY"},
            },
            &cli.BoolFlag{
                Name:    "rpcauthdisabled",
                Usage:   "Disable RPC authentication",
            },
            &cli.StringFlag{
                Name:    "wallet-file",
                Value:   "",
                Usage:   "Open specific wallet file",
            },
            &cli.BoolFlag{
                Name:    "generate",
                Usage:   "Generate new wallet",
            },
            &cli.BoolFlag{
                Name:    "offline",
                Usage:   "Run in offline mode",
            },
            &cli.BoolFlag{
                Name:    "testnet",
                Usage:   "Connect to testnet",
            },
            &cli.BoolFlag{
                Name:    "stagenet",
                Usage:   "Connect to stagenet",
            },
            &cli.BoolFlag{
                Name:    "trusted-daemon",
                Usage:   "Enable commands that require trusted daemon",
            },
            &cli.BoolFlag{
                Name:    "daemon-ssl",
                Usage:   "Use SSL for daemon connection",
            },
            &cli.StringFlag{
                Name:    "daemon-ssl-cert",
                Value:   "",
                Usage:   "SSL certificate for daemon",
            },
            &cli.StringFlag{
                Name:    "daemon-ssl-private-key",
                Value:   "",
                Usage:   "SSL private key for daemon",
            },
            &cli.StringFlag{
                Name:    "daemon-ssl-ca-cert",
                Value:   "",
                Usage:   "SSL CA certificate for daemon",
            },
            &cli.BoolFlag{
                Name:    "daemon-ssl-allow-any-cert",
                Usage:   "Allow any SSL certificate (insecure)",
            },
        },
        Action: runWalletCLI,
    }

    if err := app.Run(os.Args); err != nil {
        log.Fatal(err)
    }
}

func getDefaultWalletDir() string {
    home, err := os.UserHomeDir()
    if err != nil {
        return "./antdchain_wallet"
    }

    return filepath.Join(home, ".antdchain")
}

func runWalletCLI(c *cli.Context) error {
    walletDir := c.String("wallet-dir")
    daemonHost := c.String("daemon-host")
    daemonRPCPort := c.Int("daemon-rpc-port")
    daemonP2PPort := c.Int("daemon-p2p-port")
    rpcUser := c.String("rpc-user")
    rpcPassword := c.String("rpc-password")
    apiKey := c.String("api-key")
    authDisabled := c.Bool("rpcauthdisabled")
    testnet := c.Bool("testnet")
    stagenet := c.Bool("stagenet")
    useSSL := c.Bool("daemon-ssl")
    offline := c.Bool("offline")

    // Determine protocol
    protocol := "http"
    if useSSL {
        protocol = "https"
    }

    // Build daemon URL
    daemonURL := fmt.Sprintf("%s://%s:%d", protocol, daemonHost, daemonRPCPort)

    // Create wallet directory if it doesn't exist
    if err := os.MkdirAll(walletDir, os.ModePerm); err != nil {
        return fmt.Errorf("failed to create wallet directory: %w", err)
    }

    fmt.Println("=== ANTDChain Wallet CLI ===")
    fmt.Printf("Wallet directory: %s\n", walletDir)

    if offline {
        fmt.Println("Mode: 🔒 Offline (local wallet management only)")
    } else {
        fmt.Printf("Mode: 🌐 Online\n")
        fmt.Printf("  Daemon: %s\n", daemonURL)
        fmt.Printf("  RPC Port: %d\n", daemonRPCPort)
        fmt.Printf("  P2P Port: %d\n", daemonP2PPort)
        
        if testnet {
            fmt.Printf("  Network: 🟡 Testnet\n")
        } else if stagenet {
            fmt.Printf("  Network: 🟠 Stagenet\n")
        } else {
            fmt.Printf("  Network: 🟢 Mainnet\n")
        }
        
        if authDisabled {
            fmt.Printf("  Authentication: ❌ Disabled\n")
        } else if apiKey != "" {
            fmt.Printf("  Authentication: 🔑 API Key\n")
        } else if rpcUser != "" {
            fmt.Printf("  Authentication: 👤 User/Password\n")
        }
    }
    fmt.Println("==========================")

    // Create a minimal node for the wallet console
    walletManager := wallet.NewWalletManager(walletDir)

    // Create a minimal blockchain (just for wallet operations)
    var bc *chain.Blockchain
    statePath := filepath.Join(walletDir, "state")
    genesisAddr, _ := common.ParseQuantumAddress("0x0000000000000000000000000000000000000000")

    // Only try to load blockchain if state exists
    if _, err := os.Stat(statePath); err == nil {
        var err error
        bc, err = chain.NewBlockchain(statePath, genesisAddr)
        if err != nil {
            fmt.Printf("⚠️  Could not load blockchain state: %v\n", err)
            fmt.Println("   Running in wallet-only mode...")
        }
    }

    if bc != nil {
        walletManager.SetBlockchain(bc)
    }

    // Create console node
    node, err := console.NewNode(bc, nil, walletManager, nil) // No mining, no P2P for wallet
    if err != nil {
        return fmt.Errorf("failed to create wallet node: %w", err)
    }

    // Create console
    consoleInstance := console.NewConsole(node)

    if offline {
        // Run in offline mode (local wallet management only)
        fmt.Println("\n💡 Offline mode commands:")
        fmt.Println("  createaddress    - Create new wallet")
        fmt.Println("  import <key>     - Import ML-DSA-65 private key")
        fmt.Println("  export <addr>    - Export ML-DSA-65 private key")
        fmt.Println("  listwallets      - List all quantum wallets")
        fmt.Println("  listaddresses    - Alias for listwallets")
        fmt.Println("  help             - Show all commands")
        fmt.Println()
        consoleInstance.Start()
    } else {
        // Connect to daemon
        // Create authentication config
        authConfig := &console.RPCAuthConfig{
            URL:           daemonURL,
            Username:      rpcUser,
            Password:      rpcPassword,
            APIKey:        apiKey,
            AuthDisabled:  authDisabled,
            Testnet:       testnet,
            Stagenet:      stagenet,
            AllowInsecure: c.Bool("daemon-ssl-allow-any-cert"),
        }
        
        consoleInstance.StartWalletClient(authConfig)
    }

    return nil
}
