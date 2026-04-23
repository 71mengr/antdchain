// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package king
import (
    "fmt"
    "math/big"
    "os"
    "path/filepath"
    "runtime"
    "sort"
    "strings"
    "time"

    "github.com/antdaza/antdchain/common"
    "github.com/ethereum/go-ethereum/crypto"
    "github.com/antdaza/antdchain/antdc/chain"
    "github.com/urfave/cli/v2"
)

// Returns the standard ANTDChain data directory (like Bitcoin)
func getDefaultDataDir() string {
    home, err := os.UserHomeDir()
    if err != nil {
        return "./antdchain_data" // fallback
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

// Constants for proposal types
const (
    ProposalUpdateMainKing = iota + 1
    ProposalUpdateRotatingKings
)

// GovernanceProposal represents a governance proposal
type GovernanceProposal struct {
    ID                uint64
    ProposalType      uint8
    Creator           common.QuantumAddress
    NewMainKing       common.QuantumAddress
    NewRotatingKings  []common.QuantumAddress
    CreatedAt         uint64
    ETA               uint64 // 48 hours after creation
    Executed          bool
    ExecutionBlock    uint64
    ExecutionHash     common.Hash
}

// GovernanceController interface
type GovernanceController interface {
    ProposeMainKingChange(caller common.QuantumAddress, newMainKing common.QuantumAddress, now uint64) (uint64, error)
    ProposeRotatingKingsUpdate(caller common.QuantumAddress, newKings []common.QuantumAddress, now uint64) (uint64, error)
    ExecuteProposal(id uint64, caller common.QuantumAddress, now uint64) error
    ListProposals() map[uint64]*GovernanceProposal
}

var KingCommands = &cli.Command{
    Name:        "king",
    Usage:       "Manage Main King & Rotating Kings — persistent 48h governance",
    Description: "All proposals persist on disk. Changes propagate instantly via P2P when executed.",
    Subcommands: []*cli.Command{
        {
            Name:   "status",
            Usage:  "Show current kings and balances",
            Action: cmdStatus,
        },
        {
            Name:      "propose-main",
            Usage:     "Propose new Main King (48h timelock)",
            ArgsUsage: "<new-main-king>",
            Action:    cmdProposeMain,
            Flags: []cli.Flag{
                &cli.StringFlag{Name: "key", Required: true, Usage: "Governance owner private key (hex, no 0x)"},
            },
        },
        {
            Name:      "propose-rotating",
            Usage:     "Propose new rotating kings list (48h timelock)",
            ArgsUsage: "<addr1> <addr2> ...",
            Action:    cmdProposeRotating,
            Flags: []cli.Flag{
                &cli.StringFlag{Name: "key", Required: true, Usage: "Governance owner private key (hex, no 0x)"},
            },
        },
        {
            Name:      "execute",
            Usage:     "Execute a pending proposal",
            ArgsUsage: "<id>",
            Action:    cmdExecute,
            Flags: []cli.Flag{
                &cli.StringFlag{Name: "key", Required: true, Usage: "Governance owner private key"},
                &cli.Uint64Flag{Name: "id", Required: true, Usage: "Proposal ID"},
            },
        },
        {
            Name:   "proposals",
            Usage:  "List all proposals",
            Action: cmdListProposals,
        },
    },
    Flags: []cli.Flag{
        &cli.StringFlag{
            Name:  "data",
            Value: getDefaultDataDir(),
            Usage: "Chain data directory (default: ~/.antdchain_data or Documents/Antdchain)",
        },
    },
}

func loadChain(ctx *cli.Context) (*chain.Blockchain, error) {
    dataDir := ctx.String("data")
    statePath := filepath.Join(dataDir, "state")
    return chain.NewBlockchain(statePath, common.QuantumAddress{})
}

func ownerAddr(keyHex string) (common.QuantumAddress, error) {
    if strings.HasPrefix(keyHex, "0x") {
        keyHex = keyHex[2:]
    }
    pk, err := crypto.HexToECDSA(keyHex)
    if err != nil {
        return common.QuantumAddress{}, err
    }
    return crypto.PubkeyToAddress(pk.PublicKey), nil
}

func weiToANTD(w *big.Int) string {
    if w == nil || w.Sign() == 0 {
        return "0.000000"
    }
    f := new(big.Float).Quo(new(big.Float).SetInt(w), big.NewFloat(1e18))
    s, _ := f.Float64()
    return fmt.Sprintf("%.6f", s)
}

// ===== COMMANDS =====
func cmdStatus(ctx *cli.Context) error {
    bc, err := loadChain(ctx)
    if err != nil {
        return err
    }
    defer bc.Close()

    fmt.Println("=== ANTDChain King Status ===")
    
    // Get main king from reward distributor
    mainKing := common.ParseQuantumAddress("0q5E2PeUs72XQrN5FKWwMwPnM2Z5FjTD5jY") // Default main king
    fmt.Printf("Main King : %s\n", mainKing.Hex())
    fmt.Printf("  Balance : %s ANTD\n\n", weiToANTD(bc.GetAccountBalance(mainKing)))

    // Try to get rotating kings from rotating king manager if available
    rkManager := bc.GetRotatingKingManager()
    var kings []common.QuantumAddress
    
    if rkManager != nil {
        kings = rkManager.GetKingAddresses()
       }
    
    fmt.Printf("Rotating Kings: %d\n", len(kings))
    for i, k := range kings {
        bal := bc.GetAccountBalance(k)
        eligible := "Eligible"
        threshold := new(big.Int).Mul(big.NewInt(100000), big.NewInt(1e18)) // 100,000 ANTD
        if bal.Cmp(threshold) < 0 {
            eligible = "Ineligible (<100k ANTD)"
        }
        fmt.Printf("  [%2d] %s  %s  %s ANTD\n", i, k.Hex(), eligible, weiToANTD(bal))
    }
    return nil
}

func cmdProposeMain(ctx *cli.Context) error {
    if ctx.NArg() != 1 {
        return fmt.Errorf("need exactly one address")
    }
    newKing := common.ParseQuantumAddress(ctx.Args().First())

    bc, err := loadChain(ctx)
    if err != nil {
        return err
    }
    defer bc.Close()

    addr, err := ownerAddr(ctx.String("key"))
    if err != nil {
        return err
    }

    // Try to get governance controller
    gov := bc.GetGovernance()
    if gov == nil {
        return fmt.Errorf("governance system not initialized")
    }

    // Use type assertion to get the interface
    if gc, ok := gov.(GovernanceController); ok {
        id, err := gc.ProposeMainKingChange(addr, newKing, uint64(time.Now().Unix()))
        if err != nil {
            return err
        }
        fmt.Printf("Main King proposal created! ID: %d\n", id)
        fmt.Printf("Executable after: %s\n", time.Now().Add(48*time.Hour).Format("2006-01-02 15:04 UTC"))
        return nil
    }

    return fmt.Errorf("governance controller not available")
}

func cmdProposeRotating(ctx *cli.Context) error {
    if ctx.NArg() == 0 {
        return fmt.Errorf("at least one address required")
    }
    var addrs []common.QuantumAddress
    for _, a := range ctx.Args().Slice() {
        addrs = append(addrs, common.ParseQuantumAddress(a))
    }

    bc, err := loadChain(ctx)
    if err != nil {
        return err
    }
    defer bc.Close()

    addr, err := ownerAddr(ctx.String("key"))
    if err != nil {
        return err
    }

    // Try to get governance controller
    gov := bc.GetGovernance()
    if gov == nil {
        return fmt.Errorf("governance system not initialized")
    }

    // Use type assertion to get the interface
    if gc, ok := gov.(GovernanceController); ok {
        id, err := gc.ProposeRotatingKingsUpdate(addr, addrs, uint64(time.Now().Unix()))
        if err != nil {
            return err
        }
        fmt.Printf("Rotating Kings proposal created! ID: %d (%d kings)\n", id, len(addrs))
        fmt.Printf("Executable after: %s\n", time.Now().Add(48*time.Hour).Format("2006-01-02 15:04 UTC"))
        return nil
    }

    return fmt.Errorf("governance controller not available")
}

func cmdExecute(ctx *cli.Context) error {
    id := ctx.Uint64("id")

    bc, err := loadChain(ctx)
    if err != nil {
        return err
    }
    defer bc.Close()

    addr, err := ownerAddr(ctx.String("key"))
    if err != nil {
        return err
    }

    // Try to get governance controller
    gov := bc.GetGovernance()
    if gov == nil {
        return fmt.Errorf("governance system not initialized")
    }

    // Use type assertion to get the interface
    if gc, ok := gov.(GovernanceController); ok {
        if err := gc.ExecuteProposal(id, addr, uint64(time.Now().Unix())); err != nil {
            return err
        }
        fmt.Printf("Proposal %d executed — king list updated across the network\n", id)
        return nil
    }

    return fmt.Errorf("governance controller not available")
}

func cmdListProposals(ctx *cli.Context) error {
    bc, err := loadChain(ctx)
    if err != nil {
        return err
    }
    defer bc.Close()

    // Try to get governance controller
    gov := bc.GetGovernance()
    if gov == nil {
        fmt.Println("Governance system not initialized")
        return nil
    }

    // Use type assertion to get the interface
    if gc, ok := gov.(GovernanceController); ok {
        proposals := gc.ListProposals()
        if len(proposals) == 0 {
            fmt.Println("No governance proposals")
            return nil
        }

        var ids []uint64
        for id := range proposals {
            ids = append(ids, id)
        }
        sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

        fmt.Printf("=== Governance Proposals (%d) ===\n", len(proposals))
        fmt.Printf("%-6s %-12s %-10s %-20s %s\n", "ID", "Type", "Status", "ETA", "Details")
        fmt.Println(strings.Repeat("-", 80))

        now := uint64(time.Now().Unix())
        for _, id := range ids {
            p := proposals[id]
            status := "Pending"
            if p.Executed {
                status = "Executed"
            } else if now >= p.ETA {
                status = "Ready"
            }
            eta := time.Unix(int64(p.ETA), 0).Format("2006-01-02 15:04")
            var details string
            if p.ProposalType == ProposalUpdateMainKing {
                details = fmt.Sprintf("MainKing → %s", p.NewMainKing.Hex())
            } else {
                details = fmt.Sprintf("Rotating ×%d", len(p.NewRotatingKings))
            }
            typ := "MainKing"
            if p.ProposalType == ProposalUpdateRotatingKings {
                typ = "Rotating"
            }
            fmt.Printf("%-6d %-12s %-10s %-20s %s\n", p.ID, typ, status, eta, details)
        }
        return nil
    }

    fmt.Println("Governance controller not available")
    return nil
}
