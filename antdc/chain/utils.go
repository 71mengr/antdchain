// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package chain
import (
    "bytes"
    "crypto/sha256"
    "math/big"
    "strings"

    "github.com/antdaza/antdchain/antdc/block"
    "github.com/antdaza/antdchain/antdc/tx"
)

// formatBalance formats balance in ANTD units
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

// formatWei formats wei to ANTD
func formatWei(wei *big.Int) string {
    if wei == nil {
        return "0"
    }
    // Convert wei to ANTD (1 ANTD = 10^18 wei)
    antd := new(big.Float).SetInt(wei)
    antd.Quo(antd, big.NewFloat(1e18))
    return antd.Text('f', 6)
}

// CalcTxRoot calculates the Merkle root of transactions
func CalcTxRoot(txs []*tx.Tx) common.Hash {
    // Handle nil or empty slice
    if txs == nil || len(txs) == 0 {
        return common.Hash{}
    }

    // Special case: single transaction - root is just the transaction hash
    if len(txs) == 1 {
        tx := txs[0]
        if tx == nil {
            return common.Hash{}
        }
        return tx.Hash()
    }

    // Multiple transactions: calculate Merkle tree
    hashes := make([]common.Hash, len(txs))
    for i, transaction := range txs {
        if transaction == nil {
            hashes[i] = common.Hash{}
        } else {
            hashes[i] = transaction.Hash()
        }
    }

    return computeMerkleRoot(hashes)
}

// computeMerkleRoot - Standard Bitcoin-style Merkle root calculation
func computeMerkleRoot(hashes []common.Hash) common.Hash {
    if len(hashes) == 0 {
        return common.Hash{}
    }

    // Standard Merkle tree implementation
    for len(hashes) > 1 {
        // If odd number, duplicate last hash
        if len(hashes)%2 == 1 {
            hashes = append(hashes, hashes[len(hashes)-1])
        }

        nextLevel := make([]common.Hash, len(hashes)/2)
        for i := 0; i < len(hashes); i += 2 {
            // Double SHA-256 (Bitcoin style)
            firstHash := sha256.Sum256(append(hashes[i][:], hashes[i+1][:]...))
            secondHash := sha256.Sum256(firstHash[:])
            nextLevel[i/2] = common.BytesToHash(secondHash[:])
        }
        hashes = nextLevel
    }

    return hashes[0]
}

// diffToCompact converts difficulty to compact format
func diffToCompact(diff *big.Int) uint32 {
    if diff.Sign() <= 0 {
        return 0
    }

    size := (diff.BitLen() + 7) / 8
    var compact uint32

    if size <= 3 {
        compact = uint32(diff.Int64() << uint(8*(3-size)))
    } else {
        bn := new(big.Int).Div(diff, big.NewInt(1).Lsh(big.NewInt(1), uint(8*(size-3))))
        if bn.BitLen() > 24 {
            bn = new(big.Int).Rsh(bn, 8)
            size++
        }
        compact = uint32(bn.Int64()) | uint32(size<<24)
    }

    if diff.Sign() < 0 {
        compact |= 0x00800000
    }

    return compact
}

// Extracts the ECDSA signature from Extra field (|SIG| marker)
func extractSignatureFromBlock(blk *block.Block) []byte {
    if blk == nil || blk.Header == nil || len(blk.Header.Extra) == 0 {
        return nil
    }

    extra := blk.Header.Extra
    marker := []byte("|SIG|")

    idx := bytes.Index(extra, marker)
    if idx == -1 || idx+len(marker) > len(extra) {
        return nil
    }

    return extra[idx+len(marker):]
}
