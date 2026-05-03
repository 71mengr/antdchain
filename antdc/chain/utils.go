// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package chain
import (
    "bytes"
    "math/big"
    "strings"

    "github.com/antdaza/antdchain/antdc/block"
    "github.com/antdaza/antdchain/antdc/tx"
    "github.com/antdaza/antdchain/common"
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

// CalcTxRoot calculates the transaction root using the canonical block hash algorithm.
func CalcTxRoot(txs []*tx.Tx) common.Hash {
    return block.CalculateTxHash(txs)
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
