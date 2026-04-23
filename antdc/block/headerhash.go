// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package block

import (
	"encoding/binary"

	"github.com/antdaza/antdchain/common"
)

// HashForMining computes a mining-specific hash of the block header using SHA3‑256.
// This hash is used for Proof-of-Stake eligibility checks and must be deterministic.
func HashForMining(h *Header) common.Hash {
	if h == nil {
		return common.Hash{}
	}

	buf := make([]byte, 0, 512)

	appendBytes := func(b []byte) { buf = append(buf, b...) }
	appendU64 := func(v uint64) {
		tmp := make([]byte, 8)
		binary.LittleEndian.PutUint64(tmp, v)
		appendBytes(tmp)
	}
	appendBig32BE := func(b []byte) {
		// Normalize to exactly 32 bytes (big‑endian)
		if len(b) < 32 {
			pad := make([]byte, 32-len(b))
			b = append(pad, b...)
		} else if len(b) > 32 {
			b = b[len(b)-32:]
		}
		appendBytes(b)
	}

	// Serialize fields in fixed order (must match consensus)
	appendBytes(h.ParentHash[:])
	appendBytes(h.Coinbase.Bytes()) // Quantum address: 20‑byte payload
	appendBytes(h.Root[:])
	appendBytes(h.TxHash[:])
	appendU64(h.Number.Uint64())
	appendU64(uint64(h.GasLimit))
	appendU64(uint64(h.GasUsed))
	appendU64(h.Time)
	appendBig32BE(h.Difficulty.Bytes())

	// Extra data: length (uint32 LE) + bytes
	extraLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(extraLen, uint32(len(h.Extra)))
	appendBytes(extraLen)
	appendBytes(h.Extra)

	// Use SHA3‑256 for quantum safety
	return common.ComputeHash(buf)
}
