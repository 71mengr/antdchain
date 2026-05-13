// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package chain

import (
	"math/big"
	"testing"

	"github.com/antdaza/antdchain/antdc/block"
	"github.com/antdaza/antdchain/common"
)

func TestSameHeightForkChoiceRequiresStrictlyMoreWork(t *testing.T) {
	parent := common.BytesToHash([]byte("parent"))
	current := testForkChoiceBlock(30, parent, 1_000_000, 1778677890, "current")
	lowerHashEqualWork := testForkChoiceBlock(30, parent, 1_000_000, 1778677927, "equal")

	if isBetterBlockCandidate(lowerHashEqualWork, current) {
		t.Fatalf("expected equal-work same-height fork to be rejected")
	}

	heavier := testForkChoiceBlock(30, parent, 1_000_001, 1778677927, "heavier")
	if !isBetterBlockCandidate(heavier, current) {
		t.Fatalf("expected higher-work same-height fork to be accepted")
	}

	lighter := testForkChoiceBlock(30, parent, 999_999, 1778677927, "lighter")
	if isBetterBlockCandidate(lighter, current) {
		t.Fatalf("expected lower-work same-height fork to be rejected")
	}
}

func testForkChoiceBlock(height uint64, parent common.Hash, difficulty int64, timestamp uint64, extra string) *block.Block {
	return &block.Block{
		Header: &block.Header{
			ParentHash: parent,
			Coinbase:   common.BytesToQuantumAddress([]byte(extra)),
			Number:     new(big.Int).SetUint64(height),
			Difficulty: big.NewInt(difficulty),
			GasLimit:   60_000_000,
			Time:       timestamp,
			Extra:      []byte(extra),
		},
	}
}
