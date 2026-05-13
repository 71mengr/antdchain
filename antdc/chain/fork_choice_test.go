// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package chain

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/antdaza/antdchain/antdc/block"
	"github.com/antdaza/antdchain/common"
)

func TestSameHeightForkChoiceUsesDeterministicProtocolTieBreak(t *testing.T) {
	parent := common.BytesToHash([]byte("parent"))
	current := testForkChoiceBlock(30, parent, 1_000_000, 1778677890, "current")
	laterEqualWork := testForkChoiceBlock(30, parent, 1_000_000, 1778677927, "later")

	if isBetterBlockCandidate(laterEqualWork, current) {
		t.Fatalf("expected later equal-work same-height fork to be rejected")
	}

	earlierEqualWork := testForkChoiceBlock(30, parent, 1_000_000, 1778677889, "earlier")
	if !isBetterBlockCandidate(earlierEqualWork, current) {
		t.Fatalf("expected earlier equal-work same-height fork to be accepted")
	}

	heavier := testForkChoiceBlock(30, parent, 1_000_001, 1778677927, "heavier")
	if !isBetterBlockCandidate(heavier, current) {
		t.Fatalf("expected higher-work same-height fork to be accepted")
	}

	lighter := testForkChoiceBlock(30, parent, 999_999, 1778677889, "lighter")
	if isBetterBlockCandidate(lighter, current) {
		t.Fatalf("expected lower-work same-height fork to be rejected")
	}
}

func TestSameHeightForkChoiceFallsBackToLowerHash(t *testing.T) {
	parent := common.BytesToHash([]byte("parent"))
	left := testForkChoiceBlock(31, parent, 1_000_000, 1778678000, "left")
	right := testForkChoiceBlock(31, parent, 1_000_000, 1778678000, "right")

	if left.Hash() == right.Hash() {
		t.Fatalf("test blocks unexpectedly have the same hash")
	}

	lowerHashBlock, higherHashBlock := left, right
	if bytes.Compare(left.Hash().Bytes(), right.Hash().Bytes()) > 0 {
		lowerHashBlock, higherHashBlock = right, left
	}

	if !isBetterBlockCandidate(lowerHashBlock, higherHashBlock) {
		t.Fatalf("expected lower-hash equal-work same-timestamp fork to be accepted")
	}
	if isBetterBlockCandidate(higherHashBlock, lowerHashBlock) {
		t.Fatalf("expected higher-hash equal-work same-timestamp fork to be rejected")
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
