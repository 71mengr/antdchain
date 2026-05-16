// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package chain

import (
	"bytes"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/antdaza/antdchain/antdc/block"
	"github.com/antdaza/antdchain/antdc/chain/db"
	"github.com/antdaza/antdchain/antdc/pow"
	"github.com/antdaza/antdchain/common"
	"github.com/antdaza/antdchain/difficulty"
	"github.com/hashicorp/golang-lru"
)

func TestSameHeightForkChoiceUsesDeterministicProtocolTieBreak(t *testing.T) {
	parent := common.BytesToHash([]byte("parent"))
	current := testForkChoiceBlock(30, parent, 1_000_000, 1778677890, "current")
	current.Header.MixDigest = testProofHash(0x20)
	laterEqualWork := testForkChoiceBlock(30, parent, 1_000_000, 1778677927, "later")
	laterEqualWork.Header.MixDigest = testProofHash(0x20)

	if isBetterBlockCandidate(laterEqualWork, current) {
		t.Fatalf("expected later equal-work same-height fork to be rejected")
	}

	earlierEqualWork := testForkChoiceBlock(30, parent, 1_000_000, 1778677889, "earlier")
	earlierEqualWork.Header.MixDigest = testProofHash(0x20)
	if !isBetterBlockCandidate(earlierEqualWork, current) {
		t.Fatalf("expected earlier equal-work same-height fork to be accepted")
	}

	heavier := testForkChoiceBlock(30, parent, 1_000_001, 1778677927, "heavier")
	heavier.Header.MixDigest = testProofHash(0xff)
	if !isBetterBlockCandidate(heavier, current) {
		t.Fatalf("expected higher-work same-height fork to be accepted")
	}

	lighter := testForkChoiceBlock(30, parent, 999_999, 1778677889, "lighter")
	lighter.Header.MixDigest = testProofHash(0x00)
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

func TestSameHeightForkChoicePrefersStrongerProofQuality(t *testing.T) {
	parent := common.BytesToHash([]byte("parent"))
	current := testForkChoiceBlock(32, parent, 1_000_000, 1778678200, "current")
	current.Header.MixDigest = testProofHash(0x80)
	strongerProof := testForkChoiceBlock(32, parent, 1_000_000, 1778678220, "stronger")
	strongerProof.Header.MixDigest = testProofHash(0x01)
	weakerProof := testForkChoiceBlock(32, parent, 1_000_000, 1778678190, "weaker")
	weakerProof.Header.MixDigest = testProofHash(0xf0)

	if !isBetterBlockCandidate(strongerProof, current) {
		t.Fatalf("expected lower proof hash to beat later timestamp at same height")
	}
	if isBetterBlockCandidate(weakerProof, current) {
		t.Fatalf("expected weaker proof to lose even with earlier timestamp")
	}
}

func TestValidateBasicBlockIntegrityRejectsOversizedExtraData(t *testing.T) {
	parentTime := uint64(time.Now().Unix()) - uint64(2*difficulty.TargetBlockTimeSeconds)
	parent := testForkChoiceBlock(40, common.BytesToHash([]byte("grandparent")), 1_000_000, parentTime, "parent")
	child := testForkChoiceBlock(41, parent.Hash(), 1_000_000, parentTime+uint64(difficulty.TargetBlockTimeSeconds), "child")
	child.Header.Extra = make([]byte, block.MaxExtraDataSize+1)

	bc := &Blockchain{}
	err := bc.validateBasicBlockIntegrity(child, parent)
	if err == nil {
		t.Fatal("expected oversized extra data to fail basic block validation")
	}
	if !strings.Contains(err.Error(), "extra data too large") {
		t.Fatalf("expected extra data size error, got %v", err)
	}
}

func TestHashLookupDoesNotPolluteCanonicalNumberCache(t *testing.T) {
	chainDB, err := db.NewChainDB(t.TempDir())
	if err != nil {
		t.Fatalf("create chain db: %v", err)
	}
	defer chainDB.Close()

	numberCache, err := lru.New(16)
	if err != nil {
		t.Fatalf("create number cache: %v", err)
	}
	hashCache, err := lru.New(16)
	if err != nil {
		t.Fatalf("create hash cache: %v", err)
	}

	bc := &Blockchain{
		db:                 chainDB,
		blockByNumberCache: numberCache,
		blockByHashCache:   hashCache,
	}

	parent := testForkChoiceBlock(40, common.BytesToHash([]byte("grandparent")), 1_000_000, 1778678100, "parent")
	canonical := testForkChoiceBlock(41, parent.Hash(), 1_000_000, 1778678112, "canonical")
	sideBranch := testForkChoiceBlock(41, parent.Hash(), 1_000_000, 1778678112, "side-branch")
	if canonical.Hash() == sideBranch.Hash() {
		t.Fatalf("test blocks unexpectedly have the same hash")
	}

	for _, blk := range []*block.Block{parent, canonical, sideBranch} {
		if err := chainDB.WriteBlock(blk); err != nil {
			t.Fatalf("write block %d: %v", blk.Header.Number.Uint64(), err)
		}
	}
	if err := chainDB.WriteCanonicalHash(parent.Header.Number.Uint64(), parent.Hash()); err != nil {
		t.Fatalf("write parent canonical hash: %v", err)
	}
	if err := chainDB.WriteCanonicalHash(canonical.Header.Number.Uint64(), canonical.Hash()); err != nil {
		t.Fatalf("write canonical hash: %v", err)
	}

	foundSide, err := bc.GetBlockByHash(sideBranch.Hash())
	if err != nil {
		t.Fatalf("get side branch by hash: %v", err)
	}
	if foundSide.Hash() != sideBranch.Hash() {
		t.Fatalf("expected side branch by hash, got %s", foundSide.Hash().Hex())
	}

	if got := bc.GetBlock(canonical.Header.Number.Uint64()); got == nil || got.Hash() != canonical.Hash() {
		t.Fatalf("GetBlock returned non-canonical block after hash lookup: got %v want %s", blockHashForTest(got), canonical.Hash().Hex())
	}

	bc.cacheMu.Lock()
	bc.blockByNumberCache.Add(canonical.Header.Number.Uint64(), sideBranch)
	bc.cacheMu.Unlock()

	if got := bc.GetBlock(canonical.Header.Number.Uint64()); got == nil || got.Hash() != canonical.Hash() {
		t.Fatalf("GetBlock trusted polluted number cache: got %v want %s", blockHashForTest(got), canonical.Hash().Hex())
	}
}

func blockHashForTest(blk *block.Block) string {
	if blk == nil {
		return "<nil>"
	}
	return blk.Hash().Hex()
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

func testProofHash(lastByte byte) common.Hash {
	var h common.Hash
	h[len(h)-1] = lastByte
	return h
}
