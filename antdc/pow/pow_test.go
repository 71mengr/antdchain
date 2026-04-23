// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package pow
import (
    "math/big"
    "testing"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestBLAKE3ANTD_BasicMiningAndVerification(t *testing.T) {
    // Create PoW with low difficulty for fast testing
    pow, err := NewPoW(big.NewInt(100)) // very low diff → finds solution instantly
    require.NoError(t, err)
    require.NotNil(t, pow)

    headerHash := common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")

    // Mine a solution
    nonce, mixDigest, found := pow.Mine(headerHash)
    require.True(t, found, "should find a solution with low difficulty")
    require.NotZero(t, nonce)
    require.NotEqual(t, common.Hash{}, mixDigest)

    t.Logf("Found nonce: %d, mixDigest: %s", nonce, mixDigest.Hex())

    // Verify the solution we just found
    valid := pow.Verify(headerHash, nonce, mixDigest)
    assert.True(t, valid, "our own mined block must verify correctly")
}

func TestBLAKE3ANTD_VerifyInvalidNonceFails(t *testing.T) {
    pow, _ := NewPoW(big.NewInt(1000))

    headerHash := common.HexToHash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")

    nonce, mixDigest, found := pow.Mine(headerHash)
    require.True(t, found)

    // Tamper with nonce
    badNonce := nonce + 12345

    valid := pow.Verify(headerHash, badNonce, mixDigest)
    assert.False(t, valid, "tampered nonce should fail verification")
}

func TestBLAKE3ANTD_VerifyInvalidMixDigestFails(t *testing.T) {
    pow, _ := NewPoW(big.NewInt(1000))

    headerHash := common.HexToHash("0xcafebabe")

    nonce, mixDigest, _ := pow.Mine(headerHash)

    // Tamper with mixDigest
    badMix := mixDigest
    badMix[0] ^= 0xff

    valid := pow.Verify(headerHash, nonce, badMix)
    assert.False(t, valid, "tampered mixDigest should fail")
}

func TestBLAKE3ANTD_DifficultyScaling(t *testing.T) {
    testCases := []struct {
        parentTime    uint64
        currentTime   uint64
        parentDiff    int64
        expectedMin   int64
        expectedMax   int64
    }{
        {1000, 1240, 8000000, 7500000, 8500000}, // on time (240s)
        {1000, 1080, 8000000, 3500000, 4500000}, // fast block (80s)
        {1000, 1720, 8000000, 50000000, 60000000}, // slow block (720s = 12min)
    }

    for i, tc := range testCases {
        pow, _ := NewPoW(big.NewInt(tc.parentDiff))

        newDiff := pow.CalculateNextDifficulty(tc.parentTime, tc.currentTime, big.NewInt(tc.parentDiff))

        t.Logf("Case %d: %ds → new diff %s", i, tc.currentTime-tc.parentTime, newDiff.String())

        assert.True(t, newDiff.Cmp(big.NewInt(tc.expectedMin)) >= 0)
        assert.True(t, newDiff.Cmp(big.NewInt(tc.expectedMax)) <= 0)
    }
}

func TestBLAKE3ANTD_HashRateCallbackAccuracy(t *testing.T) {
    pow, _ := NewPoW(big.NewInt(10)) // super low for instant solve

    headerHash := common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")

    var totalHashed uint64
    nonce, mixDigest, found := pow.MineWithCallback(headerHash, func(hashes uint64) {
        totalHashed += hashes
    })

    require.True(t, found)
    assert.Greater(t, totalHashed, uint64(50), "callback should report thousands of hashes even on easy diff")
    t.Logf("Total hashes tried: %d → nonce %d", totalHashed, nonce)

    valid := pow.Verify(headerHash, nonce, mixDigest)
    assert.True(t, valid)
}

func TestBLAKE3ANTD_DeterministicHash(t *testing.T) {
    pow, _ := NewPoW(big.NewInt(1000000))

    headerHash := common.HexToHash("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")

    // Same input → same output
    hash1 := blake3antd(headerHash, 1337)
    hash2 := blake3antd(headerHash, 1337)

    assert.Equal(t, hash1, hash2, "BLAKE3-ANTD must be deterministic")
}

func TestBLAKE3ANTD_SetDifficultyUpdatesTarget(t *testing.T) {
    pow, _ := NewPoW(big.NewInt(1000))

    oldTarget := new(big.Int).Set(pow.target)

    pow.SetDifficulty(big.NewInt(2000))

    assert.Equal(t, -1, pow.target.Cmp(oldTarget), "higher difficulty → smaller target")
}

func BenchmarkBLAKE3ANTD_Hash(b *testing.B) {
    pow, _ := NewPoW(big.NewInt(1_000_000_000))
    headerHash := common.HexToHash("0x4242424242424242424242424242424242424242424242424242424242424242")

    b.ResetTimer()
    b.ReportAllocs()

    for i := 0; i < b.N; i++ {
        _ = blake3antd(headerHash, uint64(i))
    }
}


