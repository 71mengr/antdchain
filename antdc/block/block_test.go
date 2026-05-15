package block

import (
	"math/big"
	"testing"
	"time"

	"github.com/antdaza/antdchain/antdc/pow"
	"github.com/antdaza/antdchain/common"
)

func TestHeaderValidateExtraDataSize(t *testing.T) {
	header := &Header{
		Number:     big.NewInt(0),
		GasLimit:   1,
		Time:       uint64(time.Now().Unix()),
		Difficulty: big.NewInt(1),
		Extra:      validExtraData(MaxExtraDataSize),
	}

	if err := header.Validate(nil); err != nil {
		t.Fatalf("expected extra data at max size to validate: %v", err)
	}

	header.Extra = make([]byte, MaxExtraDataSize+1)
	if err := header.Validate(nil); err == nil {
		t.Fatal("expected extra data larger than max size to fail validation")
	}
}

func validExtraData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i%251 + 1)
	}
	return data
}

func TestHeaderSetAndGetExtraDataDefensivelyCopy(t *testing.T) {
	header := &Header{}
	data := []byte("metadata")

	if err := header.SetExtraData(data); err != nil {
		t.Fatalf("set extra data: %v", err)
	}
	data[0] = 'M'
	if string(header.Extra) != "metadata" {
		t.Fatalf("expected SetExtraData to copy input, got %q", header.Extra)
	}

	got := header.GetExtraData()
	got[0] = 'G'
	if string(header.Extra) != "metadata" {
		t.Fatalf("expected GetExtraData to return copy, got %q", header.Extra)
	}
}

func TestValidateExtraDataContentRejectsLowInformationPatterns(t *testing.T) {
	tests := map[string][]byte{
		"all zero":       {0x00},
		"all ff":         {0xff},
		"repeating byte": []byte("aaaa"),
	}

	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			if err := ValidateExtraDataSize(data); err == nil {
				t.Fatal("expected low-information extra data pattern to fail")
			}
		})
	}
}

func TestNewHeaderUsesMinerSpecificDifficultyAtSameHeight(t *testing.T) {
	engine := pow.NewPoW()
	parent := &Block{Header: &Header{
		Difficulty: big.NewInt(pow.BaseDifficulty),
		Number:     big.NewInt(10),
		GasLimit:   1,
		Time:       uint64(time.Now().Add(time.Hour).Unix()),
	}}
	minerA := common.BytesToQuantumAddress([]byte("miner-a"))
	minerB := common.BytesToQuantumAddress([]byte("miner-b"))
	height := big.NewInt(11)

	headerA, err := NewHeader(parent, minerA, common.Hash{}, common.Hash{}, height, 1, engine, nil)
	if err != nil {
		t.Fatalf("new header for miner A: %v", err)
	}
	headerB, err := NewHeader(parent, minerB, common.Hash{}, common.Hash{}, height, 1, engine, nil)
	if err != nil {
		t.Fatalf("new header for miner B: %v", err)
	}

	if headerA.Difficulty.Cmp(headerB.Difficulty) == 0 {
		t.Fatalf("expected same-height miners to receive different full difficulties: %s", headerA.Difficulty)
	}

	expectedBase := pow.CalculateDifficultyFromWindow(big.NewInt(pow.BaseDifficulty), height.Uint64(), []uint64{1})
	if got := pow.NormalizeDifficulty(headerA.Difficulty); got.Cmp(expectedBase) != 0 {
		t.Fatalf("miner A normalized difficulty mismatch: got %s, want %s", got, expectedBase)
	}
	if got := pow.NormalizeDifficulty(headerB.Difficulty); got.Cmp(expectedBase) != 0 {
		t.Fatalf("miner B normalized difficulty mismatch: got %s, want %s", got, expectedBase)
	}
}

func TestNewHeaderRejectsMissingParentHeaderForDifficulty(t *testing.T) {
	_, err := NewHeader(&Block{}, common.QuantumAddress{}, common.Hash{}, common.Hash{}, big.NewInt(1), 1, pow.NewPoW(), nil)
	if err == nil {
		t.Fatal("expected missing parent header to fail")
	}
}
