package block

import (
	"math/big"
	"testing"
	"time"
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
