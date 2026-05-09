package nonce

import (
	"strings"
	"testing"

	"github.com/antdaza/antdchain/antdc/tx"
	"github.com/antdaza/antdchain/common"
)

func qa(seed byte) common.QuantumAddress {
	var a common.QuantumAddress
	for i := range a {
		a[i] = seed
	}
	return a
}

func TestDetermine_AutoUsesStateNonce(t *testing.T) {
	from := qa(1)
	nonce, source, replace, replaceHash, err := Determine(
		[]string{"send", from.String(), qa(2).String(), "1"},
		from,
		7,
		20,
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nonce != 7 || source != "auto" || replace || replaceHash != (common.Hash{}) {
		t.Fatalf("unexpected result: nonce=%d source=%s replace=%v hash=%v", nonce, source, replace, replaceHash)
	}
}

func TestDetermine_ReplaceRequiresPendingAtStateNonce(t *testing.T) {
	from := qa(3)
	pending := []*tx.Tx{
		tx.NewTx(from, qa(9), nil, nil, 8, 21000, nil),
		tx.NewTx(from, qa(8), nil, nil, 10, 21000, nil),
	}
	_, _, _, _, err := Determine(
		[]string{"send", from.String(), qa(2).String(), "1", "@replace"},
		from,
		9,
		11,
		pending,
	)
	if err == nil {
		t.Fatal("expected error when no pending tx exists at state nonce")
	}
	if !strings.Contains(err.Error(), "replace at nonce 9") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDetermine_ReplaceSelectsPendingAtStateNonce(t *testing.T) {
	from := qa(4)
	target := tx.NewTx(from, qa(6), nil, nil, 11, 21000, nil)
	pending := []*tx.Tx{
		tx.NewTx(from, qa(5), nil, nil, 10, 21000, nil),
		target,
		tx.NewTx(from, qa(7), nil, nil, 12, 21000, nil),
	}
	nonce, source, replace, replaceHash, err := Determine(
		[]string{"send", from.String(), qa(2).String(), "1", "replace"},
		from,
		11,
		13,
		pending,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nonce != 11 || !replace {
		t.Fatalf("unexpected replace result: nonce=%d replace=%v", nonce, replace)
	}
	if replaceHash != target.Hash() {
		t.Fatalf("unexpected hash: got %s want %s", replaceHash.String(), target.Hash().String())
	}
	if !strings.HasPrefix(source, "replace ") {
		t.Fatalf("unexpected source: %s", source)
	}
}

