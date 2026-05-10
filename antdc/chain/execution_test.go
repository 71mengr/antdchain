package chain

import (
	"testing"

	"github.com/antdaza/antdchain/antdc/tx"
)

func TestTransactionCommittedGasUsesTransactionGasLimit(t *testing.T) {
	transaction := &tx.Tx{Gas: 100000}

	if got := transactionCommittedGas(transaction); got != transaction.Gas {
		t.Fatalf("committed gas = %d, want %d", got, transaction.Gas)
	}
}

func TestTransactionCommittedGasNilTransaction(t *testing.T) {
	if got := transactionCommittedGas(nil); got != 0 {
		t.Fatalf("committed gas for nil transaction = %d, want 0", got)
	}
}
