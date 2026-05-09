package nonce

import (
	"fmt"
	"strconv"

	"github.com/antdaza/antdchain/antdc/tx"
	"github.com/antdaza/antdchain/common"
)

func Determine(parts []string, fromAddr common.QuantumAddress, stateNonce, nextNonce uint64, pending []*tx.Tx) (uint64, string, bool, common.Hash, error) {
	_ = nextNonce
	if len(parts) <= 4 {
		return stateNonce, "auto", false, common.Hash{}, nil
	}

	arg := parts[4]
	if arg != "@replace" && arg != "replace" {
		manualNonce, err := strconv.ParseUint(arg, 10, 64)
		if err != nil {
			return 0, "", false, common.Hash{}, fmt.Errorf("invalid nonce '%s': %w", arg, err)
		}
		return manualNonce, "manual", false, common.Hash{}, nil
	}

	var replaceTxHash common.Hash
	for _, pendingTx := range pending {
		if pendingTx.From == fromAddr {
			if pendingTx.Nonce == stateNonce {
				replaceTxHash = pendingTx.Hash()
				return stateNonce, fmt.Sprintf("replace %s", replaceTxHash.String()[:8]), true, replaceTxHash, nil
			}
		}
	}
	if replaceTxHash == (common.Hash{}) {
		return 0, "", true, common.Hash{}, fmt.Errorf("no pending transaction found to replace at nonce %d for sender %s", stateNonce, fromAddr.String())
	}
	return 0, "", true, common.Hash{}, fmt.Errorf("no pending transaction found to replace at nonce %d for sender %s", stateNonce, fromAddr.String())
}

