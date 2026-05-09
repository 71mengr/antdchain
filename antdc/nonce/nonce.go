package nonce

import (
	"fmt"
	"strconv"

	"github.com/antdaza/antdchain/antdc/tx"
	"github.com/antdaza/antdchain/common"
)

func Determine(parts []string, fromAddr common.QuantumAddress, stateNonce, nextNonce uint64, pending []*tx.Tx) (uint64, string, bool, common.Hash, error) {
	if len(parts) <= 4 {
		if nextNonce < stateNonce {
			nextNonce = stateNonce
		}
		return nextNonce, "auto", false, common.Hash{}, nil
	}

	arg := parts[4]
	if arg != "@replace" && arg != "replace" {
		manualNonce, err := strconv.ParseUint(arg, 10, 64)
		if err != nil {
			return 0, "", false, common.Hash{}, fmt.Errorf("invalid nonce '%s': %w", arg, err)
		}
		return manualNonce, "manual", false, common.Hash{}, nil
	}

	var (
		suggestedNonce uint64
		replaceTxHash  common.Hash
		found          bool
	)
	for _, pendingTx := range pending {
		if pendingTx.From == fromAddr {
			if !found || pendingTx.Nonce > suggestedNonce {
				found = true
				suggestedNonce = pendingTx.Nonce
				replaceTxHash = pendingTx.Hash()
			}
		}
	}
	if !found {
		return 0, "", true, common.Hash{}, fmt.Errorf("no pending transaction found to replace for sender %s", fromAddr.String())
	}

	return suggestedNonce, fmt.Sprintf("replace %s", replaceTxHash.String()[:8]), true, replaceTxHash, nil
}
