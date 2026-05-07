package fees

import "math/big"

var oneANTD = big.NewInt(1_000_000_000_000_000_000)

// AutoGasPriceByAmount returns a tiered gas price in wei based on transfer amount.
func AutoGasPriceByAmount(amount *big.Int) *big.Int {
	if amount == nil {
		return big.NewInt(1e9)
	}

	baseGwei := int64(1)
	if amount.Cmp(new(big.Int).Mul(big.NewInt(100), oneANTD)) >= 0 {
		baseGwei = 6
	} else if amount.Cmp(new(big.Int).Mul(big.NewInt(10), oneANTD)) >= 0 {
		baseGwei = 4
	} else if amount.Cmp(oneANTD) >= 0 {
		baseGwei = 2
	}

	return new(big.Int).Mul(big.NewInt(baseGwei), big.NewInt(1e9))
}

