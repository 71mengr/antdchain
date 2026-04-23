// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package chain

import (
	"errors"
	"fmt"
	"log"
	"math/big"
	"strings"

	"github.com/antdaza/antdchain/antdc/block"
	"github.com/antdaza/antdchain/antdc/crypto/quantum"
	"github.com/antdaza/antdchain/antdc/reward"
	"github.com/antdaza/antdchain/antdc/rotatingking"
	"github.com/antdaza/antdchain/common"
)

type blockExtraAwareRotatingKingManager struct {
	reward.RotatingKingManager
	forcedKing common.QuantumAddress
}

func (m *blockExtraAwareRotatingKingManager) GetCurrentKing() common.QuantumAddress {
	return m.forcedKing
}

func (m *blockExtraAwareRotatingKingManager) IsEligible(height uint64) bool {
	return m.forcedKing != (common.QuantumAddress{})
}

func rotatingKingFromBlockExtra(extra []byte) (common.QuantumAddress, bool) {
	s := string(extra)
	for _, part := range strings.Split(s, "|") {
		part = strings.TrimSpace(part)
		if !strings.HasPrefix(part, "rk=") {
			continue
		}
		addr := strings.TrimSpace(strings.TrimPrefix(part, "rk="))
		if quantum.IsValidAddress(addr) {
			parsed, err := common.ParseQuantumAddress(addr)
			if err == nil {
				return parsed, true
			}
		}
		return common.QuantumAddress{}, false
	}
	return common.QuantumAddress{}, false
}

func (bc *Blockchain) resolveRotatingKingManagerForBlock(b *block.Block) reward.RotatingKingManager {
	rkManager := bc.rotatingKingManager
	if rkManager == nil || b == nil || b.Header == nil {
		return rkManager
	}

	if forcedKing, ok := rotatingKingFromBlockExtra(b.Header.Extra); ok {
		return &blockExtraAwareRotatingKingManager{
			RotatingKingManager: rkManager,
			forcedKing:          forcedKing,
		}
	}

	return rkManager
}

// distributeBlockRewards calculates and distributes block rewards
func (bc *Blockchain) distributeBlockRewards(b *block.Block, totalFees *big.Int) (*reward.RewardDistribution, error) {
	if bc.rewardDistributor == nil {
		return nil, errors.New("reward distributor not initialized")
	}

	// Get block timestamp for time-based halving calculations
	blockTime := b.Header.Time

	rkManager := bc.resolveRotatingKingManagerForBlock(b)

	distribution, err := bc.rewardDistributor.DistributeRewards(
		bc.state,
		b.Header.Coinbase,
		totalFees,
		b.Header.Number.Uint64(),
		blockTime,
		rkManager,
		bc.pow,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to distribute rewards: %w", err)
	}

	return distribution, nil
}

// applyMinerReward applies miner rewards
func (bc *Blockchain) applyMinerReward(b *block.Block) error {
	if b.Header == nil || b.Header.Coinbase == (common.QuantumAddress{}) {
		return errors.New("invalid block or miner address")
	}

	// Calculate total fees from transactions
	totalFees := big.NewInt(0)
	for _, tx := range b.Txs {
		txFee := new(big.Int).Mul(tx.GasPrice, big.NewInt(int64(tx.Gas)))
		totalFees.Add(totalFees, txFee)
	}

	blockTime := b.Header.Time
	// Use the reward distributor to properly split rewards
	distribution, err := bc.rewardDistributor.DistributeRewards(
		bc.State(),
		b.Header.Coinbase,
		totalFees,
		b.Header.Number.Uint64(),
		blockTime,
		bc.rotatingKingManager,
		bc.pow,
	)

	if err != nil {
		return fmt.Errorf("failed to distribute rewards: %w", err)
	}

	if bc.rotatingKingManager != nil {
		go bc.syncRotatingKingForBlock(b.Header.Number.Uint64())
	}

	log.Printf("[blockchain] Rewards distributed for block %d:", b.Header.Number.Uint64())
	log.Printf("  • Miner (%s): %s ANTD (25%%)",
		b.Header.Coinbase.String()[:10], formatWei(distribution.MinerReward))
	log.Printf("  • Main King (%s): %s ANTD (10%%)",
		distribution.MainKingAddress.String()[:10], formatWei(distribution.MainKingReward))

	if distribution.RotatingKingEligible {
		log.Printf("  • Rotating King (%s): %s ANTD (65%%)",
			distribution.RotatingKingAddress.String()[:10], formatWei(distribution.RotatingKingReward))
	} else {
		log.Printf("  • Rotating King: Not eligible")
		totalMainKing := new(big.Int).Add(distribution.MainKingReward, distribution.RotatingKingReward)
		log.Printf("  • Main King receives rotating share fallback: %s ANTD", formatWei(totalMainKing))
	}

	// Log halving info if available
	if halvingInfo := distribution.HalvingInfo; halvingInfo != nil {
		blocksUntilHalving := halvingInfo["blocksUntilHalving"].(uint64)
		daysUntilHalving := halvingInfo["daysUntilHalving"].(uint64)

		if bc.logger != nil {
			bc.logger.Infof("  Current block reward: %s ANTD",
				halvingInfo["currentRewardANTD"])
			bc.logger.Infof("  Next halving in: %d blocks (%d days)",
				blocksUntilHalving, daysUntilHalving)
			bc.logger.Infof("  Estimated halving date: %s",
				halvingInfo["estimatedHalvingDate"])
			bc.logger.Infof("  Current inflation rate: %.4f%%", distribution.InflationRate)
		}
	}

	return nil
}

// logBlockValidationSuccess logs successful block validation
func (bc *Blockchain) logBlockValidationSuccess(b *block.Block, distribution *reward.RewardDistribution, gasUsed uint64, totalFees *big.Int) {
	blockHeight := b.Header.Number.Uint64()
	blockHash := b.Hash()

	log.Printf("[blockchain] ✓ Block %d validated successfully:", blockHeight)
	log.Printf("   • Hash: %s", blockHash.String()[:12])
	log.Printf("   • Transactions: %d", len(b.Txs))
	log.Printf("   • Gas used: %d/%d", gasUsed, b.Header.GasLimit)

	if totalFees != nil && totalFees.Sign() > 0 {
		log.Printf("   • Total fees: %s ANTD", formatBalance(totalFees))
	}

	log.Printf("   • Total reward: %s ANTD", formatBalance(distribution.TotalReward))
	log.Printf("   • Miner reward: %s ANTD (25%%) to %s",
		formatBalance(distribution.MinerReward), b.Header.Coinbase.String()[:10])
	log.Printf("   • Main King reward: %s ANTD (10%%) to %s",
		formatBalance(distribution.MainKingReward), distribution.MainKingAddress.String()[:10])

	// Better logging for rotating king status
	if distribution.RotatingKingEligible {
		log.Printf("   • Rotating King reward: %s ANTD (65%%) to %s",
			formatBalance(distribution.RotatingKingReward),
			distribution.RotatingKingAddress.String()[:10])

		// Log eligibility status with details
		if bc.rotatingKingManager != nil {
			king := distribution.RotatingKingAddress
			balance := bc.state.GetBalance(king)
			minRequired := big.NewInt(0)

			// Try to get minimum required
			if mgr, ok := bc.rotatingKingManager.(interface {
				GetConfig() rotatingking.RotatingKingConfig
			}); ok {
				config := mgr.GetConfig()
				minRequired = config.MinStakeRequired
			}

			log.Printf("   • Rotating King Eligibility: ✓ (Balance: %s ANTD, Required: %s ANTD)",
				formatBalance(balance), formatBalance(minRequired))
		}
	} else {
		log.Printf("   • Rotating King: Not eligible (Main King receives rotating share)")

		// ADDED: Explain why not eligible
		if bc.rotatingKingManager != nil && distribution.RotatingKingAddress != (common.QuantumAddress{}) {
			king := distribution.RotatingKingAddress
			log.Printf("   • Rotating King Address: %s", king.String()[:10])

			// Check if address is in rotation list
			addresses := bc.rotatingKingManager.GetKingAddresses()
			inList := false
			for _, addr := range addresses {
				if addr == king {
					inList = true
					break
				}
			}

			if !inList {
				log.Printf("   • ❌ Address not in rotating king list!")

				// Show current list for debugging
				if len(addresses) > 0 {
					log.Printf("   • Current rotating king list (%d addresses):", len(addresses))
					for i, addr := range addresses {
						isCurrent := (i == bc.rotatingKingManager.GetCurrentKingIndex())
						marker := " "
						if isCurrent {
							marker = "→"
						}
						log.Printf("      %s [%d] %s", marker, i, addr.String())
					}
				} else {
					log.Printf("   • ⚠️ Rotating king list is EMPTY!")
				}
			} else {
				balance := bc.state.GetBalance(king)
				minRequired := big.NewInt(0)

				// Get minimum required
				if mgr, ok := bc.rotatingKingManager.(interface {
					GetConfig() rotatingking.RotatingKingConfig
				}); ok {
					config := mgr.GetConfig()
					minRequired = config.MinStakeRequired
				}

				log.Printf("   • Balance: %s ANTD (Required: %s ANTD)",
					formatBalance(balance), formatBalance(minRequired))

				if balance.Cmp(minRequired) < 0 {
					log.Printf("   • ❌ Insufficient balance (%.1f%% of required)",
						float64(balance.Int64())/float64(minRequired.Int64())*100)
				} else {
					log.Printf("   • ✓ Balance sufficient, but other eligibility check failed")

					// Additional eligibility checks
					if eligibilityChecker, ok := bc.rotatingKingManager.(interface{ IsEligible(height uint64) bool }); ok {
						isEligible := eligibilityChecker.IsEligible(blockHeight)
						log.Printf("   • IsEligible(%d) check: %v", blockHeight, isEligible)
					}
				}
			}
		} else if bc.rotatingKingManager == nil {
			log.Printf("   • ⚠️ Rotating king manager not initialized")
		} else {
			log.Printf("   • Rotating King Address: (empty)")
		}
	}

	// Log difficulty
	log.Printf("   • Difficulty: %s", bc.pow.GetDifficulty().String())

	// Log halving info if available
	if halvingInfo := distribution.HalvingInfo; halvingInfo != nil {
		log.Printf("   • Halving Info:")
		log.Printf("     - Current block reward: %s ANTD", halvingInfo["currentRewardANTD"])
		log.Printf("     - Next halving in: %d blocks", halvingInfo["blocksUntilHalving"])
		log.Printf("     - Estimated halving date: %s", halvingInfo["estimatedHalvingDate"])
	}

	// Log inflation rate if available
	if distribution.InflationRate > 0 {
		log.Printf("   • Annual inflation rate: %.4f%%", distribution.InflationRate)
	}

	// Log rotating king rotation info
	if bc.rotatingKingManager != nil {
		rotationInfo := bc.rotatingKingManager.GetRotationInfo(blockHeight)
		if len(rotationInfo) > 0 {
			log.Printf("   • Rotating King Info:")
			log.Printf("     - Current: %s", rotationInfo["currentKing"])
			log.Printf("     - Next: %s", rotationInfo["nextKing"])
			log.Printf("     - Blocks until rotation: %d", rotationInfo["blocksUntilRotation"])

			if estTime, ok := rotationInfo["estimatedTimeUntilRotation"]; ok {
				log.Printf("     - Estimated time: %v", estTime)
			}
		}
	}
}
