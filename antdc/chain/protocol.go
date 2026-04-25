// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package chain

import (
	"fmt"
	"math/big"

	"github.com/antdaza/antdchain/antdc/block"
	"github.com/antdaza/antdchain/antdc/reward"
	"github.com/antdaza/antdchain/common"
)

const (
	protocolBlocksPerYear    = uint64(2_628_000)
	protocolBlocksPerHalving = protocolBlocksPerYear * 4
)

type protocolUpgrade struct {
	Version            uint32
	Name               string
	ActivationHeight   uint64
	ActivationTime     uint64
	InitialBlockReward *big.Int
}

var protocolUpgradeSchedule = []protocolUpgrade{
	{
		Version:            1,
		Name:               "genesis",
		ActivationHeight:   0,
		ActivationTime:     GenesisTimestamp,
		InitialBlockReward: new(big.Int).Mul(big.NewInt(200), big.NewInt(1e18)),
	},
}

func expectedUpgrade(height uint64, blockTime uint64) protocolUpgrade {
	active := protocolUpgradeSchedule[0]
	for _, upgrade := range protocolUpgradeSchedule {
		if height >= upgrade.ActivationHeight && blockTime >= upgrade.ActivationTime {
			active = upgrade
		}
	}
	return active
}

func expectedRewardForBlock(height uint64, initialReward *big.Int) *big.Int {
	if initialReward == nil {
		return big.NewInt(0)
	}

	halvingPeriod := height / protocolBlocksPerHalving
	rewardAmount := new(big.Int).Set(initialReward)

	for i := uint64(0); i < halvingPeriod; i++ {
		rewardAmount.Div(rewardAmount, big.NewInt(2))
		if rewardAmount.Cmp(big.NewInt(1e18)) < 0 {
			return big.NewInt(0)
		}
	}

	return rewardAmount
}

func protocolMainKingAddress() (common.QuantumAddress, error) {
	parsed, err := common.ParseQuantumAddress(GenesisMainKing)
	if err != nil {
		return common.QuantumAddress{}, fmt.Errorf("invalid protocol main king address: %w", err)
	}
	return common.QuantumAddress(parsed), nil
}

func applyProtocolHeaderFields(header *block.Header) error {
	if header == nil || header.Number == nil {
		return nil
	}

	height := header.Number.Uint64()
	upgrade := expectedUpgrade(height, header.Time)
	expectedReward := expectedRewardForBlock(height, upgrade.InitialBlockReward)
	mainKingAddress, err := protocolMainKingAddress()
	if err != nil {
		return err
	}

	header.ProtocolVersion = upgrade.Version
	header.UpgradeName = upgrade.Name
	header.UpgradeTimestamp = upgrade.ActivationTime
	header.MainKingAddress = mainKingAddress
	header.RewardAmount = new(big.Int).Set(expectedReward)
	header.CurrentReward = new(big.Int).Set(expectedReward)
	return nil
}

func validateProtocolHeader(header *block.Header) error {
	if header == nil {
		return fmt.Errorf("nil header")
	}
	if header.Number == nil {
		return fmt.Errorf("nil header number")
	}

	height := header.Number.Uint64()
	upgrade := expectedUpgrade(height, header.Time)
	expectedReward := expectedRewardForBlock(height, upgrade.InitialBlockReward)
	expectedMainKingAddress, err := protocolMainKingAddress()
	if err != nil {
		return err
	}

	if header.ProtocolVersion != upgrade.Version {
		return fmt.Errorf("protocol version mismatch at block %d: expected %d, got %d", height, upgrade.Version, header.ProtocolVersion)
	}
	if header.UpgradeName != upgrade.Name {
		return fmt.Errorf("upgrade name mismatch at block %d: expected %q, got %q", height, upgrade.Name, header.UpgradeName)
	}
	if header.UpgradeTimestamp != upgrade.ActivationTime {
		return fmt.Errorf("upgrade timestamp mismatch at block %d: expected %d, got %d", height, upgrade.ActivationTime, header.UpgradeTimestamp)
	}
	if header.MainKingAddress != expectedMainKingAddress {
		return fmt.Errorf("main king address mismatch at block %d: expected %s, got %s", height, expectedMainKingAddress.String(), header.MainKingAddress.String())
	}

	if header.RewardAmount == nil {
		return fmt.Errorf("reward amount missing at block %d", height)
	}
	if header.CurrentReward == nil {
		return fmt.Errorf("current reward missing at block %d", height)
	}
	if header.RewardAmount.Cmp(expectedReward) != 0 {
		return fmt.Errorf("reward amount changed without protocol upgrade at block %d: expected %s, got %s", height, expectedReward.String(), header.RewardAmount.String())
	}
	if header.CurrentReward.Cmp(expectedReward) != 0 {
		return fmt.Errorf("current reward changed without protocol upgrade at block %d: expected %s, got %s", height, expectedReward.String(), header.CurrentReward.String())
	}

	// Strong consensus safety: if local reward logic diverges from protocol rules,
	// reject immediately so modified economic params cannot be accepted silently.
	localReward := reward.CalculateBlockReward(height)
	if localReward.Cmp(expectedReward) != 0 {
		return fmt.Errorf("local reward schedule mismatch at block %d: protocol=%s local=%s", height, expectedReward.String(), localReward.String())
	}

	if height == 0 {
		if header.ParentHash != (common.Hash{}) {
			return fmt.Errorf("genesis parent hash must be zero")
		}
		if header.Coinbase != expectedMainKingAddress {
			return fmt.Errorf("genesis coinbase mismatch: expected %s, got %s", expectedMainKingAddress.String(), header.Coinbase.String())
		}
		if header.Time != GenesisTimestamp {
			return fmt.Errorf("genesis timestamp mismatch: expected %d, got %d", GenesisTimestamp, header.Time)
		}
		if header.GasLimit != GenesisGasLimit {
			return fmt.Errorf("genesis gas limit mismatch: expected %d, got %d", GenesisGasLimit, header.GasLimit)
		}
		if header.Difficulty == nil || header.Difficulty.Cmp(big.NewInt(GenesisDifficulty)) != 0 {
			return fmt.Errorf("genesis difficulty mismatch: expected %d, got %v", GenesisDifficulty, header.Difficulty)
		}
		if string(header.Extra) != GenesisExtraData {
			return fmt.Errorf("genesis extra data mismatch")
		}
	}

	return nil
}
