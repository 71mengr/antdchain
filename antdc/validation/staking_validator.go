package validation

import (
	"errors"
	"fmt"
	"log"
	"math/big"

	"github.com/antdaza/antdchain/antdc/block"
	"github.com/antdaza/antdchain/antdc/chain"
	"github.com/antdaza/antdchain/antdc/staking"
	"github.com/antdaza/antdchain/common"
)

type StakingValidator struct {
	stakingManager *staking.StakingManager
	minStakeAmount *big.Int
}

func NewStakingValidator(stakingManager *staking.StakingManager) *StakingValidator {
	minStake := new(big.Int).Mul(big.NewInt(1_000_000), big.NewInt(1e18))
	return &StakingValidator{
		stakingManager: stakingManager,
		minStakeAmount: minStake,
	}
}

func (sv *StakingValidator) ValidateBlockMiner(blk *block.Block, bc *chain.Blockchain) error {
	miner := blk.Header.Coinbase
	if miner == (common.QuantumAddress{}) {
		return errors.New("block must have a miner address")
	}

	// Check if miner has sufficient stake
	stake, err := sv.stakingManager.GetStake(miner)
	if err != nil {
		return fmt.Errorf("failed to get stake for miner %s: %v", miner.Hex(), err)
	}

	if stake.Cmp(sv.minStakeAmount) < 0 {
		return fmt.Errorf("miner %s has insufficient stake: %s < %s",
			miner.Hex(), stake.String(), sv.minStakeAmount.String())
	}

	if _, err := bc.GetBlockByHash(blk.Header.ParentHash); err != nil {
		return fmt.Errorf("failed to load parent block: %w", err)
	}

	log.Printf("[validation] Miner %s validated (stake: %s ANTD)",
		miner.Hex()[:12], new(big.Int).Div(stake, big.NewInt(1e18)).String())

	return nil
}

func (sv *StakingValidator) ValidateMinerRotation(currentMiner, nextMiner common.QuantumAddress,
	blocksMined uint64, blocksPerMiner uint64) error {

	// Check if current miner completed their blocks
	if blocksMined >= blocksPerMiner {
		// Miner should rotate
		if currentMiner == nextMiner {
			return errors.New("miner should rotate after completing blocks")
		}

		// Verify next miner is eligible
		stake, err := sv.stakingManager.GetStake(nextMiner)
		if err != nil {
			return fmt.Errorf("failed to get stake for next miner: %v", err)
		}

		if stake.Cmp(sv.minStakeAmount) < 0 {
			return fmt.Errorf("next miner %s has insufficient stake", nextMiner.Hex())
		}
	}

	return nil
}
