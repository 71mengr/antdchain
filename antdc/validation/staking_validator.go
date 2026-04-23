
package validation
import (
    "errors"
    "log"
    "math/big"

    "github.com/antdaza/antdchain/antdc/block"
    "github.com/antdaza/antdchain/antdc/chain"
    "github.com/antdaza/antdchain/antdc/staking"
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
    if blk.Header.Miner == (common.QuantumAddress{}) {
        return errors.New("block must have a miner address")
    }
    
    // Check if miner has sufficient stake
    stake, err := sv.stakingManager.GetStake(blk.Header.Miner)
    if err != nil {
        return fmt.Errorf("failed to get stake for miner %s: %v", 
            blk.Header.Miner.Hex(), err)
    }
    
    if stake.Cmp(sv.minStakeAmount) < 0 {
        return fmt.Errorf("miner %s has insufficient stake: %s < %s", 
            blk.Header.Miner.Hex(), stake.String(), sv.minStakeAmount.String())
    }
    
    // Check miner rotation rules
    parent := bc.GetBlockByHash(blk.Header.ParentHash)
    if parent != nil {
        // Get mining state from chain
        miningState := bc.GetMiningState()
        if miningState != nil {

        }
    }
    
    log.Printf("[validation] Miner %s validated (stake: %s ANTD)", 
        blk.Header.Miner.Hex()[:12], new(big.Int).Div(stake, big.NewInt(1e18)).String())
    
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
