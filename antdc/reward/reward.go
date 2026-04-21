// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root for more information.

package reward

import (
"context"
"errors"
"math/big"
"time"

"github.com/antdaza/antdchain/antdc/pow" // PoS engine
"github.com/antdaza/antdchain/antdc/rotatingking"
"github.com/antdaza/antdchain/antdc/state"
"github.com/ethereum/go-ethereum/common"
)

// Constants
var (
eligibilityThreshold = new(big.Int).Mul(big.NewInt(100_000), big.NewInt(1e18))   // 100k ANTD for rotating king bonus
initialBlockReward   = new(big.Int).Mul(big.NewInt(200), big.NewInt(1e18))       // 200 ANTD initial block reward
minStakeForPoS       = new(big.Int).Mul(big.NewInt(1_000_000), big.NewInt(1e18)) // 1M ANTD for auto-staking

// Halving parameters for 12-second blocks
secondsPerBlock  = uint64(12)                   // 12-second block time
blocksPerMinute  = uint64(60 / secondsPerBlock) // 5 blocks per minute
blocksPerHour    = blocksPerMinute * 60         // 300 blocks per hour
blocksPerDay     = blocksPerHour * 24           // 7,200 blocks per day
blocksPerWeek    = blocksPerDay * 7             // 50,400 blocks per week
blocksPerMonth   = blocksPerDay * 30            // 216,000 blocks per month (approx)
blocksPerYear    = blocksPerDay * 365           // 2,628,000 blocks per year
blocksPerHalving = blocksPerYear * 4            // 10,512,000 blocks per 4 years
genesisTimestamp = int64(1763731821)            // Dec 1, 2025, 00:00:00 UTC
maxHalvings      = uint64(64)                   // Maximum number of halvings
)

// Calculate current block reward with halving
func CalculateBlockReward(blockNumber uint64) *big.Int {
// Calculate which halving period we're in
halvingPeriod := blockNumber / blocksPerHalving

// Cap at max halvings to prevent reward from going to zero
if halvingPeriod > maxHalvings {
halvingPeriod = maxHalvings
}

// Calculate reward: initialReward / 2^halvingPeriod
reward := new(big.Int).Set(initialBlockReward)

// Divide by 2^halvingPeriod (Bitcoin-style halving)
for i := uint64(0); i < halvingPeriod; i++ {
reward.Div(reward, big.NewInt(2))

// Once reward is less than 1 ANTD (1e18), it's essentially zero
if reward.Cmp(big.NewInt(1e18)) < 0 {
return big.NewInt(0)
}
}

return reward
}

// Calculate halving based on time
func CalculateBlockRewardByTime(currentTime int64) *big.Int {
// Calculate years since genesis
secondsSinceGenesis := currentTime - genesisTimestamp
if secondsSinceGenesis < 0 {
return new(big.Int).Set(initialBlockReward)
}

// Calculate halving periods (4 years each)
secondsPerHalving := int64(4 * 365 * 24 * 60 * 60) // 4 years in seconds
halvingPeriod := uint64(secondsSinceGenesis / secondsPerHalving)

// Cap at max halvings
if halvingPeriod > maxHalvings {
halvingPeriod = maxHalvings
}

// Calculate reward: initialReward / 2^halvingPeriod
reward := new(big.Int).Set(initialBlockReward)

for i := uint64(0); i < halvingPeriod; i++ {
reward.Div(reward, big.NewInt(2))

// Once reward is less than 1 ANTD (1e18), it's essentially zero
if reward.Cmp(big.NewInt(1e18)) < 0 {
return big.NewInt(0)
}
}

return reward
}

// Get next halving information
func GetNextHalvingInfo(blockNumber uint64) map[string]interface{} {
currentHalvingPeriod := blockNumber / blocksPerHalving
nextHalvingBlock := (currentHalvingPeriod + 1) * blocksPerHalving
blocksUntilHalving := nextHalvingBlock - blockNumber

// Calculate estimated time until next halving (12-second blocks)
secondsUntilHalving := blocksUntilHalving * secondsPerBlock
daysUntilHalving := secondsUntilHalving / (24 * 60 * 60)
yearsUntilHalving := float64(daysUntilHalving) / 365.0

// Calculate current reward and next reward
currentReward := CalculateBlockReward(blockNumber)
nextReward := CalculateBlockReward(nextHalvingBlock)

// Calculate approximate halving date
secondsPerBlockFloat := float64(secondsPerBlock)
estimatedSecondsRemaining := float64(blocksUntilHalving) * secondsPerBlockFloat
estimatedHalvingTime := time.Now().Add(time.Duration(estimatedSecondsRemaining) * time.Second)

return map[string]interface{}{
"currentBlock":         blockNumber,
"currentHalvingPeriod": currentHalvingPeriod,
"nextHalvingBlock":     nextHalvingBlock,
"blocksUntilHalving":   blocksUntilHalving,
"secondsUntilHalving":  secondsUntilHalving,
"daysUntilHalving":     daysUntilHalving,
"yearsUntilHalving":    yearsUntilHalving,
"estimatedHalvingDate": estimatedHalvingTime.Format("2006-01-02 15:04:05 MST"),
"blockTimeSeconds":     secondsPerBlock,
"currentReward":        currentReward.String(),
"currentRewardANTD":    formatANTD(currentReward),
"nextReward":           nextReward.String(),
"nextRewardANTD":       formatANTD(nextReward),
"blocksPerDay":         blocksPerDay,
"blocksPerYear":        blocksPerYear,
"blocksPerHalving":     blocksPerHalving,
"maxHalvings":          maxHalvings,
"totalSupplyCap":       CalculateTotalSupplyCap().String(),
"totalSupplyCapANTD":   formatANTD(CalculateTotalSupplyCap()),
"rewardReduction":      "50%", // Each halving reduces reward by 50%
}
}

// Calculate total supply cap
func CalculateTotalSupplyCap() *big.Int {
total := big.NewInt(0)

// Sum of all block rewards through all halvings
currentReward := new(big.Int).Set(initialBlockReward)

for halving := uint64(0); halving <= maxHalvings; halving++ {
// Blocks in this halving period
var blocksInPeriod uint64
if halving == maxHalvings {
// For the last period, assume infinite blocks (but reward will be 0)
blocksInPeriod = 0
} else {
blocksInPeriod = blocksPerHalving
}

// Add reward for this period
periodReward := new(big.Int).Mul(currentReward, big.NewInt(int64(blocksInPeriod)))
total.Add(total, periodReward)

// Halve for next period
currentReward.Div(currentReward, big.NewInt(2))

// Stop if reward is effectively zero
if currentReward.Cmp(big.NewInt(1e18)) < 0 {
break
}
}

return total
}

// Calculate circulating supply up to a given block
func CalculateCirculatingSupply(blockNumber uint64) *big.Int {
total := big.NewInt(0)
currentBlock := uint64(0)
currentReward := new(big.Int).Set(initialBlockReward)
currentHalving := uint64(0)

for currentBlock < blockNumber {
// Determine how many blocks in current halving period
blocksInThisPeriod := blocksPerHalving
if currentHalving == maxHalvings {
blocksInThisPeriod = blockNumber - currentBlock // All remaining blocks
}

// Calculate blocks to process
blocksToProcess := blocksInThisPeriod
if currentBlock+blocksToProcess > blockNumber {
blocksToProcess = blockNumber - currentBlock
}

// Add rewards for these blocks
periodReward := new(big.Int).Mul(currentReward, big.NewInt(int64(blocksToProcess)))
total.Add(total, periodReward)

// Move forward
currentBlock += blocksToProcess

// If we've completed a halving period, halve the reward
if currentBlock%blocksPerHalving == 0 && currentBlock > 0 {
currentHalving++
if currentHalving > maxHalvings {
break
}
currentReward.Div(currentReward, big.NewInt(2))

// Stop if reward is effectively zero
if currentReward.Cmp(big.NewInt(1e18)) < 0 {
break
}
}
}

return total
}

// Calculate block time statistics
func GetBlockTimeStats() map[string]interface{} {
return map[string]interface{}{
"secondsPerBlock":  secondsPerBlock,
"blocksPerMinute":  blocksPerMinute,
"blocksPerHour":    blocksPerHour,
"blocksPerDay":     blocksPerDay,
"blocksPerWeek":    blocksPerWeek,
"blocksPerMonth":   blocksPerMonth,
"blocksPerYear":    blocksPerYear,
"blocksPerHalving": blocksPerHalving,
"daysPerHalving":   (blocksPerHalving * secondsPerBlock) / (24 * 60 * 60),
"yearsPerHalving":  4,
"halvingInterval":  "4 years",
}
}

func formatANTD(wei *big.Int) string {
if wei == nil {
return "0"
}

oneANTD := big.NewInt(1e18)
whole := new(big.Int).Div(wei, oneANTD)
remainder := new(big.Int).Mod(wei, oneANTD)

if remainder.Sign() == 0 {
return whole.String()
}

// Format with 6 decimal places
remainderFloat := new(big.Float).SetInt(remainder)
divisor := new(big.Float).SetInt(oneANTD)
remainderFloat.Quo(remainderFloat, divisor)

// Convert to string with fixed precision
remainderStr := remainderFloat.Text('f', 6)

// Remove leading "0."
if len(remainderStr) > 2 && remainderStr[:2] == "0." {
remainderStr = remainderStr[2:]
}

// Remove trailing zeros
for len(remainderStr) > 0 && remainderStr[len(remainderStr)-1] == '0' {
remainderStr = remainderStr[:len(remainderStr)-1]
}

if remainderStr == "" {
return whole.String()
}

return whole.String() + "." + remainderStr
}

func EligibilityThreshold() *big.Int {
return new(big.Int).Set(eligibilityThreshold)
}

func BlockReward(blockNumber uint64) *big.Int {
return CalculateBlockReward(blockNumber)
}

// RewardDistributor now knows both Main King and PoS engine
type RewardDistributor struct {
mainKing common.Address
}

func NewRewardDistributor(mainKing common.Address) *RewardDistributor {
return &RewardDistributor{
mainKing: mainKing,
}
}

func (rd *RewardDistributor) GetMainKing() common.Address {
return rd.mainKing
}

// RewardDistribution records what was distributed in a block
type RewardDistribution struct {
BlockNumber          uint64
Miner                common.Address
TotalReward          *big.Int
MinerReward          *big.Int
MainKingReward       *big.Int
RotatingKingReward   *big.Int
MainKingAddress      common.Address
RotatingKingAddress  common.Address
RotatingKingEligible bool
HalvingInfo          map[string]interface{} // Added halving info
InflationRate        float64                // Annual inflation rate at this block
}

type RotatingKingManager interface {
GetCurrentKing() common.Address
GetNextKing() common.Address
GetRotationInfo(height uint64) map[string]interface{}
ShouldRotate(blockHeight uint64) bool
RotateToNextKing(blockHeight uint64, blockHash common.Hash) error
RecordRewardDistribution(king common.Address, reward *big.Int, blockHeight uint64)
GetKingRewards(king common.Address) *big.Int
GetTotalRewardsDistributed() *big.Int
GetKingStats(king common.Address) map[string]interface{}
GetRotationInterval() uint64
IsCurrentKing(address common.Address) bool
GetCurrentKingIndex() int
GetKingAddresses() []common.Address
IsKing(address common.Address) bool
UpdateKingAddresses(newAddresses []common.Address) error
GetRotationHistory(int) []rotatingking.KingRotation
GetKingRewardMultiplier() *big.Float
ForceRotate(index int, reason string) error
IsEligible(height uint64) bool
ForceRotateToAddress(newKing common.Address, reason string) error

SyncBlocks(ctx context.Context, blockHeight uint64) error
GetSyncState() (*rotatingking.SyncState, error)
GetDBMetrics() *rotatingking.DBMetrics
BackupDatabase(backupPath string) error
GetLastSyncedBlock() (*rotatingking.BlockSyncRecord, error)
GetRotationHistoryFromDB(fromBlock, toBlock uint64) ([]rotatingking.KingRotation, error)
Close() error
}

// Calculate annual inflation rate at current block
func CalculateInflationRate(blockNumber uint64, circulatingSupply *big.Int) float64 {
if circulatingSupply == nil || circulatingSupply.Sign() == 0 {
return 0
}

// Annual block production
annualBlocks := blocksPerYear

// Current block reward
currentReward := CalculateBlockReward(blockNumber)

// Annual new supply
annualNewSupply := new(big.Float).SetInt(new(big.Int).Mul(currentReward, big.NewInt(int64(annualBlocks))))

// Circulating supply as float
circulatingSupplyFloat := new(big.Float).SetInt(circulatingSupply)

// Inflation rate = (annual new supply / circulating supply) * 100
inflationRate := new(big.Float).Quo(annualNewSupply, circulatingSupplyFloat)
inflationRate.Mul(inflationRate, big.NewFloat(100))

rate, _ := inflationRate.Float64()
return rate
}

func (rd *RewardDistributor) DistributeRewards(
statedb *state.State,
miner common.Address,
transactionFees *big.Int,
blockNumber uint64,
blockTime uint64,
rkManager RotatingKingManager,
posEngine *pow.PoW,
) (*RewardDistribution, error) {

if rkManager == nil {
return nil, errors.New("rotating king manager required")
}
if posEngine == nil {
return nil, errors.New("PoS engine required for auto-staking")
}

// Calculate block reward with halving
blockReward := CalculateBlockReward(blockNumber)

// Get halving info for logging
halvingInfo := GetNextHalvingInfo(blockNumber)

// Calculate circulating supply for inflation rate
circulatingSupply := CalculateCirculatingSupply(blockNumber)
inflationRate := CalculateInflationRate(blockNumber, circulatingSupply)

// Total reward = halved block reward + all transaction fees
totalReward := new(big.Int).Set(blockReward)
if transactionFees != nil && transactionFees.Sign() > 0 {
totalReward.Add(totalReward, transactionFees)
}

if totalReward.Sign() == 0 {
return &RewardDistribution{
BlockNumber:          blockNumber,
Miner:                miner,
TotalReward:          big.NewInt(0),
MinerReward:          big.NewInt(0),
MainKingReward:       big.NewInt(0),
RotatingKingReward:   big.NewInt(0),
MainKingAddress:      rd.mainKing,
RotatingKingAddress:  common.Address{},
RotatingKingEligible: false,
HalvingInfo:          halvingInfo,
InflationRate:        inflationRate,
}, nil
}

// Get current rotating king
currentRotatingKing := rkManager.GetCurrentKing()

// DEBUG: Log rotating king info
// log.Printf("[reward] Current rotating king: %s", currentRotatingKing.Hex())

// Check rotating king eligibility
rotatingEligible := false
if currentRotatingKing != (common.Address{}) {
balance := statedb.GetBalance(currentRotatingKing)
// DEBUG: Log balance info
// log.Printf("[reward] Rotating king balance: %s, threshold: %s",
//     formatANTD(balance), formatANTD(eligibilityThreshold))
if balance.Cmp(eligibilityThreshold) >= 0 {
rotatingEligible = true
}
}

// DEBUG: Log eligibility result
// log.Printf("[reward] Rotating king eligible: %v", rotatingEligible)

// Distribution ratio:
//   Miner(Staker) = 50 parts
//   Main King     = 20 parts
//   Rotating King = 130 parts (if eligible)
// Total = 200 parts.
// If rotating king is ineligible, Main King receives the rotating king share.
totalParts := big.NewInt(200)
minerParts := big.NewInt(50)
mainParts := big.NewInt(20)
rotatingParts := big.NewInt(130)

minerReward := new(big.Int).Mul(totalReward, minerParts)
minerReward.Div(minerReward, totalParts)

mainReward := new(big.Int).Mul(totalReward, mainParts)
mainReward.Div(mainReward, totalParts)

var rotReward *big.Int
if rotatingEligible {
rotReward = new(big.Int).Mul(totalReward, rotatingParts)
rotReward.Div(rotReward, totalParts)
} else {
rotReward = big.NewInt(0)
fallback := new(big.Int).Mul(totalReward, rotatingParts)
fallback.Div(fallback, totalParts)
mainReward.Add(mainReward, fallback)
}

// Handle remainder due to integer division
allocated := new(big.Int).Add(minerReward, mainReward)
allocated.Add(allocated, rotReward)
remainder := new(big.Int).Sub(totalReward, allocated)

// Distribute remainder to rotating king if eligible, otherwise to main king
if remainder.Sign() > 0 {
if rotatingEligible {
rotReward.Add(rotReward, remainder)
} else {
mainReward.Add(mainReward, remainder)
}
}

// DEBUG: Log reward amounts
// log.Printf("[reward] Rewards: miner=%s, main=%s, rotating=%s",
//     formatANTD(minerReward), formatANTD(mainReward), formatANTD(rotReward))

// === APPLY REWARDS TO STATE ===
statedb.AddBalance(miner, minerReward)
statedb.AddBalance(rd.mainKing, mainReward)

if rotatingEligible && rotReward.Sign() > 0 {
statedb.AddBalance(currentRotatingKing, rotReward)
// Record the reward distribution
rkManager.RecordRewardDistribution(currentRotatingKing, rotReward, blockNumber)
}

// === AUTO-REGISTER / UPDATE STAKERS IN PoS ENGINE ===
// This is the core of the new design: anyone with ≥1M ANTD becomes a block producer automatically

posEngine.AutoRegisterIfEligible(miner, statedb.GetBalance(miner))
posEngine.AutoRegisterIfEligible(rd.mainKing, statedb.GetBalance(rd.mainKing))

if rotatingEligible {
posEngine.AutoRegisterIfEligible(currentRotatingKing, statedb.GetBalance(currentRotatingKing))
}

return &RewardDistribution{
BlockNumber:          blockNumber,
Miner:                miner,
TotalReward:          totalReward,
MinerReward:          minerReward,
MainKingReward:       mainReward,
RotatingKingReward:   rotReward,
MainKingAddress:      rd.mainKing,
RotatingKingAddress:  currentRotatingKing,
RotatingKingEligible: rotatingEligible,
HalvingInfo:          halvingInfo,
InflationRate:        inflationRate,
}, nil
}

// GetHalvingStats returns comprehensive halving statistics
func GetHalvingStats() map[string]interface{} {
stats := make(map[string]interface{})

stats["initialBlockReward"] = initialBlockReward.String()
stats["initialBlockRewardANTD"] = formatANTD(initialBlockReward)
stats["secondsPerBlock"] = secondsPerBlock
stats["blocksPerHalving"] = blocksPerHalving
stats["blocksPerYear"] = blocksPerYear
stats["yearsPerHalving"] = 4
stats["maxHalvings"] = maxHalvings
stats["genesisTimestamp"] = genesisTimestamp
stats["genesisDate"] = time.Unix(genesisTimestamp, 0).UTC().Format(time.RFC3339)

// Calculate total supply cap
totalSupply := CalculateTotalSupplyCap()
stats["totalSupplyCap"] = totalSupply.String()
stats["totalSupplyCapANTD"] = formatANTD(totalSupply)

// Calculate halving schedule
halvingSchedule := make([]map[string]interface{}, 0)
currentReward := new(big.Int).Set(initialBlockReward)
cumulativeBlocks := uint64(0)

for halving := uint64(0); halving <= maxHalvings; halving++ {
startBlock := halving * blocksPerHalving
endBlock := (halving + 1) * blocksPerHalving

// Calculate period duration
periodSeconds := blocksPerHalving * secondsPerBlock
periodDays := periodSeconds / (24 * 60 * 60)
periodYears := float64(periodDays) / 365.0

// Calculate period supply
periodSupply := new(big.Int).Mul(currentReward, big.NewInt(int64(blocksPerHalving)))
cumulativeBlocks += blocksPerHalving

scheduleEntry := map[string]interface{}{
"halving":          halving,
"startBlock":       startBlock,
"endBlock":         endBlock,
"periodYears":      periodYears,
"periodDays":       periodDays,
"blockReward":      currentReward.String(),
"blockRewardANTD":  formatANTD(currentReward),
"periodSupply":     periodSupply.String(),
"periodSupplyANTD": formatANTD(periodSupply),
}

halvingSchedule = append(halvingSchedule, scheduleEntry)

// Halve for next period
currentReward.Div(currentReward, big.NewInt(2))

// Stop if reward is effectively zero
if currentReward.Cmp(big.NewInt(1e18)) < 0 {
break
}
}

stats["halvingSchedule"] = halvingSchedule
stats["blockTimeStats"] = GetBlockTimeStats()

return stats
}

// GetCurrentHalvingPhase returns detailed info about current halving phase
func GetCurrentHalvingPhase(blockNumber uint64) map[string]interface{} {
halvingPeriod := blockNumber / blocksPerHalving
phaseProgress := float64(blockNumber%blocksPerHalving) / float64(blocksPerHalving) * 100

// Calculate time metrics
blocksRemaining := blocksPerHalving - (blockNumber % blocksPerHalving)
secondsRemaining := blocksRemaining * secondsPerBlock
daysRemaining := secondsRemaining / (24 * 60 * 60)
estimatedCompletion := time.Now().Add(time.Duration(secondsRemaining) * time.Second)

return map[string]interface{}{
"halvingPeriod":       halvingPeriod,
"phaseProgress":       phaseProgress,
"blocksIntoPhase":     blockNumber % blocksPerHalving,
"blocksRemaining":     blocksRemaining,
"secondsRemaining":    secondsRemaining,
"daysRemaining":       daysRemaining,
"estimatedCompletion": estimatedCompletion.Format("2006-01-02 15:04:05 MST"),
"completionTimestamp": estimatedCompletion.Unix(),
"isHalvingImminent":   blocksRemaining < blocksPerDay, // Less than 1 day remaining
}
}
