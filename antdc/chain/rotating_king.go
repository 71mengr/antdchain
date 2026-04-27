// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package chain
import (
"context"
"errors"
"fmt"
"log"
"reflect"
"time"

"github.com/antdaza/antdchain/antdc/pow"
"github.com/antdaza/antdchain/antdc/block"
"github.com/antdaza/antdchain/common"
"github.com/antdaza/antdchain/antdc/reward"
"github.com/antdaza/antdchain/antdc/rotatingking"

)

// processRotatingKingForBlock handles rotating king updates for a validated block
func (bc *Blockchain) processRotatingKingForBlock(b *block.Block, distribution *reward.RewardDistribution) error {
if bc.rotatingKingManager == nil {
return nil // Rotating king system not enabled
}

blockHeight := b.Header.Number.Uint64()
currentKing := bc.rotatingKingManager.GetCurrentKing()

// Keep local rotating king state aligned with the king used by the accepted block.
// This is especially important for synced nodes, where reward distribution can be
// derived from block extra data (rk=...) and may differ from stale local state.
if distribution.RotatingKingAddress != (common.QuantumAddress{}) &&
distribution.RotatingKingAddress != currentKing {
if err := bc.rotatingKingManager.ForceRotateToAddress(
distribution.RotatingKingAddress,
fmt.Sprintf("sync-to-block-%d", blockHeight),
); err != nil {
return fmt.Errorf("failed to align rotating king state with block %d: %w", blockHeight, err)
}
}

// Record reward if rotating king was eligible
if distribution.RotatingKingEligible && distribution.RotatingKingReward.Sign() > 0 {
bc.rotatingKingManager.RecordRewardDistribution(
distribution.RotatingKingAddress,
distribution.RotatingKingReward,
blockHeight,
)
}

// Perform rotation if due at this block height
if bc.rotatingKingManager.ShouldRotate(blockHeight) {
if err := bc.rotatingKingManager.RotateToNextKing(blockHeight, b.Hash()); err != nil {
return fmt.Errorf("rotation failed: %w", err)
}
}

if bc.pow != nil {
bc.pow.SetPriorityMiner(bc.rotatingKingManager.GetCurrentKing(), pow.RotatingKingHashrateBoostPercent)
}

// Update sync state
if syncable, ok := bc.rotatingKingManager.(interface{ UpdateLastSyncedBlock(uint64) error }); ok {
if err := syncable.UpdateLastSyncedBlock(blockHeight); err != nil {
log.Printf("[blockchain] Failed to update rotating king sync state: %v", err)
}
}

return nil
}

/*
// updateRotatingKingForBlock updates rotating king for a block
func (bc *Blockchain) updateRotatingKingForBlock(b *block.Block, blockHeight uint64) {
    if bc.rotatingKingManager == nil {
        return
    }

    // Check if we should rotate based on this block
    shouldRotate := false
    if rkm, ok := bc.rotatingKingManager.(interface{ ShouldRotate(uint64) bool }); ok {
        shouldRotate = rkm.ShouldRotate(blockHeight)
    }

    // Check for king rotation eligibility
    currentKing := bc.rotatingKingManager.GetCurrentKing()
    if currentKing != (common.QuantumAddress{}) {
        if shouldRotate {
            // Perform rotation
            if err := bc.rotatingKingManager.RotateToNextKing(blockHeight, b.Hash()); err != nil {
                log.Printf("[blockchain] Rotating king rotation failed at block %d: %v", blockHeight, err)
            } else {
                log.Printf("[blockchain] Rotating king rotated at block %d", blockHeight)
            }
        }
    }

    // Update sync state in rotating king database
    if syncable, ok := bc.rotatingKingManager.(interface{ GetSyncState() (*rotatingking.SyncState, error) }); ok {
        syncState, err := syncable.GetSyncState()
        if err == nil && syncState != nil {
            // Update last synced block
            syncState.LastSyncedBlock = blockHeight
            syncState.LastSyncTime = time.Now()
            syncState.IsSyncing = bc.syncing.Load()

            if savable, ok := bc.rotatingKingManager.(interface{ SaveSyncState(*rotatingking.SyncState) error }); ok {
                if err := savable.SaveSyncState(syncState); err != nil {
                    log.Printf("[blockchain] Failed to save rotating king sync state: %v", err)
                }
            }
        }
    }
}

// syncRotatingKingDatabase synchronizes the rotating king database with blockchain
func (bc *Blockchain) syncRotatingKingDatabase(targetHeight uint64) {
    if bc.rotatingKingManager == nil {
        return
    }

    log.Printf("[blockchain] Starting rotating king database sync to height %d", targetHeight)

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
    defer cancel()

    // Get current rotating king state
    syncState, err := bc.rotatingKingManager.GetSyncState()
    if err != nil {
        log.Printf("[blockchain] Failed to get rotating king sync state: %v", err)
        return
    }

    startHeight := syncState.LastSyncedBlock
    if startHeight >= targetHeight {
        log.Printf("[blockchain] Rotating king database already synced to height %d", startHeight)
        return
    }

    // Sync in batches
    batchSize := uint64(1000)
    for height := startHeight + 1; height <= targetHeight; height += batchSize {
        endHeight := height + batchSize - 1
        if endHeight > targetHeight {
            endHeight = targetHeight
        }

        select {
        case <-ctx.Done():
            log.Printf("[blockchain] Rotating king database sync cancelled")
            return
        default:
            // Sync this batch
            if err := bc.rotatingKingManager.SyncBlocks(ctx, endHeight); err != nil {
                log.Printf("[blockchain] Rotating king sync failed at height %d: %v", endHeight, err)
                // Continue with next batch
                continue
            }

            log.Printf("[blockchain] Rotating king database synced to height %d", endHeight)
        }
    }

    log.Printf("[blockchain] Rotating king database sync completed to height %d", targetHeight)
}

// syncRotatingKingForBlock syncs rotating king database for a specific block
func (bc *Blockchain) syncRotatingKingForBlock(blockHeight uint64) {
    if bc.rotatingKingManager == nil {
        return
    }

    // Skip if we're already at or past this height
    syncState, err := bc.rotatingKingManager.GetSyncState()
    if err == nil && syncState != nil && syncState.LastSyncedBlock >= blockHeight {
        return
    }

    // Sync with 10-second timeout
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    if err := bc.rotatingKingManager.SyncBlocks(ctx, blockHeight); err != nil {
        log.Printf("[blockchain] Rotating king sync failed for block %d: %v", blockHeight, err)
        return
    }

    log.Printf("[blockchain] Rotating king database synced to block %d", blockHeight)
}*/

// startPeriodicRotatingKingSync starts periodic synchronization
func (bc *Blockchain) startPeriodicRotatingKingSync(interval time.Duration) {
ticker := time.NewTicker(interval)
defer ticker.Stop()

for {
select {
case <-ticker.C:
if bc.rotatingKingManager == nil {
continue
}

currentHeight := bc.GetChainHeight()
if currentHeight == 0 {
continue
}

// Check if sync is needed
syncState, err := bc.rotatingKingManager.GetSyncState()
if err != nil {
log.Printf("[blockchain] Failed to get sync state: %v", err)
continue
}

// Skip if already synced
if syncState != nil && syncState.LastSyncedBlock >= currentHeight {
continue
}

// Perform sync
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
if err := bc.rotatingKingManager.SyncBlocks(ctx, currentHeight); err != nil {
log.Printf("[blockchain] Periodic sync failed: %v", err)
} else {
log.Printf("[blockchain] Periodic sync completed to height %d", currentHeight)
}
cancel()
}
}
}

// initRotatingKingSync initializes rotating king sync on startup
func (bc *Blockchain) initRotatingKingSync() {
if bc.rotatingKingManager == nil {
return
}

// Sync rotating king database on startup
go func() {
time.Sleep(5 * time.Second) // Wait for P2P to initialize

log.Printf("[blockchain] Initializing rotating king database sync...")

ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

// Try to sync with peers
if syncable, ok := bc.rotatingKingManager.(interface {
SyncDatabaseWithPeers(ctx context.Context) error
}); ok {
if err := syncable.SyncDatabaseWithPeers(ctx); err != nil {
log.Printf("[blockchain] Rotating king peer sync failed: %v", err)
}
}

// Then sync with local blockchain
currentHeight := bc.GetChainHeight()
if syncable, ok := bc.rotatingKingManager.(interface {
SyncBlocks(ctx context.Context, blockHeight uint64) error
}); ok {
if err := syncable.SyncBlocks(ctx, currentHeight); err != nil {
log.Printf("[blockchain] Rotating king blockchain sync failed: %v", err)
}
}
}()
}

// SyncRotatingKings manually triggers rotating king sync
func (bc *Blockchain) SyncRotatingKings() error {
if bc.rotatingKingManager == nil {
return errors.New("rotating king manager not initialized")
}

log.Printf("[blockchain] Manually triggering rotating king sync...")

// Sync with blockchain first
currentHeight := bc.GetChainHeight()
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

if err := bc.rotatingKingManager.SyncBlocks(ctx, currentHeight); err != nil {
return fmt.Errorf("blockchain sync failed: %w", err)
}

// Then sync with peers
if syncable, ok := bc.rotatingKingManager.(interface {
SyncDatabaseWithPeers(ctx context.Context) error
}); ok {
if err := syncable.SyncDatabaseWithPeers(ctx); err != nil {
log.Printf("[blockchain] Peer sync failed (continuing): %v", err)
}
}

log.Printf("[blockchain] Rotating king sync completed")
return nil
}

// GetRotatingKingDatabaseMetrics returns rotating king database metrics
func (bc *Blockchain) GetRotatingKingDatabaseMetrics() (*rotatingking.DBMetrics, error) {
if bc.rotatingKingManager == nil {
return nil, errors.New("rotating king manager not initialized")
}

// Check if manager has GetDBMetrics method
v := reflect.ValueOf(bc.rotatingKingManager)
method := v.MethodByName("GetDBMetrics")
if !method.IsValid() {
return nil, errors.New("rotating king manager doesn't support metrics")
}

results := method.Call(nil)
if len(results) > 0 {
if metrics, ok := results[0].Interface().(*rotatingking.DBMetrics); ok {
return metrics, nil
}
}

return nil, errors.New("failed to get database metrics")
}

// BackupRotatingKingDatabase creates a backup of the rotating king database
func (bc *Blockchain) BackupRotatingKingDatabase(backupPath string) error {
if bc.rotatingKingManager == nil {
return errors.New("rotating king manager not initialized")
}

// Check if manager has BackupDatabase method
v := reflect.ValueOf(bc.rotatingKingManager)
method := v.MethodByName("BackupDatabase")
if !method.IsValid() {
return errors.New("rotating king manager doesn't support backup")
}

results := method.Call([]reflect.Value{reflect.ValueOf(backupPath)})
if len(results) > 0 {
if err, ok := results[0].Interface().(error); ok && err != nil {
return err
}
}

return nil
}

// GetRotatingKingSyncState returns rotating king synchronization status
func (bc *Blockchain) GetRotatingKingSyncState() (*rotatingking.SyncState, error) {
if bc.rotatingKingManager == nil {
return nil, errors.New("rotating king manager not initialized")
}

return bc.rotatingKingManager.GetSyncState()
}

// GetDatabaseSyncHeight returns the current database sync height
func (bc *Blockchain) GetDatabaseSyncHeight() uint64 {
// Try to get from rotating king manager first
if rkManager := bc.GetRotatingKingManager(); rkManager != nil {
// Check if the manager has GetSyncState method
if manager, ok := rkManager.(interface {
GetSyncState() (*rotatingking.SyncState, error)
}); ok {
syncState, err := manager.GetSyncState()
if err == nil && syncState != nil {
return syncState.LastSyncedBlock
}
}
}

// Fallback: return current chain height
if latest := bc.Latest(); latest != nil {
return latest.Header.Number.Uint64()
}
return 0
}

// ShouldSyncDatabase returns whether database should be synced
func (bc *Blockchain) ShouldSyncDatabase() bool {
// Default: always sync if we have a rotating king manager
return bc.GetRotatingKingManager() != nil
}

// MarkDatabaseSynced marks database as synced to a specific height
func (bc *Blockchain) MarkDatabaseSynced(height uint64) {
log.Printf("[blockchain] Database marked as synced to height %d", height)
}

// initDefaultRotatingKing initializes default rotating king
func (bc *Blockchain) initDefaultRotatingKing(defaultAddress common.QuantumAddress) {
if bc.rotatingKingManager == nil {
return
}

// Use reflection to find and call the setup method
v := reflect.ValueOf(bc.rotatingKingManager)

// Try different method names
methodNames := []string{"InitializeWithAddress", "SetDefaultKing", "AddDefaultAddress", "Init"}

for _, methodName := range methodNames {
method := v.MethodByName(methodName)
if method.IsValid() {
// Call with the default address
params := []reflect.Value{reflect.ValueOf(defaultAddress)}
method.Call(params)
log.Printf("✅ Rotating King system initialized with address: %s", defaultAddress.String())
return
}
}

log.Printf("⚠️  Could not initialize rotating king with address %s", defaultAddress.String())
}
