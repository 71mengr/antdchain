// rotatingking/dbinterface.go
package rotatingking
import (
//    "context"
    "encoding/json"
    "time"
    "math/big"
    "github.com/antdaza/antdchain/common"
)

// Define all data structures needed for the interface
type RotatingKingState struct {
    CurrentKingIndex         int                         `json:"currentKingIndex"`
    RotationHeight           uint64                      `json:"rotationHeight"`
    NextRotationAt           uint64                      `json:"nextRotationAt"`
    LastUpdated              time.Time                   `json:"lastUpdated"`
    RotationCount            uint64                      `json:"rotationCount"`
    KingsHistory             []KingRotation              `json:"kingsHistory"`
    TotalRewardsDistributed  *big.Int                    `json:"totalRewardsDistributed"`
    KingRewards              map[common.QuantumAddress]*big.Int `json:"kingRewards"`
}

func (s RotatingKingState) MarshalJSON() ([]byte, error) {
    type alias RotatingKingState
    kingRewards := make(map[string]*big.Int, len(s.KingRewards))
    for addr, amount := range s.KingRewards {
        kingRewards[addr.String()] = amount
    }
    return json.Marshal(struct {
        alias
        KingRewards map[string]*big.Int `json:"kingRewards"`
    }{
        alias:       alias(s),
        KingRewards: kingRewards,
    })
}

func (s *RotatingKingState) UnmarshalJSON(data []byte) error {
    type alias RotatingKingState
    aux := struct {
        alias
        KingRewards map[string]*big.Int `json:"kingRewards"`
    }{}
    if err := json.Unmarshal(data, &aux); err != nil {
        return err
    }
    *s = RotatingKingState(aux.alias)
    s.KingRewards = make(map[common.QuantumAddress]*big.Int, len(aux.KingRewards))
    for addrStr, amount := range aux.KingRewards {
        addr, err := common.ParseQuantumAddress(addrStr)
        if err != nil {
            return err
        }
        s.KingRewards[addr] = amount
    }
    return nil
}

type RotatingKingConfig struct {
    RotationInterval uint64           `json:"rotationInterval"`
    RotationOffset   uint64           `json:"rotationOffset"`
    KingAddresses    []common.QuantumAddress `json:"kingAddresses"`
    ActivationDelay  uint64           `json:"activationDelay"`
    MinStakeRequired *big.Int         `json:"minStakeRequired"`
}

type KingRotation struct {
    BlockHeight  uint64         `json:"blockHeight"`
    PreviousKing common.QuantumAddress `json:"previousKing"`
    NewKing      common.QuantumAddress `json:"newKing"`
    Timestamp    time.Time      `json:"timestamp"`
    Reward       *big.Int       `json:"reward"`
    WasEligible  bool           `json:"wasEligible"`
    Reason       string         `json:"reason,omitempty"`
}

type BlockSyncRecord struct {
    BlockHeight    uint64      `json:"blockHeight"`
    BlockHash      common.Hash `json:"blockHash"`
    Timestamp      time.Time   `json:"timestamp"`
    RotationEvents []string    `json:"rotationEvents"`
    SyncDuration   int64       `json:"syncDuration"`
}

type SyncState struct {
    LastSyncedBlock uint64    `json:"lastSyncedBlock"`
    LastSyncTime    time.Time `json:"lastSyncTime"`
    SyncProgress    float64   `json:"syncProgress"`
    TotalBlocks     uint64    `json:"totalBlocks"`
    IsSyncing       bool      `json:"isSyncing"`
    SyncError       string    `json:"syncError,omitempty"`
    PeerCount       int       `json:"peerCount"`
}

type DBMetrics struct {
    TotalRecords   int64     `json:"totalRecords"`
    DatabaseSize   int64     `json:"databaseSize"`
    DBSize         int64     `json:"dbSize"`
    LastBackupTime time.Time `json:"lastBackupTime"`
    SyncLatency    float64   `json:"syncLatency"`
    WriteCount      int64     `json:"writeCount"`      
    ReadCount       int64     `json:"readCount"`       
    BatchWriteSize  int64     `json:"batchWriteSize"`  
    LastWriteTime   time.Time `json:"lastWriteTime"`   
    WriteLatency    float64   `json:"writeLatency"`    
    ReadLatency     float64   `json:"readLatency"`     
    ActiveConnections int     `json:"activeConnections"` 
}

// RotatingKingDatabase defines the database interface for rotating king
type RotatingKingDatabase interface {
    WriteRotatingKingState(state *RotatingKingState) error
    ReadRotatingKingState() (*RotatingKingState, error)
    WriteRotatingKingConfig(config *RotatingKingConfig) error
    ReadRotatingKingConfig() (*RotatingKingConfig, error)
    WriteRotationEvent(event *KingRotation) error
    GetRotationEvents(fromBlock, toBlock uint64) ([]KingRotation, error)
    WriteBlockSyncRecord(record *BlockSyncRecord) error
    GetLastBlockSyncRecord() (*BlockSyncRecord, error)
    WriteSyncState(state *SyncState) error
    ReadSyncState() (*SyncState, error)
    GetMetrics() *DBMetrics
    Close() error
}
