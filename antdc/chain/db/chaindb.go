// chain/db/chaindb.go
package db

import (
    "encoding/json"
    "errors"
    "fmt"
    "os"
    "path/filepath"
    
    "github.com/cockroachdb/pebble"
    "github.com/ethereum/go-ethereum/common"
    "github.com/ethereum/go-ethereum/rlp"
    "github.com/antdaza/antdchain/antdc/block"
    "github.com/antdaza/antdchain/antdc/rotatingking"
)

var (
    ErrNotFound = pebble.ErrNotFound
)

type ChainDB struct {
    db      *pebble.DB
    dataDir string
}

func NewChainDB(dataDir string) (*ChainDB, error) {
    if err := os.MkdirAll(dataDir, os.ModePerm); err != nil {
        return nil, err
    }

    opts := &pebble.Options{
        BytesPerSync:               512 * 1024, // 512 KiB
        MemTableSize:               64 << 20,   // 64 MiB
        MemTableStopWritesThreshold: 4,
        L0CompactionThreshold:      4,
        L0StopWritesThreshold:      12,
        LBaseMaxBytes:              256 << 20, // 256 MiB
        // Remove or comment out MaxConcurrentCompactions
        // MaxConcurrentCompactions:   pebble.DefaultMaxConcurrentCompactions,
    }

    db, err := pebble.Open(filepath.Join(dataDir, "chain"), opts)
    if err != nil {
        return nil, fmt.Errorf("failed to open pebble: %w", err)
    }

    return &ChainDB{db: db, dataDir: dataDir}, nil
}

func (d *ChainDB) Close() error {
    return d.db.Close()
}

// DB returns the underlying pebble.DB instance
func (d *ChainDB) DB() *pebble.DB {
    return d.db
}

// GetHeadBlockHash gets the current head block hash
func (d *ChainDB) GetHeadBlockHash() (common.Hash, error) {
    data, closer, err := d.db.Get([]byte("head"))
    if err != nil {
        if errors.Is(err, pebble.ErrNotFound) {
            return common.Hash{}, nil
        }
        return common.Hash{}, err
    }
    defer closer.Close()
    
    return common.BytesToHash(data), nil
}

// WriteHeadBlockHash writes the head block hash
func (d *ChainDB) WriteHeadBlockHash(hash common.Hash) error {
    return d.db.Set([]byte("head"), hash[:], pebble.Sync)
}

// GetCanonicalHash gets the canonical hash for a block number
func (d *ChainDB) GetCanonicalHash(number uint64) (common.Hash, error) {
    data, closer, err := d.db.Get(CanonicalHashKey(number)) // Remove "db." prefix
    if err != nil {
        if errors.Is(err, pebble.ErrNotFound) {
            return common.Hash{}, nil
        }
        return common.Hash{}, err
    }
    defer closer.Close()
    
    return common.BytesToHash(data), nil
}

// WriteCanonicalHash writes the canonical hash for a block number
func (d *ChainDB) WriteCanonicalHash(number uint64, hash common.Hash) error {
    return d.db.Set(CanonicalHashKey(number), hash[:], pebble.Sync) // Remove "db." prefix
}

// ──────────────────────────────────────────────
// Write / Read Headers
// ──────────────────────────────────────────────

func (d *ChainDB) WriteHeader(header *block.Header) error {
    num := header.Number.Uint64()
    hash := header.Hash()

    batch := d.db.NewBatch()
    defer batch.Close()

    // Canonical header by number
    data, err := rlp.EncodeToBytes(header)
    if err != nil {
        return err
    }
    batch.Set(HeaderByNumberKey(num), data, pebble.Sync) // Use HeaderByNumberKey

    // Optional: fast lookup by hash
    batch.Set(HeaderByHashKey(hash), data, pebble.Sync) // Use HeaderByHashKey

    // Update canonical pointer
    batch.Set(CanonicalHashKey(num), hash[:], pebble.Sync) // Use CanonicalHashKey

    return batch.Commit(pebble.Sync)
}

func (d *ChainDB) ReadHeaderByNumber(n uint64) (*block.Header, error) {
    data, closer, err := d.db.Get(HeaderByNumberKey(n)) // Use HeaderByNumberKey
    if err != nil {
        if errors.Is(err, ErrNotFound) {
            return nil, nil
        }
        return nil, err
    }
    defer closer.Close()

    var h block.Header
    if err := rlp.DecodeBytes(data, &h); err != nil {
        return nil, err
    }
    return &h, nil
}

func (d *ChainDB) ReadHeaderByHash(hash common.Hash) (*block.Header, error) {
    data, closer, err := d.db.Get(HeaderByHashKey(hash)) // Use HeaderByHashKey
    if err != nil {
        if errors.Is(err, ErrNotFound) {
            return nil, nil
        }
        return nil, err
    }
    defer closer.Close()

    var h block.Header
    if err := rlp.DecodeBytes(data, &h); err != nil {
        return nil, err
    }
    return &h, nil
}

// ──────────────────────────────────────────────
// Write / Read full blocks
// ──────────────────────────────────────────────

func (d *ChainDB) WriteBlock(b *block.Block) error {
    hash := b.Hash()
    num := b.Header.Number.Uint64()

    batch := d.db.NewBatch()
    defer batch.Close()

    // Full block - use RLP encoding
    fullData, err := rlp.EncodeToBytes(b)
    if err != nil {
        return err
    }
    batch.Set(BlockByHashKey(hash), fullData, pebble.Sync) // Use BlockByHashKey

    // Also store header separately for fast header sync later
    headerData, err := rlp.EncodeToBytes(b.Header)
    if err != nil {
        return err
    }
    batch.Set(HeaderByNumberKey(num), headerData, pebble.Sync) // Use HeaderByNumberKey
    batch.Set(HeaderByHashKey(hash), headerData, pebble.Sync) // Use HeaderByHashKey

    // IMPORTANT: Do not update canonical mapping here.
    // A block can be persisted as a side-branch/fork candidate.
    // Canonical selection must be performed explicitly by chain logic.

    return batch.Commit(pebble.Sync)
}

func (d *ChainDB) ReadBlockByHash(hash common.Hash) (*block.Block, error) {
    data, closer, err := d.db.Get(BlockByHashKey(hash)) // Use BlockByHashKey
    if err != nil {
        if errors.Is(err, ErrNotFound) {
            return nil, nil
        }
        return nil, err
    }
    defer closer.Close()

    var b block.Block
    if err := rlp.DecodeBytes(data, &b); err != nil {
        return nil, err
    }
    return &b, nil
}

func (d *ChainDB) HasBlock(hash common.Hash) (bool, error) {
    _, closer, err := d.db.Get(BlockByHashKey(hash)) // Use BlockByHashKey
    if err != nil {
        if errors.Is(err, ErrNotFound) {
            return false, nil
        }
        return false, err
    }
    closer.Close()
    return true, nil
}

// Key functions - remove these from here, they're in keys.go
// Keep only the rotating king methods in this file

// WriteRotatingKingState writes rotating king state
func (d *ChainDB) WriteRotatingKingState(state *rotatingking.RotatingKingState) error {
    data, err := json.Marshal(state)
    if err != nil {
        return fmt.Errorf("failed to marshal rotating king state: %w", err)
    }
    return d.db.Set(rotatingKingStateKey(), data, pebble.Sync)
}

// ReadRotatingKingState reads rotating king state
func (d *ChainDB) ReadRotatingKingState() (*rotatingking.RotatingKingState, error) {
    data, closer, err := d.db.Get(rotatingKingStateKey())
    if err != nil {
        if errors.Is(err, ErrNotFound) {
            return nil, nil
        }
        return nil, err
    }
    defer closer.Close()

    var state rotatingking.RotatingKingState
    if err := json.Unmarshal(data, &state); err != nil {
        return nil, fmt.Errorf("failed to unmarshal rotating king state: %w", err)
    }
    return &state, nil
}

// WriteRotatingKingConfig writes rotating king configuration
func (d *ChainDB) WriteRotatingKingConfig(config *rotatingking.RotatingKingConfig) error {
    data, err := json.Marshal(config)
    if err != nil {
        return fmt.Errorf("failed to marshal rotating king config: %w", err)
    }
    return d.db.Set(rotatingKingConfigKey(), data, pebble.Sync)
}

// ReadRotatingKingConfig reads rotating king configuration
func (d *ChainDB) ReadRotatingKingConfig() (*rotatingking.RotatingKingConfig, error) {
    data, closer, err := d.db.Get(rotatingKingConfigKey())
    if err != nil {
        if errors.Is(err, ErrNotFound) {
            return nil, nil
        }
        return nil, err
    }
    defer closer.Close()

    var config rotatingking.RotatingKingConfig
    if err := json.Unmarshal(data, &config); err != nil {
        return nil, fmt.Errorf("failed to unmarshal rotating king config: %w", err)
    }
    return &config, nil
}

// GetRotatingKingDatabase returns the rotating king database interface
func (d *ChainDB) GetRotatingKingDatabase() rotatingking.RotatingKingDatabase {
    return NewPebbleRotatingKingDB(d.db)
}

// Key functions for rotating king
func rotatingKingStateKey() []byte {
    return []byte("rk_state")
}

func rotatingKingConfigKey() []byte {
    return []byte("rk_config")
}

func syncStateKey() []byte {
    return []byte("rk_sync_state")
}
