// cache.go — Hot block cache with proper invalidation & metrics

package chain
import (
    "sync"

    "github.com/hashicorp/golang-lru/v2/simplelru"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
    "github.com/antdaza/antdchain/antdc/block"
    "github.com/antdaza/antdchain/common"
)

var (
    hotBlockCacheHits = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "antdchain_hot_block_cache_hits_total",
        Help: "Cache hits in hot block LRU",
    }, []string{"type"}) // "number" or "hash"

    hotBlockCacheMisses = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "antdchain_hot_block_cache_misses_total",
        Help: "Cache misses in hot block LRU",
    }, []string{"type"})
)

// HotBlockCache — small LRU for recently accessed / produced blocks
type HotBlockCache struct {
    byNumber *simplelru.LRU[uint64, *block.Block]
    byHash   *simplelru.LRU[common.Hash, *block.Block]
    mu       sync.RWMutex
}

func NewHotBlockCache(size int) (*HotBlockCache, error) {
    byNum, err := simplelru.NewLRU[uint64, *block.Block](size, nil)
    if err != nil {
        return nil, err
    }

    byHash, err := simplelru.NewLRU[common.Hash, *block.Block](size, nil)
    if err != nil {
        return nil, err
    }

    return &HotBlockCache{
        byNumber: byNum,
        byHash:   byHash,
    }, nil
}

// Add adds a block to both caches
func (c *HotBlockCache) Add(blk *block.Block) {
    if blk == nil || blk.Header == nil {
        return
    }

    c.mu.Lock()
    defer c.mu.Unlock()

    num := blk.Header.Number.Uint64()
    hash := blk.Hash()

    c.byNumber.Add(num, blk)
    c.byHash.Add(hash, blk)
}

// GetByNumber tries to fetch by height
func (c *HotBlockCache) GetByNumber(num uint64) (*block.Block, bool) {
    c.mu.RLock()
    blk, ok := c.byNumber.Get(num)
    c.mu.RUnlock()

    if ok {
        hotBlockCacheHits.WithLabelValues("number").Inc()
    } else {
        hotBlockCacheMisses.WithLabelValues("number").Inc()
    }
    return blk, ok
}

// GetByHash tries to fetch by hash
func (c *HotBlockCache) GetByHash(hash common.Hash) (*block.Block, bool) {
    c.mu.RLock()
    blk, ok := c.byHash.Get(hash)
    c.mu.RUnlock()

    if ok {
        hotBlockCacheHits.WithLabelValues("hash").Inc()
    } else {
        hotBlockCacheMisses.WithLabelValues("hash").Inc()
    }
    return blk, ok
}

// Remove removes a block (used on reorg/truncate)
func (c *HotBlockCache) Remove(hash common.Hash, num uint64) {
    c.mu.Lock()
    defer c.mu.Unlock()

    c.byHash.Remove(hash)
    c.byNumber.Remove(num)
}

// Purge clears the entire cache (on deep reorg or truncate)
func (c *HotBlockCache) Purge() {
    c.mu.Lock()
    defer c.mu.Unlock()

    c.byNumber.Purge()
    c.byHash.Purge()
}
