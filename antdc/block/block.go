// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package block

import (
        "crypto/sha256"
        "encoding/binary"
        "encoding/hex"
        "encoding/json"
        "errors"
        "fmt"
        "math"
        "math/big"
        "strings"
        "time"

        "github.com/ethereum/go-ethereum/common"
        "github.com/antdaza/antdchain/antdc/tx"
        "github.com/antdaza/antdchain/antdc/pow"
)

// Constants for block validation
const (
        MaxBlockSize         = 8 * 1024 * 1024 // 8MB maximum block size
        MaxTransactions      = 10000           // Maximum transactions per block
        MaxUncles            = 2               // Maximum uncle blocks
        BlockTimeTarget      = 15              // 15 second target block time
        FutureBlockThreshold = 30              // Reject blocks more than 30 seconds in future
        DifficultyAdjustment = 1024            // Difficulty adjustment divisor
)

var (
        // Common errors
        ErrInvalidParentHash   = errors.New("invalid parent hash")
        ErrGasLimitExceeded    = errors.New("gas limit exceeded")
        ErrInvalidTimestamp    = errors.New("invalid timestamp")
        ErrInvalidDifficulty   = errors.New("invalid difficulty")
        ErrInvalidNumber       = errors.New("invalid block number")
        ErrTxRootMismatch      = errors.New("transaction root mismatch")
        ErrBlockTooLarge       = errors.New("block size exceeds limit")
        ErrTooManyTransactions = errors.New("too many transactions")
        ErrTooManyUncles       = errors.New("too many uncles")
        ErrDuplicateUncle      = errors.New("duplicate uncle")
        ErrUncleNumberTooHigh  = errors.New("uncle number too high")
)

// Header represents a block header with Ethereum-style fields
type Header struct {
        ParentHash  common.Hash    `json:"parentHash"`
        UncleHash   common.Hash    `json:"sha3Uncles"`
        Coinbase    common.Address `json:"miner"`
        Root        common.Hash    `json:"stateRoot"`
        TxHash      common.Hash    `json:"transactionsRoot"`
        ReceiptHash common.Hash    `json:"receiptsRoot"`
        Bloom       []byte         `json:"logsBloom"`
        Difficulty  *big.Int       `json:"difficulty"`
        Number      *big.Int       `json:"number"`
        GasLimit    uint64         `json:"gasLimit"`
        GasUsed     uint64         `json:"gasUsed"`
        Time        uint64         `json:"timestamp"`
        Extra       []byte         `json:"extraData"`
        MixDigest   common.Hash    `json:"mixHash"`
        Nonce       BlockNonce     `json:"nonce"`

        TxRoot common.Hash `json:"-"`
}

type BlockNonce [8]byte

// Creates a new header with safe defaults and validation
// NewHeader creates a new block header with safe defaults and validation
// It calculates the difficulty using the PoS engine if provided.
// For genesis block (number == 0), special handling is applied.
func NewHeader(
    parent *Block,                    // parent block (nil only for genesis)
    coinbase common.Address,
    root common.Hash,                 // state root after execution
    txHash common.Hash,               // transactions Merkle root
    number *big.Int,
    gasLimit uint64,
    powEngine *pow.PoW,               // PoS engine for difficulty calculation
) (*Header, error) {

    if number == nil {
        return nil, ErrInvalidNumber
    }

    if number.Sign() < 0 {
        return nil, ErrInvalidNumber
    }

    if gasLimit == 0 {
        return nil, errors.New("gas limit cannot be zero")
    }

    currentTime := uint64(time.Now().Unix())

    // Determine difficulty
    var difficulty *big.Int
    if number.Sign() == 0 {
        // Genesis block — fixed difficulty
        difficulty = big.NewInt(1)
    } else if powEngine != nil && parent != nil {
        // Normal block — use dynamic PoS difficulty
        difficulty = powEngine.CalculateExpectedDifficulty(
            number.Uint64(),
            parent.Header.Time,
            currentTime,
        )
    } else {
        // Fallback (testing or no engine) — default to 1
        difficulty = big.NewInt(1)
    }

    // Parent hash
    var parentHash common.Hash
    if parent != nil {
        parentHash = parent.Hash()
    } else if number.Sign() != 0 {
        return nil, errors.New("parent block required for non-genesis block")
    }

    header := &Header{
        ParentHash: parentHash,
        UncleHash:  CalculateUncleHash(nil), // empty for now
        Coinbase:   coinbase,
        Root:       root,
        TxHash:     txHash,
        TxRoot:     txHash, // compatibility field
        Bloom:      make([]byte, 256), // empty bloom filter
        Difficulty: difficulty,
        Number:     new(big.Int).Set(number),
        GasLimit:   gasLimit,
        GasUsed:    0,
        Time:       currentTime,
        Extra:      []byte("ANTDChain"),
        MixDigest:  common.Hash{},
        Nonce:      BlockNonce{},
    }

    // Genesis block special handling
    if number.Sign() == 0 {
        if parentHash != (common.Hash{}) {
            return nil, ErrInvalidParentHash
        }
        // Optional: override genesis timestamp if you have a fixed value
        // header.Time = 1710000000 // example fixed genesis time
    }

    return header, nil
}

// Validate performs basic header validation
func (h *Header) Validate(parent *Header) error {
        if h == nil {
                return errors.New("header is nil")
        }

        // Validate number
        if h.Number == nil {
                return ErrInvalidNumber
        }
        if h.Number.Sign() < 0 {
                return ErrInvalidNumber
        }

        // Validate parent relationship
        if parent != nil {
                expectedNumber := new(big.Int).Add(parent.Number, big.NewInt(1))
                if h.Number.Cmp(expectedNumber) != 0 {
                        return fmt.Errorf("block number mismatch: expected %s, got %s",
                                expectedNumber, h.Number)
                }
                if h.ParentHash != parent.Hash() {
                        return ErrInvalidParentHash
                }
        } else if h.Number.Sign() != 0 {
                // Non-genesis block must have a parent
                return ErrInvalidParentHash
        }

        // Validate gas
        if h.GasUsed > h.GasLimit {
                return ErrGasLimitExceeded
        }
        if h.GasLimit == 0 {
                return errors.New("gas limit cannot be zero")
        }

        // Validate timestamp
        if err := h.validateTimestamp(parent); err != nil {
                return err
        }

        // Validate difficulty
        if h.Difficulty == nil || h.Difficulty.Sign() <= 0 {
                return ErrInvalidDifficulty
        }

        // Validate extra data size (prevent spam)
        if len(h.Extra) > 1024 {
                return errors.New("extra data too large")
        }

        return nil
}

// Check if the block timestamp is reasonable
func (h *Header) validateTimestamp(parent *Header) error {
        blockTime := h.TimestampTime()

        // Check if timestamp is too far in future
        if blockTime.After(time.Now().Add(FutureBlockThreshold * time.Second)) {
                return ErrInvalidTimestamp
        }

        // For non-genesis blocks, check against parent timestamp
        if parent != nil {
                parentTime := parent.TimestampTime()
                if blockTime.Before(parentTime) {
                        return errors.New("block timestamp before parent")
                }
        }

        return nil
}

func (h *Header) Hash() common.Hash {
    if h == nil {
        return common.Hash{}
    }

    // Use SHA-256 (Bitcoin style) instead of Keccak-256 (Ethereum style)
    hasher := sha256.New()

    // Write all header fields in consistent order
    hasher.Write(h.ParentHash[:])
    hasher.Write(h.UncleHash[:])
    hasher.Write(h.Coinbase[:])
    hasher.Write(h.Root[:])
    hasher.Write(h.TxHash[:])
    hasher.Write(h.ReceiptHash[:])

    // Bloom filter must be serialized as fixed 256 bytes.
    // IMPORTANT: nil bloom and empty bloom must hash identically.
    bloomBytes := make([]byte, 256)
    if len(h.Bloom) > 0 {
        copyLen := len(h.Bloom)
        if copyLen > len(bloomBytes) {
            copyLen = len(bloomBytes)
        }
        copy(bloomBytes, h.Bloom[:copyLen])
    }
    hasher.Write(bloomBytes)

    // Number as fixed 32 bytes
    hasher.Write(safeBigIntToBytes(h.Number, 32))

    // Gas fields
    buf := make([]byte, 8)
    binary.BigEndian.PutUint64(buf, h.GasLimit)
    hasher.Write(buf)
    binary.BigEndian.PutUint64(buf, h.GasUsed)
    hasher.Write(buf)

    // Timestamp
    binary.BigEndian.PutUint64(buf, h.Time)
    hasher.Write(buf)

    // Difficulty as fixed 32 bytes
    hasher.Write(safeBigIntToBytes(h.Difficulty, 32))

    // Extra data with length prefix
    extra := h.Extra
    if extra == nil {
        extra = []byte{}
    }
    binary.BigEndian.PutUint64(buf, uint64(len(extra)))
    hasher.Write(buf)
    hasher.Write(extra)

    // PoW fields
    hasher.Write(h.MixDigest[:])
    hasher.Write(h.Nonce[:])

    return common.BytesToHash(hasher.Sum(nil))
}

// Block represents a full block with transactions and uncles
type Block struct {
        Header *Header   `json:"header"`
        Txs    []*tx.Tx  `json:"transactions"`
        Uncles []*Header `json:"uncles"`
}

// Creates a new block
func NewBlock(header *Header, txs []*tx.Tx, uncles []*Header) (*Block, error) {
    if header == nil {
        return nil, errors.New("header cannot be nil")
    }

    if txs == nil {
        txs = []*tx.Tx{}
    }
    if uncles == nil {
        uncles = []*Header{}
    }

    b := &Block{
        Header: header,
        Txs:    txs,
        Uncles: uncles,
    }

    // CRITICAL: Only update fields that are actually empty
    // TxHash was pre-calculated in CreateMiningBlock → DO NOT TOUCH IT
    if header.UncleHash == (common.Hash{}) {
        header.UncleHash = CalculateUncleHash(uncles)
    }

    // GasUsed must always be calculated from transactions
    header.GasUsed = b.CalculateGasUsed()

    // Sync legacy field
    header.TxRoot = header.TxHash

    return b, nil
}

// Only updates empty fields
func (b *Block) updateHeaderPreserveExisting() error {
    // Only calculate transaction hash if it's empty
    if b.Header.TxHash == (common.Hash{}) {
        b.Header.TxHash = CalculateTxHash(b.Txs)
    }
    b.Header.TxRoot = b.Header.TxHash // Sync compatibility field

    // Only calculate uncle hash if it's empty
    if b.Header.UncleHash == (common.Hash{}) {
        b.Header.UncleHash = CalculateUncleHash(b.Uncles)
    }

    // Always calculate gas used
    b.Header.GasUsed = b.CalculateGasUsed()

    return nil
}

// Updates header fields based on block contents
func (b *Block) updateHeader() error {
    // Only calculate transaction hash if it's empty
    if b.Header.TxHash == (common.Hash{}) {
        b.Header.TxHash = CalculateTxHash(b.Txs)
    }
    b.Header.TxRoot = b.Header.TxHash // Sync compatibility field

    // Only calculate uncle hash if it's empty
    if b.Header.UncleHash == (common.Hash{}) {
        b.Header.UncleHash = CalculateUncleHash(b.Uncles)
    }

    // Calculate gas used
    b.Header.GasUsed = b.CalculateGasUsed()

    return nil
}
// CalculateTxHash computes the Merkle root of transactions
func CalculateTxHash(txs []*tx.Tx) common.Hash {
    // Handle nil or empty slice
    if txs == nil || len(txs) == 0 {
        return common.Hash{}
    }

    // Special case: single transaction - root is just the transaction hash
    if len(txs) == 1 {
        tx := txs[0]
        if tx == nil {
            return common.Hash{}
        }
        return tx.Hash()
    }

    // Multiple transactions: calculate Merkle tree
    hashes := make([]common.Hash, len(txs))
    for i, transaction := range txs {
        if transaction == nil {
            hashes[i] = common.Hash{}
        } else {
            hashes[i] = transaction.Hash()
        }
    }

    // Use SHA-256 Bitcoin-style Merkle root to match chain.CalcTxRoot
    return computeMerkleRoot(hashes)
}

func computeMerkleRoot(hashes []common.Hash) common.Hash {
    if len(hashes) == 0 {
        return common.Hash{}
    }

    // Standard Merkle tree implementation
    for len(hashes) > 1 {
        // If odd number, duplicate last hash
        if len(hashes)%2 == 1 {
            hashes = append(hashes, hashes[len(hashes)-1])
        }

        nextLevel := make([]common.Hash, len(hashes)/2)
        for i := 0; i < len(hashes); i += 2 {
            // Double SHA-256 (Bitcoin style) - MUST match chain
            firstHash := sha256.Sum256(append(hashes[i][:], hashes[i+1][:]...))
            secondHash := sha256.Sum256(firstHash[:])
            nextLevel[i/2] = common.BytesToHash(secondHash[:])
        }
        hashes = nextLevel
    }

    return hashes[0]
}

// CalculateUncleHash computes the hash of uncle headers
func CalculateUncleHash(uncles []*Header) common.Hash {
    if len(uncles) == 0 {
        return common.Hash{}
    }

    // Bitcoin-style: Double SHA-256 of concatenated uncle hashes
    hasher := sha256.New()
    for _, uncle := range uncles {
        hasher.Write(uncle.Hash().Bytes())
    }
    firstHash := hasher.Sum(nil)

    // Second SHA-256
    secondHasher := sha256.New()
    secondHasher.Write(firstHash)
    return common.BytesToHash(secondHasher.Sum(nil))
}

// CalculateGasUsed computes total gas used by all transactions
func (b *Block) CalculateGasUsed() uint64 {
        totalGas := uint64(0)
        for _, t := range b.Txs {
                totalGas += t.Gas
        }
        return totalGas
}

// Hash computes the canonical hash of the entire block
func (b *Block) Hash() common.Hash {
        if b == nil || b.Header == nil {
                return common.Hash{}
        }
        return b.Header.Hash()
}

// Validate performs comprehensive block validation
func (b *Block) Validate(parent *Block) error {
        if b == nil {
                return errors.New("block is nil")
        }

        // Validate header
        if err := b.Header.Validate(parent.Header); err != nil {
                return fmt.Errorf("header validation failed: %w", err)
        }

        // Validate block size
        if err := b.validateSize(); err != nil {
                return err
        }

        // Validate transaction count
        if len(b.Txs) > MaxTransactions {
                return ErrTooManyTransactions
        }

        // Validate transaction root
        calculatedTxHash := CalculateTxHash(b.Txs)
        if calculatedTxHash != b.Header.TxHash {
                return ErrTxRootMismatch
        }

        // Validate uncle count
        if len(b.Uncles) > MaxUncles {
                return ErrTooManyUncles
        }

        // Validate uncles
        if err := b.validateUncles(parent); err != nil {
                return fmt.Errorf("uncle validation failed: %w", err)
        }

        // Validate gas used matches transactions
        if b.Header.GasUsed != b.CalculateGasUsed() {
                return errors.New("gas used doesn't match transaction gas")
        }

        return nil
}

// validateSize checks if the block size is within limits
func (b *Block) validateSize() error {
        size := b.Size()
        if size > MaxBlockSize {
                return fmt.Errorf("%w: %d > %d", ErrBlockTooLarge, size, MaxBlockSize)
        }
        return nil
}

// validateUncles performs uncle block validation
func (b *Block) validateUncles(parent *Block) error {
        if len(b.Uncles) == 0 {
                return nil
        }

        uncleHashes := make(map[common.Hash]bool)
        for i, uncle := range b.Uncles {
                // Check for duplicates
                uncleHash := uncle.Hash()
                if uncleHashes[uncleHash] {
                        return ErrDuplicateUncle
                }
                uncleHashes[uncleHash] = true

                // Validate uncle header
                if err := uncle.Validate(nil); err != nil {
                        return fmt.Errorf("uncle %d invalid: %w", i, err)
                }

                // Check uncle number is less than current block
                if uncle.Number.Cmp(b.Header.Number) >= 0 {
                        return ErrUncleNumberTooHigh
                }

                // Check uncle is not too old (within 6 generations)
                if new(big.Int).Sub(b.Header.Number, uncle.Number).Cmp(big.NewInt(6)) > 0 {
                        return errors.New("uncle too old")
                }

                // Check uncle is not an ancestor
                if parent != nil && isAncestor(parent, uncle) {
                        return errors.New("uncle is ancestor")
                }
        }

        return nil
}

// Checks if a header is an ancestor of a block
func isAncestor(block *Block, ancestor *Header) bool {
        current := block
        for current != nil && current.Header.Number.Sign() > 0 {
                if current.Header.ParentHash == ancestor.Hash() {
                        return true
                }

                break
        }
        return false
}

// Size returns the estimated block size in bytes
func (b *Block) Size() int {
    // More accurate estimation based on typical component sizes
    size := 0

    // Header components (more detailed breakdown)
    size += 32  // ParentHash
    size += 32  // UncleHash
    size += 20  // Coinbase
    size += 32  // Root
    size += 32  // TxHash
    size += 32  // ReceiptHash
    size += 256 // Bloom (fixed 256 bytes)
    size += 32  // Difficulty
    size += 32  // Number
    size += 8   // GasLimit
    size += 8   // GasUsed
    size += 8   // Time
    size += len(b.Header.Extra) // Extra data
    size += 32  // MixDigest
    size += 8   // Nonce

    // Transactions (average 150-200 bytes per transaction)
    size += len(b.Txs) * 160

    // Uncles (each uncle is similar to header)
    size += len(b.Uncles) * 512

    return size
}

// Serialize converts block to JSON
func (b *Block) Serialize() ([]byte, error) {
        if b.Header != nil {
                b.Header.SyncRoots()
        }
        data, err := json.MarshalIndent(b, "", "  ")
        if err != nil {
                return nil, fmt.Errorf("failed to serialize block: %w", err)
        }
        return data, nil
}

// Deserialize creates block from JSON
func Deserialize(data []byte) (*Block, error) {
        var blk Block
        if err := json.Unmarshal(data, &blk); err != nil {
                return nil, fmt.Errorf("failed to deserialize block: %w", err)
        }
        if blk.Header != nil {
                blk.Header.SyncRoots()
        }
        return &blk, nil
}

// Returns the header timestamp as time.Time
func (h *Header) TimestampTime() time.Time {
        return time.Unix(int64(h.Time), 0)
}

// SyncRoots ensures compatibility fields are synchronized
func (h *Header) SyncRoots() {
        h.TxRoot = h.TxHash
}

// String returns a string representation of the block nonce
func (n BlockNonce) String() string {
        return hex.EncodeToString(n[:])
}

// MarshalText implements encoding.TextMarshaler
func (n BlockNonce) MarshalText() ([]byte, error) {
        return []byte("0x" + hex.EncodeToString(n[:])), nil
}

// UnmarshalText implements encoding.TextUnmarshaler
func (n *BlockNonce) UnmarshalText(text []byte) error {
        text = []byte(strings.TrimPrefix(string(text), "0x"))
        if len(text) != 16 {
                return errors.New("invalid nonce length")
        }
        _, err := hex.Decode(n[:], text)
        return err
}

// Custom JSON unmarshal for Header
func (h *Header) UnmarshalJSON(data []byte) error {
        type Alias Header
        aux := &struct {
                Number     interface{} `json:"number"`
                Difficulty interface{} `json:"difficulty"`
                Nonce      string      `json:"nonce"`
                *Alias
        }{
                Alias: (*Alias)(h),
        }

        if err := json.Unmarshal(data, &aux); err != nil {
                return err
        }

        // Parse nonce
        if err := h.Nonce.UnmarshalText([]byte(aux.Nonce)); err != nil {
                return fmt.Errorf("invalid nonce: %w", err)
        }

        // Parse number
        if num, err := parseBigInt(aux.Number); err != nil {
                return fmt.Errorf("invalid number: %w", err)
        } else {
                h.Number = num
        }

        // Parse difficulty
        if diff, err := parseBigInt(aux.Difficulty); err != nil {
                return fmt.Errorf("invalid difficulty: %w", err)
        } else {
                h.Difficulty = diff
        }

        h.SyncRoots()
        return nil
}

// MarshalJSON implements custom JSON marshaling for Header
func (h Header) MarshalJSON() ([]byte, error) {
        type Alias Header

        numStr := "0"
        if h.Number != nil {
                numStr = h.Number.String()
        }

        diffStr := "0"
        if h.Difficulty != nil {
                diffStr = h.Difficulty.String()
        }

        return json.Marshal(&struct {
                Number     string `json:"number"`
                Difficulty string `json:"difficulty"`
                Nonce      string `json:"nonce"`
                *Alias
        }{
                Number:     numStr,
                Difficulty: diffStr,
                Nonce:      "0x" + hex.EncodeToString(h.Nonce[:]),
                Alias:      (*Alias)(&h),
        })
}

// parseBigInt safely parses big.Int from various JSON types
func parseBigInt(v interface{}) (*big.Int, error) {
        switch val := v.(type) {
        case float64:
                if val != math.Trunc(val) {
                        return nil, errors.New("big.Int cannot have fractional part")
                }
                if val < 0 {
                        return nil, errors.New("big.Int cannot be negative")
                }
                return big.NewInt(int64(val)), nil
        case string:
                cleaned := strings.Trim(val, "\" ")
                if cleaned == "" {
                        return big.NewInt(0), nil
                }
                // Handle hex strings
                if strings.HasPrefix(cleaned, "0x") {
                        num := new(big.Int)
                        if _, success := num.SetString(cleaned[2:], 16); !success {
                                return nil, fmt.Errorf("invalid hex big.Int: %s", cleaned)
                        }
                        return num, nil
                }
                // Handle decimal strings
                num := new(big.Int)
                if _, success := num.SetString(cleaned, 10); !success {
                        return nil, fmt.Errorf("invalid decimal big.Int: %s", cleaned)
                }
                return num, nil
        case nil:
                return big.NewInt(0), nil
        default:
                return nil, fmt.Errorf("unsupported type for big.Int: %T", v)
        }
}

// safeBigIntToBytes converts big.Int to fixed-size byte slice
func safeBigIntToBytes(value *big.Int, size int) []byte {
        if value == nil {
                return make([]byte, size)
        }

        bytes := value.Bytes()
        if len(bytes) > size {
                return bytes[len(bytes)-size:]
        }
        if len(bytes) < size {
                result := make([]byte, size)
                copy(result[size-len(bytes):], bytes)
                return result
        }
        return bytes
}
