// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package state
import (
"bytes"
"crypto/sha256"
"encoding/binary"
"errors"
"fmt"
"math/big"
"sort"

"github.com/antdaza/antdchain/common"
"github.com/ethereum/go-ethereum/crypto"
"github.com/syndtr/goleveldb/leveldb"
)

// Account represents a ANTDChain account.
type Account struct {
Nonce    uint64
Balance  *big.Int
Root     common.Hash // storage root
CodeHash []byte      // code hash
}

// State wraps the LevelDB database holding account state.
type State struct {
db *leveldb.DB
}

// NewState opens a new state database at the given path.
func NewState(path string) (*State, error) {
db, err := leveldb.OpenFile(path, nil)
if err != nil {
return nil, err
}
return &State{db: db}, nil
}

// Close closes the database.
func (s *State) Close() error {
return s.db.Close()
}

// ---------------------
// Balance / Nonce
// ---------------------
func (s *State) GetBalance(addr common.QuantumAddress) *big.Int {
key := append([]byte("balance:"), addr[:]...)
data, err := s.db.Get(key, nil)
if err != nil {
return big.NewInt(0)
}
return new(big.Int).SetBytes(data)
}

func (s *State) AddBalance(addr common.QuantumAddress, amount *big.Int) error {
key := append([]byte("balance:"), addr[:]...)
current := s.GetBalance(addr)
newBal := new(big.Int).Add(current, amount)
if newBal.Sign() < 0 {
return errors.New("negative balance")
}
return s.db.Put(key, newBal.Bytes(), nil)
}

func (s *State) SetBalance(addr common.QuantumAddress, balance *big.Int) error {
key := append([]byte("balance:"), addr[:]...)
if balance.Sign() < 0 {
return errors.New("negative balance")
}
return s.db.Put(key, balance.Bytes(), nil)
}

func (s *State) GetNonce(addr common.QuantumAddress) uint64 {
key := append([]byte("nonce:"), addr[:]...)
data, err := s.db.Get(key, nil)
if err != nil {
return 0
}
return binary.BigEndian.Uint64(data)
}

func (s *State) SetNonce(addr common.QuantumAddress, nonce uint64) error {
key := append([]byte("nonce:"), addr[:]...)
data := make([]byte, 8)
binary.BigEndian.PutUint64(data, nonce)
return s.db.Put(key, data, nil)
}

// ---------------------
// Code
// ---------------------
func (s *State) SetCode(addr common.QuantumAddress, code []byte) error {
key := append([]byte("code:"), addr[:]...)
if err := s.db.Put(key, code, nil); err != nil {
return err
}
codeHash := crypto.Keccak256(code)
codeKey := append([]byte("codehash:"), addr[:]...)
return s.db.Put(codeKey, codeHash, nil)
}

func (s *State) GetCode(addr common.QuantumAddress) []byte {
key := append([]byte("code:"), addr[:]...)
data, err := s.db.Get(key, nil)
if err != nil {
return nil
}
return data
}

func (s *State) GetCodeHash(addr common.QuantumAddress) common.Hash {
key := append([]byte("codehash:"), addr[:]...)
data, err := s.db.Get(key, nil)
if err != nil {
return common.Hash{}
}
return common.BytesToHash(data)
}

// ---------------------
// Storage
// ---------------------
func (s *State) GetStorage(addr, key common.QuantumAddress) common.Hash {
storageKey := append(append([]byte("storage:"), addr[:]...), key[:]...)
data, err := s.db.Get(storageKey, nil)
if err != nil {
return common.Hash{}
}
return common.BytesToHash(data)
}

func (s *State) SetStorage(addr, key common.QuantumAddress, value common.Hash) error {
storageKey := append(append([]byte("storage:"), addr[:]...), key[:]...)
return s.db.Put(storageKey, value[:], nil)
}

// ---------------------
// State Root
// ---------------------
func (s *State) Root() common.Hash {
hasher := sha256.New()

var kvs []struct {
Key   []byte
Value []byte
}
iter := s.db.NewIterator(nil, nil)
for iter.Next() {
key := iter.Key()
if bytes.HasPrefix(key, []byte("meta:")) {
continue
}
kvs = append(kvs, struct {
Key   []byte
Value []byte
}{
append([]byte{}, key...),
append([]byte{}, iter.Value()...),
})
}
iter.Release()
if err := iter.Error(); err != nil {
return common.Hash{}
}

sort.Slice(kvs, func(i, j int) bool {
return string(kvs[i].Key) < string(kvs[j].Key)
})

for _, kv := range kvs {
hasher.Write(kv.Key)
hasher.Write(kv.Value)
}

return common.BytesToHash(hasher.Sum(nil))
}

// Commit computes and persists the latest state root.
func (s *State) Commit(blockNumber uint64) (common.Hash, error) {
root := s.Root()

latestKey := []byte("meta:state_root:latest")
if err := s.db.Put(latestKey, root.Bytes(), nil); err != nil {
return common.Hash{}, err
}

heightKey := []byte(fmt.Sprintf("meta:state_root:%d", blockNumber))
if err := s.db.Put(heightKey, root.Bytes(), nil); err != nil {
return common.Hash{}, err
}

return root, nil
}

// Reset validates that the in-memory reference is aligned with the requested root.
func (s *State) Reset(root common.Hash) error {
currentRoot := s.Root()
if currentRoot != root {
return fmt.Errorf("state root mismatch: current %s, expected %s", currentRoot.Hex(), root.Hex())
}
return nil
}

// ---------------------
// Clone
// ---------------------
// Clone creates a deep copy of the current state into a new LevelDB instance.
func (s *State) Clone(path string) (*State, error) {
newDB, err := leveldb.OpenFile(path, nil)
if err != nil {
return nil, err
}

iter := s.db.NewIterator(nil, nil)
batch := new(leveldb.Batch)
for iter.Next() {
batch.Put(iter.Key(), iter.Value())
}
iter.Release()
if err := iter.Error(); err != nil {
return nil, err
}

if err := newDB.Write(batch, nil); err != nil {
return nil, err
}

return &State{db: newDB}, nil
}
