// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package tx

import (
"crypto/sha256"
"encoding/binary"
"encoding/hex"
"encoding/json"
"errors"
"fmt"
"math/big"
"strings"
"time"

"github.com/antdaza/antdchain/antdc/crypto/quantum"
chaincommon "github.com/antdaza/antdchain/common"
"github.com/ethereum/go-ethereum/common"
"github.com/ethereum/go-ethereum/rlp"
)

const MaxQuantumSignatureSize = quantum.MLDSA65SignatureSize

type Tx struct {
From      chaincommon.QuantumAddress
To        *chaincommon.QuantumAddress
PubKey    []byte
Value     *big.Int
Data      []byte
Nonce     uint64
Gas       uint64
GasPrice  *big.Int
Signature []byte
Timestamp uint64 `json:"timestamp"`
}

func NewTx(from, to chaincommon.QuantumAddress, value *big.Int, data []byte, nonce, gas uint64, gasPrice *big.Int) *Tx {
return &Tx{From: from, To: &to, Value: value, Data: data, Nonce: nonce, Gas: gas, GasPrice: gasPrice}
}

func (tx *Tx) Serialize() ([]byte, error) { return rlp.EncodeToBytes(tx) }

func Deserialize(data []byte) (*Tx, error) {
var tx Tx
if err := rlp.DecodeBytes(data, &tx); err != nil {
return nil, err
}
return &tx, nil
}

func (tx *Tx) Sign(privKey, pubKey []byte) error {
sig, err := quantum.Sign(privKey, tx.HashForSigning())
if err != nil {
return err
}
tx.Signature = sig
tx.PubKey = append([]byte(nil), pubKey...)
from, err := chaincommon.ParseQuantumAddress(quantum.PubKeyToAddress(pubKey))
if err != nil {
return err
}
tx.From = from
return nil
}

func (tx *Tx) Hash() common.Hash { return common.BytesToHash(tx.HashForSigning()) }

func (tx *Tx) HashForSigning() []byte {
hasher := sha256.New()
if tx.To != nil {
hasher.Write(tx.To[:])
}
if tx.Value != nil {
hasher.Write(tx.Value.Bytes())
}
hasher.Write(tx.Data)
_ = binary.Write(hasher, binary.BigEndian, tx.Nonce)
_ = binary.Write(hasher, binary.BigEndian, tx.Gas)
if tx.GasPrice != nil {
hasher.Write(tx.GasPrice.Bytes())
}
_ = binary.Write(hasher, binary.BigEndian, tx.Timestamp)
return hasher.Sum(nil)
}

func (tx *Tx) Verify() (bool, error) {
if len(tx.PubKey) == 0 {
return false, errors.New("missing public key")
}
if !quantum.Verify(tx.PubKey, tx.HashForSigning(), tx.Signature) {
return false, errors.New("invalid signature")
}
addr, err := chaincommon.ParseQuantumAddress(quantum.PubKeyToAddress(tx.PubKey))
if err != nil {
return false, err
}
if addr != tx.From {
return false, errors.New("from address does not match signer")
}
return true, nil
}

func (tx *Tx) Validate() error {
if tx.Gas == 0 {
return errors.New("zero gas")
}
if tx.GasPrice == nil || tx.GasPrice.Sign() <= 0 {
return errors.New("invalid gas price")
}
if tx.Value == nil || tx.Value.Sign() < 0 {
return errors.New("invalid value")
}
if len(tx.PubKey) != quantum.MLDSA65PublicKeySize {
return errors.New("invalid public key length")
}
if len(tx.Signature) != MaxQuantumSignatureSize {
return errors.New("invalid signature length")
}
return nil
}

func (tx Tx) MarshalJSON() ([]byte, error) {
var to *string
if tx.To != nil {
s := tx.To.String()
to = &s
}
valStr := ""
if tx.Value != nil {
valStr = tx.Value.String()
}
gpStr := ""
if tx.GasPrice != nil {
gpStr = tx.GasPrice.String()
}
return json.Marshal(struct {
From      string  `json:"from"`
To        *string `json:"to"`
PubKey    string  `json:"pubKey"`
Value     string  `json:"value"`
Data      string  `json:"data"`
Nonce     uint64  `json:"nonce"`
Gas       uint64  `json:"gas"`
GasPrice  string  `json:"gasPrice"`
Signature string  `json:"signature"`
}{
From:      tx.From.String(),
To:        to,
PubKey:    hex.EncodeToString(tx.PubKey),
Value:     valStr,
Data:      hex.EncodeToString(tx.Data),
Nonce:     tx.Nonce,
Gas:       tx.Gas,
GasPrice:  gpStr,
Signature: hex.EncodeToString(tx.Signature),
})
}

func (tx *Tx) UnmarshalJSON(data []byte) error {
type tempTx struct {
From      string      `json:"from"`
To        *string     `json:"to"`
PubKey    string      `json:"pubKey"`
Value     interface{} `json:"value"`
Data      string      `json:"data"`
Nonce     uint64      `json:"nonce"`
Gas       uint64      `json:"gas"`
GasPrice  interface{} `json:"gasPrice"`
Signature string      `json:"signature"`
Timestamp uint64      `json:"timestamp"`
}
var aux tempTx
if err := json.Unmarshal(data, &aux); err != nil {
return err
}

tx.Nonce, tx.Gas, tx.Timestamp = aux.Nonce, aux.Gas, aux.Timestamp
if aux.Data != "" {
d, err := hex.DecodeString(aux.Data)
if err != nil {
return fmt.Errorf("invalid data hex: %w", err)
}
tx.Data = d
}
if aux.Signature != "" {
s, err := hex.DecodeString(aux.Signature)
if err != nil {
return fmt.Errorf("invalid signature hex: %w", err)
}
tx.Signature = s
}
if aux.PubKey != "" {
pk, err := hex.DecodeString(aux.PubKey)
if err != nil {
return fmt.Errorf("invalid pubKey hex: %w", err)
}
tx.PubKey = pk
}
if aux.From != "" {
fromAddr, err := chaincommon.ParseQuantumAddress(aux.From)
if err != nil {
return err
}
tx.From = fromAddr
}
if aux.To != nil {
toAddr, err := chaincommon.ParseQuantumAddress(*aux.To)
if err != nil {
return err
}
tx.To = &toAddr
}

parseBigInt := func(v interface{}) (*big.Int, error) {
switch val := v.(type) {
case float64:
return big.NewInt(int64(val)), nil
case string:
cleaned := strings.Trim(val, "\"")
if cleaned == "" {
return big.NewInt(0), nil
}
num, ok := new(big.Int).SetString(cleaned, 10)
if !ok {
return nil, fmt.Errorf("invalid big.Int string: %s", cleaned)
}
return num, nil
case nil:
return big.NewInt(0), nil
default:
return nil, fmt.Errorf("invalid type for big.Int: %T", v)
}
}

v, err := parseBigInt(aux.Value)
if err != nil {
return fmt.Errorf("invalid value: %w", err)
}
tx.Value = v

gp, err := parseBigInt(aux.GasPrice)
if err != nil {
return fmt.Errorf("invalid gasPrice: %w", err)
}
tx.GasPrice = gp
return nil
}

func NewTransferTx(from, to chaincommon.QuantumAddress, amount *big.Int, nonce uint64, gasPrice *big.Int) *Tx {
return &Tx{From: from, To: &to, Value: amount, Nonce: nonce, Gas: 21000, GasPrice: gasPrice, Timestamp: uint64(time.Now().Unix())}
}
