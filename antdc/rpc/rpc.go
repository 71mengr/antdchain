// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package rpc

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"strings"

	"github.com/antdaza/antdchain/antdc/block"
	"github.com/antdaza/antdchain/antdc/chain"
	"github.com/antdaza/antdchain/antdc/pow"
	"github.com/antdaza/antdchain/antdc/tx"
	"github.com/antdaza/antdchain/antdc/wallet"
	"github.com/antdaza/antdchain/common"

	"github.com/ethereum/go-ethereum/crypto"
)

// Server represents the JSON-RPC server.
type Server struct {
	bc *chain.Blockchain

	wallets map[string]*wallet.Wallet // In-memory wallets by address

	mining bool
}

// NewServer creates a new JSON-RPC server.
func NewServer(bc *chain.Blockchain) *Server {
	return &Server{
		bc:      bc,
		wallets: make(map[string]*wallet.Wallet),
		mining:  false,
	}
}

func (s *Server) IsMining() bool {
	return s.mining
}

// Start starts the JSON-RPC server.
func (s *Server) Start(addr string) error {

	http.HandleFunc("/", s.handle)

	return http.ListenAndServe(addr, nil)

}

// handle processes JSON-RPC requests.
func (s *Server) handle(w http.ResponseWriter, r *http.Request) {

	var req struct {
		JSONRPC string          `json:"jsonrpc"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params"`
		ID      interface{}     `json:"id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, req.ID, -32600, "Invalid Request")
		return
	}

	if req.JSONRPC != "2.0" {

		s.writeError(w, req.ID, -32600, "Invalid JSON-RPC version")

		return

	}

	var result interface{}

	var errCode int

	var errMsg string

	switch req.Method {

	case "web3_clientVersion":

		result = "ANTDChain/v1.0.0"

	case "web3_sha3":

		var input string

		if err := json.Unmarshal(req.Params, &input); err != nil {

			errCode, errMsg = -32602, "Invalid params"

			break

		}

		data, err := hex.DecodeString(input[2:])

		if err != nil {

			errCode, errMsg = -32602, "Invalid hex string"

			break

		}

		result = common.BytesToHash(crypto.Keccak256(data)).Hex()

	case "showbalance":

		var addr string

		if err := json.Unmarshal(req.Params, &addr); err != nil {

			errCode, errMsg = -32602, "Invalid params"

			break

		}

		address, err := common.ParseQuantumAddress(addr)
		if err != nil {
			errCode, errMsg = -32602, "Invalid address"
			break
		}

		balance := s.bc.State().GetBalance(address)

		result = balance.String()

	case "getblockinfo":

		var number string

		if err := json.Unmarshal(req.Params, &number); err != nil {

			errCode, errMsg = -32602, "Invalid params"

			break

		}

		var block *block.Block

		if number == "latest" {

			block = s.bc.Latest()

		} else {

			n, err := strconv.ParseUint(number, 10, 64)

			if err != nil {

				errCode, errMsg = -32602, "Invalid block number"

				break

			}

			block = s.bc.GetBlock(n)

		}

		if block == nil {

			errCode, errMsg = -32603, "Block not found"

			break

		}

		result = map[string]interface{}{

			"number": block.Header.Number.String(),

			"hash": block.Hash().Hex(),

			"timestamp": block.Header.Time,

			"difficulty": pow.DisplayDifficulty(block.Header.Difficulty),

			"gasLimit": block.Header.GasLimit,

			"gasUsed": block.Header.GasUsed,

			"txs": len(block.Txs),
		}

	case "gettx":

		var hash string

		if err := json.Unmarshal(req.Params, &hash); err != nil {

			errCode, errMsg = -32602, "Invalid params"

			break

		}

		txHash := common.HexToHash(hash)

		// Search in blocks for tx

		var found *tx.Tx

		for i := uint64(0); ; i++ {

			b := s.bc.GetBlock(i)

			if b == nil {

				break

			}

			for _, t := range b.Txs {

				if t.Hash() == txHash {

					found = t

					break

				}

			}

			if found != nil {

				break

			}

		}

		if found == nil {

			errCode, errMsg = -32603, "Transaction not found"

			break

		}

		result = map[string]interface{}{

			"hash": found.Hash().Hex(),

			"from": found.From.Hex(),

			"to": found.To.Hex(),

			"value": found.Value.String(),

			"nonce": found.Nonce,

			"gas": found.Gas,

			"gasPrice": found.GasPrice.String(),

			"data": hex.EncodeToString(found.Data),
		}

	case "send":

		var params struct {
			From string `json:"from"`

			To string `json:"to"`

			Amount string `json:"amount"`

			Fees string `json:"fees"`

			Private string `json:"private"`
		}

		if err := json.Unmarshal(req.Params, &params); err != nil {

			errCode, errMsg = -32602, "Invalid params"

			break

		}

		from, err := common.ParseQuantumAddress(params.From)
		if err != nil {
			errCode, errMsg = -32602, "Invalid from address"
			break
		}

		to, err := common.ParseQuantumAddress(params.To)
		if err != nil {
			errCode, errMsg = -32602, "Invalid to address"
			break
		}

		amount, ok1 := new(big.Int).SetString(params.Amount, 10)

		fees, ok2 := new(big.Int).SetString(params.Fees, 10)

		if !ok1 || !ok2 {

			errCode, errMsg = -32602, "Invalid amount or fees"

			break

		}

		if _, ok := s.wallets[params.From]; !ok {

			errCode, errMsg = -32603, "Wallet not found for from address"

			break

		}

		w := s.wallets[params.From]

		if w.Address() != from {

			errCode, errMsg = -32603, "Private key mismatch with from address"

			break

		}

		gas := uint64(21000) // Basic transfer gas

		gasPrice := new(big.Int).Div(fees, big.NewInt(int64(gas)))

		t, err := w.CreateTx(to, amount, nil, gas, gasPrice)

		if err != nil {

			errCode, errMsg = -32603, err.Error()

			break

		}

		if err := s.bc.TxPool().AddTransaction(t, s.bc); err != nil {

			errCode, errMsg = -32603, err.Error()

			break

		}

		result = t.Hash().Hex()

	case "createaddress":

		w, err := wallet.NewWallet(s.bc, "")

		if err != nil {

			errCode, errMsg = -32603, "Failed to create wallet"

			break

		}

		addr := w.Address().String()

		s.wallets[addr] = w

		privateKey, err := w.PrivateKey()

		if err != nil {

			errCode, errMsg = -32603, "Failed to access private key"

			break

		}

		result = map[string]string{

			"address": addr,

			"private": hex.EncodeToString(privateKey),
		}

	case "import":

		var params struct {
			Private string `json:"private"`
		}

		if err := json.Unmarshal(req.Params, &params); err != nil {

			errCode, errMsg = -32602, "Invalid params"

			break

		}

		privKey, err := hex.DecodeString(strings.TrimPrefix(params.Private, "0x"))

		if err != nil {

			errCode, errMsg = -32603, "Invalid ML-DSA-65 private key"

			break

		}

		w, err := wallet.NewWalletWithKey(s.bc, privKey, "")

		if err != nil {

			errCode, errMsg = -32603, "Failed to create wallet"

			break

		}

		addr := w.Address().String()

		s.wallets[addr] = w

		result = map[string]string{"address": addr}

	case "export":

		var addr string

		if err := json.Unmarshal(req.Params, &addr); err != nil {

			errCode, errMsg = -32602, "Invalid params"

			break

		}

		w, ok := s.wallets[addr]

		if !ok {

			errCode, errMsg = -32603, "Wallet not found"

			break

		}

		result = map[string]string{

			"address": addr,

			"private": hex.EncodeToString(mustPrivateKeyBytes(w)),
		}

	case "startmining":

		var enable bool

		if err := json.Unmarshal(req.Params, &enable); err != nil {

			errCode, errMsg = -32602, "Invalid params"

			break

		}

		s.mining = enable

		result = map[string]string{"status": fmt.Sprintf("Mining %t", enable)}

	case "deploy":

		var params struct {
			From string `json:"from"`

			Data string `json:"data"`

			Value string `json:"value"`

			Private string `json:"private"`
		}

		if err := json.Unmarshal(req.Params, &params); err != nil {

			errCode, errMsg = -32602, "Invalid params"

			break

		}

		if _, ok := s.wallets[params.From]; !ok {

			errCode, errMsg = -32603, "Wallet not found"

			break

		}

		w := s.wallets[params.From]

		dataBytes, err := hex.DecodeString(params.Data[2:])

		if err != nil {

			errCode, errMsg = -32602, "Invalid bytecode"

			break

		}

		value, ok := new(big.Int).SetString(params.Value, 10)

		if !ok {

			errCode, errMsg = -32602, "Invalid value"

			break

		}

		gas := uint64(3000000) // Contract deployment gas

		gasPrice := big.NewInt(1000000000) // 1 Gwei

		t, err := w.CreateTx(common.QuantumAddress{}, value, dataBytes, gas, gasPrice)

		if err != nil {

			errCode, errMsg = -32603, err.Error()

			break

		}

		if err := s.bc.TxPool().AddTransaction(t, s.bc); err != nil {

			errCode, errMsg = -32603, err.Error()

			break

		}

		result = t.Hash().Hex()

	default:

		errCode, errMsg = -32601, "Method not found"

	}

	s.writeResponse(w, req.ID, result, errCode, errMsg)

}

// writeResponse writes a JSON-RPC response.
func (s *Server) writeResponse(w http.ResponseWriter, id interface{}, result interface{}, errCode int, errMsg string) {

	resp := struct {
		JSONRPC string `json:"jsonrpc"`

		Result interface{} `json:"result,omitempty"`

		Error *struct {
			Code int `json:"code"`

			Message string `json:"message"`
		} `json:"error,omitempty"`

		ID interface{} `json:"id"`
	}{

		JSONRPC: "2.0",

		ID: id,
	}

	if errCode != 0 {

		resp.Error = &struct {
			Code int `json:"code"`

			Message string `json:"message"`
		}{Code: errCode, Message: errMsg}

	} else {

		resp.Result = result

	}

	w.Header().Set("Content-Type", "application/json")

	json.NewEncoder(w).Encode(resp)

}

// writeError writes a JSON-RPC error.
func (s *Server) writeError(w http.ResponseWriter, id interface{}, code int, message string) {
	s.writeResponse(w, id, nil, code, message)
}

func mustPrivateKeyBytes(w *wallet.Wallet) []byte {
	key, err := w.PrivateKey()
	if err != nil {
		return nil
	}
	return key
}
