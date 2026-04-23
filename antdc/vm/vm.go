// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package vm
import (
"context"

"errors"

"math/big"

"github.com/antdaza/antdchain/antdc/state"
"github.com/antdaza/antdchain/common"
"github.com/antdaza/antdchain/antdc/tx"
)

// Instruction represents a ANTDVM instruction.
type Instruction struct {
Opcode string

Args []interface{}
}

// VM represents the ANTDChain Virtual Machine.

type VM struct {
s *state.State

gas uint64

stack []interface{}

storage map[common.Hash]interface{}
}

// NewVM creates a new ANTDVM instance.

func NewVM(s *state.State, gasLimit uint64) *VM {

return &VM{

s: s,

gas: gasLimit,

stack: make([]interface{}, 0),

storage: make(map[common.Hash]interface{}),
}

}

// Execute runs a transaction in the ANTDVM.

func (v *VM) Execute(ctx context.Context, t *tx.Tx) (ret []byte, gasUsed uint64, err error) {

if err := t.Validate(); err != nil {

return nil, 0, err

}

gasUsed = 21000 // Base gas for simple transactions

if v.gas < gasUsed {

return nil, gasUsed, errors.New("insufficient gas")

}

v.gas -= gasUsed

// Handle simple transfer

if t.Data == nil || len(t.Data) == 0 {

from := common.BytesToQuantumAddress(t.From[:])
if !v.canTransfer(from, t.Value) {

return nil, gasUsed, errors.New("insufficient balance")

}

to := common.BytesToQuantumAddress((*t.To)[:])
v.transfer(from, to, t.Value)

v.s.SetNonce(from, t.Nonce+1)

return v.s.Root().Bytes(), gasUsed, nil

}

// Handle contract execution
instructions, err := parseInstructions(t.Data)

if err != nil {

return nil, gasUsed, err

}

for _, instr := range instructions {

select {

case <-ctx.Done():

return nil, gasUsed, ctx.Err()

default:

if err := v.executeInstruction(instr); err != nil {

return nil, gasUsed, err

}

}

}

return v.s.Root().Bytes(), gasUsed, nil

}

// parseInstructions converts transaction data to instructions (simplified).

func parseInstructions(data []byte) ([]Instruction, error) {

// Simplified: assume data is a list of opcodes and args (e.g., "PUSH 100 TRANSFER 0x... 50")

//TODO: use a proper bytecode format

instr := []Instruction{

{Opcode: "PUSH", Args: []interface{}{big.NewInt(100)}},

{Opcode: "TRANSFER", Args: []interface{}{common.ParseQuantumAddress("0x123..."), big.NewInt(50)}},
}

return instr, nil

}

// executeInstruction executes a single instruction.

func (v *VM) executeInstruction(instr Instruction) error {

switch instr.Opcode {

case "PUSH":

if len(instr.Args) != 1 {

return errors.New("PUSH requires 1 argument")

}

v.stack = append(v.stack, instr.Args[0])

v.gas -= 100 // Gas cost

case "POP":

if len(v.stack) == 0 {

return errors.New("stack underflow")

}

v.stack = v.stack[:len(v.stack)-1]

v.gas -= 50

case "ADD":

if len(v.stack) < 2 {

return errors.New("stack underflow")

}

a, b := v.stack[len(v.stack)-2], v.stack[len(v.stack)-1]

ai, ok1 := a.(*big.Int)

bi, ok2 := b.(*big.Int)

if !ok1 || !ok2 {

return errors.New("ADD requires big.Int")

}

v.stack = v.stack[:len(v.stack)-2]

v.stack = append(v.stack, new(big.Int).Add(ai, bi))

v.gas -= 200

case "SUB":

if len(v.stack) < 2 {

return errors.New("stack underflow")

}

a, b := v.stack[len(v.stack)-2], v.stack[len(v.stack)-1]

ai, ok1 := a.(*big.Int)

bi, ok2 := b.(*big.Int)

if !ok1 || !ok2 {

return errors.New("SUB requires big.Int")

}

v.stack = v.stack[:len(v.stack)-2]

v.stack = append(v.stack, new(big.Int).Sub(ai, bi))

v.gas -= 200

case "STORE":

if len(v.stack) == 0 || len(instr.Args) != 1 {

return errors.New("STORE requires 1 argument and 1 stack value")

}

key, ok := instr.Args[0].(common.Hash)

if !ok {

return errors.New("STORE requires common.Hash key")

}

v.storage[key] = v.stack[len(v.stack)-1]

v.stack = v.stack[:len(v.stack)-1]

v.gas -= 1000

case "LOAD":

if len(instr.Args) != 1 {

return errors.New("LOAD requires 1 argument")

}

key, ok := instr.Args[0].(common.Hash)

if !ok {

return errors.New("LOAD requires common.Hash key")

}

v.stack = append(v.stack, v.storage[key])

v.gas -= 800

case "TRANSFER":

if len(instr.Args) != 2 {

return errors.New("TRANSFER requires 2 arguments")

}

to, ok1 := instr.Args[0].(common.QuantumAddress)

amount, ok2 := instr.Args[1].(*big.Int)

if !ok1 || !ok2 {

return errors.New("TRANSFER requires address and big.Int")

}

if !v.canTransfer(common.QuantumAddress{}, amount) { // Assume contract address is zero

return errors.New("insufficient balance")

}

v.transfer(common.QuantumAddress{}, to, amount)

v.gas -= 21000

case "HALT":

return nil

default:

return errors.New("unknown opcode: " + instr.Opcode)

}

if v.gas < 0 {

return errors.New("out of gas")

}

return nil

}

// canTransfer checks if the sender has enough balance.

func (v *VM) canTransfer(from common.QuantumAddress, value *big.Int) bool {

return v.s.GetBalance(from).Cmp(value) >= 0

}

// transfer moves balance from sender to recipient.

func (v *VM) transfer(from, to common.QuantumAddress, value *big.Int) {

if value.Sign() == 0 {

return

}

v.s.AddBalance(from, new(big.Int).Neg(value))

v.s.AddBalance(to, value)

}
