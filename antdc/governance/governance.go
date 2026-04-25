// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package governance

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/antdaza/antdchain/antdc/reward"
	"github.com/antdaza/antdchain/common"
	"github.com/ethereum/go-ethereum/log"
)

const GovernanceDelay = 48 * 60 * 60 // 48 hours

type ProposalType uint8

const (
	ProposalUpdateMainKing ProposalType = iota
	ProposalUpdateRotatingKings
)

type GovernanceProposal struct {
	ID                uint64                  `json:"id"`
	ProposalType      ProposalType            `json:"type"`
	Executor          common.QuantumAddress   `json:"executor"`
	Executed          bool                    `json:"executed"`
	ETA               uint64                  `json:"eta"`
	NewMainKing       common.QuantumAddress   `json:"new_main_king,omitempty"`
	NewRotatingKings  []common.QuantumAddress `json:"new_rotating_kings,omitempty"`
	CreatedTimestamp  uint64                  `json:"created_timestamp"`
	ExecutedTimestamp uint64                  `json:"executed_timestamp,omitempty"`
}

var (
	ErrNotAuthorized    = errors.New("governance: not authorized")
	ErrProposalExecuted = errors.New("governance: proposal already executed")
	ErrDelayNotPassed   = errors.New("governance: timelock delay not passed")
	ErrInvalidParams    = errors.New("governance: invalid proposal parameters")
)

type KingUpdateListener func(mainKing common.QuantumAddress, rotatingKings []common.QuantumAddress)

type GovernanceController struct {
	owner             common.QuantumAddress
	proposals         map[uint64]*GovernanceProposal
	rewardDistributor *reward.RewardDistributor
	rotatingKingMgr   reward.RotatingKingManager // Use interface instead of concrete type
	dataDir           string
	atomicNextID      uint64
	onKingUpdate      KingUpdateListener
}

// NewGovernanceController — now takes rotating king manager directly
func NewGovernanceController(owner common.QuantumAddress, distributor *reward.RewardDistributor, rkMgr reward.RotatingKingManager, dataDir string) (*GovernanceController, error) {
	if owner == (common.QuantumAddress{}) {
		return nil, errors.New("governance owner cannot be zero address")
	}
	if rkMgr == nil {
		return nil, errors.New("rotating king manager required")
	}

	govDir := filepath.Join(dataDir, "governance")
	if err := os.MkdirAll(govDir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to create governance dir: %w", err)
	}

	gc := &GovernanceController{
		owner:             owner,
		proposals:         make(map[uint64]*GovernanceProposal),
		rewardDistributor: distributor,
		rotatingKingMgr:   rkMgr,
		dataDir:           dataDir,
		atomicNextID:      1,
	}

	if err := gc.loadProposals(); err != nil {
		return nil, err
	}

	if len(gc.proposals) > 0 {
		var max uint64
		for id := range gc.proposals {
			if id > max {
				max = id
			}
		}
		atomic.StoreUint64(&gc.atomicNextID, max+1)
	}

	log.Info("Governance controller initialized", "owner", owner.Hex(), "proposals", len(gc.proposals))
	return gc, nil
}

func (gc *GovernanceController) SetKingUpdateListener(fn KingUpdateListener) {
	gc.onKingUpdate = fn
}

func (gc *GovernanceController) ProposeMainKingChange(caller common.QuantumAddress, newMainKing common.QuantumAddress, now uint64) (uint64, error) {
	if caller != gc.owner {
		return 0, ErrNotAuthorized
	}
	if newMainKing == (common.QuantumAddress{}) {
		return 0, ErrInvalidParams
	}

	id := gc.nextID()
	prop := &GovernanceProposal{
		ID:               id,
		ProposalType:     ProposalUpdateMainKing,
		Executor:         caller,
		ETA:              now + GovernanceDelay,
		NewMainKing:      newMainKing,
		CreatedTimestamp: now,
	}

	gc.proposals[id] = prop
	if err := gc.saveProposal(prop); err != nil {
		delete(gc.proposals, id)
		return 0, err
	}

	log.Info("Main King change proposed", "id", id, "new", newMainKing.Hex())
	return id, nil
}

func (gc *GovernanceController) ProposeRotatingKingsUpdate(caller common.QuantumAddress, newKings []common.QuantumAddress, now uint64) (uint64, error) {
	if caller != gc.owner {
		return 0, ErrNotAuthorized
	}
	if len(newKings) == 0 || len(newKings) > 1000 {
		return 0, ErrInvalidParams
	}

	id := gc.nextID()
	copied := make([]common.QuantumAddress, len(newKings))
	copy(copied, newKings)

	prop := &GovernanceProposal{
		ID:               id,
		ProposalType:     ProposalUpdateRotatingKings,
		Executor:         caller,
		ETA:              now + GovernanceDelay,
		NewRotatingKings: copied,
		CreatedTimestamp: now,
	}

	gc.proposals[id] = prop
	if err := gc.saveProposal(prop); err != nil {
		delete(gc.proposals, id)
		return 0, err
	}

	log.Info("Rotating Kings update proposed", "id", id, "count", len(newKings))
	return id, nil
}

func (gc *GovernanceController) ExecuteProposal(id uint64, caller common.QuantumAddress, now uint64) error {
	prop, exists := gc.proposals[id]
	if !exists {
		return errors.New("proposal not found")
	}
	if prop.Executed {
		return ErrProposalExecuted
	}
	if now < prop.ETA {
		return ErrDelayNotPassed
	}

	switch prop.ProposalType {
	case ProposalUpdateMainKing:
		log.Info("Governance: Main King update requested", "id", id, "new", prop.NewMainKing.Hex())
		// TODO: Implement actual main king update in reward distributor

	case ProposalUpdateRotatingKings:
		if err := gc.rotatingKingMgr.UpdateKingAddresses(prop.NewRotatingKings); err != nil {
			return fmt.Errorf("failed to update rotating kings: %w", err)
		}
		log.Info("Governance: Rotating Kings updated", "id", id, "count", len(prop.NewRotatingKings))
	}

	prop.Executed = true
	prop.ExecutedTimestamp = now
	if err := gc.saveProposal(prop); err != nil {
		log.Warn("Failed to save executed proposal", "id", id, "err", err)
	}

	if gc.onKingUpdate != nil {
		go func() {
			// We need methods to get current kings
			main := common.QuantumAddress{} // TODO: Get from rewardDistributor
			rot := gc.rotatingKingMgr.GetKingAddresses()
			gc.onKingUpdate(main, rot)
		}()
	}

	return nil
}

func (gc *GovernanceController) proposalPath(id uint64) string {
	return filepath.Join(gc.dataDir, "governance", fmt.Sprintf("proposal_%d.json", id))
}

func (gc *GovernanceController) saveProposal(p *GovernanceProposal) error {
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(gc.proposalPath(p.ID), data, 0644)
}

func (gc *GovernanceController) loadProposals() error {
	dir := filepath.Join(gc.dataDir, "governance")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "proposal_") || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var p GovernanceProposal
		if err := json.Unmarshal(data, &p); err != nil {
			continue
		}
		gc.proposals[p.ID] = &p
	}
	return nil
}

func (gc *GovernanceController) nextID() uint64 {
	return atomic.AddUint64(&gc.atomicNextID, 1) - 1
}

func (gc *GovernanceController) GetProposal(id uint64) (*GovernanceProposal, bool) {
	p, ok := gc.proposals[id]
	return p, ok
}

func (gc *GovernanceController) ListProposals() map[uint64]*GovernanceProposal {
	cpy := make(map[uint64]*GovernanceProposal)
	for id, p := range gc.proposals {
		cpy[id] = p
	}
	return cpy
}

func (gc *GovernanceController) CanExecute(id uint64, now uint64) bool {
	p, ok := gc.proposals[id]
	return ok && !p.Executed && now >= p.ETA
}
