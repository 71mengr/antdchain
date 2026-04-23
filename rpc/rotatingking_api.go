package rpc
import (
    "context"
    "fmt"
    "time"
    "math/big"

    "github.com/antdaza/antdchain/common"
    "github.com/antdaza/antdchain/antdc/reward"
)

type RotatingKingAPI struct {
    manager reward.RotatingKingManager
}

func NewRotatingKingAPI(manager reward.RotatingKingManager) *RotatingKingAPI {
    return &RotatingKingAPI{manager: manager}
}

func (api *RotatingKingAPI) List(ctx context.Context) ([]string, error) {
    if api.manager == nil {
        return []string{}, fmt.Errorf("rotating king manager not available")
    }

    addresses := api.manager.GetKingAddresses()
    result := make([]string, len(addresses))
    for i, addr := range addresses {
        result[i] = addr.String()
    }
    return result, nil
}

func (api *RotatingKingAPI) Status(ctx context.Context) (map[string]interface{}, error) {
    if api.manager == nil {
        return nil, fmt.Errorf("rotating king manager not available")
    }

    // Try to get height from context or use default
    height := uint64(0)

    info := api.manager.GetRotationInfo(height)
    if info == nil {
        info = make(map[string]interface{})
    }

    // Ensure basic fields
    info["current_king"] = api.manager.GetCurrentKing().String()
    info["next_king"] = api.manager.GetNextKing().String()
    info["king_count"] = len(api.manager.GetKingAddresses())
    info["status"] = "online"
    info["timestamp"] = time.Now().Unix()

    return info, nil
}

func (api *RotatingKingAPI) Address(ctx context.Context) (string, error) {
    if api.manager == nil {
        return "", fmt.Errorf("rotating king manager not available")
    }

    return api.manager.GetCurrentKing().String(), nil
}

func (api *RotatingKingAPI) Next(ctx context.Context) (string, error) {
    if api.manager == nil {
        return "", fmt.Errorf("rotating king manager not available")
    }

    return api.manager.GetNextKing().String(), nil
}

func (api *RotatingKingAPI) Cycle(ctx context.Context) (map[string]interface{}, error) {
    if api.manager == nil {
        return nil, fmt.Errorf("rotating king manager not available")
    }

    height := uint64(0)
    info := api.manager.GetRotationInfo(height)

    if info == nil {
        info = make(map[string]interface{})
    }

    // Add cycle-specific info
    info["rotation_interval"] = api.manager.GetRotationInterval()
    info["current_king_index"] = api.manager.GetCurrentKingIndex()

    return info, nil
}

func (api *RotatingKingAPI) History(ctx context.Context, limit int) ([]map[string]interface{}, error) {
    if api.manager == nil {
        return nil, fmt.Errorf("rotating king manager not available")
    }

    if limit <= 0 || limit > 100 {
        limit = 10
    }

    // Check if manager has GetRotationHistory method using reflection
    // For now, return empty
    // TODO: Do it correctly
    result := make([]map[string]interface{}, 0)

    // Try to get history if method exists
    // This is a bit hacky but works lol
    history := make([]interface{}, 0)
    if h, ok := interface{}(api.manager).(interface{ GetRotationHistory(int) []interface{} }); ok {
        history = h.GetRotationHistory(limit)
    }

    for _, item := range history {
        if m, ok := item.(map[string]interface{}); ok {
            result = append(result, m)
        }
    }

    return result, nil
}

func (api *RotatingKingAPI) Info(ctx context.Context, address string) (map[string]interface{}, error) {
    if api.manager == nil {
        return nil, fmt.Errorf("rotating king manager not available")
    }

    // Parse the quantum address (0q...)
    addr, err := common.ParseQuantumAddress(address)
    if err != nil {
        return nil, fmt.Errorf("invalid quantum address: %w", err)
    }

    result := map[string]interface{}{
        "address":    addr.String(), // 0q format
        "is_king":    false,
        "is_current": false,
        "rewards":    "0",
    }

    // Check if address is a king
    addresses := api.manager.GetKingAddresses()
    for i, a := range addresses {
        if a == addr {
            result["is_king"] = true
            result["position"] = i + 1
            result["is_current"] = i == api.manager.GetCurrentKingIndex()
            break
        }
    }

    // Load rewards if the manager supports it
    if rewards := api.manager.GetKingRewards(addr); rewards != nil {
        result["rewards"] = rewards.String()
    }

    return result, nil
}

func (api *RotatingKingAPI) Rewards(ctx context.Context, address string) (string, error) {
    if api.manager == nil {
        return "", fmt.Errorf("rotating king manager not available")
    }

    addr := common.ParseQuantumAddress(address)

    // Try to get rewards if method exists
    if m, ok := interface{}(api.manager).(interface{ GetKingRewards(common.QuantumAddress) *big.Int }); ok {
        rewards := m.GetKingRewards(addr)
        if rewards != nil {
            return rewards.String(), nil
        }
    }

    return "0", nil
}

func (api *RotatingKingAPI) Test(ctx context.Context) (string, error) {
    if api.manager == nil {
        return "Rotating King RPC: Manager not available", nil
    }

    return fmt.Sprintf("Rotating King RPC: Online with %d kings", len(api.manager.GetKingAddresses())), nil
}
