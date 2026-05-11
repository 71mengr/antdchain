package p2p

import "strings"

// DefaultBootstrapPeers contains static seed nodes for initial peer discovery.
// Add reliable, publicly reachable peers here in multiaddr format.
var DefaultBootstrapPeers = []string{
	"/ip4/129.151.164.202/tcp/3000/p2p/12D3KooWPqCk91U2fMTef8rio91hngtC3XoS9p3tVKjJqeMp2PLi",
        "/ip4/192.168.75.210/tcp/3000/p2p/12D3KooWQftuGtxb6btk2mk2R617swaLiYhNVF9W1sotRCGd5UvW",
}

// ResolveBootstrapPeers merges configured peers with default peers,
// removes empty entries, and deduplicates while preserving order.
func ResolveBootstrapPeers(configured []string) []string {
	merged := make([]string, 0, len(configured)+len(DefaultBootstrapPeers))
	seen := make(map[string]struct{}, len(configured)+len(DefaultBootstrapPeers))

	add := func(addr string) {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			return
		}
		if _, exists := seen[addr]; exists {
			return
		}
		seen[addr] = struct{}{}
		merged = append(merged, addr)
	}

	for _, addr := range configured {
		add(addr)
	}
	for _, addr := range DefaultBootstrapPeers {
		add(addr)
	}

	return merged
}
