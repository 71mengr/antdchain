// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package main

import (
	"fmt"

	"github.com/libp2p/go-libp2p"
	"github.com/multiformats/go-multiaddr"
)

func main() {

	// Create a new libp2p host on a random port

	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip6/::1/tcp/0"))

	if err != nil {

		fmt.Printf("Error creating libp2p host: %v\n", err)

		return

	}

	defer h.Close()

	// Get the peer ID

	peerID := h.ID()

	// Get the multiaddress

	addr := h.Addrs()[0]

	port, err := addr.ValueForProtocol(multiaddr.P_TCP)

	if err != nil {

		fmt.Printf("Error getting TCP port: %v\n", err)

		return

	}

	// Construct the full multiaddress

	multiaddr := fmt.Sprintf("%s/p2p/%s", addr.String(), peerID.String())

	// Print results

	fmt.Printf("Generated Peer ID: %s\n", peerID.String())

	fmt.Printf("Full Multiaddress: %s\n", multiaddr)

	fmt.Printf("Use this multiaddress as a bootstrap node: /ip6/::1/tcp/%s/p2p/%s\n", port, peerID.String())

}
