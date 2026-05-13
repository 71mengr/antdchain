# ANTDChain - Fast PoW Layer 1 Blockchain with Rotating King Governance

## 🚀 Overview

ANTDChain is a high-performance Proof-of-Work Layer 1 blockchain featuring a unique **Rotating King** governance system. Built for speed, security, and decentralized governance, it combines Ethereum-compatible tooling with innovative economic models.

## ✨ Key Features

### 🏗️ **Technical Architecture**
- **Custom PoW Algorithm**: BLAKE3 + Argon2id hybrid for ASIC resistance
- **EVM Compatibility**: Full Ethereum Virtual Machine support
- **Lightning-Fast Sync**: Optimized peer-to-peer synchronization
- **Checkpoint System**: Trustless bootstrapping with peer-validated checkpoints
- **Database-Backed State**: Persistent storage with automatic recovery

### 👑 **Rotating King Governance**
- **Dynamic Royalty System**: 5% of block rewards distributed to rotating kings
- **Stake-Based Eligibility**: 100,000 ANTD minimum stake requirement
- **Automatic Rotation**: Kings rotate every 100 blocks automatically
- **Ineligibility Protection**: Addresses below minimum stake auto-removed
- **Network Consensus**: King lists synchronized across all nodes

### 🔧 **Core Components**

#### 1. **Blockchain Engine**
- Multi-threaded block validation
- Orphan block rejection (no orphan storage)
- Chain reorganization protection
- State trie with Merkle proofs
- Transaction pool with confirmation system

#### 2. **P2P Network Layer**
- libp2p-based networking with GossipSub
- Multi-topic pub/sub for blocks, transactions, and king events
- DHT-based peer discovery
- Rate-limited message handling
- Direct block push optimization

#### 3. **Mining System**
- CPU-mining optimized PoW
- Dynamic difficulty adjustment
- Mining templates for stratum compatibility
- Real-time hash rate monitoring

#### 4. **Wallet & Transaction System**
- Hierarchical Deterministic (HD) wallets
- Secure key storage with encryption
- Transaction signing with ECDSA
- Nonce management and fee calculation

#### 5. **JSON-RPC API**
- Full Ethereum-compatible API
- `eth_`, `net_`, `web3_` namespace support
- Web interface with real-time monitoring
- Health check endpoints

## 📊 **Economic Model**

### Tokenomics
- **Native Token**: ANTD (1 ANTD = 10^18 base units)
- **Block Reward**: 1 ANTD + transaction fees
- **Reward Distribution**:
  - 90% → Miner
  - 5% → Main King (fixed address)
  - 5% → Rotating King (eligible addresses)

### Staking Requirements
- **Minimum Stake**: 100,000 ANTD for king eligibility
- **Rotation Interval**: Every 100 blocks (~50 minutes)
- **Activation Delay**: 2 blocks after rotation

## 🛠️ **Getting Started**

### Prerequisites
- Go 1.21+
- GCC compiler (for CGO)
- 4GB+ RAM recommended
- 50GB+ storage for full node

### Installation

#### From Source
```bash
# Clone repository
git clone https://github.com/antdaza/antdchain.git
cd antdchain

# Build for Ubuntu
make ubuntu-build

# Or build for your platform
go build -o antdchain ./cmd/antdchain
```

#### Quick Start
```bash
# Start a node with default settings
./antdchain --data-dir ./antdchain-data --p2p-port 3000

# Start with mining enabled
./antdchain --startmining --miner-address YOUR_ADDRESS

# Connect to testnet
./antdchain --bootstrap "/ip6/2001:db8::100/tcp/3000/p2p/12D3KooW..."

# Open console interface
./antdchain --console
```

### Configuration

#### Data Directory Structure
```
antdchain-data/
├── blocks/          # Block storage
├── state/           # Blockchain state
├── wallets/         # Encrypted wallets
├── p2p-key.hex      # Node identity
└── checkpoints.json # Trusted checkpoints
```

#### Network Ports
- **P2P**: 3000 (default)
- **JSON-RPC**: 8089 (default)
- **Web Interface**: 8090 (default)

## 🔌 **API Reference**

### JSON-RPC Endpoints
```bash
# Get chain height
curl -X POST http://localhost:8089/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'

# Get balance
curl -X POST http://localhost:8089/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x...","latest"],"id":1}'

# Send transaction
curl -X POST http://localhost:8089/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0x..."],"id":1}'
```

### Web Interface
Access at: `http://localhost:8090`
- Real-time block explorer
- Network status dashboard
- Wallet management
- Mining statistics

## 👑 **King Management Commands**

```bash
# List current kings
./antdchain king list

# Check eligibility status
./antdchain king status

# Force rotate to specific king
./antdchain king rotate --index 2 --reason "maintenance"

# Cleanup ineligible kings
./antdchain king cleanup

# Sync configurations from peers
./antdchain king sync

# Debug king database
./antdchain king debug
```

## 🔄 **Network Synchronization**

### Sync Modes
1. **Fast Sync**: Downloads blocks sequentially from highest peer
2. **Gossip Sync**: Receives blocks via pub/sub (real-time)
3. **Database Sync**: Rotating king state synchronization
4. **Config Sync**: King list consensus synchronization

### Peer Discovery
- **Bootstrap Nodes**: Manual configuration
- **mDNS**: Local network discovery
- **DHT**: Global peer discovery
- **GossipSub**: Topic-based peer finding

## 🔒 **Security Features**

### Node Security
- Persistent peer identity with encrypted storage
- Rate limiting per peer (transactions, blocks, events)
- Checkpoint validation for chain integrity
- Transaction nonce and signature verification

### Wallet Security
- BIP-39 mnemonic phrases
- Encrypted keystore (AES-256)
- Hierarchical deterministic wallets
- Air-gapped signing support

### Network Security
- Proof-of-Work spam protection
- Sybil attack resistance via stake requirements
- Eclipse attack prevention with diverse peer connections
- Message authentication and encryption

## 📈 **Monitoring & Maintenance**

### Logging
```bash
# View logs in JSON format
tail -f antdchain-data/node.log | jq .

# Monitor sync status
./antdchain --console
> debug sync
> debug peers
> debug kings
```

### Performance Metrics
- Block propagation time
- Transaction throughput
- Network latency
- Memory usage
- Database performance

### Health Checks
```bash
# API health check
curl http://localhost:8089/health

# Check sync status
curl -X POST http://localhost:8089/rpc \
  -d '{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1}'
```

## 🔧 **Advanced Configuration**

### Custom Genesis
```json
{
  "genesis": {
    "miner": "0qANA3c85k94LTyTXLGDdEzmLE32b1qhYZF",
    "timestamp": 1763731821,
    "difficulty": "1",
    "alloc": {
      "0qANA3c85k94LTyTXLGDdEzmLE32b1qhYZF": "1000000000000000000000000"
    }
  }
}
```

### Mining Configuration
```bash
# Custom difficulty
./antdchain --mining-difficulty 1000

# Set CPU threads
./antdchain --mining-threads 4

# Target block time
./antdchain --target-block-time 30
```

### P2P Configuration
```bash
# Custom bootstrap nodes
./antdchain --bootstrap "/ip6/2001:db8::100/tcp/3000/p2p/...,/ip6/2001:db8::101/tcp/3000/p2p/..."

# Disable discovery
./antdchain --no-mdns --no-dht

# Maximum peers
./antdchain --max-peers 100 --min-peers 10
```

## 🚨 **Troubleshooting**

### Common Issues

#### Node Won't Sync
```bash
# Force resync from genesis
rm -rf antdchain-data/blocks/*
rm -rf antdchain-data/state/*

# Check peer connections
./antdchain --console
> debug peers
> force sync
```

#### Mining Not Working
```bash
# Check miner address
./antdchain --console
> get miner

# Verify PoW difficulty
> debug pow

# Check block template
> debug mining
```

#### King List Issues
```bash
# Force config sync
./antdchain king sync

# Clean ineligible kings
./antdchain king cleanup

# Reset to defaults
rm -rf antdchain-data/kingdb/*
```

#### Database Corruption
```bash
# Create backup
./antdchain king backup ./king-backup

# Restore from backup
cp -r ./king-backup/* antdchain-data/kingdb/
```

## 🤝 **Contributing**

### Development Setup
```bash
# Clone with submodules
git clone --recursive https://github.com/antdaza/antdchain.git

# Install dependencies
go mod download

# Run tests
go test ./...

# Build development version
make dev-build
```

### Code Structure
```
antdchain/
├── cmd/antdchain/          # Main executable
├── antdc/                  # Core blockchain implementation
│   ├── chain/            # Blockchain logic
│   ├── p2p/              # Network layer
│   ├── mining/           # PoW implementation
│   ├── tx/               # Transaction system
│   ├── wallet/           # Wallet management
│   ├── rotatingking/     # King governance
│   └── vm/               # EVM implementation
├── console/              # Interactive console
└── web/                  # Web interface
```

### Testing
```bash
# Run unit tests
go test ./antdc/...

# Run integration tests
go test -tags=integration ./...

# Benchmark performance
go test -bench=. ./antdc/chain/...
```

## 📚 **Documentation**

### Further Reading
- [Whitepaper](docs/whitepaper.md) - Technical specifications
- [API Documentation](docs/api.md) - Complete API reference
- [Mining Guide](docs/mining.md) - Mining setup and optimization
- [Governance](docs/governance.md) - King system details
- [Security](docs/security.md) - Security best practices

### Community
- **Repository**: https://github.com/antdaza/antdchain
- **Issues**: https://github.com/antdaza/antdchain/issues
- **Discussions**: Community forum (coming soon)

## 📄 **License**

antdchain is released under the **MIT License**. See [LICENSE](LICENSE) for details.

## 🙏 **Acknowledgments**

- Ethereum Foundation for EVM specification
- libp2p team for networking stack
- Go Ethereum for inspiration and tooling
- All contributors and testers

---

**Disclaimer**: ANTDChain is experimental software. Use at your own risk. Always backup your wallets and private keys.

**Version**: v1.0.0 - December 2025

