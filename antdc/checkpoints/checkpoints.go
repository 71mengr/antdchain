// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package checkpoints
import (
        "crypto"
        "crypto/ecdsa"
        "crypto/ed25519"
        "crypto/rand"
        "crypto/rsa"
        "crypto/sha256"
        "crypto/x509"
        "encoding/base64"
        "encoding/json"
        "encoding/pem"
        "errors"
        "fmt"
        "io"
        "math/big"
        "net/http"
        "os"
        "path/filepath"
        "sort"
        "strconv"
        "strings"
        "sync"
        "time"

        "github.com/antdaza/antdchain/common"
    //    "github.com/ethereum/go-ethereum/common"
        "github.com/sirupsen/logrus"
        "golang.org/x/crypto/ssh"
)

// Checkpoint represents a signed block checkpoint
type Checkpoint struct {
        Height        uint64         `json:"height"`
        Hash          common.Hash    `json:"hash"`
        ParentHash    common.Hash    `json:"parentHash,omitempty"`
        Miner         common.QuantumAddress `json:"miner"`
        RotatingKing  common.QuantumAddress `json:"rotatingKing"`
        Timestamp     time.Time      `json:"timestamp"`
        TotalDiff     *BigInt        `json:"totalDifficulty,omitempty"`
        GasUsed       uint64         `json:"gasUsed,omitempty"`
        TxCount       int            `json:"txCount,omitempty"`
        StateRoot     common.Hash    `json:"stateRoot,omitempty"`
        TxRoot        common.Hash    `json:"txRoot,omitempty"`
        Signatures    []Signature    `json:"signatures,omitempty"`
        Verifications int            `json:"verifications"`
        Source        string         `json:"source"`
        ChainID       string         `json:"chainId,omitempty"`
        IsGenesis     bool           `json:"isGenesis,omitempty"`
}

// BigInt is a wrapper for JSON marshaling
type BigInt struct {
        *big.Int
}

func (b BigInt) MarshalJSON() ([]byte, error) {
        return json.Marshal(b.String())
}

func (b *BigInt) UnmarshalJSON(p []byte) error {
        var s string
        if err := json.Unmarshal(p, &s); err != nil {
                return err
        }

        var ok bool
        b.Int, ok = new(big.Int).SetString(s, 10)
        if !ok {
                return fmt.Errorf("invalid big integer: %s", s)
        }
        return nil
}

// Signature represents a cryptographic signature
type Signature struct {
        Authority   string    `json:"authority"`
        Algorithm   string    `json:"algorithm"` // "ssh-rsa", "ssh-ed25519", "ecdsa-sha2-nistp256"
        Signature   string    `json:"signature"` // Base64 encoded
        PublicKey   string    `json:"publicKey"` // SSH public key string
        Timestamp   time.Time `json:"timestamp"`
        Fingerprint string    `json:"fingerprint,omitempty"`
}

// ==== REMOTE CONFIGURATION ====

// RemoteConfig defines a remote checkpoint source
type RemoteConfig struct {
        URL           string        `json:"url"`
        Name          string        `json:"name"`
        Priority      int           `json:"priority"`
        Enabled       bool          `json:"enabled"`
        Timeout       time.Duration `json:"timeout"`
        RetryCount    int           `json:"retryCount"`
        PublicKeys    []string      `json:"publicKeys"` // Allowed public keys for verification
        ChainID       string        `json:"chainId"`    // Expected chain ID
        LastModified  time.Time     `json:"lastModified,omitempty"`
        ETag          string        `json:"etag,omitempty"`
}

// Holds local checkpoint data with SSH keys
type LocalConfig struct {
        PrivateKey     string          `json:"privateKey,omitempty"` // PEM encoded private key
        PublicKey      string          `json:"publicKey"`            // SSH public key
        AuthorityName  string          `json:"authorityName"`
        Weight         int             `json:"weight"`
        Checkpoints    []Checkpoint    `json:"checkpoints"`
        TrustedRemotes []RemoteConfig  `json:"trustedRemotes"`
        LastSync       time.Time       `json:"lastSync,omitempty"`
        SyncInterval   time.Duration   `json:"syncInterval"`
        ChainID        string          `json:"chainId,omitempty"`
        GenesisHash    common.Hash     `json:"genesisHash"` // Genesis block hash - must match!
}

// ==== SSH KEY MANAGEMENT ====

// SSHKeyPair holds SSH keys for signing/verification
type SSHKeyPair struct {
        PrivateKey crypto.Signer
        PublicKey  ssh.PublicKey
        PublicStr  string
        Type       string
        Comment    string
}

// ParseSSHPrivateKey parses an SSH private key from PEM data
func ParseSSHPrivateKey(pemData string) (*SSHKeyPair, error) {
        block, _ := pem.Decode([]byte(pemData))
        if block == nil {
                return nil, errors.New("failed to parse PEM block")
        }

        var signer crypto.Signer
        var err error

        switch block.Type {
        case "RSA PRIVATE KEY":
                signer, err = x509.ParsePKCS1PrivateKey(block.Bytes)
        case "EC PRIVATE KEY":
                signer, err = x509.ParseECPrivateKey(block.Bytes)
        case "PRIVATE KEY":
                key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
                if err != nil {
                        return nil, err
                }
                var ok bool
                signer, ok = key.(crypto.Signer)
                if !ok {
                        return nil, errors.New("not a signer private key")
                }
        default:
                return nil, fmt.Errorf("unsupported key type: %s", block.Type)
        }

        if err != nil {
                return nil, err
        }

        // Extract public key
        var sshPubKey ssh.PublicKey
        switch k := signer.Public().(type) {
        case *rsa.PublicKey:
                sshPubKey, err = ssh.NewPublicKey(k)
        case *ecdsa.PublicKey:
                sshPubKey, err = ssh.NewPublicKey(k)
        case ed25519.PublicKey:
                sshPubKey, err = ssh.NewPublicKey(k)
        default:
                return nil, fmt.Errorf("unsupported public key type: %T", k)
        }

        if err != nil {
                return nil, err
        }

        return &SSHKeyPair{
                PrivateKey: signer,
                PublicKey:  sshPubKey,
                PublicStr:  string(ssh.MarshalAuthorizedKey(sshPubKey)),
                Type:       sshPubKey.Type(),
                Comment:    "antdchain-checkpoint-authority",
        }, nil
}

// ParseSSHPublicKey parses an SSH public key from string
func ParseSSHPublicKey(pubKeyStr string) (ssh.PublicKey, error) {
        pubKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(pubKeyStr))
        return pubKey, err
}

type Checkpoints struct {
        mu            sync.RWMutex
        checkpoints   map[uint64]*Checkpoint
        sortedHeights []uint64
        logger        *logrus.Logger
        dataDir       string
        config        *LocalConfig
        keyPair       *SSHKeyPair
        trustedKeys   map[string]ssh.PublicKey
        httpClient    *http.Client
        quitCh        chan struct{}
        syncStatus    SyncStatus
        initialized   bool
        genesisValid  bool
}

type SyncStatus struct {
        LastSyncTime   time.Time `json:"lastSyncTime"`
        LastSyncHeight uint64    `json:"lastSyncHeight"`
        IsSyncing      bool      `json:"isSyncing"`
        SyncErrors     []string  `json:"syncErrors,omitempty"`
        RemoteCount    int       `json:"remoteCount"`
}

// Creates a new checkpoint manager with SSH key support
func NewCheckpoints(dataDir string, configPath string, actualGenesisHash common.Hash) (*Checkpoints, error) {
    logger := logrus.New()
    logger.SetFormatter(&logrus.JSONFormatter{
        TimestampFormat: "2006-01-02T15:04:05.000Z07:00",
    })
    logger.SetLevel(logrus.InfoLevel)

    cp := &Checkpoints{
        checkpoints: make(map[uint64]*Checkpoint),
        logger:      logger,
        dataDir:     dataDir,
        trustedKeys: make(map[string]ssh.PublicKey),
        httpClient: &http.Client{
            Timeout: 30 * time.Second,
            Transport: &http.Transport{
                MaxIdleConns:        10,
                IdleConnTimeout:     90 * time.Second,
                DisableCompression:  false,
                DisableKeepAlives:   false,
                MaxIdleConnsPerHost: 10,
            },
        },
        quitCh: make(chan struct{}),
    }

    // Ensure data directory exists
    if err := os.MkdirAll(dataDir, os.ModePerm); err != nil {
        return nil, fmt.Errorf("failed to create checkpoint data directory: %w", err)
    }

    // Load or create configuration
    if err := cp.loadConfig(configPath); err != nil {
        // If we still failed to load/create config, return error
        return nil, fmt.Errorf("failed to load or create config: %w", err)
    }

    logger.WithFields(logrus.Fields{
        "configGenesisHash": cp.config.GenesisHash.String(),
        "actualGenesisHash": actualGenesisHash.String(),
    }).Info("Setting genesis hash")

    // If config genesis hash is already set and does not match the running chain,
    // fail fast to prevent mixing checkpoints between different chains.
    if cp.config.GenesisHash != (common.Hash{}) && cp.config.GenesisHash != actualGenesisHash {
        return nil, fmt.Errorf("genesis hash mismatch in checkpoint config: expected %s, got %s", cp.config.GenesisHash.String(), actualGenesisHash.String())
    }

    // Initialize config genesis hash when first run (or empty config).
    if cp.config.GenesisHash == (common.Hash{}) {
        cp.config.GenesisHash = actualGenesisHash
        if err := cp.saveConfig(); err != nil {
            logger.WithError(err).Warn("Failed to save config with genesis hash")
        }
    }

    cp.genesisValid = cp.config.GenesisHash == actualGenesisHash
    logger.WithField("genesisHash", cp.config.GenesisHash.String()).Info("Genesis hash configured")

    //Initialize SSH key (will generate if not exists)
    if err := cp.initSSHKey(); err != nil {
        return nil, fmt.Errorf("failed to init SSH key: %w", err)
    }

    //Load trusted public keys (if any)
    if err := cp.loadTrustedKeys(); err != nil {
        logger.WithError(err).Warn("Failed to load trusted keys")
    }

    //Load existing checkpoints
    cp.loadCheckpointsFromConfig()

    //Ensure we have a genesis checkpoint
    cp.ensureGenesisCheckpoint(actualGenesisHash)

    //Verify all loaded checkpoints
    cp.verifyAllCheckpoints()

    // Start sync if configured
    if cp.config.SyncInterval > 0 {
        go cp.startSyncLoop()
    }

    cp.initialized = true
    logger.WithFields(logrus.Fields{
        "checkpoints":   len(cp.checkpoints),
        "authority":     cp.config.AuthorityName,
        "weight":        cp.config.Weight,
        "keyType":       cp.keyPair.Type,
        "genesisValid":  cp.genesisValid,
        "configPath":    configPath,
    }).Info("Checkpoint manager initialized successfully")

    return cp, nil
}

func (c *Checkpoints) createDefaultConfig() error {
    // Create a minimal default configuration
    config := LocalConfig{
        AuthorityName:  "ANTDChain Node",
        Weight:         100,
        SyncInterval:   1 * time.Hour,
        ChainID:        "antdchain-mainnet",
        TrustedRemotes: []RemoteConfig{},
        Checkpoints:    []Checkpoint{},
    }
    
    c.config = &config
    
    // Save to file
    return c.saveConfig()
}

func (c *Checkpoints) removeInvalidGenesisCheckpoints(actualGenesisHash common.Hash) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Remove any checkpoint at height 0 that doesn't match the actual genesis hash
	if cp, exists := c.checkpoints[0]; exists && cp.Hash != actualGenesisHash {
		delete(c.checkpoints, 0)
		c.logger.WithFields(logrus.Fields{
			"expected": actualGenesisHash.String(),
			"found":    cp.Hash.String(),
		}).Warn("Removed invalid genesis checkpoint")
	}

	// Also clean up config
	var validCheckpoints []Checkpoint
	for _, cp := range c.config.Checkpoints {
		if cp.Height != 0 || cp.Hash == actualGenesisHash {
			validCheckpoints = append(validCheckpoints, cp)
		}
	}
	c.config.Checkpoints = validCheckpoints
}

func (c *Checkpoints) ensureGenesisCheckpoint(actualGenesisHash common.Hash) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if we already have a valid genesis checkpoint
	if cp, exists := c.checkpoints[0]; exists && cp.Hash == actualGenesisHash {
		cp.IsGenesis = true
		return
	}

	minerAddr, err := common.ParseQuantumAddress("0qANA3c85k94LTyTXLGDdEzmLE32b1qhYZF")
	if err != nil {
	    // Fallback to zero address or return error
	    minerAddr = common.QuantumAddress{}
	    c.logger.WithError(err).Error("Failed to parse genesis miner address")
	}

	kingAddr, err := common.ParseQuantumAddress("0qANA3c85k94LTyTXLGDdEzmLE32b1qhYZF")
	if err != nil {
	    kingAddr = common.QuantumAddress{}
	    c.logger.WithError(err).Error("Failed to parse genesis rotating king address")
	}

	// Create a genesis checkpoint
	genesisCp := &Checkpoint{
		Height:        0,
		Hash:          actualGenesisHash,
		Miner:         minerAddr,
		RotatingKing:  kingAddr,
		Timestamp:     time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		Source:        "autogenerated",
		Verifications: 1,
		IsGenesis:     true,
		ChainID:       c.config.ChainID,
	}

	// Sign it if we have a key
	if c.keyPair != nil {
		signature, err := c.signCheckpoint(genesisCp)
		if err == nil {
			genesisCp.Signatures = append(genesisCp.Signatures, *signature)
		}
	}

	c.checkpoints[0] = genesisCp
	c.config.Checkpoints = append([]Checkpoint{*genesisCp}, c.config.Checkpoints...)
	
	c.logger.WithFields(logrus.Fields{
		"hash": genesisCp.Hash.String(),
		"miner": genesisCp.Miner.String()[:8],
	}).Info("Created genesis checkpoint")
}

func (c *Checkpoints) getGenesisHeight() uint64 {
	if cp, exists := c.checkpoints[0]; exists {
		return cp.Height
	}
	return 0
}

// ==== CONFIGURATION MANAGEMENT ====

func (c *Checkpoints) loadConfig(configPath string) error {
    // Try to load from provided path first
    data, err := os.ReadFile(configPath)
    if err != nil {
        // Fall back to default location
        defaultPath := filepath.Join(c.dataDir, "checkpoints.json")
        data, err = os.ReadFile(defaultPath)
        if err != nil {
            // Config file doesn't exist - create a default one
            c.logger.Info("No checkpoint config found, creating default configuration")
            
            // Create default config
            if err := c.createDefaultConfig(); err != nil {
                return fmt.Errorf("failed to create default config: %w", err)
            }
            
            // Try to load the newly created config
            data, err = os.ReadFile(defaultPath)
            if err != nil {
                return fmt.Errorf("failed to read newly created config: %w", err)
            }
        }
    }

    var config LocalConfig
    if err := json.Unmarshal(data, &config); err != nil {
        return fmt.Errorf("failed to parse config: %w", err)
    }

    // Set defaults
    if config.SyncInterval == 0 {
        config.SyncInterval = 1 * time.Hour
    }
    if config.Weight == 0 {
        config.Weight = 100
    }
    if config.ChainID == "" {
        config.ChainID = "antdchain-mainnet"
    }

    c.config = &config
    return nil
}

func (c *Checkpoints) saveConfig() error {
        configPath := filepath.Join(c.dataDir, "checkpoints.json")
        data, err := json.MarshalIndent(c.config, "", "  ")
        if err != nil {
                return err
        }
        return os.WriteFile(configPath, data, 0600)
}

// ==== SSH KEY INITIALIZATION ====
func (c *Checkpoints) initSSHKey() error {
        // If we have a private key in config, use it
        if c.config.PrivateKey != "" {
                keyPair, err := ParseSSHPrivateKey(c.config.PrivateKey)
                if err != nil {
                        return fmt.Errorf("failed to parse private key: %w", err)
                }
                c.keyPair = keyPair
                c.logger.Info("Loaded existing SSH key pair")
                return nil
        }

        // Generate new key pair
        c.logger.Info("Generating new SSH key pair...")

        // Generate RSA key
        privateKey, err := rsa.GenerateKey(rand.Reader, 4096)
        if err != nil {
                return fmt.Errorf("failed to generate RSA key: %w", err)
        }

        // Convert to SSH public key
        sshPubKey, err := ssh.NewPublicKey(&privateKey.PublicKey)
        if err != nil {
                return fmt.Errorf("failed to create SSH public key: %w", err)
        }

        // Save private key in PEM format
        privPEM := &pem.Block{
                Type:  "RSA PRIVATE KEY",
                Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
        }

        c.keyPair = &SSHKeyPair{
                PrivateKey: privateKey,
                PublicKey:  sshPubKey,
                PublicStr:  string(ssh.MarshalAuthorizedKey(sshPubKey)),
                Type:       sshPubKey.Type(),
                Comment:    c.config.AuthorityName,
        }

        // Update config with new keys
        c.config.PrivateKey = string(pem.EncodeToMemory(privPEM))
        c.config.PublicKey = c.keyPair.PublicStr

        // Save config
        if err := c.saveConfig(); err != nil {
                c.logger.WithError(err).Warn("Failed to save config with new keys")
        }

        c.logger.WithFields(logrus.Fields{
                "type":    c.keyPair.Type,
                "comment": c.keyPair.Comment,
        }).Info("Generated new SSH key pair")

        return nil
}

func (c *Checkpoints) loadTrustedKeys() error {
        for _, remote := range c.config.TrustedRemotes {
                for _, pubKeyStr := range remote.PublicKeys {
                        pubKey, err := ParseSSHPublicKey(pubKeyStr)
                        if err != nil {
                                c.logger.WithError(err).Warnf("Failed to parse public key for %s", remote.Name)
                                continue
                        }

                        fingerprint := ssh.FingerprintSHA256(pubKey)
                        c.trustedKeys[fingerprint] = pubKey

                        c.logger.WithFields(logrus.Fields{
                                "remote":      remote.Name,
                                "type":        pubKey.Type(),
                                "fingerprint": fingerprint[:16],
                        }).Debug("Loaded trusted public key")
                }
        }

        c.logger.Infof("Loaded %d trusted public keys", len(c.trustedKeys))
        return nil
}

// ==== CHECKPOINT VERIFICATION ====

func (c *Checkpoints) verifyAllCheckpoints() {
        c.mu.Lock()
        defer c.mu.Unlock()

        validCount := 0
        invalidCount := 0

        for height, cp := range c.checkpoints {
                // Special validation for genesis block
                if height == 0 {
                        if cp.Hash != c.config.GenesisHash {
                                c.logger.WithFields(logrus.Fields{
                                        "height":     height,
                                        "expected":   c.config.GenesisHash.String(),
                                        "actual":     cp.Hash.String(),
                                }).Error("GENESIS CHECKPOINT MISMATCH - REMOVING INVALID CHECKPOINT")
                                delete(c.checkpoints, height)
                                invalidCount++
                                continue
                        }
                        cp.IsGenesis = true
                }

                // Verify signatures if present
                if len(cp.Signatures) > 0 {
                        validSigs := c.verifyCheckpointSignatures(cp)
                        if validSigs == 0 {
                                c.logger.WithField("height", height).Warn("Checkpoint has no valid signatures")
                        }
                }

                validCount++
        }

        c.updateSortedHeights()

        if invalidCount > 0 {
                c.logger.WithFields(logrus.Fields{
                        "valid":   validCount,
                        "invalid": invalidCount,
                }).Warn("Removed invalid checkpoints")

                // Save cleaned config
                go c.saveCheckpointsToConfig()
        }
}

// CHECKPOINT MANAGEMENT
func (c *Checkpoints) loadCheckpointsFromConfig() {
        c.mu.Lock()
        defer c.mu.Unlock()

        for _, cp := range c.config.Checkpoints {
                // Mark genesis block
                if cp.Height == 0 {
                        cp.IsGenesis = true
                }
                c.checkpoints[cp.Height] = &cp
        }
        c.updateSortedHeights()

        c.logger.Infof("Loaded %d checkpoints from config", len(c.config.Checkpoints))
}

func (c *Checkpoints) ValidateBlock(height uint64, blockHash common.Hash) error {

       if c == nil {
         return nil
        }

        if !c.initialized || !c.genesisValid {
                return errors.New("checkpoint manager not properly initialized")
        }

        c.mu.RLock()
        defer c.mu.RUnlock()

        cp, exists := c.checkpoints[height]
        if !exists {
                // No checkpoint for this height
                return nil
        }

        // Special validation for genesis block
        if height == 0 {
                if cp.Hash != blockHash {
                        return fmt.Errorf("GENESIS BLOCK MISMATCH: expected %s, got %s",
                                cp.Hash.String(), blockHash.String())
                }
                return nil
        }

        if cp.Hash != blockHash {
                c.logger.WithFields(logrus.Fields{
                        "height":     height,
                        "expected":   cp.Hash.String(),
                        "received":   blockHash.String(),
                        "signatures": len(cp.Signatures),
                }).Error("Checkpoint mismatch")

                // Verify signatures to ensure checkpoint validity
                validSigs := c.verifyCheckpointSignatures(cp)
                if validSigs > 0 {
                        return fmt.Errorf("checkpoint mismatch at height %d (%d valid signatures)", height, validSigs)
                }

                return fmt.Errorf("checkpoint mismatch at height %d (no valid signatures)", height)
        }

        // Verify signatures if we have them
        if len(cp.Signatures) > 0 {
                validSigs := c.verifyCheckpointSignatures(cp)
                c.logger.WithFields(logrus.Fields{
                        "height":    height,
                        "validSigs": validSigs,
                        "totalSigs": len(cp.Signatures),
                }).Debug("Checkpoint signature verification")
        }

        return nil
}

func (c *Checkpoints) ValidateBlockWithContext(height uint64, blockHash, parentHash common.Hash,
        miner, rotatingKing common.QuantumAddress) error {

        if !c.initialized || !c.genesisValid {
                return errors.New("checkpoint manager not properly initialized")
        }

        c.mu.RLock()
        defer c.mu.RUnlock()

        cp, exists := c.checkpoints[height]
        if !exists {
                return nil
        }

        // Basic hash validation
        if cp.Hash != blockHash {
                return fmt.Errorf("checkpoint hash mismatch at height %d", height)
        }

        // Optional context validation
        if cp.ParentHash != (common.Hash{}) && cp.ParentHash != parentHash {
                c.logger.WithFields(logrus.Fields{
                        "height":   height,
                        "expected": cp.ParentHash.String(),
                        "received": parentHash.String(),
                }).Warn("Parent hash mismatch")
        }

        if cp.Miner != (common.QuantumAddress{}) && cp.Miner != miner {
                c.logger.WithFields(logrus.Fields{
                        "height":   height,
                        "expected": cp.Miner.String(),
                        "received": miner.String(),
                }).Warn("Miner address mismatch")
        }

        if cp.RotatingKing != (common.QuantumAddress{}) && cp.RotatingKing != rotatingKing {
                c.logger.WithFields(logrus.Fields{
                        "height":   height,
                        "expected": cp.RotatingKing.String(),
                        "received": rotatingKing.String(),
                }).Warn("Rotating king mismatch")
        }

        return nil
}

// ==== SIGNATURE MANAGEMENT ====

func (c *Checkpoints) signCheckpoint(cp *Checkpoint) (*Signature, error) {
        // Create message to sign
        message := c.createSignatureMessage(cp)

        // Sign with private key
        var signatureBytes []byte
        var err error

        switch key := c.keyPair.PrivateKey.(type) {
        case *rsa.PrivateKey:
                hashed := sha256.Sum256(message)
                signatureBytes, err = rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, hashed[:])
        case *ecdsa.PrivateKey:
                hashed := sha256.Sum256(message)
                r, s, err := ecdsa.Sign(rand.Reader, key, hashed[:])
                if err == nil {
                        // Pad to 32 bytes each
                        rBytes := r.Bytes()
                        sBytes := s.Bytes()
                        padded := make([]byte, 64)
                        copy(padded[32-len(rBytes):32], rBytes)
                        copy(padded[64-len(sBytes):64], sBytes)
                        signatureBytes = padded
                }
        default:
                return nil, errors.New("unsupported private key type")
        }

        if err != nil {
                return nil, fmt.Errorf("failed to sign: %w", err)
        }

        signature := &Signature{
                Authority:   c.config.AuthorityName,
                Algorithm:   c.keyPair.Type,
                Signature:   base64.StdEncoding.EncodeToString(signatureBytes),
                PublicKey:   c.keyPair.PublicStr,
                Timestamp:   time.Now(),
                Fingerprint: ssh.FingerprintSHA256(c.keyPair.PublicKey),
        }

        return signature, nil
}

func (c *Checkpoints) verifySignature(cp *Checkpoint, sig *Signature) bool {
        // Parse public key
        pubKey, err := ParseSSHPublicKey(sig.PublicKey)
        if err != nil {
                c.logger.WithError(err).Warn("Failed to parse public key")
                return false
        }

        // Verify fingerprint matches
        fingerprint := ssh.FingerprintSHA256(pubKey)
        if sig.Fingerprint != "" && sig.Fingerprint != fingerprint {
                c.logger.WithFields(logrus.Fields{
                        "expected": sig.Fingerprint,
                        "actual":   fingerprint,
                }).Warn("Fingerprint mismatch")
                return false
        }

        // Check if key is trusted
        isTrusted := false
        for trustedFingerprint := range c.trustedKeys {
                if trustedFingerprint == fingerprint {
                        isTrusted = true
                        break
                }
        }

        if !isTrusted && sig.Authority != c.config.AuthorityName {
                c.logger.WithField("fingerprint", fingerprint[:16]).Warn("Untrusted public key")
                return false
        }

        // Create message
        message := c.createSignatureMessage(cp)

        // Decode signature
        sigBytes, err := base64.StdEncoding.DecodeString(sig.Signature)
        if err != nil {
                c.logger.WithError(err).Warn("Failed to decode signature")
                return false
        }

        // Verify signature based on algorithm
        switch pubKey.Type() {
        case ssh.KeyAlgoRSA:
                rsaKey, ok := pubKey.(ssh.CryptoPublicKey).CryptoPublicKey().(*rsa.PublicKey)
                if !ok {
                        c.logger.Warn("Not an RSA public key")
                        return false
                }

                hashed := sha256.Sum256(message)
                err = rsa.VerifyPKCS1v15(rsaKey, crypto.SHA256, hashed[:], sigBytes)
                return err == nil

        case ssh.KeyAlgoECDSA256, ssh.KeyAlgoECDSA384, ssh.KeyAlgoECDSA521:
                ecdsaKey, ok := pubKey.(ssh.CryptoPublicKey).CryptoPublicKey().(*ecdsa.PublicKey)
                if !ok {
                        c.logger.Warn("Not an ECDSA public key")
                        return false
                }

                // ECDSA signatures are 64 bytes (32 r + 32 s)
                if len(sigBytes) != 64 {
                        c.logger.WithField("length", len(sigBytes)).Warn("Invalid ECDSA signature length")
                        return false
                }

                r := new(big.Int).SetBytes(sigBytes[:32])
                s := new(big.Int).SetBytes(sigBytes[32:64])
                hashed := sha256.Sum256(message)

                return ecdsa.Verify(ecdsaKey, hashed[:], r, s)

        case ssh.KeyAlgoED25519:
                edKey, ok := pubKey.(ssh.CryptoPublicKey).CryptoPublicKey().(ed25519.PublicKey)
                if !ok {
                        c.logger.Warn("Not an Ed25519 public key")
                        return false
                }

                return ed25519.Verify(edKey, message, sigBytes)

        default:
                c.logger.WithField("algorithm", pubKey.Type()).Warn("Unsupported algorithm")
                return false
        }
}

func (c *Checkpoints) verifyCheckpointSignatures(cp *Checkpoint) int {
        validSigs := 0
        for _, sig := range cp.Signatures {
                if c.verifySignature(cp, &sig) {
                        validSigs++
                }
        }
        return validSigs
}

func (c *Checkpoints) createSignatureMessage(cp *Checkpoint) []byte {
        // Create deterministic message for signing
        parts := []string{
                strconv.FormatUint(cp.Height, 10),
                cp.Hash.String(),
                cp.Miner.String(),
                cp.RotatingKing.String(),
                strconv.FormatInt(cp.Timestamp.Unix(), 10),
                cp.Source,
        }

        if cp.ChainID != "" {
                parts = append(parts, cp.ChainID)
        }
        if cp.IsGenesis {
                parts = append(parts, "genesis")
        }

        return []byte(strings.Join(parts, "|"))
}

// ==== SYNC WITH REMOTE SOURCES ====

func (c *Checkpoints) startSyncLoop() {
        ticker := time.NewTicker(c.config.SyncInterval)
        defer ticker.Stop()

        for {
                select {
                case <-ticker.C:
                        c.syncWithRemotes()
                case <-c.quitCh:
                        c.logger.Info("Sync loop stopped")
                        return
                }
        }
}

func (c *Checkpoints) syncWithRemotes() {
        if !c.initialized || !c.genesisValid {
                c.logger.Error("Cannot sync: checkpoint manager not properly initialized")
                return
        }

        c.syncStatus.IsSyncing = true
        c.syncStatus.LastSyncTime = time.Now()
        defer func() { c.syncStatus.IsSyncing = false }()

        var errors []string

        for _, remote := range c.config.TrustedRemotes {
                if !remote.Enabled {
                        continue
                }

                c.logger.WithField("remote", remote.Name).Info("Syncing with remote")

                checkpoints, err := c.fetchRemoteCheckpoints(remote)
                if err != nil {
                        msg := fmt.Sprintf("%s: %v", remote.Name, err)
                        errors = append(errors, msg)
                        c.logger.WithError(err).Warn("Failed to fetch from remote")
                        continue
                }

                added := c.processRemoteCheckpoints(checkpoints, remote)
                c.logger.WithFields(logrus.Fields{
                        "remote": remote.Name,
                        "added":  added,
                        "total":  len(checkpoints),
                }).Info("Processed remote checkpoints")
        }

        c.syncStatus.SyncErrors = errors
        c.syncStatus.RemoteCount = len(c.config.TrustedRemotes)

        // Update last sync height
        if latest, exists := c.GetLatestCheckpoint(); exists {
                c.syncStatus.LastSyncHeight = latest.Height
        }

        // Save updated checkpoints
        c.saveCheckpointsToConfig()
}

func (c *Checkpoints) fetchRemoteCheckpoints(remote RemoteConfig) ([]Checkpoint, error) {
        req, err := http.NewRequest("GET", remote.URL, nil)
        if err != nil {
                return nil, err
        }

        // Add headers for caching
        if !remote.LastModified.IsZero() {
                req.Header.Set("If-Modified-Since", remote.LastModified.Format(http.TimeFormat))
        }
        if remote.ETag != "" {
                req.Header.Set("If-None-Match", remote.ETag)
        }

        resp, err := c.httpClient.Do(req)
        if err != nil {
                return nil, err
        }
        defer resp.Body.Close()

        if resp.StatusCode == http.StatusNotModified {
                return nil, nil // No new data
        }

        if resp.StatusCode != http.StatusOK {
                return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
        }

        // Read response
        body, err := io.ReadAll(resp.Body)
        if err != nil {
                return nil, err
        }

        // Parse response
        var response struct {
                Checkpoints []Checkpoint `json:"checkpoints"`
                ChainID     string       `json:"chainId"`
                Timestamp   time.Time    `json:"timestamp"`
                Signatures  []Signature  `json:"signatures,omitempty"`
        }

        if err := json.Unmarshal(body, &response); err != nil {
                return nil, fmt.Errorf("failed to parse response: %w", err)
        }

        // Verify chain ID matches
        if remote.ChainID != "" && response.ChainID != remote.ChainID {
                return nil, fmt.Errorf("chain ID mismatch: expected %s, got %s", remote.ChainID, response.ChainID)
        }

        // Verify response signatures if present
        if len(response.Signatures) > 0 {
                // Create a temporary checkpoint for verification
                tempCp := &Checkpoint{
                        Height:    uint64(len(response.Checkpoints)),
                        Timestamp: response.Timestamp,
                        Source:    "remote-sync",
                        ChainID:   response.ChainID,
                }

                // Verify at least one signature from trusted key
                verified := false
                for _, sig := range response.Signatures {
                        if c.verifySignature(tempCp, &sig) {
                                verified = true
                                break
                        }
                }

                if !verified {
                        return nil, errors.New("no valid trusted signature found in response")
                }
        }

        return response.Checkpoints, nil
}

func (c *Checkpoints) processRemoteCheckpoints(checkpoints []Checkpoint, remote RemoteConfig) int {
        c.mu.Lock()
        defer c.mu.Unlock()

        added := 0
        for _, remoteCp := range checkpoints {
                // Special validation for genesis block
                if remoteCp.Height == 0 {
                        if remoteCp.Hash != c.config.GenesisHash {
                                c.logger.WithFields(logrus.Fields{
                                        "remote":   remote.Name,
                                        "expected": c.config.GenesisHash.String(),
                                        "actual":   remoteCp.Hash.String(),
                                }).Error("Remote genesis hash mismatch - rejecting checkpoint")
                                continue
                        }
                        remoteCp.IsGenesis = true
                }

                // Skip if we already have this checkpoint
                if existing, exists := c.checkpoints[remoteCp.Height]; exists {
                        // If hashes match, increase verification count
                        if existing.Hash == remoteCp.Hash {
                                existing.Verifications++
                                continue
                        }

                        // Conflict - keep the one with more verifications
                        if existing.Verifications > remoteCp.Verifications {
                                continue
                        }
                }

                // Verify signatures before accepting
                validSigs := 0
                for _, sig := range remoteCp.Signatures {
                        if c.verifySignature(&remoteCp, &sig) {
                                validSigs++
                        }
                }

                if validSigs == 0 {
                        c.logger.WithFields(logrus.Fields{
                                "height": remoteCp.Height,
                                "remote": remote.Name,
                        }).Warn("No valid signatures for remote checkpoint")
                        continue
                }

                // Add to our checkpoints
                c.checkpoints[remoteCp.Height] = &remoteCp
                added++
        }

        c.updateSortedHeights()
        return added
}

// ==== CHECKPOINT OPERATIONS ====

func (c *Checkpoints) AddCheckpoint(height uint64, hash, parentHash common.Hash,
        miner, rotatingKing common.QuantumAddress, txCount int, gasUsed uint64) error {

        if !c.initialized || !c.genesisValid {
                return errors.New("checkpoint manager not properly initialized")
        }

        c.mu.Lock()
        defer c.mu.Unlock()

        if hash == (common.Hash{}) {
                return errors.New("invalid hash")
        }

        // Special validation for genesis block
        if height == 0 {
                if hash != c.config.GenesisHash {
                        return fmt.Errorf("genesis hash mismatch: expected %s, got %s",
                                c.config.GenesisHash.String(), hash.String())
                }
        }

        // Check for existing
        if existing, exists := c.checkpoints[height]; exists {
                if existing.Hash == hash {
                        existing.Verifications++
                        return nil
                }

                // Conflict resolution
                if existing.Verifications > 5 {
                        c.logger.WithField("height", height).Warn("Checkpoint conflict, keeping existing")
                        return fmt.Errorf("checkpoint conflict at height %d", height)
                }

                // Replace if existing has low confidence
                delete(c.checkpoints, height)
        }

        // Create new checkpoint
        cp := &Checkpoint{
                Height:        height,
                Hash:          hash,
                ParentHash:    parentHash,
                Miner:         miner,
                RotatingKing:  rotatingKing,
                Timestamp:     time.Now(),
                GasUsed:       gasUsed,
                TxCount:       txCount,
                Verifications: 1,
                Source:        "local",
                ChainID:       c.config.ChainID,
                IsGenesis:     height == 0,
        }

        // Sign with our key
        signature, err := c.signCheckpoint(cp)
        if err != nil {
                return fmt.Errorf("failed to sign checkpoint: %w", err)
        }

        cp.Signatures = append(cp.Signatures, *signature)
        c.checkpoints[height] = cp
        c.updateSortedHeights()

        // Save to config
        go c.saveCheckpointsToConfig()

        c.logger.WithFields(logrus.Fields{
                "height":   height,
                "hash":     hash.String()[:12],
                "miner":    miner.String()[:8],
                "rotating": rotatingKing.String()[:8],
        }).Info("Added new checkpoint")

        return nil
}

func (c *Checkpoints) GetCheckpoint(height uint64) (*Checkpoint, bool) {
        c.mu.RLock()
        defer c.mu.RUnlock()

        cp, exists := c.checkpoints[height]
        return cp, exists
}

func (c *Checkpoints) GetLatestCheckpoint() (*Checkpoint, bool) {
        c.mu.RLock()
        defer c.mu.RUnlock()

        if len(c.sortedHeights) == 0 {
                return nil, false
        }

        latestHeight := c.sortedHeights[len(c.sortedHeights)-1]
        return c.checkpoints[latestHeight], true
}

func (c *Checkpoints) GetCheckpointsInRange(start, end uint64) []*Checkpoint {
        c.mu.RLock()
        defer c.mu.RUnlock()

        var result []*Checkpoint
        for _, height := range c.sortedHeights {
                if height >= start && height <= end {
                        result = append(result, c.checkpoints[height])
                }
        }
        return result
}

func (c *Checkpoints) updateSortedHeights() {
        heights := make([]uint64, 0, len(c.checkpoints))
        for height := range c.checkpoints {
                heights = append(heights, height)
        }
        sort.Slice(heights, func(i, j int) bool {
                return heights[i] < heights[j]
        })
        c.sortedHeights = heights
}

// ==== PERSISTENCE ====
func (c *Checkpoints) saveCheckpointsToConfig() {
        c.mu.RLock()
        checkpoints := make([]Checkpoint, 0, len(c.checkpoints))
        for _, cp := range c.checkpoints {
                checkpoints = append(checkpoints, *cp)
        }
        c.mu.RUnlock()

        // Update config
        c.config.Checkpoints = checkpoints
        c.config.LastSync = time.Now()

        // Save to file
        if err := c.saveConfig(); err != nil {
                c.logger.WithError(err).Error("Failed to save checkpoints")
        }
}

// ==== UTILITIES ====
func (c *Checkpoints) ExportCheckpoints() ([]byte, error) {
        c.mu.RLock()
        defer c.mu.RUnlock()

        checkpoints := make([]Checkpoint, 0, len(c.checkpoints))
        for _, cp := range c.checkpoints {
                checkpoints = append(checkpoints, *cp)
        }

        response := struct {
                Checkpoints []Checkpoint `json:"checkpoints"`
                ChainID     string       `json:"chainId"`
                Timestamp   time.Time    `json:"timestamp"`
                Signatures  []Signature  `json:"signatures"`
                GenesisHash common.Hash  `json:"genesisHash"`
        }{
                Checkpoints: checkpoints,
                ChainID:     c.config.ChainID,
                Timestamp:   time.Now(),
                GenesisHash: c.config.GenesisHash,
        }

        // Sign the entire export
        signature, err := c.signCheckpoint(&Checkpoint{
                Height:    uint64(len(checkpoints)),
                Timestamp: response.Timestamp,
                Source:    "export",
                ChainID:   c.config.ChainID,
        })
        if err == nil {
                response.Signatures = []Signature{*signature}
        }

        return json.MarshalIndent(response, "", "  ")
}

func (c *Checkpoints) GetStats() map[string]interface{} {
        c.mu.RLock()
        defer c.mu.RUnlock()

        stats := map[string]interface{}{
                "totalCheckpoints": len(c.checkpoints),
                "authority":        c.config.AuthorityName,
                "weight":           c.config.Weight,
                "keyType":          c.keyPair.Type,
                "trustedKeys":      len(c.trustedKeys),
                "remotes":          len(c.config.TrustedRemotes),
                "syncStatus":       c.syncStatus,
                "initialized":      c.initialized,
                "genesisValid":     c.genesisValid,
                "genesisHash":      c.config.GenesisHash.String(),
        }

        if latest, exists := c.GetLatestCheckpoint(); exists {
                stats["latestHeight"] = latest.Height
                stats["latestHash"] = latest.Hash.String()
        }

        return stats
}

func (c *Checkpoints) CanTruncate(height uint64) error {
        if !c.initialized || !c.genesisValid {
                return errors.New("checkpoint manager not properly initialized")
        }

        if latest, exists := c.GetLatestCheckpoint(); exists {
                if height < latest.Height {
                        return fmt.Errorf("cannot truncate below checkpoint at height %d", latest.Height)
                }
        }
        return nil
}

// ==== SHUTDOWN ====
func (c *Checkpoints) Stop() {
        c.logger.Info("Stopping checkpoint manager...")
        close(c.quitCh)

        // Save final state
        if c.initialized && c.genesisValid {
                c.saveCheckpointsToConfig()
        }

        c.logger.Info("Checkpoint manager stopped")
}

func CreateSampleConfig(outputPath string, actualGenesisHash common.Hash) error {
    // Ensure directory exists
    if err := os.MkdirAll(filepath.Dir(outputPath), os.ModePerm); err != nil {
        return fmt.Errorf("failed to create directory: %w", err)
    }

    // Generate a sample key pair
    privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
    if err != nil {
        return err
    }

    // Convert to SSH public key
    sshPubKey, err := ssh.NewPublicKey(&privateKey.PublicKey)
    if err != nil {
        return err
    }

    // Create private key PEM
    privPEM := &pem.Block{
        Type:  "RSA PRIVATE KEY",
        Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
    }

        minerAddr, err := common.ParseQuantumAddress("0qANA3c85k94LTyTXLGDdEzmLE32b1qhYZF")
        if err != nil {
            minerAddr = common.QuantumAddress{}
        }

        kingAddr, err := common.ParseQuantumAddress("0x59910bc89803bD090C298db4C7457075d830094a")
        if err != nil {
            kingAddr = common.QuantumAddress{}
        }

    // Create configuration with actual genesis hash
    config := LocalConfig{
        AuthorityName: "ANTDChain Node",
        Weight:        100,
        PrivateKey:    string(pem.EncodeToMemory(privPEM)),
        PublicKey:     string(ssh.MarshalAuthorizedKey(sshPubKey)),
        SyncInterval:  1 * time.Hour,
        ChainID:       "antdchain-mainnet",
        GenesisHash:   actualGenesisHash,
        Checkpoints: []Checkpoint{
            {
                Height:        0,
                Hash:          actualGenesisHash,
                Miner:         minerAddr,
                RotatingKing:  kingAddr,
                Timestamp:     time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
                Source:        "genesis",
                Verifications: 1,
                IsGenesis:     true,
            },
        },
        TrustedRemotes: []RemoteConfig{
            {
                URL:        "https://checkpoints.antdaza.site/mainnet",
                Name:       "Official ANTDChain Checkpoints",
                Priority:   1,
                Enabled:    true,
                Timeout:    30 * time.Second,
                RetryCount: 3,
                PublicKeys: []string{
                    "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCgEzkBc7eqa1P3CuF3DlqeR1hgor7ulrd745KrL59qleThH/KaEg6cnkhsvDpTzDQpKeHs/MDGUyb/5IoKbKglGPLaPJOiEniwmVN4+dOT7om4EaFVxkAxQecHyKn8XIW6VBnUPqWW8gHdeF9q7E5p247Xec9gqM7XUFwm6N1lm9GM91f4CqBs8GKElS/477fjzceIQBWaRfszt+dKJ7D2xJ8JylMZsstx+6t1vuDTdNiMkvqy0rCgYBdf1ttXFdDHvquku7ARbGqNjKeOtm2dC7AqzklqpRn4AAT/k3FhS2AtUIhWckH5vaB7HQUkQaVRAUGDgDp0GZvgO3ziKxxN",
                },
                ChainID: "antdchain-mainnet",
            },
        },
    }

    data, err := json.MarshalIndent(config, "", "  ")
    if err != nil {
        return err
    }

    return os.WriteFile(outputPath, data, 0600)
}
