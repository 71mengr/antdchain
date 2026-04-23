// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.


package wallet
import (
    "crypto/aes"
    "crypto/cipher"
    "crypto/rand"
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "errors"
    "fmt"
    "io"
    "log"
    "math/big"
    "os"
    "strings"
    "path/filepath"
    "runtime"
    "sync"
    "time"

    "golang.org/x/crypto/scrypt"

    "github.com/antdaza/antdchain/antdc/accounts/keystore"
    "github.com/antdaza/antdchain/antdc/chain"
    "github.com/antdaza/antdchain/antdc/crypto/quantum"
    "github.com/antdaza/antdchain/antdc/tx"
    "github.com/antdaza/antdchain/common"
)

// Security constants
const (
    // Encryption parameters
    scryptN          = 1 << 18 // CPU/memory cost (262144)
    scryptR          = 8       // Block size
    scryptP          = 1       // Parallelization
    keyLength        = 32      // AES-256 key length
    saltLength       = 32      // Salt length
    nonceLength      = 12      // GCM nonce length
    authTagLength    = 16      // GCM authentication tag length

    // Security settings
    minPasswordLength  = 8
    maxUnlockTime      = 30 * time.Minute // Auto-lock after 30 minutes
    failedAttemptLimit = 5                 // Max failed unlock attempts
    lockDelayBase      = 5 * time.Second   // Base lock delay for failed attempts
)

// EncryptedWallet represents the encrypted wallet storage format
type EncryptedWallet struct {
    Address      string    `json:"address"`
    EncryptedKey string    `json:"encrypted_key"`
    Salt         string    `json:"salt"`
    Nonce        string    `json:"nonce"`
    KDF          string    `json:"kdf"` // Only "scrypt" supported
    KDFParams    KDFParams `json:"kdf_params"`
    Version      string    `json:"version"`  // Wallet format version
    Checksum     string    `json:"checksum"` // Integrity check
}

// KDFParams contains parameters for key derivation
type KDFParams struct {
    N      int `json:"n"`      // scrypt: CPU/memory cost
    R      int `json:"r"`      // scrypt: block size
    P      int `json:"p"`      // scrypt: parallelization
    KeyLen int `json:"key_len"`
}

// WalletSecurity manages wallet security state
type WalletSecurity struct {
    mu             sync.RWMutex
    failedAttempts map[string]int         // address -> failed unlock attempts
    unlockTime    map[string]time.Time   // address -> unlock timestamp
    lockTimers    map[string]*time.Timer // address -> auto-lock timer
}

// Wallet manages keys and transaction creation
type Wallet struct {
    privKey  []byte
    pubKey   []byte
    addr     common.QuantumAddress
    bc       *chain.Blockchain
    isLocked bool
    security *WalletSecurity
    dataDir  string // Add dataDir to Wallet struct
}

// WalletManager manages wallets and their security state
type WalletManager struct {
    mu       sync.RWMutex
    wallets  map[string]*Wallet // address -> wallet
    locked   map[string]bool    // address -> locked status
    dataDir  string             // Directory to store wallet data
    security *WalletSecurity
    blockchain *chain.Blockchain
}

// NewWalletSecurity creates a new wallet security manager
func NewWalletSecurity() *WalletSecurity {
    return &WalletSecurity{
        failedAttempts: make(map[string]int),
        unlockTime:    make(map[string]time.Time),
        lockTimers:    make(map[string]*time.Timer),
    }
}

// NewWalletManager creates a new wallet manager
func NewWalletManager(dataDir string) *WalletManager {
    return &WalletManager{
        wallets:  make(map[string]*Wallet),
        locked:   make(map[string]bool),
        dataDir:  dataDir,
        security: NewWalletSecurity(),
    }
}

// DataDir returns the wallet manager storage directory.
func (wm *WalletManager) DataDir() string {
    return wm.dataDir
}

func getKeystoreDir() string {
    home, err := os.UserHomeDir()
    if err != nil {
        return filepath.Join(".", ".antdchain", "keystore")
    }

    switch runtime.GOOS {
    case "windows":
        return filepath.Join(home, "AppData", "Local", "Antdchain", "keystore")
    case "darwin":
        return filepath.Join(home, "Library", "Application Support", "Antdchain", "keystore")
    default:
        return filepath.Join(home, ".antdchain", "keystore")
    }
}

// generateKey derives encryption key using scrypt
func generateKey(password string, salt []byte) ([]byte, error) {
    if len(password) < minPasswordLength {
        return nil, fmt.Errorf("password must be at least %d characters", minPasswordLength)
    }
    return scrypt.Key([]byte(password), salt, scryptN, scryptR, scryptP, keyLength)
}

// encryptData encrypts data using AES-256-GCM
func encryptData(data, key []byte) ([]byte, []byte, error) {
    block, err := aes.NewCipher(key)
    if err != nil {
        return nil, nil, fmt.Errorf("failed to create AES cipher: %w", err)
    }

    gcm, err := cipher.NewGCM(block)
    if err != nil {
        return nil, nil, fmt.Errorf("failed to create GCM mode: %w", err)
    }

    nonce := make([]byte, gcm.NonceSize())
    if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
        return nil, nil, fmt.Errorf("failed to generate nonce: %w", err)
    }

    ciphertext := gcm.Seal(nil, nonce, data, nil)
    return ciphertext, nonce, nil
}

// decryptData decrypts data using AES-256-GCM
func decryptData(ciphertext, key, nonce []byte) ([]byte, error) {
    if len(key) != keyLength {
        return nil, errors.New("decryption key must be 32 bytes for AES-256")
    }
    if len(ciphertext) < authTagLength {
        return nil, errors.New("encrypted data too short")
    }
    if len(nonce) != nonceLength {
        return nil, errors.New("invalid nonce size")
    }

    block, err := aes.NewCipher(key)
    if err != nil {
        return nil, fmt.Errorf("failed to create AES cipher: %w", err)
    }

    gcm, err := cipher.NewGCM(block)
    if err != nil {
        return nil, fmt.Errorf("failed to create GCM mode: %w", err)
    }

    plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
    if err != nil {
        return nil, fmt.Errorf("decryption failed: %w", err)
    }

    return plaintext, nil
}

// calculateChecksum creates a checksum for data integrity
func calculateChecksum(data []byte) string {
    hash := sha256.Sum256(data)
    return hex.EncodeToString(hash[:])
}

// NewWallet creates a new wallet with a generated keypair
func NewWallet(bc *chain.Blockchain, dataDir string) (*Wallet, error) {
    privKeyBytes, pubKeyBytes, err := quantum.GenerateKeyPair()
    if err != nil {
        return nil, fmt.Errorf("failed to generate key: %w", err)
    }
    addr, err := common.ParseQuantumAddress(quantum.PubKeyToAddress(pubKeyBytes))
    if err != nil {
        return nil, fmt.Errorf("failed to parse address: %w", err)
    }
    return &Wallet{
        privKey:  privKeyBytes,
        pubKey:   pubKeyBytes,
        addr:     addr,
        bc:       bc,
        dataDir:  dataDir, // Store dataDir
        isLocked: true,
        security: NewWalletSecurity(),
    }, nil
}

// NewWalletWithKey creates a wallet with an existing private key
func NewWalletWithKey(bc *chain.Blockchain, privKey []byte, dataDir string) (*Wallet, error) {
	if len(privKey) == 0 {
		return nil, errors.New("private key cannot be nil")
	}
	pubKey, err := quantum.DerivePublicKey(privKey)
	if err != nil {
		return nil, fmt.Errorf("failed to derive public key: %w", err)
	}
	addr, err := common.ParseQuantumAddress(quantum.PubKeyToAddress(pubKey))
	if err != nil {
		return nil, fmt.Errorf("failed to parse address: %w", err)
	}
	return &Wallet{
		privKey:  append([]byte(nil), privKey...),
		pubKey:   pubKey,
		addr:     addr,
		bc:       bc,
		dataDir:  dataDir, // Store dataDir
		isLocked: true,
		security: NewWalletSecurity(),
	}, nil
}

// NewWalletWithAddress creates a locked wallet placeholder from an address.
func NewWalletWithAddress(bc *chain.Blockchain, addr common.QuantumAddress, dataDir string) *Wallet {
	return &Wallet{
		addr:     addr,
		bc:       bc,
		dataDir:  dataDir,
		isLocked: true,
		security: NewWalletSecurity(),
	}
}

// Address returns the wallet address
func (w *Wallet) Address() common.QuantumAddress {
    return w.addr
}

// PrivateKey returns the wallet's private key if unlocked
func (w *Wallet) PrivateKey() ([]byte, error) {
    if w.isLocked {
        return nil, errors.New("wallet is locked")
    }
    return append([]byte(nil), w.privKey...), nil
}

// IsLocked returns whether the wallet is locked
func (w *Wallet) IsLocked() bool {
    return w.isLocked
}

// Lock locks the wallet
func (w *Wallet) Lock() {
    w.isLocked = true
    w.privKey = nil // Clear private key from memory
    w.security.clearSecurityState(w.addr.String())
}

// Unlock unlocks the wallet with a private key
func (w *Wallet) Unlock(privKey []byte) error {
    if len(privKey) == 0 {
        return errors.New("private key cannot be nil")
    }
    pubKey, err := quantum.DerivePublicKey(privKey)
    if err != nil {
        return fmt.Errorf("failed to derive public key: %w", err)
    }
    addr, err := common.ParseQuantumAddress(quantum.PubKeyToAddress(pubKey))
    if err != nil {
        return fmt.Errorf("failed to parse address: %w", err)
    }
    if addr != w.addr {
        return errors.New("private key does not match wallet address")
    }
    w.privKey = append([]byte(nil), privKey...)
    w.pubKey = pubKey
    w.isLocked = false
    return nil
}

// Nonce returns the next valid nonce considering both blockchain state and pending transactions
func (w *Wallet) Nonce() uint64 {
    if w.bc == nil {
        log.Printf("⚠️  Blockchain is nil in wallet Nonce() method")
        return 0
    }

    state := w.bc.State()
    if state == nil {
        log.Printf("⚠️  State is nil in wallet Nonce() method")
        return 0
    }

    // Get nonce from blockchain state
    stateNonce := state.GetNonce(common.BytesToQuantumAddress(w.addr.Bytes()))
    
    // Check if there are pending transactions in the pool
    txPool := w.bc.TxPool()
    if txPool == nil {
        log.Printf("⚠️  Transaction pool is nil in wallet Nonce() method")
        return stateNonce
    }

    // Get pending transactions for this address
    pendingTxs := txPool.GetPendingTransactionsByNonce(common.BytesToQuantumAddress(w.addr.Bytes()))
    
    var nextNonce uint64
    
    if len(pendingTxs) > 0 {
        // We have pending transactions - use the highest nonce + 1
        highestPending := pendingTxs[len(pendingTxs)-1].Nonce
        if highestPending >= stateNonce {
            nextNonce = highestPending + 1
        } else {
            nextNonce = stateNonce
        }
        log.Printf("🔍 Wallet.Nonce() for %s: state=%d, pending=%d, using=%d", 
            w.addr.String(), stateNonce, len(pendingTxs), nextNonce)
    } else {
        // No pending transactions - use state nonce
        nextNonce = stateNonce
        log.Printf("🔍 Wallet.Nonce() for %s: state=%d, no pending, using=%d", 
         w.addr.String(), stateNonce, nextNonce)
    }

    return nextNonce
}

// CreateTx creates and signs a transaction with proper nonce handling
func (w *Wallet) CreateTx(to common.QuantumAddress, value *big.Int, data []byte, gas uint64, gasPrice *big.Int) (*tx.Tx, error) {
    if w.isLocked {
        return nil, errors.New("wallet is locked")
    }

    // Get the correct nonce considering both state and pending transactions
    nonce := w.Nonce()
    log.Printf("🔍 Creating transaction: from=%s, to=%s, nonce=%d, value=%s",
        w.addr.String(), to.String(), nonce, value.String())

    if gas == 0 {
        gas = 21000
    }
    if gasPrice == nil {
        gasPrice = big.NewInt(1e9) // 1 Gwei
    }

    t := tx.NewTx(w.addr, to, value, data, nonce, gas, gasPrice)
    if err := t.Sign(w.privKey, w.pubKey); err != nil {
        return nil, fmt.Errorf("failed to sign transaction: %w", err)
    }

    log.Printf("✅ Transaction created: hash=%s, nonce=%d", t.Hash().Hex(), nonce)
    return t, nil
}

// Encrypt encrypts the wallet's private key
func (w *Wallet) Encrypt(password string) (*EncryptedWallet, error) {
    // If wallet is locked, we need the private key from storage
    privKey := w.privKey
    if privKey == nil {
        // Load from encrypted storage
        walletFile := filepath.Join(w.dataDir, "wallets.encrypted.json") // Use w.dataDir instead of w.bc.DataDir()
        data, err := os.ReadFile(walletFile)
        if err != nil && !os.IsNotExist(err) {
            return nil, fmt.Errorf("failed to read wallet file: %w", err)
        }
        if os.IsNotExist(err) {
            // If no wallet file exists, but wallet is locked, we can't encrypt without the key
            return nil, errors.New("cannot encrypt locked wallet without existing encrypted data")
        }

        var encryptedWallets []EncryptedWallet
        if err := json.Unmarshal(data, &encryptedWallets); err != nil {
            return nil, fmt.Errorf("invalid wallet file format: %w", err)
        }

        var encrypted *EncryptedWallet
        for _, ew := range encryptedWallets {
            if ew.Address == w.addr.String() {
                encrypted = &ew
                break
            }
        }
        if encrypted == nil {
            return nil, fmt.Errorf("no encrypted wallet found for %s", w.addr.String())
        }

        decryptedWallet, err := Decrypt(encrypted, password, w.bc, w.dataDir)
        if err != nil {
            return nil, fmt.Errorf("failed to decrypt wallet for encryption: %w", err)
        }
        privKey = decryptedWallet.privKey
    }

    privKeyBytes := append([]byte(nil), privKey...)
    salt := make([]byte, saltLength)
    if _, err := rand.Read(salt); err != nil {
        return nil, fmt.Errorf("failed to generate salt: %w", err)
    }

    key, err := generateKey(password, salt)
    if err != nil {
        return nil, fmt.Errorf("failed to derive key: %w", err)
    }

    ciphertext, nonce, err := encryptData(privKeyBytes, key)
    if err != nil {
        return nil, fmt.Errorf("failed to encrypt private key: %w", err)
    }

    encrypted := &EncryptedWallet{
        Address:      w.addr.String(),
        EncryptedKey: hex.EncodeToString(ciphertext),
        Salt:         hex.EncodeToString(salt),
        Nonce:        hex.EncodeToString(nonce),
        KDF:          "scrypt",
        KDFParams: KDFParams{
            N:      scryptN,
            R:      scryptR,
            P:      scryptP,
            KeyLen: keyLength,
        },
        Version: "1.0",
    }

    checksumData := []byte(encrypted.Address + encrypted.EncryptedKey + encrypted.Salt + encrypted.Nonce)
    encrypted.Checksum = calculateChecksum(checksumData)
    return encrypted, nil
}

// Decrypt decrypts an encrypted wallet
func Decrypt(encrypted *EncryptedWallet, password string, bc *chain.Blockchain, dataDir string) (*Wallet, error) {
    checksumData := []byte(encrypted.Address + encrypted.EncryptedKey + encrypted.Salt + encrypted.Nonce)
    if calculateChecksum(checksumData) != encrypted.Checksum {
        return nil, errors.New("wallet data integrity check failed")
    }

    ciphertext, err := hex.DecodeString(encrypted.EncryptedKey)
    if err != nil {
        return nil, fmt.Errorf("failed to decode encrypted key: %w", err)
    }
    salt, err := hex.DecodeString(encrypted.Salt)
    if err != nil {
        return nil, fmt.Errorf("failed to decode salt: %w", err)
    }
    nonce, err := hex.DecodeString(encrypted.Nonce)
    if err != nil {
        return nil, fmt.Errorf("failed to decode nonce: %w", err)
    }

    key, err := generateKey(password, salt)
    if err != nil {
        return nil, fmt.Errorf("failed to derive key: %w", err)
    }

    privKeyBytes, err := decryptData(ciphertext, key, nonce)
    if err != nil {
        return nil, errors.New("decryption failed: wrong password or corrupted data")
    }

    pubKey, err := quantum.DerivePublicKey(privKeyBytes)
    if err != nil {
        return nil, fmt.Errorf("failed to derive public key: %w", err)
    }

    addr, err := common.ParseQuantumAddress(quantum.PubKeyToAddress(pubKey))
    if err != nil {
        return nil, fmt.Errorf("failed to parse address: %w", err)
    }
    if addr.String() != encrypted.Address {
        return nil, errors.New("address verification failed")
    }

    return &Wallet{
        privKey:  privKeyBytes,
        pubKey:   pubKey,
        addr:     addr,
        bc:       bc,
        dataDir:  dataDir,
        isLocked: true,
        security: NewWalletSecurity(),
    }, nil
}

// AddWallet adds a wallet to the manager
func (wm *WalletManager) AddWallet(addr string, w *Wallet) {
    wm.mu.Lock()
    defer wm.mu.Unlock()
    wm.wallets[addr] = w
    wm.locked[addr] = w.IsLocked()
    log.Printf("Added wallet: %s", addr)
}

// GetWallet retrieves a wallet by address
func (wm *WalletManager) GetWallet(addr string) *Wallet {
    wm.mu.RLock()
    defer wm.mu.RUnlock()
    return wm.wallets[addr]
}

// IsLocked checks if a wallet is locked
func (wm *WalletManager) IsLocked(addr string) bool {
    wm.mu.RLock()
    defer wm.mu.RUnlock()
    return wm.locked[addr]
}

// Lock locks a wallet
func (wm *WalletManager) Lock(addr string) bool {
    wm.mu.Lock()
    defer wm.mu.Unlock()
    if w, exists := wm.wallets[addr]; exists {
        w.Lock()
        wm.locked[addr] = true
        log.Printf("Locked wallet: %s", addr)
        return true
    }
    return false
}

// Unlock unlocks a wallet with a password
func (wm *WalletManager) Unlock(addr, password string) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	w := wm.wallets[addr]
	if w == nil {
		wm.security.recordFailedAttempt(addr)
		return fmt.Errorf("wallet not found: %s", addr)
	}

	if wm.security.isLockedOut(addr) {
		return fmt.Errorf("wallet temporarily locked due to too many failed attempts")
	}

	parsedAddr, err := common.ParseQuantumAddress(addr)
	if err != nil {
		wm.security.recordFailedAttempt(addr)
		return fmt.Errorf("invalid address: %w", err)
	}

	privKey, err := keystore.Unlock(parsedAddr, password, getKeystoreDir())

	if err != nil {
		wm.security.recordFailedAttempt(addr)
		return fmt.Errorf("failed to decrypt keystore: %w", err)
	}

	// Update wallet with decrypted private key
	if err := w.Unlock(privKey); err != nil {
		wm.security.recordFailedAttempt(addr)
		return fmt.Errorf("failed to unlock wallet: %w", err)
	}

	wm.locked[addr] = false
	wm.security.recordSuccessfulUnlock(addr)

	wm.security.setAutoLockTimer(addr, func() {
		wm.Lock(addr)
		log.Printf("Auto-locked wallet: %s", addr)
	})

	return nil
}

// ListWallets returns a list of wallets with lock status
func (wm *WalletManager) ListWallets() []string {
    wm.mu.RLock()
    defer wm.mu.RUnlock()
    var addresses []string
    for addr, w := range wm.wallets {
        status := "🔓"
        if w.IsLocked() {
            status = "🔒"
        }
        addresses = append(addresses, fmt.Sprintf("%s %s", status, addr))
    }
    if len(addresses) == 0 {
        addresses = append(addresses, "No wallets found")
    }
    return addresses
}

// SaveWallets saves encrypted wallets to a JSON file
func (wm *WalletManager) SaveWallets(password string) error {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	ksDir := getKeystoreDir()
	saved := 0
	for addr, w := range wm.wallets {
		privKey, err := w.PrivateKey()
		if err != nil {
			return fmt.Errorf("wallet %s is locked; unlock it before save", addr)
		}
		if _, err := keystore.ImportAccount(privKey, password, ksDir); err != nil && !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("failed to save wallet %s to keystore: %w", addr, err)
		}
		saved++
	}

	log.Printf("Saved %d encrypted wallets to %s", saved, ksDir)
	return nil
}

// LoadWallets loads and decrypts wallets from a JSON file
func (wm *WalletManager) LoadWallets(chainInterface interface{}, password string) error {
	_ = password
	wm.mu.Lock()
	defer wm.mu.Unlock()
	wm.wallets = make(map[string]*Wallet)
	wm.locked = make(map[string]bool)

	// Type assertion to get the blockchain
	bc, ok := chainInterface.(*chain.Blockchain)
	if !ok {
		return errors.New("invalid chain type, expected *chain.Blockchain")
	}

	addresses, err := keystore.ListAccounts(getKeystoreDir())
	if err != nil {
		return fmt.Errorf("failed to list keystore accounts: %w", err)
	}
	if len(addresses) == 0 {
		return errors.New("wallet file does not exist")
	}

	for _, addr := range addresses {
		w := NewWalletWithAddress(bc, addr, wm.dataDir)
		wm.wallets[addr.String()] = w
		wm.locked[addr.String()] = true
		log.Printf("Loaded wallet: %s", addr.String())
	}

	return nil
}

// GetBalance returns the balance of an address
func (wm *WalletManager) GetBalance(addr common.QuantumAddress) *big.Int {
    if wm.blockchain == nil {
        log.Printf("⚠️  Blockchain not initialized in GetBalance")
        return big.NewInt(0)
    }

    state := wm.blockchain.State()
    if state == nil {
        log.Printf("⚠️  State is nil in GetBalance")
        return big.NewInt(0)
    }

    return state.GetBalance(common.BytesToQuantumAddress(addr.Bytes()))
}

// CreateNewWallet creates a new wallet and adds it to the manager
func (wm *WalletManager) CreateNewWallet() (*Wallet, error) {
    if wm.blockchain == nil {
        return nil, fmt.Errorf("blockchain not initialized")
    }

    w, err := NewWallet(wm.blockchain, wm.dataDir)
    if err != nil {
        return nil, fmt.Errorf("failed to create wallet: %w", err)
    }
    addr := w.Address().String()
    wm.AddWallet(addr, w)
    return w, nil
}

// ImportWallet imports a wallet from a private key
func (wm *WalletManager) ImportWallet(privateKeyHex string) (*Wallet, error) {
    if wm.blockchain == nil {
        return nil, fmt.Errorf("blockchain not initialized")
    }

    privKey, err := hex.DecodeString(strings.TrimPrefix(privateKeyHex, "0x"))
    if err != nil {
        return nil, fmt.Errorf("invalid private key: %w", err)
    }
    w, err := NewWalletWithKey(wm.blockchain, privKey, wm.dataDir)
    if err != nil {
        return nil, fmt.Errorf("failed to import wallet: %w", err)
    }
    addr := w.Address().String()
    wm.AddWallet(addr, w)
    return w, nil
}

// ExportWallet exports a wallet's private key
func (wm *WalletManager) ExportWallet(addr, password string) (string, error) {
    w := wm.GetWallet(addr)
    if w == nil {
        return "", fmt.Errorf("wallet not found: %s", addr)
    }
    if w.IsLocked() {
        if err := wm.Unlock(addr, password); err != nil {
            return "", fmt.Errorf("wallet %s is locked and unlock failed: %w", addr, err)
        }
    }
    privKey, err := w.PrivateKey()
    if err != nil {
        return "", fmt.Errorf("failed to get private key: %w", err)
    }
    return hex.EncodeToString(privKey), nil
}

// GetOrCreateMinerWallet returns any existing wallet or creates a new one
func (wm *WalletManager) GetOrCreateMinerWallet() (*Wallet, error) {
    wm.mu.RLock()
    // Safely copy addresses while holding read lock
    var addresses []string
    for addr := range wm.wallets {
        addresses = append(addresses, addr)
    }
    wm.mu.RUnlock()

    // If we have any wallet, return the first one
    if len(addresses) > 0 {
        wm.mu.RLock()
        defer wm.mu.RUnlock()
        if w, exists := wm.wallets[addresses[0]]; exists {
            return w, nil
        }
    }

    // No wallet exists — create one
    return wm.CreateNewWallet()
}

// WalletSecurity methods
func (ws *WalletSecurity) clearSecurityState(addr string) {
    ws.mu.Lock()
    defer ws.mu.Unlock()
    delete(ws.failedAttempts, addr)
    delete(ws.unlockTime, addr)
    if timer, exists := ws.lockTimers[addr]; exists {
        timer.Stop()
        delete(ws.lockTimers, addr)
    }
}

func (ws *WalletSecurity) isLockedOut(addr string) bool {
    ws.mu.RLock()
    defer ws.mu.RUnlock()
    if attempts, exists := ws.failedAttempts[addr]; exists && attempts >= failedAttemptLimit {
        lockoutDuration := lockDelayBase * time.Duration(1<<(attempts-failedAttemptLimit))
        if time.Since(ws.unlockTime[addr]) < lockoutDuration {
            return true
        }
        ws.mu.RUnlock()
        ws.mu.Lock()
        ws.failedAttempts[addr] = 0
        ws.mu.Unlock()
        ws.mu.RLock()
    }
    return false
}

func (ws *WalletSecurity) recordFailedAttempt(addr string) {
    ws.mu.Lock()
    defer ws.mu.Unlock()
    ws.failedAttempts[addr]++
    ws.unlockTime[addr] = time.Now()
    log.Printf("Failed unlock attempt for wallet %s (attempt %d)", addr, ws.failedAttempts[addr])
}

func (ws *WalletSecurity) recordSuccessfulUnlock(addr string) {
    ws.mu.Lock()
    defer ws.mu.Unlock()
    ws.failedAttempts[addr] = 0
    ws.unlockTime[addr] = time.Now()
}

func (ws *WalletSecurity) setAutoLockTimer(addr string, lockFunc func()) {
    ws.mu.Lock()
    defer ws.mu.Unlock()
    if timer, exists := ws.lockTimers[addr]; exists {
        timer.Stop()
    }
    ws.lockTimers[addr] = time.AfterFunc(maxUnlockTime, lockFunc)
}

// SetBlockchain sets the blockchain reference for the wallet manager
func (wm *WalletManager) SetBlockchain(bc *chain.Blockchain) {
    wm.mu.Lock()
    defer wm.mu.Unlock()
    wm.blockchain = bc

    // Also update blockchain reference in all existing wallets
    for _, wallet := range wm.wallets {
        wallet.bc = bc
    }

    log.Printf("✅ Blockchain reference set in wallet manager")
}

// SendTransactionWithNonce sends transaction with proper nonce management
func (wm *WalletManager) SendTransactionWithNonce(from, to common.QuantumAddress, amount *big.Int, password string, nonce uint64) (*tx.Tx, error) {
    addr := from.String()
    w := wm.GetWallet(addr)
    if w == nil {
        return nil, fmt.Errorf("wallet not found: %s", addr)
    }

    // Unlock wallet if needed
    if w.IsLocked() {
        if password == "" {
            return nil, fmt.Errorf("wallet %s is locked and no password provided", addr)
        }
        if err := wm.Unlock(addr, password); err != nil {
            return nil, fmt.Errorf("wallet %s is locked and unlock failed: %w", addr, err)
        }
        defer wm.Lock(addr)
    }

    // Get the correct nonce
    var nextNonce uint64
    if nonce == 0 {
        // Auto nonce selection
        nextNonce = w.Nonce()
    } else {
        // Manual nonce specified
        nextNonce = nonce
    }

    log.Printf("�� SendTransaction: from=%s, using nonce=%d", from.String(), nextNonce)

    // Create transaction with the specified nonce
    transaction, err := w.CreateTx(to, amount, nil, 0, nil)
    if err != nil {
        return nil, fmt.Errorf("failed to create transaction: %w", err)
    }

    // Verify the transaction has the correct nonce
    if transaction.Nonce != nextNonce {
        log.Printf("⚠️  Transaction nonce mismatch: tx has %d, expected %d. Correcting...",
            transaction.Nonce, nextNonce)

        // Recreate transaction with correct nonce
        transaction = tx.NewTx(from, to, amount, nil, nextNonce, 21000, big.NewInt(1e9))
        if err := transaction.Sign(w.privKey, w.pubKey); err != nil {
            return nil, fmt.Errorf("failed to sign corrected transaction: %w", err)
        }
        log.Printf("✅ Recreated transaction with correct nonce: %d", nextNonce)
    }

    // Add to transaction pool
    if err := wm.blockchain.TxPool().AddTx(transaction, wm.blockchain); err != nil {
        // If it's a nonce conflict, suggest the correct nonce
        if strings.Contains(err.Error(), "invalid nonce") || strings.Contains(err.Error(), "already pending") {
            suggestedNonce := w.Nonce()
            return nil, fmt.Errorf("nonce conflict: %w (try nonce %d)", err, suggestedNonce)
        }
        return nil, fmt.Errorf("failed to add transaction to pool: %w", err)
    }

    log.Printf("✅ Transaction sent successfully: %s (nonce: %d)", transaction.Hash().Hex(), nextNonce)
    return transaction, nil
}

// Override the original SendTransaction to use the new nonce management
func (wm *WalletManager) SendTransaction(from, to common.QuantumAddress, amount *big.Int, password string) (*tx.Tx, error) {
    return wm.SendTransactionWithNonce(from, to, amount, password, 0)
}
