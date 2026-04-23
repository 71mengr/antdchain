
package tlsconfig
import (
    "crypto/rand"
    "crypto/rsa"
    "crypto/tls"
    "crypto/x509"
    "crypto/x509/pkix"
    "encoding/pem"
    "fmt"
    "math/big"
    "net"
    "os"
    "path/filepath"
    "time"

    "github.com/sirupsen/logrus"
)

// TLSConfig holds TLS configuration
type TLSConfig struct {
    EnableTLS      bool
    RPCCertFile    string
    RPCKeyFile     string
    WebCertFile    string
    WebKeyFile     string
    AutoGenerate   bool
    Domain         string
    TLSOnly        bool
    MinVersion     uint16
    CipherSuites   []uint16
}

// DefaultTLSConfig returns secure default TLS configuration
func DefaultTLSConfig() TLSConfig {
    return TLSConfig{
        EnableTLS:    false,
        MinVersion:   tls.VersionTLS12,
        CipherSuites: []uint16{
            tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
            tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
            tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
            tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
            tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
        },
    }
}

// GenerateSelfSignedCert creates a self-signed certificate for development
func GenerateSelfSignedCert(domain, certPath, keyPath string) error {
    // Create directory if it doesn't exist
    certDir := filepath.Dir(certPath)
    if err := os.MkdirAll(certDir, os.ModePerm); err != nil {
        return fmt.Errorf("failed to create certificate directory: %w", err)
    }

    // Generate private key
    priv, err := rsa.GenerateKey(rand.Reader, 2048)
    if err != nil {
        return fmt.Errorf("failed to generate private key: %w", err)
    }

    // Create certificate template
    template := x509.Certificate{
        SerialNumber: big.NewInt(1),
        Subject: pkix.Name{
            Organization: []string{"ANTDChain Development"},
            CommonName:   domain,
        },
        NotBefore:             time.Now(),
        NotAfter:              time.Now().Add(365 * 24 * time.Hour), // 1 year
        KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
        ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
        BasicConstraintsValid: true,
        DNSNames:              []string{domain, "localhost"},
        IPAddresses: []net.IP{
            net.IPv4(127, 0, 0, 1),
            net.IPv6loopback,
        },
    }

    // Create certificate
    derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
    if err != nil {
        return fmt.Errorf("failed to create certificate: %w", err)
    }

    // Write certificate file
    certOut, err := os.Create(certPath)
    if err != nil {
        return fmt.Errorf("failed to open %s for writing: %w", certPath, err)
    }
    defer certOut.Close()

    if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
        return fmt.Errorf("failed to write certificate data: %w", err)
    }

    // Write private key file
    keyOut, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
    if err != nil {
        return fmt.Errorf("failed to open %s for writing: %w", keyPath, err)
    }
    defer keyOut.Close()

    privBytes, err := x509.MarshalPKCS8PrivateKey(priv)
    if err != nil {
        return fmt.Errorf("failed to marshal private key: %w", err)
    }

    if err := pem.Encode(keyOut, &pem.Block{Type: "PRIVATE KEY", Bytes: privBytes}); err != nil {
        return fmt.Errorf("failed to write private key data: %w", err)
    }

    return nil
}

// LoadOrGenerateCert loads existing certificate or generates a new one
func LoadOrGenerateCert(certPath, keyPath string, autoGenerate bool, domain string, logger *logrus.Logger) (tls.Certificate, error) {
    // Check if certificate files exist
    certExists := false
    keyExists := false

    if _, err := os.Stat(certPath); err == nil {
        certExists = true
    }
    if _, err := os.Stat(keyPath); err == nil {
        keyExists = true
    }

    // If both exist, load them
    if certExists && keyExists {
        logger.Infof("Loading existing TLS certificate from %s", certPath)
        return tls.LoadX509KeyPair(certPath, keyPath)
    }

    // If auto-generate is enabled, generate new certificates
    if autoGenerate {
        logger.Infof("Auto-generating self-signed TLS certificate for %s", domain)
        
        // Generate certificates
        if err := GenerateSelfSignedCert(domain, certPath, keyPath); err != nil {
            return tls.Certificate{}, fmt.Errorf("failed to generate certificates: %w", err)
        }
        
        logger.Infof("Generated certificates: %s, %s", certPath, keyPath)
        return tls.LoadX509KeyPair(certPath, keyPath)
    }

    return tls.Certificate{}, fmt.Errorf("certificate files not found and auto-generation disabled")
}

// CreateTLSConfig creates a secure TLS configuration
func CreateTLSConfig(cert tls.Certificate, config TLSConfig) *tls.Config {
    return &tls.Config{
        Certificates: []tls.Certificate{cert},
        MinVersion:   config.MinVersion,
        CipherSuites: config.CipherSuites,
        NextProtos:   []string{"h2", "http/1.1"},
        ClientAuth:   tls.NoClientCert,
        CurvePreferences: []tls.CurveID{
            tls.X25519,
            tls.CurveP256,
        },
    }
}
