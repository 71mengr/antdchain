
package tlsconfig
import (
    "context"
    "crypto/x509"
    "encoding/pem"
    "fmt"
    "os"
    "time"

    "github.com/sirupsen/logrus"
)

// CertificateMonitor monitors and renews certificates
type CertificateMonitor struct {
    certPath    string
    keyPath     string
    domain      string
    logger      *logrus.Logger
    renewBefore time.Duration
}

// NewCertificateMonitor creates a new certificate monitor
func NewCertificateMonitor(certPath, keyPath, domain string, logger *logrus.Logger) *CertificateMonitor {
    return &CertificateMonitor{
        certPath:    certPath,
        keyPath:     keyPath,
        domain:      domain,
        logger:      logger,
        renewBefore: 30 * 24 * time.Hour, // Renew 30 days before expiry
    }
}

// CheckExpiry checks if certificate needs renewal
func (m *CertificateMonitor) CheckExpiry() (bool, error) {
    data, err := os.ReadFile(m.certPath)
    if err != nil {
        return false, fmt.Errorf("failed to read certificate: %w", err)
    }
    
    block, _ := pem.Decode(data)
    if block == nil {
        return false, fmt.Errorf("invalid certificate format")
    }
    
    cert, err := x509.ParseCertificate(block.Bytes)
    if err != nil {
        return false, fmt.Errorf("failed to parse certificate: %w", err)
    }
    
    // Check if certificate expires soon
    timeUntilExpiry := time.Until(cert.NotAfter)
    if timeUntilExpiry < m.renewBefore {
        m.logger.Warnf("Certificate expires in %v, needs renewal", timeUntilExpiry)
        return true, nil
    }
    
    m.logger.Debugf("Certificate valid for %v", timeUntilExpiry)
    return false, nil
}

// AutoRenew monitors and renews certificates
func (m *CertificateMonitor) AutoRenew(ctx context.Context) {
    ticker := time.NewTicker(24 * time.Hour) // Check daily
    defer ticker.Stop()
    
    for {
        select {
        case <-ctx.Done():
            m.logger.Info("Certificate monitor stopped")
            return
        case <-ticker.C:
            needsRenewal, err := m.CheckExpiry()
            if err != nil {
                m.logger.Errorf("Failed to check certificate expiry: %v", err)
                continue
            }
            
            if needsRenewal {
                m.logger.Info("Renewing expired certificate...")
                if err := GenerateSelfSignedCert(m.domain, m.certPath, m.keyPath); err != nil {
                    m.logger.Errorf("Failed to renew certificate: %v", err)
                } else {
                    m.logger.Info("✓ Certificate renewed successfully")
                }
            }
        }
    }
}
