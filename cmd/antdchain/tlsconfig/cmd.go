
package tlsconfig
import (
    "crypto/tls"
    "crypto/x509"
    "encoding/pem"
    "fmt"
    "os"
    "path/filepath"

    "github.com/sirupsen/logrus"
    "github.com/urfave/cli/v2"
)

// TLSCommands provides certificate management commands
var TLSCommands = &cli.Command{
    Name:  "tls",
    Usage: "TLS certificate management",
    Subcommands: []*cli.Command{
        {
            Name:  "generate",
            Usage: "Generate TLS certificates",
            Flags: []cli.Flag{
                &cli.StringFlag{
                    Name:     "domain",
                    Value:    "localhost",
                    Usage:    "Domain name for certificate",
                    Required: true,
                },
                &cli.StringFlag{
                    Name:  "output-dir",
                    Value: "./tls",
                    Usage: "Output directory for certificates",
                },
                &cli.StringFlag{
                    Name:  "type",
                    Value: "both",
                    Usage: "Certificate type: rpc, web, or both",
                },
            },
            Action: func(c *cli.Context) error {
                domain := c.String("domain")
                outputDir := c.String("output-dir")
                certType := c.String("type")
                
                logger := logrus.New()
                logger.SetFormatter(&logrus.TextFormatter{})
                
                if err := os.MkdirAll(outputDir, os.ModePerm); err != nil {
                    return fmt.Errorf("failed to create output directory: %w", err)
                }
                
                certs := []struct {
                    name string
                    cert string
                    key  string
                }{}
                
                if certType == "rpc" || certType == "both" {
                    certs = append(certs, struct {
                        name string
                        cert string
                        key  string
                    }{
                        name: "RPC",
                        cert: filepath.Join(outputDir, "rpc.cert"),
                        key:  filepath.Join(outputDir, "rpc.key"),
                    })
                }
                
                if certType == "web" || certType == "both" {
                    certs = append(certs, struct {
                        name string
                        cert string
                        key  string
                    }{
                        name: "Web",
                        cert: filepath.Join(outputDir, "web.cert"),
                        key:  filepath.Join(outputDir, "web.key"),
                    })
                }
                
                for _, cert := range certs {
                    logger.Infof("Generating %s certificate for %s...", cert.name, domain)
                    
                    if err := GenerateSelfSignedCert(domain, cert.cert, cert.key); err != nil {
                        return fmt.Errorf("failed to generate %s certificate: %w", cert.name, err)
                    }
                    
                    logger.Infof("✓ Generated %s certificate: %s", cert.name, cert.cert)
                    logger.Infof("✓ Generated %s private key: %s", cert.name, cert.key)
                }
                
                logger.Info("\n🎉 Certificates generated successfully!")
                logger.Info("To use these certificates with ANTDChain:")
                logger.Infof("  --rpc-tls-cert %s", filepath.Join(outputDir, "rpc.cert"))
                logger.Infof("  --rpc-tls-key %s", filepath.Join(outputDir, "rpc.key"))
                logger.Infof("  --web-tls-cert %s", filepath.Join(outputDir, "web.cert"))
                logger.Infof("  --web-tls-key %s", filepath.Join(outputDir, "web.key"))
                
                return nil
            },
        },
        {
            Name:  "verify",
            Usage: "Verify TLS certificates",
            Flags: []cli.Flag{
                &cli.StringFlag{
                    Name:     "cert",
                    Usage:    "Certificate file to verify",
                    Required: true,
                },
                &cli.StringFlag{
                    Name:  "key",
                    Usage: "Private key file to verify",
                },
            },
            Action: func(c *cli.Context) error {
                certFile := c.String("cert")
                keyFile := c.String("key")
                
                logger := logrus.New()
                logger.SetFormatter(&logrus.TextFormatter{})
                
                logger.Infof("Verifying certificate: %s", certFile)
                
                // Load certificate
                if keyFile != "" {
                    cert, err := tls.LoadX509KeyPair(certFile, keyFile)
                    if err != nil {
                        return fmt.Errorf("failed to load certificate pair: %w", err)
                    }
                    
                    logger.Info("✓ Certificate and key pair are valid")
                    
                    // Parse certificate for details
                    x509Cert, err := x509.ParseCertificate(cert.Certificate[0])
                    if err != nil {
                        return fmt.Errorf("failed to parse certificate: %w", err)
                    }
                    
                    logger.Infof("  Subject: %s", x509Cert.Subject.CommonName)
                    logger.Infof("  Issuer: %s", x509Cert.Issuer.CommonName)
                    logger.Infof("  Valid from: %s", x509Cert.NotBefore.Format("2006-01-02"))
                    logger.Infof("  Valid until: %s", x509Cert.NotAfter.Format("2006-01-02"))
                    logger.Infof("  DNS Names: %v", x509Cert.DNSNames)
                    
                } else {
                    // Just verify certificate file
                    data, err := os.ReadFile(certFile)
                    if err != nil {
                        return fmt.Errorf("failed to read certificate file: %w", err)
                    }
                    
                    block, _ := pem.Decode(data)
                    if block == nil || block.Type != "CERTIFICATE" {
                        return fmt.Errorf("file does not contain a valid PEM certificate")
                    }
                    
                    logger.Info("✓ Certificate file format is valid")
                }
                
                return nil
            },
        },
    },
}
