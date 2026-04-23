
package server
import (
    "context"
    "crypto/tls"
    "fmt"
    "net/http"
    "time"

    "github.com/sirupsen/logrus"
    "github.com/antdaza/antdchain/cmd/antdchain/tlsconfig"
)

// TLSServer manages HTTP and HTTPS servers
type TLSServer struct {
    httpServer  *http.Server
    httpsServer *http.Server
    logger      *logrus.Logger
    config      tlsconfig.TLSConfig
}

// NewTLSServer creates a new TLS-enabled server
func NewTLSServer(addr string, handler http.Handler, logger *logrus.Logger, config tlsconfig.TLSConfig) *TLSServer {
    return &TLSServer{
        httpServer: &http.Server{
            Addr:         addr,
            Handler:      handler,
            ReadTimeout:  15 * time.Second,
            WriteTimeout: 15 * time.Second,
            IdleTimeout:  30 * time.Second,
        },
        logger: logger,
        config: config,
    }
}

// SetupHTTPS configures the HTTPS server
func (s *TLSServer) SetupHTTPS(cert tls.Certificate) error {
    tlsConfig := tlsconfig.CreateTLSConfig(cert, s.config)
    
    s.httpsServer = &http.Server{
        Addr:         s.httpServer.Addr,
        Handler:      s.httpServer.Handler,
        TLSConfig:    tlsConfig,
        ReadTimeout:  15 * time.Second,
        WriteTimeout: 15 * time.Second,
        IdleTimeout:  30 * time.Second,
    }
    
    return nil
}

// IsHTTPSConfigured returns true if HTTPS is configured
func (s *TLSServer) IsHTTPSConfigured() bool {
    return s.httpsServer != nil
}

// Start starts the server(s)
func (s *TLSServer) Start() error {
    if s.config.EnableTLS && s.httpsServer != nil {
        s.logger.Infof("Starting HTTPS server on %s", s.httpsServer.Addr)
        
        go func() {
            if err := s.httpsServer.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
                s.logger.Errorf("HTTPS server failed: %v", err)
            }
        }()
        
        // If TLS-only mode, don't start HTTP server
        if s.config.TLSOnly {
            return nil
        }
        
        // Start HTTP server that redirects to HTTPS
        s.logger.Infof("Starting HTTP redirect server on %s", s.httpServer.Addr)
        redirectHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            httpsURL := fmt.Sprintf("https://%s%s", r.Host, r.URL.Path)
            if r.URL.RawQuery != "" {
                httpsURL += "?" + r.URL.RawQuery
            }
            http.Redirect(w, r, httpsURL, http.StatusMovedPermanently)
        })
        
        s.httpServer.Handler = redirectHandler
    }
    
    s.logger.Infof("Starting HTTP server on %s", s.httpServer.Addr)
    return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down all servers
func (s *TLSServer) Shutdown(ctx context.Context) error {
    var err error
    
    if s.httpsServer != nil {
        s.logger.Info("Shutting down HTTPS server...")
        if shutdownErr := s.httpsServer.Shutdown(ctx); shutdownErr != nil {
            err = fmt.Errorf("HTTPS server shutdown error: %w", shutdownErr)
        }
    }
    
    s.logger.Info("Shutting down HTTP server...")
    if shutdownErr := s.httpServer.Shutdown(ctx); shutdownErr != nil {
        if err != nil {
            err = fmt.Errorf("%w, HTTP server shutdown error: %v", err, shutdownErr)
        } else {
            err = fmt.Errorf("HTTP server shutdown error: %w", shutdownErr)
        }
    }
    
    return err
}
