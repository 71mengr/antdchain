
package tlsconfig
import (
    "encoding/json"
    "fmt"
    "os"
    "path/filepath"
)

// TLSConfigFile represents TLS configuration in a file
type TLSConfigFile struct {
    EnableTLS    bool   `json:"enable_tls"`
    RPCCertFile  string `json:"rpc_cert_file"`
    RPCKeyFile   string `json:"rpc_key_file"`
    WebCertFile  string `json:"web_cert_file"`
    WebKeyFile   string `json:"web_key_file"`
    AutoGenerate bool   `json:"auto_generate"`
    Domain       string `json:"domain"`
    TLSOnly      bool   `json:"tls_only"`
}

// LoadConfigFile loads TLS configuration from file
func LoadConfigFile(path string) (*TLSConfigFile, error) {
    data, err := os.ReadFile(path)
    if err != nil {
        return nil, fmt.Errorf("failed to read config file: %w", err)
    }
    
    var config TLSConfigFile
    if err := json.Unmarshal(data, &config); err != nil {
        return nil, fmt.Errorf("failed to parse config file: %w", err)
    }
    
    return &config, nil
}

// SaveConfigFile saves TLS configuration to file
func SaveConfigFile(config *TLSConfigFile, path string) error {
    // Create directory if it doesn't exist
    dir := filepath.Dir(path)
    if err := os.MkdirAll(dir, os.ModePerm); err != nil {
        return fmt.Errorf("failed to create directory: %w", err)
    }
    
    data, err := json.MarshalIndent(config, "", "  ")
    if err != nil {
        return fmt.Errorf("failed to marshal config: %w", err)
    }
    
    if err := os.WriteFile(path, data, 0600); err != nil {
        return fmt.Errorf("failed to write config file: %w", err)
    }
    
    return nil
}
