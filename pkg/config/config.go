package config

import (
	"fmt"
	"os"
	"path/filepath"

	yaml "gopkg.in/yaml.v3"
)

type SASL struct {
	Mechanism    string   `yaml:"mechanism"`
	Username     string   `yaml:"username"`
	Password     string   `yaml:"password"`
	ClientID     string   `yaml:"clientID"`
	ClientSecret string   `yaml:"clientSecret"`
	TokenURL     string   `yaml:"tokenURL"`
	Scopes       []string `yaml:"scopes"`
	Token        string   `yaml:"token"`
	Version      int16    `yaml:"version"`
}

type TLS struct {
	Cafile        string
	Clientfile    string
	Clientkeyfile string
	Insecure      bool
}

type SchemaRegistryCredentials struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type Cluster struct {
	Name                      string
	Version                   string                     `yaml:"version"`
	Brokers                   []string                   `yaml:"brokers"`
	SASL                      *SASL                      `yaml:"SASL"`
	TLS                       *TLS                       `yaml:"TLS"`
	SecurityProtocol          string                     `yaml:"security-protocol"`
	SchemaRegistryURL         string                     `yaml:"schema-registry-url"`
	SchemaRegistryCredentials *SchemaRegistryCredentials `yaml:"schema-registry-credentials"`
}

type Config struct {
	CurrentCluster  string `yaml:"current-cluster"`
	ClusterOverride string `yaml:"-"`
	Clusters        []*Cluster `yaml:"clusters"`
	// configPath is the file path used for reading and writing this config.
	configPath string `yaml:"-"`
}

func (c *Config) HasCluster(name string) bool {
	for _, cluster := range c.Clusters {
		if cluster.Name == name {
			return true
		}
	}
	return false
}

func (c *Config) SetCurrentCluster(name string) error {
	var oldCluster string
	if c.ActiveCluster() != nil {
		oldCluster = c.ActiveCluster().Name
	}
	for _, cluster := range c.Clusters {
		if cluster.Name == name {
			c.CurrentCluster = name

			if err := c.Write(); err != nil {
				// "Revert" change to the cluster struct, either
				// everything is successful or nothing.
				c.CurrentCluster = oldCluster
				return err

			}
			return nil

		}
	}
	return fmt.Errorf("could not find cluster with name %v", name)
}

func (c *Config) ActiveCluster() *Cluster {
	if c == nil {
		return nil
	}

	toSearch := c.ClusterOverride
	if c.ClusterOverride == "" {
		toSearch = c.CurrentCluster
	}

	if toSearch == "" {
		return nil
	}

	for _, cluster := range c.Clusters {
		if cluster.Name == toSearch {
			// Make copy of cluster struct, using a pointer leads to unintended
			// behavior where modifications on currentCluster are written back
			// into the config
			c := *cluster
			return &c
		}
	}
	return nil
}

func (c *Config) Write() error {
	configPath := c.configPath
	if configPath == "" {
		var err error
		configPath, err = getDefaultConfigPath()
		if err != nil {
			return err
		}
	}
	configDir := filepath.Dir(configPath)
	if err := os.MkdirAll(configDir, 0700); err != nil {
		return fmt.Errorf("create config directory: %w", err)
	}

	tmpFile, err := os.CreateTemp(configDir, "config.*.tmp")
	if err != nil {
		return fmt.Errorf("create temp config file: %w", err)
	}
	tmpPath := tmpFile.Name()

	encoder := yaml.NewEncoder(tmpFile)
	if err := encoder.Encode(&c); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("encode config: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("close temp config file: %w", err)
	}
	if err := os.Chmod(tmpPath, 0600); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("chmod temp config file: %w", err)
	}
	if err := os.Rename(tmpPath, configPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename temp config file: %w", err)
	}
	return nil
}

func ReadConfig(cfgPath string) (c Config, err error) {
	resolvedPath, err := resolveConfigPath(cfgPath)
	if err != nil {
		return Config{}, err
	}

	file, err := os.OpenFile(resolvedPath, os.O_RDONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return Config{configPath: resolvedPath}, nil
		}
		return Config{}, fmt.Errorf("open config file: %w", err)
	}
	defer file.Close()
	decoder := yaml.NewDecoder(file)
	err = decoder.Decode(&c)
	if err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	c.configPath = resolvedPath
	return c, nil
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

func resolveConfigPath(cfgPath string) (string, error) {
	if cfgPath == "" {
		return getDefaultConfigPath()
	}
	if !fileExists(cfgPath) {
		return "", fmt.Errorf("config file %q does not exist", cfgPath)
	}
	return cfgPath, nil
}

func getDefaultConfigPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("could not determine home directory: %w", err)
	}

	return filepath.Join(home, ".kaf", "config"), nil
}
