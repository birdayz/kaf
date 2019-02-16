package kaf

import (
	"fmt"
	"os"
	"path/filepath"

	homedir "github.com/mitchellh/go-homedir"
	yaml "gopkg.in/yaml.v2"
)

type SASL struct {
	Mechanism string
	Username  string
	Password  string
}

type Cluster struct {
	Name             string
	Brokers          []string `yaml:"brokers"`
	SASL             *SASL    `yaml:"SASL"`
	SecurityProtocol string   `yaml:"security-protocol"`
}

type Config struct {
	CurrentCluster string     `yaml:"current-cluster"`
	Clusters       []*Cluster `yaml:"clusters"`
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
	return fmt.Errorf("Could not find cluster with name %v", name)
}

func (c *Config) ActiveCluster() *Cluster {
	if c == nil || c.CurrentCluster == "" {
		return nil
	}

	for _, cluster := range c.Clusters {
		if cluster.Name == c.CurrentCluster {
			return cluster
		}
	}
	return nil
}

func (c *Config) Write() error {
	home, err := homedir.Dir()
	if err != nil {
		return err
	}

	configPath := filepath.Join(home, ".kaf", "config")
	file, err := os.OpenFile(configPath, os.O_TRUNC|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}

	encoder := yaml.NewEncoder(file)
	return encoder.Encode(&c)
}

func ReadConfig() (c Config, err error) {
	file, err := os.OpenFile(getDefaultConfigPath(), os.O_RDONLY, 0644)
	if err != nil {
		return Config{}, err
	}
	decoder := yaml.NewDecoder(file)
	err = decoder.Decode(&c)
	if err != nil {
		return Config{}, err
	}
	return c, nil
}

func getDefaultConfigPath() string {
	home, err := homedir.Dir()
	if err != nil {
		panic(err)
	}

	return filepath.Join(home, ".kaf", "config")
}
