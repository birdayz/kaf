package kaf

import (
	"os"

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
	SASL             SASL     `yaml:"SASL"`
	SecurityProtocol string
}

type Config struct {
	Clusters []Cluster `yaml:"clusters"`
}

func ReadConfig(path string) (c *Config, err error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	decoder := yaml.NewDecoder(file)
	err = decoder.Decode(&c)
	if err != nil {
		return nil, err
	}
	return c, nil
}
