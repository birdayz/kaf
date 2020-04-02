package kaf

import (
	"errors"
	"path/filepath"
	"strings"

	"os"

	"github.com/magiconair/properties"
	homedir "github.com/mitchellh/go-homedir"
)

// Default confluent cloud config file path
var defaultCcloudSubpath = filepath.Join(".ccloud", "config")

func TryFindCcloudConfigFile() (string, error) {
	homedir, err := homedir.Dir()
	if err != nil {

		return "", err
	}

	absoluteDefaultPath := filepath.Join(homedir, defaultCcloudSubpath)

	_, err = os.Stat(absoluteDefaultPath)
	if err == nil {

		return absoluteDefaultPath, nil
	}
	return "", os.ErrNotExist
}

func extractValue(key, input string) (unquoted string, ok bool) {
	if strings.HasPrefix(input, key+"=") {
		return strings.TrimRight(strings.Replace(strings.TrimPrefix(input, key+"="), "\"", "", -1), ";"), true
	}
	return
}

// TODO return []string for brokers
func ParseConfluentCloudConfig(path string) (username, password, broker string, err error) {
	p := properties.MustLoadFile(path, properties.UTF8).Map()
	if _, ok := p["sasl.jaas.config"]; !ok {
		err = errors.New("invalid or unsupported confluent cloud config")
		return
	}
	if _, ok := p["bootstrap.servers"]; !ok {
		err = errors.New("invalid or unsupported confluent cloud config")
		return
	}

	words := strings.Split(p["sasl.jaas.config"], " ")

	jaasOk := true
	for _, word := range words {
		if result, ok := extractValue("username", word); ok {
			username = result
			jaasOk = jaasOk && ok
		}
		if result, ok := extractValue("password", word); ok {
			password = result
			jaasOk = jaasOk && ok
		}

	}

	if !jaasOk {
		return "", "", "", errors.New("Could not parse sasl.jaas.config from ccloud")
	}

	broker = p["bootstrap.servers"]

	return
}
