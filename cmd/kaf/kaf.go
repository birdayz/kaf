// Copyright © 2018 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"strings"

	sarama "github.com/birdayz/sarama"
	"github.com/magiconair/properties"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

func getConfig() (config *sarama.Config) {
	config = sarama.NewConfig()
	config.Version = sarama.V1_0_0_0
	config.Producer.Return.Successes = true

	if viper.IsSet("saslUsername") {
		config.Net.TLS.Enable = true
		config.Net.SASL.Enable = true
		config.Net.SASL.User = viper.GetString("saslUsername")
		config.Net.SASL.Password = viper.GetString("saslPassword")
	}

	return config
}

var rootCmd = &cobra.Command{
	Use:   "kaf",
	Short: "Kafka CLI",
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.kafka/config)")
}

func extractValue(key, input string) (unquoted string, ok bool) {
	if strings.HasPrefix(input, key+"=") {
		return strings.TrimRight(strings.Replace(strings.TrimPrefix(input, key+"="), "\"", "", -1), ";"), true
	}
	return
}

func tryParseConfluentCloud(path string) (username, password, host string, err error) {
	p := properties.MustLoadFile(path, properties.UTF8)
	saslJaasConfig := p.MustGetString("sasl.jaas.config")

	words := strings.Split(saslJaasConfig, " ")

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

	host = p.MustGetString("bootstrap.servers")

	return
}

var ccloudSubpath = filepath.Join(".ccloud", "config")

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	home, err := homedir.Dir()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	ccloudPath := filepath.Join(home, ccloudSubpath)
	if _, err := os.Stat(ccloudPath); err == nil {
		ccloudUser, ccloudPassword, host, err := tryParseConfluentCloud(ccloudPath)
		if err == nil {
			viper.Set("saslUsername", ccloudUser)
			viper.Set("saslPassword", ccloudPassword)
			viper.Set("brokers", []string{host})

		}
	} else {
		fmt.Println(ccloudPath, err)
	}

	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Search config in home directory with name ".x" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName("config")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
