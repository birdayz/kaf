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
	"fmt"
	"path/filepath"

	"os"

	sarama "github.com/birdayz/sarama"
	"github.com/infinimesh/kaf"
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
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.kaf/config)")
	rootCmd.PersistentFlags().StringSliceP("brokers", "b", []string{"localhost:9092"}, "Comma separated list of broker ip:port pairs")
	viper.BindPFlag("brokers", rootCmd.PersistentFlags().Lookup("brokers"))

}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	home, err := homedir.Dir()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if cfgFile == "" {
		cfgFile = filepath.Join(home, ".kaf", "config")
	}

	config, _ := kaf.ReadConfig(cfgFile)
	viper.AutomaticEnv() // read in environment variables that match

	// Extract current cluster

	switch len(config.Clusters) {
	case 0:
		break
	case 1:
		defaultCluster := config.Clusters[0]
		viper.Set("brokers", config.Clusters[0].Brokers)
		viper.Set("saslUsername", defaultCluster.SASL.Username)
		viper.Set("saslPassword", defaultCluster.SASL.Password)
	default:
	}

	// TODO: write "fake" viper config, and pass it to viper. This will
	// preserve the override-priority where flags will override the config.
	// This is not the case if we call viper.Set for our manually parsed
	// config
}
