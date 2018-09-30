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

	"os"

	sarama "github.com/birdayz/sarama"
	"github.com/infinimesh/kaf"
	"github.com/spf13/cobra"
)

var cfgFile string

func getConfig() (saramaConfig *sarama.Config) {
	saramaConfig = sarama.NewConfig()
	saramaConfig.Version = sarama.V1_0_0_0
	saramaConfig.Producer.Return.Successes = true

	if cluster := currentCluster; cluster.SecurityProtocol == "SASL_SSL" {
		saramaConfig.Net.TLS.Enable = true
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.User = cluster.SASL.Username
		saramaConfig.Net.SASL.Password = cluster.SASL.Password
	}

	return saramaConfig
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

var config *kaf.Config
var currentCluster *kaf.Cluster

var brokersFlag []string

func init() {

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.kaf/config)")
	rootCmd.PersistentFlags().StringSliceVarP(&brokersFlag, "brokers", "b", nil, "Comma separated list of broker ip:port pairs")

	config, _ = kaf.ReadConfig()

	// Flag has highest priority
	if brokersFlag != nil {
		currentCluster = &kaf.Cluster{
			Brokers: brokersFlag,
		}
	} else {
		if cluster := config.ActiveCluster(); cluster != nil {
			currentCluster = cluster
		} else {
			currentCluster = &kaf.Cluster{
				Brokers: []string{"localhost:9092"},
			}
		}
	}

}

func getClusterAdmin() (admin sarama.ClusterAdmin, err error) {
	return sarama.NewClusterAdmin(currentCluster.Brokers, getConfig())
}

func getClient() (client sarama.Client, err error) {
	return sarama.NewClient(currentCluster.Brokers, getConfig())
}
