package main

import (
	"fmt"

	"crypto/tls"
	"crypto/x509"
	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"io/ioutil"
	"os"

	"github.com/infinimesh/kaf"
)

var cfgFile string

func getConfig() (saramaConfig *sarama.Config) {
	saramaConfig = sarama.NewConfig()
	saramaConfig.Version = sarama.V1_0_0_0
	saramaConfig.Producer.Return.Successes = true

	if cluster := currentCluster; cluster.SecurityProtocol == "SASL_SSL" {
		saramaConfig.Net.TLS.Enable = true
		if cluster.TLS != nil {
			tlsConfig := &tls.Config{
				InsecureSkipVerify: cluster.TLS.Insecure,
			}
			if cluster.TLS.Cafile != "" {
				caCert, err := ioutil.ReadFile(cluster.TLS.Cafile)
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
				caCertPool := x509.NewCertPool()
				caCertPool.AppendCertsFromPEM(caCert)
				tlsConfig.RootCAs = caCertPool
			}
			saramaConfig.Net.TLS.Config = tlsConfig

		} else {
			saramaConfig.Net.TLS.Config = &tls.Config{InsecureSkipVerify: false}
		}
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.User = cluster.SASL.Username
		saramaConfig.Net.SASL.Password = cluster.SASL.Password
	}

	return saramaConfig
}

var rootCmd = &cobra.Command{
	Use:   "kaf",
	Short: "Kafka Command Line utility for cluster management",
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

var config kaf.Config
var currentCluster *kaf.Cluster

var brokersFlag []string

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.kaf/config)")
	rootCmd.PersistentFlags().StringSliceVarP(&brokersFlag, "brokers", "b", nil, "Comma separated list of broker ip:port pairs")

	cobra.OnInitialize(onInit)
}

func onInit() {
	var err error
	config, err = kaf.ReadConfig()
	if err != nil && !os.IsNotExist(err) {
		panic(err)
	}

	// Flag is highest priority override
	if brokersFlag != nil {
		currentCluster = &kaf.Cluster{
			Brokers: brokersFlag,
		}
	} else {
		// If no override from flag is set, get current cluster from config file
		if cluster := config.ActiveCluster(); cluster != nil {
			currentCluster = cluster
		} else {
			// Default to localhost:9092 with no security
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
