package main

import (
	"fmt"

	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"log"
	"os"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"

	"github.com/infinimesh/kaf"
	"github.com/infinimesh/kaf/avro"
)

var cfgFile string

func getConfig() (saramaConfig *sarama.Config) {
	saramaConfig = sarama.NewConfig()
	saramaConfig.Version = sarama.V1_0_0_0
	saramaConfig.Producer.Return.Successes = true

	cluster := currentCluster
	if cluster.SASL != nil {
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.User = cluster.SASL.Username
		saramaConfig.Net.SASL.Password = cluster.SASL.Password
	}
	if cluster.SecurityProtocol == "SASL_SSL" {
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
var schemaRegistryURL string
var verbose bool

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.kaf/config)")
	rootCmd.PersistentFlags().StringSliceVarP(&brokersFlag, "brokers", "b", nil, "Comma separated list of broker ip:port pairs")
	rootCmd.PersistentFlags().StringVar(&schemaRegistryURL, "schema-registry", "", "URL to a Confluent schema registry. Used for attempting to decode Avro-encoded messages")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Whether to turn on sarama logging")
	cobra.OnInitialize(onInit)
}

func onInit() {
	var err error
	config, err = kaf.ReadConfig()
	if err != nil && !os.IsNotExist(err) {
		errorExit("Config %s does not exist", config)
	}

	cluster := config.ActiveCluster()
	if cluster != nil {
		// Use active cluster from config
		currentCluster = cluster
	} else {
		// Create sane default if not configured
		currentCluster = &kaf.Cluster{
			Brokers: []string{"localhost:9092"},
		}
	}

	// Any set flags override the configuration
	if schemaRegistryURL != "" {
		currentCluster.SchemaRegistryURL = schemaRegistryURL
	}

	if brokersFlag != nil {
		currentCluster.Brokers = brokersFlag
	}

	if verbose {
		sarama.Logger = log.New(os.Stderr, "[sarama] ", log.Lshortfile | log.LstdFlags)
	}

}

func getClusterAdmin() (admin sarama.ClusterAdmin) {
	clusterAdmin, err := sarama.NewClusterAdmin(currentCluster.Brokers, getConfig())
	if err != nil {
		errorExit("Unable to get cluster admin: %v\n", err)
	}
	return clusterAdmin
}

func getClient() (client sarama.Client) {
	client, err := sarama.NewClient(currentCluster.Brokers, getConfig())
	if err != nil {
		errorExit("Unable to get client: %v\n", err)
	}
	return client
}

func getSchemaCache() (cache *avro.SchemaCache) {
	if currentCluster.SchemaRegistryURL == "" {
		return nil
	}
	cache, err := avro.NewSchemaCache(currentCluster.SchemaRegistryURL)
	if err != nil {
		errorExit("Unable to get schema cache :%v\n", err)
	}
	return cache
}

func errorExit(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
	os.Exit(1)
}