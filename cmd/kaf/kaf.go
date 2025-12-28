package main

import (
	"fmt"
	"io"

	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/mattn/go-colorable"
	"github.com/spf13/cobra"

	"github.com/birdayz/kaf/pkg/avro"
	"github.com/birdayz/kaf/pkg/config"
	"github.com/birdayz/kaf/pkg/proto"
)

var cfgFile string

func getKgoOpts() []kgo.Opt {
	cluster := currentCluster
	opts := []kgo.Opt{
		kgo.SeedBrokers(cluster.Brokers...),
	}

	if cluster.SASL != nil {
		switch cluster.SASL.Mechanism {
		case "SCRAM-SHA-512":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: cluster.SASL.Username,
				Pass: cluster.SASL.Password,
			}.AsSha512Mechanism()))
		case "SCRAM-SHA-256":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: cluster.SASL.Username,
				Pass: cluster.SASL.Password,
			}.AsSha256Mechanism()))
		case "PLAIN":
			opts = append(opts, kgo.SASL(plain.Auth{
				User: cluster.SASL.Username,
				Pass: cluster.SASL.Password,
			}.AsMechanism()))
		case "OAUTHBEARER", "AWS_MSK_IAM":
			tokenProvider := newTokenProvider()
			token, err := tokenProvider.Token()
			if err != nil {
				errorExit("Unable to get OAuth token: %v\n", err)
			}
			opts = append(opts, kgo.SASL(oauth.Auth{
				Token: token,
			}.AsMechanism()))
		}
	}

	if cluster.TLS != nil || cluster.SecurityProtocol == "SASL_SSL" {
		tlsConfig := &tls.Config{}
		if cluster.TLS != nil {
			tlsConfig.InsecureSkipVerify = cluster.TLS.Insecure

			if cluster.TLS.Cafile != "" {
				caCert, err := os.ReadFile(cluster.TLS.Cafile)
				if err != nil {
					errorExit("Unable to read Cafile :%v\n", err)
				}
				caCertPool := x509.NewCertPool()
				caCertPool.AppendCertsFromPEM(caCert)
				tlsConfig.RootCAs = caCertPool
			}

			if cluster.TLS.Clientfile != "" && cluster.TLS.Clientkeyfile != "" {
				clientCert, err := os.ReadFile(cluster.TLS.Clientfile)
				if err != nil {
					errorExit("Unable to read Clientfile :%v\n", err)
				}
				clientKey, err := os.ReadFile(cluster.TLS.Clientkeyfile)
				if err != nil {
					errorExit("Unable to read Clientkeyfile :%v\n", err)
				}

				cert, err := tls.X509KeyPair([]byte(clientCert), []byte(clientKey))
				if err != nil {
					errorExit("Unable to create KeyPair: %v\n", err)
				}
				tlsConfig.Certificates = []tls.Certificate{cert}
			}
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	return opts
}

var (
	outWriter io.Writer = os.Stdout
	errWriter io.Writer = os.Stderr
	inReader  io.Reader = os.Stdin

	colorableOut io.Writer = colorable.NewColorableStdout()
)

// Will be replaced by GitHub action and by goreleaser
// see https://goreleaser.com/customization/build/
var commit string = "HEAD"
var version string = "latest"

var rootCmd = &cobra.Command{
	Use:     "kaf",
	Short:   "Kafka Command Line utility for cluster management",
	Version: fmt.Sprintf("%s (%s)", version, commit),
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		outWriter = cmd.OutOrStdout()
		errWriter = cmd.ErrOrStderr()
		inReader = cmd.InOrStdin()

		if outWriter != os.Stdout {
			colorableOut = outWriter
		}
	},
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

var cfg config.Config
var currentCluster *config.Cluster

var (
	brokersFlag       []string
	schemaRegistryURL string
	protoFiles        []string
	protoExclude      []string
	decodeMsgPack     bool
	verbose           bool
	clusterOverride   string
)

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.kaf/config)")
	rootCmd.PersistentFlags().StringSliceVarP(&brokersFlag, "brokers", "b", nil, "Comma separated list of broker ip:port pairs")
	rootCmd.PersistentFlags().StringVar(&schemaRegistryURL, "schema-registry", "", "URL to a Confluent schema registry. Used for attempting to decode Avro-encoded messages")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Whether to turn on verbose logging")
	rootCmd.PersistentFlags().StringVarP(&clusterOverride, "cluster", "c", "", "set a temporary current cluster")
	cobra.OnInitialize(onInit)
}

var setupProtoDescriptorRegistry = func(cmd *cobra.Command, args []string) {
	if protoType != "" {
		r, err := proto.NewDescriptorRegistry(protoFiles, protoExclude)
		if err != nil {
			errorExit("Failed to load protobuf files: %v\n", err)
		}
		reg = r
	}
}

func onInit() {
	var err error
	cfg, err = config.ReadConfig(cfgFile)
	if err != nil {
		errorExit("Invalid config: %v", err)
	}

	cfg.ClusterOverride = clusterOverride

	cluster := cfg.ActiveCluster()
	if cluster != nil {
		// Use active cluster from config
		currentCluster = cluster
	} else {
		// Create sane default if not configured
		currentCluster = &config.Cluster{
			Brokers: []string{"localhost:9092"},
		}
	}

	// Any set flags override the configuration
	if schemaRegistryURL != "" {
		currentCluster.SchemaRegistryURL = schemaRegistryURL
		currentCluster.SchemaRegistryCredentials = nil
	}

	if brokersFlag != nil {
		currentCluster.Brokers = brokersFlag
	}

	if verbose {
		// Franz-go uses a different logging approach - can be configured per client
	}
}

func getClusterAdmin() *kadm.Client {
	cl, err := kgo.NewClient(getKgoOpts()...)
	if err != nil {
		errorExit("Unable to create kafka client: %v\n", err)
	}
	return kadm.NewClient(cl)
}

func getClient() *kgo.Client {
	cl, err := kgo.NewClient(getKgoOpts()...)
	if err != nil {
		errorExit("Unable to get client: %v\n", err)
	}
	return cl
}

func getClientFromOpts(opts []kgo.Opt) *kgo.Client {
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		errorExit("Unable to get client: %v\n", err)
	}
	return cl
}

func getSchemaCache() (cache *avro.SchemaCache) {
	if currentCluster.SchemaRegistryURL == "" {
		return nil
	}
	var username, password string
	if creds := currentCluster.SchemaRegistryCredentials; creds != nil {
		username = creds.Username
		password = creds.Password
	}
	cache, err := avro.NewSchemaCache(currentCluster.SchemaRegistryURL, username, password)
	if err != nil {
		errorExit("Unable to get schema cache :%v\n", err)
	}
	return cache
}

func errorExit(format string, a ...interface{}) {
	fmt.Fprintf(errWriter, format+"\n", a...)
	os.Exit(1)
}
