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
	"github.com/birdayz/kaf/pkg/commands"
	"github.com/birdayz/kaf/pkg/config"
	"github.com/birdayz/kaf/pkg/proto"
)

var cfgFile string

// brokersChanged checks if broker list has changed
// This is needed for tests where each test uses a different testcontainer
func brokersChanged(oldBrokers, newBrokers []string) bool {
	if len(oldBrokers) != len(newBrokers) {
		return true
	}
	for i := range oldBrokers {
		if oldBrokers[i] != newBrokers[i] {
			return true
		}
	}
	return false
}

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

	// Global kafka client - created once at startup, reused throughout lifecycle
	globalClient       *kgo.Client
	globalAdmin        *kadm.Client
	globalClientBrokers []string // Track brokers used to create global client
)

// Will be replaced by GitHub action and by goreleaser
// see https://goreleaser.com/customization/build/
var commit string = "HEAD"
var version string = "latest"

// commandsInstance will hold the Commands struct created after clients are initialized
var commandsInstance *commands.Commands

var rootCmd = &cobra.Command{
	Use:     "kaf",
	Short:   "Kafka Command Line utility for cluster management",
	Version: fmt.Sprintf("%s (%s)", version, commit),
	// Note: We don't close the client in PersistentPostRun because:
	// 1. In normal CLI usage, the process exits immediately after the command, and OS cleans up resources
	// 2. In tests, we want to reuse the client across multiple command invocations (with same broker)
	// 3. We only close and recreate when brokers change (handled lazily in getClient/getClusterAdmin)
	// This prevents connection accumulation from rapid create/close cycles in tests
}

func setupRootCmd() {
	rootCmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		outWriter = cmd.OutOrStdout()
		errWriter = cmd.ErrOrStderr()
		inReader = cmd.InOrStdin()

		if outWriter != os.Stdout {
			colorableOut = outWriter
		}

		// Update IO for this command execution
		if commandsInstance != nil {
			commandsInstance.SetIO(outWriter, errWriter, inReader)
		}
	}
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

var commandsRegistered bool

func init() {
	setupRootCmd()

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.kaf/config)")
	rootCmd.PersistentFlags().StringSliceVarP(&brokersFlag, "brokers", "b", nil, "Comma separated list of broker ip:port pairs")
	rootCmd.PersistentFlags().StringVar(&schemaRegistryURL, "schema-registry", "", "URL to a Confluent schema registry. Used for attempting to decode Avro-encoded messages")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Whether to turn on verbose logging")
	rootCmd.PersistentFlags().StringVarP(&clusterOverride, "cluster", "c", "", "set a temporary current cluster")

	// onInit runs before command execution via cobra.OnInitialize
	cobra.OnInitialize(onInit)
}


// registerCommands registers commands from the Commands instance
// This should be called after the Commands instance is created
func registerCommands() {
	if commandsRegistered {
		return
	}
	commandsRegistered = true

	cmds := getOrCreateCommands()
	groupCmd := cmds.GetGroupCmd()
	groupsCmd := cmds.GetGroupsCmd()

	if groupCmd == nil || groupsCmd == nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Failed to get commands from Commands instance\n")
		return
	}

	rootCmd.AddCommand(groupCmd)
	rootCmd.AddCommand(groupsCmd)
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

	if brokersFlag != nil && len(brokersFlag) > 0 {
		// Make a copy to avoid slice aliasing issues with cobra flags
		currentCluster.Brokers = make([]string, len(brokersFlag))
		copy(currentCluster.Brokers, brokersFlag)
	}

	if verbose {
		// Franz-go uses a different logging approach - can be configured per client
	}

	// Register commands after config is set and currentCluster is updated
	registerCommands()
}

func getClusterAdmin() *kadm.Client {
	// Lazily create global client if needed or recreate if brokers changed
	if currentCluster != nil {
		needsRecreate := globalClient == nil || brokersChanged(globalClientBrokers, currentCluster.Brokers)

		if needsRecreate {
			// Close existing client if present (but don't wait/block)
			if globalClient != nil {
				globalClient.Close()
			}
			globalClient = nil
			globalAdmin = nil

			currentOpts := getKgoOpts()
			var err error
			globalClient, err = kgo.NewClient(currentOpts...)
			if err != nil {
				errorExit("Unable to create kafka client: %v\n", err)
			}
			globalAdmin = kadm.NewClient(globalClient)
			// Copy broker list to track what we created the client with
			globalClientBrokers = make([]string, len(currentCluster.Brokers))
			copy(globalClientBrokers, currentCluster.Brokers)
		}
	}

	if globalAdmin == nil {
		errorExit("Kafka admin client not initialized\n")
	}
	return globalAdmin
}

func getClient() *kgo.Client {
	// Lazily create global client if needed or recreate if brokers changed
	if currentCluster != nil {
		needsRecreate := globalClient == nil || brokersChanged(globalClientBrokers, currentCluster.Brokers)

		if needsRecreate {
			// Close existing client if present (but don't wait/block)
			if globalClient != nil {
				globalClient.Close()
			}
			globalClient = nil
			globalAdmin = nil

			currentOpts := getKgoOpts()
			var err error
			globalClient, err = kgo.NewClient(currentOpts...)
			if err != nil {
				errorExit("Unable to create kafka client: %v\n", err)
			}
			globalAdmin = kadm.NewClient(globalClient)
			// Copy broker list to track what we created the client with
			globalClientBrokers = make([]string, len(currentCluster.Brokers))
			copy(globalClientBrokers, currentCluster.Brokers)
		}
	}

	if globalClient == nil {
		errorExit("Kafka client not initialized\n")
	}
	return globalClient
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

// getOrCreateCommands returns the commands instance, creating it if needed
// This is useful for tests that need direct access to commands
func getOrCreateCommands() *commands.Commands {
	// Recreate if cluster changed or instance doesn't exist
	needsRecreate := commandsInstance == nil ||
		(commandsInstance.Cluster != nil && currentCluster != nil &&
			brokersChanged(commandsInstance.Cluster.Brokers, currentCluster.Brokers))

	if needsRecreate {
		client := getClient()
		admin := getClusterAdmin()
		schemaCache := getSchemaCache()

		commandsInstance = commands.NewCommands(client, admin, currentCluster, schemaCache)
		commandsInstance.SetIO(outWriter, errWriter, inReader)
	} else if commandsInstance != nil {
		// Update cluster reference in case it changed
		commandsInstance.Cluster = currentCluster
		// Also update clients in case they were recreated
		commandsInstance.Client = getClient()
		commandsInstance.Admin = getClusterAdmin()
	}
	return commandsInstance
}

// resetCommands resets the commands instance - used in tests
func resetCommands() {
	// First, remove the commands
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "group" || cmd.Use == "groups" {
			rootCmd.RemoveCommand(cmd)
		}
	}

	// Reset state
	commandsInstance = nil
	commandsRegistered = false

	// Don't re-register yet - let onInit() handle it via cobra.OnInitialize
	// This ensures currentCluster is set with the correct brokers first
}
