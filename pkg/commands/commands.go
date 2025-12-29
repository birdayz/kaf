package commands

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/pkg/sasl/plain"

	"github.com/birdayz/kaf/pkg/avro"
	"github.com/birdayz/kaf/pkg/config"
)

// Commands holds the Kafka clients and configuration needed by all commands
type Commands struct {
	// Kafka clients
	Client *kgo.Client
	Admin  *kadm.Client

	// Configuration
	Cluster      *config.Cluster
	SchemaCache  *avro.SchemaCache

	// IO
	OutWriter io.Writer
	ErrWriter io.Writer
	InReader  io.Reader
}

// NewCommands creates a new Commands instance with the given clients and config
func NewCommands(client *kgo.Client, admin *kadm.Client, cluster *config.Cluster, schemaCache *avro.SchemaCache) *Commands {
	return &Commands{
		Client:      client,
		Admin:       admin,
		Cluster:     cluster,
		SchemaCache: schemaCache,
	}
}

// SetIO sets the IO streams for commands
func (c *Commands) SetIO(out, err io.Writer, in io.Reader) {
	c.OutWriter = out
	c.ErrWriter = err
	c.InReader = in
}

// GetGroupCmd returns the group command
func (c *Commands) GetGroupCmd() *cobra.Command {
	return c.createGroupCmd()
}

// GetGroupsCmd returns the groups command
func (c *Commands) GetGroupsCmd() *cobra.Command {
	return c.createGroupsCmd()
}

// GetConsumeCmd returns the consume command
func (c *Commands) GetConsumeCmd() *cobra.Command {
	return c.createConsumeCmd()
}

// getKgoOpts returns kafka client options based on cluster configuration
func (c *Commands) getKgoOpts() []kgo.Opt {
	cluster := c.Cluster
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
			// OAuth handling would need token provider from main
			// For now, skip - this can be added when needed
		}
	}

	if cluster.TLS != nil || cluster.SecurityProtocol == "SASL_SSL" {
		tlsConfig := &tls.Config{}
		if cluster.TLS != nil {
			tlsConfig.InsecureSkipVerify = cluster.TLS.Insecure

			if cluster.TLS.Cafile != "" {
				caCert, err := os.ReadFile(cluster.TLS.Cafile)
				if err != nil {
					fmt.Fprintf(c.ErrWriter, "Unable to read Cafile :%v\n", err)
					return opts
				}
				caCertPool := x509.NewCertPool()
				caCertPool.AppendCertsFromPEM(caCert)
				tlsConfig.RootCAs = caCertPool
			}

			if cluster.TLS.Clientfile != "" && cluster.TLS.Clientkeyfile != "" {
				clientCert, err := os.ReadFile(cluster.TLS.Clientfile)
				if err != nil {
					fmt.Fprintf(c.ErrWriter, "Unable to read Clientfile :%v\n", err)
					return opts
				}
				clientKey, err := os.ReadFile(cluster.TLS.Clientkeyfile)
				if err != nil {
					fmt.Fprintf(c.ErrWriter, "Unable to read Clientkeyfile :%v\n", err)
					return opts
				}

				cert, err := tls.X509KeyPair([]byte(clientCert), []byte(clientKey))
				if err != nil {
					fmt.Fprintf(c.ErrWriter, "Unable to create KeyPair: %v\n", err)
					return opts
				}
				tlsConfig.Certificates = []tls.Certificate{cert}
			}
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	return opts
}

// validTopicArgs provides tab completion for topic names
func (c *Commands) validTopicArgs(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if len(args) != 0 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	topics, err := c.Admin.ListTopics(cmd.Context())
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	var topicNames []string
	for _, topic := range topics.Sorted() {
		topicNames = append(topicNames, topic.Topic)
	}

	return topicNames, cobra.ShellCompDirectiveNoFileComp
}
