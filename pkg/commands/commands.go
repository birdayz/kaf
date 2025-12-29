package commands

import (
	"io"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

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
