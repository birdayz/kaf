package app

import (
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/hokaccha/go-prettyjson"
	"github.com/mattn/go-colorable"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/birdayz/kaf/pkg/avro"
	kafclient "github.com/birdayz/kaf/pkg/client"
	"github.com/birdayz/kaf/pkg/config"
	"github.com/birdayz/kaf/pkg/group"
	"github.com/birdayz/kaf/pkg/proto"
	"github.com/birdayz/kaf/pkg/topic"
)

// App holds all shared mutable state for the CLI. It is created once per
// invocation and threaded into every command package.
type App struct {
	// I/O
	OutWriter    io.Writer
	ErrWriter    io.Writer
	InReader     io.Reader
	ColorableOut io.Writer

	// Config state
	Cfg            config.Config
	CurrentCluster *config.Cluster
	CfgFile        string
	ClusterOverride string
	BrokersFlag     []string
	SchemaRegistryURL string

	// Shared decode state
	SchemaCache   *avro.SchemaCache
	Reg           *proto.DescriptorRegistry
	Keyfmt        *prettyjson.Formatter
	ProtoType     string
	KeyProtoType  string
	ProtoFiles    []string
	ProtoExclude  []string
	DecodeMsgPack bool

	// Display
	NoHeaderFlag bool

	// Root command reference (for completion generation)
	Root *cobra.Command
}

// New creates an App with sane defaults.
func New() *App {
	keyfmt := prettyjson.NewFormatter()
	keyfmt.Newline = " " // Replace newline with space to avoid condensed output.
	keyfmt.Indent = 0

	return &App{
		OutWriter:    os.Stdout,
		ErrWriter:    os.Stderr,
		InReader:     os.Stdin,
		ColorableOut: colorable.NewColorableStdout(),
		Keyfmt:       keyfmt,
	}
}

// InitConfig reads the config file and resolves the active cluster.
// Called by PersistentPreRunE on the root command.
func (a *App) InitConfig() error {
	var err error
	a.Cfg, err = config.ReadConfig(a.CfgFile)
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	a.Cfg.ClusterOverride = a.ClusterOverride

	cluster := a.Cfg.ActiveCluster()
	if cluster != nil {
		a.CurrentCluster = cluster
	} else {
		a.CurrentCluster = &config.Cluster{
			Brokers: []string{"localhost:9092"},
		}
	}

	if a.SchemaRegistryURL != "" {
		a.CurrentCluster.SchemaRegistryURL = a.SchemaRegistryURL
		a.CurrentCluster.SchemaRegistryCredentials = nil
	}

	if a.BrokersFlag != nil {
		a.CurrentCluster.Brokers = a.BrokersFlag
	}

	return nil
}

// NewKafClient creates a franz-go based client from the current cluster config.
func (a *App) NewKafClient(opts ...kgo.Opt) (*kafclient.Client, error) {
	cl, err := kafclient.New(a.CurrentCluster, opts...)
	if err != nil {
		return nil, fmt.Errorf("unable to create client: %w", err)
	}
	return cl, nil
}

// NewSchemaCache creates a schema cache from the current cluster config.
// Returns (nil, nil) if no schema registry is configured.
func (a *App) NewSchemaCache() (*avro.SchemaCache, error) {
	if a.CurrentCluster.SchemaRegistryURL == "" {
		return nil, nil
	}
	var username, password string
	if creds := a.CurrentCluster.SchemaRegistryCredentials; creds != nil {
		username = creds.Username
		password = creds.Password
	}
	cache, err := avro.NewSchemaCache(a.CurrentCluster.SchemaRegistryURL, username, password)
	if err != nil {
		return nil, fmt.Errorf("unable to get schema cache: %w", err)
	}
	return cache, nil
}

// AddProtoFlags installs the shared protobuf flags on cmd.
func (a *App) AddProtoFlags(cmd *cobra.Command) {
	cmd.Flags().StringSliceVar(&a.ProtoFiles, "proto-include", []string{}, "Path to proto files")
	cmd.Flags().StringSliceVar(&a.ProtoExclude, "proto-exclude", []string{}, "Proto exclusions (path prefixes)")
	cmd.Flags().StringVar(&a.ProtoType, "proto-type", "", "Fully qualified name of the proto message type. Example: com.test.SampleMessage")
	cmd.Flags().StringVar(&a.KeyProtoType, "key-proto-type", "", "Fully qualified name of the proto key type. Example: com.test.SampleMessage")
}

// SetupProtoDescriptorRegistry loads proto files if --proto-type is set.
func (a *App) SetupProtoDescriptorRegistry(cmd *cobra.Command, args []string) error {
	if a.ProtoType != "" {
		r, err := proto.NewDescriptorRegistry(a.ProtoFiles, a.ProtoExclude)
		if err != nil {
			return fmt.Errorf("failed to load protobuf files: %w", err)
		}
		a.Reg = r
	}
	return nil
}

// AddNoHeadersFlag installs --no-headers on cmd.
func (a *App) AddNoHeadersFlag(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&a.NoHeaderFlag, "no-headers", false, "Hide table headers")
}

// ValidTopicArgs provides shell completion for topic names.
func (a *App) ValidTopicArgs(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	cl, err := a.NewKafClient()
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}
	defer cl.Close()

	topics, err := topic.List(cmd.Context(), cl.Admin)
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}
	names := make([]string, 0, len(topics))
	for _, t := range topics {
		names = append(names, t.Name)
	}
	return names, cobra.ShellCompDirectiveNoFileComp
}

// ValidGroupArgs provides shell completion for group names.
func (a *App) ValidGroupArgs(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	cl, err := a.NewKafClient()
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}
	defer cl.Close()

	names, err := group.ListGroupNames(cmd.Context(), cl.Admin)
	if err != nil {
		return nil, cobra.ShellCompDirectiveError
	}
	return names, cobra.ShellCompDirectiveNoFileComp
}

// ValidConfigArgs provides shell completion for cluster names.
func (a *App) ValidConfigArgs(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	clusterList := make([]string, 0, len(a.Cfg.Clusters))
	for _, cluster := range a.Cfg.Clusters {
		clusterList = append(clusterList, cluster.Name)
	}
	return clusterList, cobra.ShellCompDirectiveNoFileComp
}

const (
	TabwriterMinWidth       = 6
	TabwriterMinWidthNested = 2
	TabwriterWidth          = 4
	TabwriterPadding        = 3
	TabwriterPadChar        = ' '
	TabwriterFlags          = 0
)

// NewTabWriter creates a standard tabwriter for CLI output.
func NewTabWriter(w io.Writer) *tabwriter.Writer {
	return tabwriter.NewWriter(w, TabwriterMinWidth, TabwriterWidth, TabwriterPadding, TabwriterPadChar, TabwriterFlags)
}

