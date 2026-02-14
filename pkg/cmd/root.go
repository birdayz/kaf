package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/birdayz/kaf/pkg/app"
	"github.com/birdayz/kaf/pkg/cmd/completion"
	kafconfig "github.com/birdayz/kaf/pkg/cmd/config"
	"github.com/birdayz/kaf/pkg/cmd/consume"
	"github.com/birdayz/kaf/pkg/cmd/group"
	"github.com/birdayz/kaf/pkg/cmd/node"
	"github.com/birdayz/kaf/pkg/cmd/produce"
	"github.com/birdayz/kaf/pkg/cmd/query"
	"github.com/birdayz/kaf/pkg/cmd/topic"
)

// Execute is the single entry point for the CLI.
func Execute(version, commit string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	a := app.New()

	root := &cobra.Command{
		Use:          "kaf",
		Short:        "Kafka Command Line utility for cluster management",
		Version:      fmt.Sprintf("%s (%s)", version, commit),
		SilenceUsage: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			a.OutWriter = cmd.OutOrStdout()
			a.ErrWriter = cmd.ErrOrStderr()
			a.InReader = cmd.InOrStdin()

			if a.OutWriter != os.Stdout {
				a.ColorableOut = a.OutWriter
			}

			return a.InitConfig()
		},
	}

	root.PersistentFlags().StringVar(&a.CfgFile, "config", "", "config file (default is $HOME/.kaf/config)")
	root.PersistentFlags().StringSliceVarP(&a.BrokersFlag, "brokers", "b", nil, "Comma separated list of broker ip:port pairs")
	root.PersistentFlags().StringVar(&a.SchemaRegistryURL, "schema-registry", "", "URL to a Confluent schema registry. Used for attempting to decode Avro-encoded messages")
	root.PersistentFlags().StringVarP(&a.ClusterOverride, "cluster", "c", "", "set a temporary current cluster")

	root.AddCommand(
		consume.NewCommand(a),
		produce.NewCommand(a),
		topic.NewCommand(a),
		topic.NewTopicsAlias(a),
		group.NewCommand(a),
		group.NewGroupsAlias(a),
		node.NewCommand(a),
		node.NewNodesAlias(a),
		query.NewCommand(a),
		kafconfig.NewCommand(a),
		completion.NewCommand(root, a),
	)

	a.Root = root
	return root.ExecuteContext(ctx)
}
