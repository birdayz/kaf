package main

import (
	"context"
	"fmt"
	"text/tabwriter"

	"sort"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(nodeCommand)
	rootCmd.AddCommand(nodesCommand)
	nodeCommand.AddCommand(nodeLsCommand)
	nodeLsCommand.Flags().BoolVar(&noHeaderFlag, "no-headers", false, "Hide table headers")
	nodesCommand.Flags().BoolVar(&noHeaderFlag, "no-headers", false, "Hide table headers")
}

var nodesCommand = &cobra.Command{
	Use:   "nodes",
	Short: "List nodes in a cluster",
	Run:   nodeLsCommand.Run,
}

var nodeCommand = &cobra.Command{
	Use:   "node",
	Short: "Describe and List nodes",
}

var nodeLsCommand = &cobra.Command{
	Use:   "ls",
	Short: "List nodes in a cluster",
	Run: func(cmd *cobra.Command, args []string) {
		admin := getClusterAdmin()
		defer admin.Close()

		ctx := context.Background()
		brokers, err := admin.ListBrokers(ctx)
		if err != nil {
			errorExit("Unable to describe cluster: %v\n", err)
		}

		sort.Slice(brokers, func(i, j int) bool {
			return brokers[i].NodeID < brokers[j].NodeID
		})

		w := tabwriter.NewWriter(outWriter, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
		if !noHeaderFlag {
			_, _ = fmt.Fprintf(w, "ID\tADDRESS\t\n")
		}

		for _, broker := range brokers {
			_, _ = fmt.Fprintf(w, "%v\t%v\t\n", broker.NodeID, fmt.Sprintf("%s:%d", broker.Host, broker.Port))
		}

		w.Flush()
	},
}
