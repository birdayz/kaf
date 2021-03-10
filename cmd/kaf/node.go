package main

import (
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

		brokers, ctlID, err := admin.DescribeCluster()
		if err != nil {
			errorExit("Unable to describe cluster: %v\n", err)
		}

		sort.Slice(brokers, func(i, j int) bool {
			return brokers[i].ID() < brokers[j].ID()
		})

		w := tabwriter.NewWriter(outWriter, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
		if !noHeaderFlag {
			_, _ = fmt.Fprintf(w, "ID\tADDRESS\tcontroller\t\n")
		}

		for _, broker := range brokers {
			_, _ = fmt.Fprintf(w, "%v\t%v\t%v\t\n", broker.ID(), broker.Addr(), broker.ID() == ctlID)
		}

		w.Flush()
	},
}
