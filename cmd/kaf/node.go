package main

import (
	"fmt"
	"os"
	"text/tabwriter"

	"sort"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(nodeCommand)
	nodeCommand.AddCommand(nodeLsCommand)
	nodeLsCommand.Flags().BoolVar(&noHeaderFlag, "no-headers", false, "Hide table headers")
}

var nodeCommand = &cobra.Command{
	Use:   "node",
	Short: "Describe and List nodes",
}

var nodeLsCommand = &cobra.Command{
	Use:   "ls",
	Short: "List nodes in a cluster",
	Run: func(cmd *cobra.Command, args []string) {
		admin, err := getClusterAdmin()
		if err != nil {
			panic(err)
		}

		brokers, _, err := admin.DescribeCluster()
		if err != nil {
			panic(err)
		}

		sort.Slice(brokers, func(i, j int) bool {
			return brokers[i].ID() < brokers[j].ID()
		})

		w := tabwriter.NewWriter(os.Stdout, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
		if !noHeaderFlag {
			fmt.Fprintf(w, "ID\tADDRESS\t\n")
		}

		for _, broker := range brokers {
			fmt.Fprintf(w, "%v\t%v\t\n", broker.ID(), broker.Addr())
		}

		w.Flush()
	},
}
