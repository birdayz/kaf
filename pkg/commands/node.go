package commands

import (
	"context"
	"fmt"
	"sort"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

func (c *Commands) GetNodeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "node",
		Short: "Describe and List nodes",
	}

	cmd.AddCommand(c.createNodeLsCmd())

	return cmd
}

func (c *Commands) GetNodesCmd() *cobra.Command {
	var noHeaderFlag bool

	cmd := &cobra.Command{
		Use:   "nodes",
		Short: "List nodes in a cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			brokers, err := c.Admin.ListBrokers(ctx)
			if err != nil {
				return fmt.Errorf("unable to describe cluster: %w", err)
			}

			sort.Slice(brokers, func(i, j int) bool {
				return brokers[i].NodeID < brokers[j].NodeID
			})

			w := tabwriter.NewWriter(c.OutWriter, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
			if !noHeaderFlag {
				fmt.Fprintf(w, "ID\tADDRESS\t\n")
			}

			for _, broker := range brokers {
				fmt.Fprintf(w, "%v\t%v\t\n", broker.NodeID, fmt.Sprintf("%s:%d", broker.Host, broker.Port))
			}

			w.Flush()
			return nil
		},
	}

	cmd.Flags().BoolVar(&noHeaderFlag, "no-headers", false, "Hide table headers")

	return cmd
}

func (c *Commands) createNodeLsCmd() *cobra.Command {
	var noHeaderFlag bool

	cmd := &cobra.Command{
		Use:   "ls",
		Short: "List nodes in a cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			brokers, err := c.Admin.ListBrokers(ctx)
			if err != nil {
				return fmt.Errorf("unable to describe cluster: %w", err)
			}

			sort.Slice(brokers, func(i, j int) bool {
				return brokers[i].NodeID < brokers[j].NodeID
			})

			w := tabwriter.NewWriter(c.OutWriter, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
			if !noHeaderFlag {
				fmt.Fprintf(w, "ID\tADDRESS\t\n")
			}

			for _, broker := range brokers {
				fmt.Fprintf(w, "%v\t%v\t\n", broker.NodeID, fmt.Sprintf("%s:%d", broker.Host, broker.Port))
			}

			w.Flush()
			return nil
		},
	}

	cmd.Flags().BoolVar(&noHeaderFlag, "no-headers", false, "Hide table headers")

	return cmd
}
