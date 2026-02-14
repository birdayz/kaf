package node

import (
	"fmt"
	"sort"

	"github.com/spf13/cobra"

	"github.com/birdayz/kaf/pkg/app"
	pkgnode "github.com/birdayz/kaf/pkg/node"
)

// NewCommand returns the "kaf node" command with subcommands.
func NewCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "node",
		Short: "Describe and List nodes",
	}

	lsCmd := newListCommand(a)
	cmd.AddCommand(lsCmd)

	return cmd
}

// NewNodesAlias returns the top-level "nodes" alias.
func NewNodesAlias(a *app.App) *cobra.Command {
	return &cobra.Command{
		Use:   "nodes",
		Short: "List nodes in a cluster",
		RunE:  listNodesRunE(a),
	}
}

func newListCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List nodes in a cluster",
		RunE:    listNodesRunE(a),
	}
	a.AddNoHeadersFlag(cmd)
	return cmd
}

func listNodesRunE(a *app.App) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		cl, err := a.NewKafClient()
		if err != nil {
			return err
		}
		defer cl.Close()

		nodes, err := pkgnode.List(cmd.Context(), cl.Admin)
		if err != nil {
			return fmt.Errorf("unable to list nodes: %w", err)
		}

		sort.Slice(nodes, func(i, j int) bool {
			return nodes[i].ID < nodes[j].ID
		})

		w := app.NewTabWriter(a.OutWriter)
		if !a.NoHeaderFlag {
			fmt.Fprintf(w, "ID\tADDRESS\tCONTROLLER\t\n")
		}

		for _, n := range nodes {
			fmt.Fprintf(w, "%v\t%v\t%v\t\n", n.ID, n.Addr(), n.IsController)
		}

		w.Flush()
		return nil
	}
}
