package config

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"

	"github.com/birdayz/kaf/pkg/app"
	"github.com/birdayz/kaf/pkg/config"
)

// NewCommand returns the "kaf config" command with subcommands.
func NewCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Handle kaf configuration",
	}

	cmd.AddCommand(
		newCurrentContextCommand(a),
		newUseClusterCommand(a),
		newGetClustersCommand(a),
		newAddClusterCommand(a),
		newRemoveClusterCommand(a),
		newSelectClusterCommand(a),
		newAddEventhubCommand(a),
		newImportCommand(a),
	)

	return cmd
}

func newCurrentContextCommand(a *app.App) *cobra.Command {
	return &cobra.Command{
		Use:   "current-context",
		Short: "Displays the current context",
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Fprintln(a.OutWriter, a.Cfg.CurrentCluster)
		},
	}
}

func newUseClusterCommand(a *app.App) *cobra.Command {
	return &cobra.Command{
		Use:               "use-cluster [NAME]",
		Short:             "Sets the current cluster in the configuration",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: a.ValidConfigArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if err := a.Cfg.SetCurrentCluster(name); err != nil {
				return fmt.Errorf("cluster with name %v not found", name)
			}
			fmt.Fprintf(a.OutWriter, "Switched to cluster \"%v\".\n", name)
			return nil
		},
	}
}

func newGetClustersCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get-clusters",
		Short: "Display clusters in the configuration file",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			if !a.NoHeaderFlag {
				fmt.Fprintln(a.OutWriter, "  NAME")
			}
			for _, cluster := range a.Cfg.Clusters {
				marker := "  "
				if cluster.Name == a.Cfg.CurrentCluster {
					marker = "* "
				}
				fmt.Fprintf(a.OutWriter, "%s%s\n", marker, cluster.Name)
			}
		},
	}
	a.AddNoHeadersFlag(cmd)
	return cmd
}

func newAddClusterCommand(a *app.App) *cobra.Command {
	var brokerVersion string

	cmd := &cobra.Command{
		Use:   "add-cluster [NAME]",
		Short: "Add cluster",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if a.Cfg.HasCluster(name) {
				return fmt.Errorf("could not add cluster: cluster with name '%v' exists already", name)
			}

			a.Cfg.Clusters = append(a.Cfg.Clusters, &config.Cluster{
				Name:              name,
				Brokers:           a.BrokersFlag,
				SchemaRegistryURL: a.SchemaRegistryURL,
				Version:           brokerVersion,
			})
			if err := a.Cfg.Write(); err != nil {
				return fmt.Errorf("unable to write config: %w", err)
			}
			fmt.Fprintln(a.OutWriter, "Added cluster.")
			return nil
		},
	}

	cmd.Flags().StringVar(&brokerVersion, "broker-version", "", "Broker version (stored in config, not used by franz-go which auto-negotiates)")
	return cmd
}

func newRemoveClusterCommand(a *app.App) *cobra.Command {
	return &cobra.Command{
		Use:               "remove-cluster [NAME]",
		Short:             "remove cluster",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: a.ValidConfigArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]

			pos := -1
			for i, cluster := range a.Cfg.Clusters {
				if cluster.Name == name {
					pos = i
					break
				}
			}

			if pos == -1 {
				return fmt.Errorf("could not delete cluster: cluster with name '%v' does not exist", name)
			}

			a.Cfg.Clusters = append(a.Cfg.Clusters[:pos], a.Cfg.Clusters[pos+1:]...)

			if err := a.Cfg.Write(); err != nil {
				return fmt.Errorf("unable to write config: %w", err)
			}
			fmt.Fprintln(a.OutWriter, "Removed cluster.")
			return nil
		},
	}
}

func newSelectClusterCommand(a *app.App) *cobra.Command {
	return &cobra.Command{
		Use:   "select-cluster",
		Short: "Interactively select a cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			var clusterNames []string
			pos := 0
			for k, cluster := range a.Cfg.Clusters {
				clusterNames = append(clusterNames, cluster.Name)
				if cluster.Name == a.Cfg.CurrentCluster {
					pos = k
				}
			}

			searcher := func(input string, index int) bool {
				cluster := clusterNames[index]
				name := strings.ReplaceAll(strings.ToLower(cluster), " ", "")
				input = strings.ReplaceAll(strings.ToLower(input), " ", "")
				return strings.Contains(name, input)
			}

			p := promptui.Select{
				Label:     "Select cluster",
				Items:     clusterNames,
				Searcher:  searcher,
				Size:      10,
				CursorPos: pos,
			}

			_, selected, err := p.Run()
			if err != nil {
				// User cancelled (e.g. Ctrl-C). Not an error.
				return nil
			}

			if err := a.Cfg.SetCurrentCluster(selected); err != nil {
				return fmt.Errorf("cluster with name %v not found", selected)
			}
			fmt.Fprintf(a.OutWriter, "Switched to cluster \"%v\".\n", selected)
			return nil
		},
	}
}

func newAddEventhubCommand(a *app.App) *cobra.Command {
	var connString string

	cmd := &cobra.Command{
		Use:     "add-eventhub [NAME]",
		Example: "kaf config add-eventhub my-eventhub --eh-connstring 'Endpoint=sb://......AccessKey=....'",
		Short:   "Add Azure EventHub",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if a.Cfg.HasCluster(name) {
				return fmt.Errorf("could not add cluster: cluster with name '%v' exists already", name)
			}

			r := regexp.MustCompile(`^Endpoint=sb://(.*)\.servicebus.*$`)
			hubName := r.FindStringSubmatch(connString)
			if len(hubName) != 2 {
				return fmt.Errorf("failed to determine EventHub name from Connection String -- check your ConnectionString")
			}

			a.Cfg.Clusters = append(a.Cfg.Clusters, &config.Cluster{
				Name:              name,
				Brokers:           []string{hubName[1] + ".servicebus.windows.net:9093"},
				SchemaRegistryURL: a.SchemaRegistryURL,
				SASL: &config.SASL{
					Mechanism: "PLAIN",
					Username:  "$ConnectionString",
					Password:  connString,
				},
				SecurityProtocol: "SASL_SSL",
			})
			if err := a.Cfg.Write(); err != nil {
				return fmt.Errorf("unable to write config: %w", err)
			}
			fmt.Fprintln(a.OutWriter, "Added EventHub.")
			return nil
		},
	}

	cmd.Flags().StringVar(&connString, "eh-connstring", "", "EventHub ConnectionString")
	return cmd
}

func newImportCommand(a *app.App) *cobra.Command {
	return &cobra.Command{
		Use:   "import [ccloud]",
		Short: "Import configurations into the $HOME/.kaf/config file",
		RunE: func(cmd *cobra.Command, args []string) error {
			path, err := config.TryFindCcloudConfigFile()
			if err != nil {
				return fmt.Errorf("could not find Confluent Cloud config file: %w", err)
			}
			fmt.Fprintf(a.OutWriter, "Detected Confluent Cloud config in file %v\n", path)
			username, password, broker, err := config.ParseConfluentCloudConfig(path)
			if err != nil {
				return fmt.Errorf("failed to parse Confluent Cloud config: %w", err)
			}

			newCluster := &config.Cluster{
				Name:    "ccloud",
				Brokers: []string{broker},
				SASL: &config.SASL{
					Username:  username,
					Password:  password,
					Mechanism: "PLAIN",
				},
				SecurityProtocol: "SASL_SSL",
			}

			var found bool
			for i, c := range a.Cfg.Clusters {
				if c.Name == "ccloud" {
					found = true
					a.Cfg.Clusters[i] = newCluster
					break
				}
			}

			if !found {
				fmt.Fprintln(a.OutWriter, "Wrote new entry to config file")
				a.Cfg.Clusters = append(a.Cfg.Clusters, newCluster)
			}

			if a.Cfg.CurrentCluster == "" {
				a.Cfg.CurrentCluster = newCluster.Name
			}
			if err = a.Cfg.Write(); err != nil {
				return fmt.Errorf("failed to write config: %w", err)
			}
			return nil
		},
		ValidArgs: []string{"ccloud"},
		Args: func(cmd *cobra.Command, args []string) error {
			if err := cobra.OnlyValidArgs(cmd, args); err != nil {
				return err
			}
			return cobra.ExactArgs(1)(cmd, args)
		},
	}
}
