package main

import (
	"fmt"
	"os"

	"github.com/birdayz/kaf/pkg/config"
	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
)

func init() {
	configCmd.AddCommand(configImportCmd)
	configCmd.AddCommand(configUseCmd)
	configCmd.AddCommand(configLsCmd)
	configCmd.AddCommand(configAddClusterCmd)
	configCmd.AddCommand(configSelectCluster)
	rootCmd.AddCommand(configCmd)

	configLsCmd.Flags().BoolVar(&noHeaderFlag, "no-headers", false, "Hide table headers")
}

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Handle kaf configuration",
}

var configUseCmd = &cobra.Command{
	Use:   "use-cluster [NAME]",
	Short: "Sets the current cluster in the configuration",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		name := args[0]
		if err := cfg.SetCurrentCluster(name); err != nil {
			fmt.Printf("Cluster with name %v not found\n", name)
		} else {
			fmt.Printf("Switched to cluster \"%v\".\n", name)
		}
	},
}

var configLsCmd = &cobra.Command{
	Use:   "get-clusters",
	Short: "Display clusters in the configuration file",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		if !noHeaderFlag {
			fmt.Println("NAME")
		}
		for _, cluster := range cfg.Clusters {
			fmt.Println(cluster.Name)
		}
	},
}

var configSelectCluster = &cobra.Command{
	Use:   "select-cluster",
	Short: "Interactively select a cluster",
	Run: func(cmd *cobra.Command, args []string) {
		var clusterNames []string
		for _, cluster := range cfg.Clusters {
			clusterNames = append(clusterNames, cluster.Name)
		}
		p := promptui.Select{
			Label: "Select cluster",
			Items: clusterNames,
		}

		_, selected, err := p.Run()
		if err != nil {
			os.Exit(0)
		}

		// How to have selection on currently selected cluster?

		// TODO copy pasta
		if err := cfg.SetCurrentCluster(selected); err != nil {
			fmt.Printf("Cluster with selected %v not found\n", selected)
		}
	},
}

var configAddClusterCmd = &cobra.Command{
	Use:   "add-cluster [NAME]",
	Short: "add cluster",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		name := args[0]
		for _, cluster := range cfg.Clusters {
			if cluster.Name == name {
				errorExit("Could not add cluster: cluster with name '%v' exists already.", name)
			}
		}

		cfg.Clusters = append(cfg.Clusters, &config.Cluster{
			Name:              name,
			Brokers:           brokersFlag,
			SchemaRegistryURL: schemaRegistryURL,
		})
		err := cfg.Write()
		if err != nil {
			errorExit("Unable to write config: %v\n", err)
		}
		fmt.Println("Added cluster.")
	},
}

var configImportCmd = &cobra.Command{
	Use:   "import [ccloud]",
	Short: "Import configurations into the $HOME/.kaf/config file",
	Run: func(cmd *cobra.Command, args []string) {
		if path, err := config.TryFindCcloudConfigFile(); err == nil {
			fmt.Printf("Detected Confluent Cloud config in file %v\n", path)
			if username, password, broker, err := config.ParseConfluentCloudConfig(path); err == nil {

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
				for i, newCluster := range cfg.Clusters {
					if newCluster.Name == "confluent cloud" {
						found = true
						cfg.Clusters[i] = newCluster
						break
					}
				}

				if !found {
					fmt.Println("Wrote new entry to config file")
					cfg.Clusters = append(cfg.Clusters, newCluster)
				}

				if cfg.CurrentCluster == "" {
					cfg.CurrentCluster = newCluster.Name
				}
				cfg.Write()

			}
		}
	},
	ValidArgs: []string{"ccloud"},
	Args: func(cmd *cobra.Command, args []string) error {
		if err := cobra.OnlyValidArgs(cmd, args); err != nil {
			return err
		}

		if err := cobra.ExactArgs(1)(cmd, args); err != nil {
			return err
		}
		return nil
	},
}
