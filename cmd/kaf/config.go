package main

import (
	"fmt"

	"github.com/infinimesh/kaf"
	"github.com/spf13/cobra"
)

func init() {
	configCmd.AddCommand(configImportCmd)
	configCmd.AddCommand(configUseCmd)
	configCmd.AddCommand(configLsCmd)
	configCmd.AddCommand(configAddClusterCmd)
	rootCmd.AddCommand(configCmd)
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
		if err := config.SetCurrentCluster(name); err != nil {
			fmt.Printf("Cluster with name %v not found\n", name)
		} else {
			if err := config.Write(); err != nil {
				panic(err)
			}
			fmt.Printf("Switched to cluster \"%v\".\n", name)
		}
	},
}

var configLsCmd = &cobra.Command{
	Use:   "get-clusters",
	Short: "Display clusters in the configuration file",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("NAME")
		for _, cluster := range config.Clusters {
			fmt.Println(cluster.Name)
		}
	},
}

var configAddClusterCmd = &cobra.Command{
	Use:   "add-cluster [NAME]",
	Short: "add cluster",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		config.Clusters = append(config.Clusters, &kaf.Cluster{
			Name:    args[0],
			Brokers: brokersFlag,
		})
		err := config.Write()
		if err != nil {
			panic(err)
		}
		fmt.Println("Added cluster.")
	},
}

var configImportCmd = &cobra.Command{
	Use:   "import [ccloud]",
	Short: "Import configurations into the $HOME/.kaf/config file",
	Run: func(cmd *cobra.Command, args []string) {
		if path, err := kaf.TryFindCcloudConfigFile(); err == nil {
			fmt.Printf("Detected Confluent Cloud config in file %v\n", path)
			if username, password, broker, err := kaf.ParseConfluentCloudConfig(path); err == nil {

				newCluster := &kaf.Cluster{
					Name:    "ccloud",
					Brokers: []string{broker},
					SASL: &kaf.SASL{
						Username:  username,
						Password:  password,
						Mechanism: "PLAIN",
					},
					SecurityProtocol: "SASL_SSL",
				}

				var found bool
				for i, newCluster := range config.Clusters {
					if newCluster.Name == "confluent cloud" {
						found = true
						config.Clusters[i] = newCluster
						break
					}
				}

				if !found {
					fmt.Println("Wrote new entry to config file")
					config.Clusters = append(config.Clusters, newCluster)
				}

				if config.CurrentCluster == "" {
					config.CurrentCluster = newCluster.Name
				}
				config.Write()

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
