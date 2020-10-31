package main

import (
	"fmt"
	"os"
	"strings"

	"regexp"

	"github.com/Shopify/sarama"
	"github.com/birdayz/kaf/pkg/config"
	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
)

var (
	flagEhConnString  string
	flagBrokerVersion string
)

func init() {
	configCmd.AddCommand(configImportCmd)
	configCmd.AddCommand(configUseCmd)
	configCmd.AddCommand(configLsCmd)
	configCmd.AddCommand(configAddClusterCmd)
	configCmd.AddCommand(configRemoveClusterCmd)
	configCmd.AddCommand(configSelectCluster)
	configCmd.AddCommand(configCurrentContext)
	configCmd.AddCommand(configAddEventhub)
	rootCmd.AddCommand(configCmd)

	configLsCmd.Flags().BoolVar(&noHeaderFlag, "no-headers", false, "Hide table headers")
	configAddEventhub.Flags().StringVar(&flagEhConnString, "eh-connstring", "", "EventHub ConnectionString")
	configAddClusterCmd.Flags().StringVar(&flagBrokerVersion, "broker-version", "", fmt.Sprintf("Broker Version. Available Versions: %v", sarama.SupportedVersions))
}

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Handle kaf configuration",
}

var configCurrentContext = &cobra.Command{
	Use:   "current-context",
	Short: "Displays the current context",
	Args:  cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(cfg.CurrentCluster)
	},
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

var configAddEventhub = &cobra.Command{
	Use:     "add-eventhub [NAME]",
	Example: "esp config add-eventhub my-eventhub --eh-connstring 'Endpoint=sb://......AccessKey=....'",
	Short:   "Add Azure EventHub",
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		name := args[0]
		for _, cluster := range cfg.Clusters {
			if cluster.Name == name {
				errorExit("Could not add cluster: cluster with name '%v' exists already.", name)
			}
		}

		// Parse hub name from ConnString
		r, _ := regexp.Compile(`^Endpoint=sb://(.*)\.servicebus.*$`)
		hubName := r.FindStringSubmatch(flagEhConnString)
		if len(hubName) != 2 {
			errorExit("Failed to determine EventHub name from Connection String. Check your ConnectionString")
		}

		cfg.Clusters = append(cfg.Clusters, &config.Cluster{
			Name:              name,
			Brokers:           []string{hubName[1] + ".servicebus.windows.net:9093"},
			SchemaRegistryURL: schemaRegistryURL,
			SASL: &config.SASL{
				Mechanism: "PLAIN",
				Username:  "$ConnectionString",
				Password:  flagEhConnString,
			},
			SecurityProtocol: "SASL_SSL",
		})
		err := cfg.Write()
		if err != nil {
			errorExit("Unable to write config: %v\n", err)
		}
		fmt.Println("Added EventHub.")
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

		searcher := func(input string, index int) bool {
			cluster := clusterNames[index]
			name := strings.Replace(strings.ToLower(cluster), " ", "", -1)
			input = strings.Replace(strings.ToLower(input), " ", "", -1)
			return strings.Contains(name, input)
		}

		p := promptui.Select{
			Label:    "Select cluster",
			Items:    clusterNames,
			Searcher: searcher,
			Size:     10,
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
	Short: "Add cluster",
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
			Version:           flagBrokerVersion,
		})
		err := cfg.Write()
		if err != nil {
			errorExit("Unable to write config: %v\n", err)
		}
		fmt.Println("Added cluster.")
	},
}

var configRemoveClusterCmd = &cobra.Command{
	Use:   "remove-cluster [NAME]",
	Short: "remove cluster",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		name := args[0]

		var pos = -1
		for i, cluster := range cfg.Clusters {
			if cluster.Name == name {
				pos = i
				break
			}
		}

		if pos == -1 {
			errorExit("Could not delete cluster: cluster with name '%v' not exists.", name)
		}

		cfg.Clusters = append(cfg.Clusters[:pos], cfg.Clusters[pos+1:]...)

		err := cfg.Write()
		if err != nil {
			errorExit("Unable to write config: %v\n", err)
		}
		fmt.Println("Removed cluster.")
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
				err = cfg.Write()
				if err != nil {
					errorExit("Failed to write config: %w", err)
				}

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
