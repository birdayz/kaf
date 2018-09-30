package main

import (
	"fmt"

	"github.com/infinimesh/kaf"
	"github.com/spf13/cobra"
)

func init() {
	configCmd.AddCommand(configImportCmd)
	rootCmd.AddCommand(configCmd)
}

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Handle kaf configuration",
}

var configImportCmd = &cobra.Command{
	Use:   "import",
	Short: "Import configurations into the $HOME/.kaf/config file",
	Run: func(cmd *cobra.Command, args []string) {
		if path, err := kaf.TryFindCcloudConfigFile(); err == nil {
			fmt.Printf("Detected Confluent Cloud config in file %v\n", path)
			if username, password, broker, err := kaf.ParseConfluentCloudConfig(path); err == nil {
				cluster := &kaf.Cluster{
					Name:    "confluent cloud",
					Brokers: []string{broker},
					SASL: &kaf.SASL{
						Username:  username,
						Password:  password,
						Mechanism: "PLAIN",
					},
					SecurityProtocol: "SASL_SSL",
				}

				// config := kaf.Config{
				// 	Clusters: []*kaf.Cluster{cluster},
				// }

				config.Clusters = append(config.Clusters, cluster)
				config.Write()

			}
		}
		// fmt.Println(args)
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
