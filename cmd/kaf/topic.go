package main

import (
	"fmt"

	sarama "github.com/birdayz/sarama"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func init() {
	rootCmd.AddCommand(topicCmd)
	topicCmd.AddCommand(createTopicCmd)

	createTopicCmd.Flags().StringP("partitions", "p", "1", "Number of partitions")
	viper.BindPFlag("partitions", createTopicCmd.Flags().Lookup("partitions"))
	createTopicCmd.Flags().StringP("replicas", "r", "1", "Number of replicas")
	viper.BindPFlag("replicas", createTopicCmd.Flags().Lookup("replicas"))

}

var topicCmd = &cobra.Command{
	Use:   "topic",
	Short: "Create and describe topics.",
}

var createTopicCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a topic",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		admin, err := getClusterAdmin()
		if err != nil {
			panic(err)
		}

		err = admin.CreateTopic(args[0], &sarama.TopicDetail{
			NumPartitions:     viper.GetInt32("partitions"),
			ReplicationFactor: int16(viper.GetInt("replicas")),
		}, false)
		if err != nil {
			fmt.Printf("Could not create topic %v: %v\n", args[0], err.Error())
		} else {
			fmt.Printf("Created topic %v\n.", args[0])
		}
	},
}
