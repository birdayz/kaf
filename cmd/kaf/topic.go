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
	topicCmd.AddCommand(deleteTopicCmd)

	createTopicCmd.Flags().Int32P("partitions", "p", int32(1), "Number of partitions")
	viper.BindPFlag("partitions", createTopicCmd.Flags().Lookup("partitions"))
	createTopicCmd.Flags().Int16P("replicas", "r", int16(1), "Number of replicas")
	viper.BindPFlag("replicas", createTopicCmd.Flags().Lookup("replicas"))

}

var topicCmd = &cobra.Command{
	Use:   "topic",
	Short: "Create and describe topics.",
}

var createTopicCmd = &cobra.Command{
	Use:   "create TOPIC",
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
			fmt.Printf("Created topic %v.\n", args[0])
		}
	},
}

var deleteTopicCmd = &cobra.Command{
	Use:   "delete TOPIC",
	Short: "Delete a topic",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		admin, err := getClusterAdmin()
		if err != nil {
			panic(err)
		}

		err = admin.DeleteTopic(args[0])
		if err != nil {
			fmt.Printf("Could not delete topic %v: %v\n", args[0], err.Error())
		} else {
			fmt.Printf("Deleted topic %v.\n", args[0])
		}
	},
}
