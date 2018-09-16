package main

import (
	"fmt"
	"os"
	"sort"
	"text/tabwriter"

	sarama "github.com/birdayz/sarama"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func init() {
	rootCmd.AddCommand(topicCmd)
	topicCmd.AddCommand(createTopicCmd)
	topicCmd.AddCommand(deleteTopicCmd)
	topicCmd.AddCommand(lsTopicsCmd)
	topicCmd.AddCommand(describeTopicCmd)

	createTopicCmd.Flags().Int32P("partitions", "p", int32(1), "Number of partitions")
	viper.BindPFlag("partitions", createTopicCmd.Flags().Lookup("partitions"))
	createTopicCmd.Flags().Int16P("replicas", "r", int16(1), "Number of replicas")
	viper.BindPFlag("replicas", createTopicCmd.Flags().Lookup("replicas"))

}

var topicCmd = &cobra.Command{
	Use:   "topic",
	Short: "Create and describe topics.",
}

var lsTopicsCmd = &cobra.Command{
	Use:     "ls",
	Aliases: []string{"list"},
	Short:   "List topics",
	Args:    cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		admin, err := getClusterAdmin()
		if err != nil {
			panic(err)
		}

		topics, err := admin.ListTopics()
		if err != nil {
			panic(err)
		}

		sortedTopics := make(
			[]struct {
				name string
				sarama.TopicDetail
			}, len(topics))

		i := 0
		for name, topic := range topics {
			sortedTopics[i].name = name
			sortedTopics[i].TopicDetail = topic
			i++
		}

		sort.Slice(sortedTopics, func(i int, j int) bool {
			return sortedTopics[i].name < sortedTopics[j].name
		})

		w := tabwriter.NewWriter(os.Stdout, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
		fmt.Fprintf(w, "NAME\tPARTITIONS\tREPLICAS\tINTERNAL\t\n")

		for _, topic := range sortedTopics {
			moreDetail, err := admin.DescribeTopic([]string{topic.name})
			if err != nil {
				panic(err)
			}
			fmt.Fprintf(w, "%v\t%v\t%v\t%v\t\n", topic.name, topic.NumPartitions, topic.ReplicationFactor, moreDetail[0].IsInternal)
		}
		w.Flush()
	},
}

var describeTopicCmd = &cobra.Command{
	Use:   "describe",
	Short: "Describe topic",
	Long:  "Describe a topic. Default values of the configuration are omitted.",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		admin, err := getClusterAdmin()
		if err != nil {
			panic(err)
		}

		topicDetails, err := admin.DescribeTopic([]string{args[0]})
		if err != nil {
			panic(err)
		}

		if topicDetails[0].Err == sarama.ErrUnknownTopicOrPartition {
			fmt.Printf("Topic %v not found.\n", args[0])
			return
		}

		cfg, err := admin.DescribeConfig(sarama.ConfigResource{
			Type: sarama.TopicResource,
			Name: args[0],
		})

		var compacted bool
		for _, e := range cfg {
			if e.Name == "cleanup.policy" && e.Value == "compact" {
				compacted = true
			}
		}

		if err != nil {
			panic(err)
		}

		w := tabwriter.NewWriter(os.Stdout, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
		detail := topicDetails[0]
		fmt.Fprintf(w, "Name:\t%v\t\n", detail.Name)
		fmt.Fprintf(w, "Internal:\t%v\t\n", detail.IsInternal)
		fmt.Fprintf(w, "Compacted:\t%v\t\n", compacted)
		fmt.Fprintf(w, "Partitions:\n")

		w.Flush()
		w.Init(os.Stdout, tabwriterMinWidthNested, 4, 2, tabwriterPadChar, tabwriterFlags)

		fmt.Fprintf(w, "\tPartition\tLeader\tReplicas\tISR\t\n")
		fmt.Fprintf(w, "\t---------\t------\t--------\t---\t\n")

		sort.Slice(detail.Partitions, func(i, j int) bool { return detail.Partitions[i].ID < detail.Partitions[j].ID })

		for _, partition := range detail.Partitions {
			fmt.Fprintf(w, "\t%v\t%v\t%v\t%v\t\n", partition.ID, partition.Leader, partition.Replicas, partition.Isr)
		}
		fmt.Fprintf(w, "Config:\n")
		fmt.Fprintf(w, "\tName\tValue\tReadOnly\tSensitive\t\n")
		fmt.Fprintf(w, "\t----\t-----\t--------\t---------\t\n")

		for _, entry := range cfg {
			if entry.Default {
				continue
			}
			fmt.Fprintf(w, "\t%v\t%v\t%v\t%v\t\n", entry.Name, entry.Value, entry.ReadOnly, entry.Sensitive)
		}

		w.Flush()
	},
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
