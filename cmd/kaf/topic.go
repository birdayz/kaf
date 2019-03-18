package main

import (
	"fmt"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
)

var (
	partitionsFlag int32
	replicasFlag   int16
	noHeaderFlag   bool
	compactFlag    bool
)

func init() {
	rootCmd.AddCommand(topicCmd)
	rootCmd.AddCommand(topicsCmd)
	topicCmd.AddCommand(createTopicCmd)
	topicCmd.AddCommand(deleteTopicCmd)
	topicCmd.AddCommand(lsTopicsCmd)
	topicCmd.AddCommand(describeTopicCmd)

	createTopicCmd.Flags().Int32VarP(&partitionsFlag, "partitions", "p", int32(1), "Number of partitions")
	createTopicCmd.Flags().Int16VarP(&replicasFlag, "replicas", "r", int16(1), "Number of replicas")
	createTopicCmd.Flags().BoolVarP(&compactFlag, "compact", "c", false, "Enable topic compaction")

	lsTopicsCmd.Flags().BoolVar(&noHeaderFlag, "no-headers", false, "Hide table headers")
}

var topicCmd = &cobra.Command{
	Use:   "topic",
	Short: "Create and describe topics.",
}

var topicsCmd = &cobra.Command{
	Use:   "topics",
	Short: "List topics",
	Run:   lsTopicsCmd.Run,
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

		if !noHeaderFlag {
			fmt.Fprintf(w, "NAME\tPARTITIONS\tREPLICAS\t\n")
		}

		for _, topic := range sortedTopics {
			fmt.Fprintf(w, "%v\t%v\t%v\t\n", topic.name, topic.NumPartitions, topic.ReplicationFactor)
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

		topicDetails, err := admin.DescribeTopics([]string{args[0]})
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

		detail := topicDetails[0]
		sort.Slice(detail.Partitions, func(i, j int) bool { return detail.Partitions[i].ID < detail.Partitions[j].ID })

		w := tabwriter.NewWriter(os.Stdout, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
		fmt.Fprintf(w, "Name:\t%v\t\n", detail.Name)
		fmt.Fprintf(w, "Internal:\t%v\t\n", detail.IsInternal)
		fmt.Fprintf(w, "Compacted:\t%v\t\n", compacted)
		fmt.Fprintf(w, "Partitions:\n")

		w.Flush()
		w.Init(os.Stdout, tabwriterMinWidthNested, 4, 2, tabwriterPadChar, tabwriterFlags)

		fmt.Fprintf(w, "\tPartition\tHigh Watermark\tLeader\tReplicas\tISR\t\n")
		fmt.Fprintf(w, "\t---------\t--------------\t------\t--------\t---\t\n")

		partitions := make([]int32, 0, len(detail.Partitions))
		for _, partition := range detail.Partitions {
			partitions = append(partitions, partition.ID)
		}
		highWatermarks := getHighWatermarks(args[0], partitions)

		for _, partition := range detail.Partitions {
			fmt.Fprintf(w, "\t%v\t%v\t%v\t%v\t%v\t\n", partition.ID, highWatermarks[partition.ID], partition.Leader, partition.Replicas, partition.Isr)
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

		compact := "delete"
		if compactFlag {
			compact = "compact"
		}
		err = admin.CreateTopic(args[0], &sarama.TopicDetail{
			NumPartitions:     partitionsFlag,
			ReplicationFactor: replicasFlag,
			ConfigEntries: map[string]*string{
				"cleanup.policy": &compact,
			},
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
