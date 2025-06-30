package main

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/IBM/sarama"
	"github.com/spf13/cobra"
)

var (
	partitionsFlag           int32
	partitionAssignmentsFlag string
	replicasFlag             int16
	noHeaderFlag             bool
	compactFlag              bool
)

func init() {
	rootCmd.AddCommand(topicCmd)
	rootCmd.AddCommand(topicsCmd)
	topicCmd.AddCommand(createTopicCmd)
	topicCmd.AddCommand(deleteTopicCmd)
	topicCmd.AddCommand(lsTopicsCmd)
	topicCmd.AddCommand(describeTopicCmd)
	topicCmd.AddCommand(addConfigCmd)
	topicCmd.AddCommand(removeConfigCmd)
	topicCmd.AddCommand(topicSetConfig)
	topicCmd.AddCommand(updateTopicCmd)
	topicCmd.AddCommand(lagCmd)

	createTopicCmd.Flags().Int32VarP(&partitionsFlag, "partitions", "p", int32(1), "Number of partitions")
	createTopicCmd.Flags().Int16VarP(&replicasFlag, "replicas", "r", int16(1), "Number of replicas")
	createTopicCmd.Flags().BoolVar(&compactFlag, "compact", false, "Enable topic compaction")

	lsTopicsCmd.Flags().BoolVar(&noHeaderFlag, "no-headers", false, "Hide table headers")
	topicsCmd.Flags().BoolVar(&noHeaderFlag, "no-headers", false, "Hide table headers")
	updateTopicCmd.Flags().Int32VarP(&partitionsFlag, "partitions", "p", int32(-1), "Number of partitions")
	updateTopicCmd.Flags().StringVar(&partitionAssignmentsFlag, "partition-assignments", "", "Partition Assignments. Optional. If set in combination with -p, an assignment must be provided for each new partition. Example: '[[1,2,3],[1,2,3]]' (JSON Array syntax) assigns two new partitions to brokers 1,2,3. If used by itself, a reassignment must be provided for all partitions.")
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

var topicSetConfig = &cobra.Command{
	Use:     "set-config",
	Short:   "set topic config. requires Kafka >=2.3.0 on broker side and kaf cluster config.",
	Example: "kaf topic set-config topic.name \"cleanup.policy=delete\"",
	Args:    cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		admin := getClusterAdmin()

		topic := args[0]

		splt := strings.Split(args[1], ",")
		configs := make(map[string]sarama.IncrementalAlterConfigsEntry)

		for _, kv := range splt {
			s := strings.Split(kv, "=")

			if len(s) != 2 {
				continue
			}

			key := s[0]
			value := s[1]
			configs[key] = sarama.IncrementalAlterConfigsEntry{
				Operation: sarama.IncrementalAlterConfigsOperationSet,
				Value:     &value,
			}
		}

		if len(configs) < 1 {
			errorExit("No valid configs found")
		}

		err := admin.IncrementalAlterConfig(sarama.TopicResource, topic, configs, false)
		if err != nil {
			errorExit("Unable to alter topic config: %v\n", err)
		}
		fmt.Printf("\xE2\x9C\x85 Updated config.")
	},
}

var updateTopicCmd = &cobra.Command{
	Use:     "update",
	Short:   "Update topic",
	Example: "kaf topic update -p 5 --partition-assignments '[[1,2,3],[1,2,3]]'",
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		admin := getClusterAdmin()

		if partitionsFlag == -1 && partitionAssignmentsFlag == "" {
			errorExit("Number of partitions and/or partition assigments must be given")
		}

		var assignments [][]int32
		if partitionAssignmentsFlag != "" {
			if err := json.Unmarshal([]byte(partitionAssignmentsFlag), &assignments); err != nil {
				errorExit("Invalid partition assignments: %v", err)
			}
		}

		if partitionsFlag != int32(-1) {
			err := admin.CreatePartitions(args[0], partitionsFlag, assignments, false)
			if err != nil {
				errorExit("Failed to create partitions: %v", err)
			}
		} else {
			// Needs at least Kafka version 2.4.0.
			err := admin.AlterPartitionReassignments(args[0], assignments)
			if err != nil {
				errorExit("Failed to reassign the partition assigments: %v", err)
			}
		}
		fmt.Printf("\xE2\x9C\x85 Updated topic!\n")
	},
}

var lsTopicsCmd = &cobra.Command{
	Use:     "ls",
	Aliases: []string{"list"},
	Short:   "List topics",
	Args:    cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		admin := getClusterAdmin()

		topics, err := admin.ListTopics()
		if err != nil {
			errorExit("Unable to list topics: %v\n", err)
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

		w := tabwriter.NewWriter(outWriter, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)

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
	Use:               "describe",
	Short:             "Describe topic",
	Long:              "Describe a topic. Default values of the configuration are omitted.",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: validTopicArgs,
	Run: func(cmd *cobra.Command, args []string) {
		admin := getClusterAdmin()

		topicDetails, err := admin.DescribeTopics([]string{args[0]})
		if err != nil {
			errorExit("Unable to describe topics: %v\n", err)
		}

		if topicDetails[0].Err == sarama.ErrUnknownTopicOrPartition {
			fmt.Printf("Topic %v not found.\n", args[0])
			return
		}

		cfg, err := admin.DescribeConfig(sarama.ConfigResource{
			Type: sarama.TopicResource,
			Name: args[0],
		})
		if err != nil {
			errorExit("Unable to describe config: %v\n", err)
		}

		var compacted bool
		for _, e := range cfg {
			if e.Name == "cleanup.policy" {
				for _, setting := range strings.Split(e.Value, ",") {
					if setting == "compact" {
						compacted = true
					}
				}
			}
		}

		detail := topicDetails[0]
		sort.Slice(detail.Partitions, func(i, j int) bool { return detail.Partitions[i].ID < detail.Partitions[j].ID })

		w := tabwriter.NewWriter(outWriter, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
		fmt.Fprintf(w, "Name:\t%v\t\n", detail.Name)
		fmt.Fprintf(w, "Internal:\t%v\t\n", detail.IsInternal)
		fmt.Fprintf(w, "Compacted:\t%v\t\n", compacted)
		fmt.Fprintf(w, "Partitions:\n")

		w.Flush()
		w.Init(outWriter, tabwriterMinWidthNested, 4, 2, tabwriterPadChar, tabwriterFlags)

		fmt.Fprintf(w, "\tPartition\tHigh Watermark\tLeader\tReplicas\tISR\t\n")
		fmt.Fprintf(w, "\t---------\t--------------\t------\t--------\t---\t\n")

		partitions := make([]int32, 0, len(detail.Partitions))
		for _, partition := range detail.Partitions {
			partitions = append(partitions, partition.ID)
		}
		highWatermarks := getHighWatermarks(admin, args[0], partitions)
		highWatermarksSum := 0

		for _, partition := range detail.Partitions {
			sortedReplicas := partition.Replicas
			sort.Slice(sortedReplicas, func(i, j int) bool { return sortedReplicas[i] < sortedReplicas[j] })

			sortedISR := partition.Isr
			sort.Slice(sortedISR, func(i, j int) bool { return sortedISR[i] < sortedISR[j] })

			highWatermarksSum += int(highWatermarks[partition.ID])

			fmt.Fprintf(w, "\t%v\t%v\t%v\t%v\t%v\t\n", partition.ID, highWatermarks[partition.ID], partition.Leader, sortedReplicas, sortedISR)
		}

		w.Flush()

		fmt.Fprintf(w, "Summed HighWatermark:\t%d\n", highWatermarksSum)
		w.Flush()

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
		admin := getClusterAdmin()

		topicName := args[0]
		compact := "delete"
		if compactFlag {
			compact = "compact"
		}
		err := admin.CreateTopic(topicName, &sarama.TopicDetail{
			NumPartitions:     partitionsFlag,
			ReplicationFactor: replicasFlag,
			ConfigEntries: map[string]*string{
				"cleanup.policy": &compact,
			},
		}, false)
		if err != nil {
			errorExit("Could not create topic %v: %v\n", topicName, err.Error())
		} else {
			w := tabwriter.NewWriter(outWriter, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
			fmt.Fprintf(w, "\xE2\x9C\x85 Created topic!\n")
			fmt.Fprintln(w, "\tTopic Name:\t", topicName)
			fmt.Fprintln(w, "\tPartitions:\t", partitionsFlag)
			fmt.Fprintln(w, "\tReplication Factor:\t", replicasFlag)
			fmt.Fprintln(w, "\tCleanup Policy:\t", compact)
			w.Flush()
		}
	},
}

var addConfigCmd = &cobra.Command{
	Use:   "add-config TOPIC KEY VALUE",
	Short: "Add config key/value pair to topic",
	Args:  cobra.ExactArgs(3), // TODO how to unset ? support empty VALUE ?
	Run: func(cmd *cobra.Command, args []string) {
		admin := getClusterAdmin()

		topic := args[0]
		key := args[1]
		value := args[2]

		err := admin.AlterConfig(sarama.TopicResource, topic, map[string]*string{
			key: &value,
		}, false)
		if err != nil {
			errorExit("failed to update topic config: %v", err)
		} else {
			fmt.Printf("Added config %v=%v to topic %v.\n", key, value, topic)
		}
	},
}

var removeConfigCmd = &cobra.Command{
	Use:   "rm-config TOPIC ATTR1,ATTR2...",
	Short: "Remove attributes from topic",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		admin := getClusterAdmin()

		topic := args[0]
		attrsToRemove := strings.Split(args[1], ",")

		updatedTopicConfigs := make(map[string]*string)

		allTopicConfigs, err := admin.DescribeConfig(sarama.ConfigResource{
			Type: sarama.TopicResource,
			Name: topic,
		})
		if err != nil {
			errorExit("failed to describe topic config: %v", err)
		}

		for _, v := range allTopicConfigs {
			if !slices.Contains(attrsToRemove, v.Name) {
				updatedTopicConfigs[v.Name] = &v.Value
			}
		}

		err = admin.AlterConfig(sarama.TopicResource, topic, updatedTopicConfigs, false)
		if err != nil {
			errorExit("failed to remove attributes from topic config: %v", err)
		}
		fmt.Printf("Removed attributes %v from topic %v.\n", attrsToRemove, topic)
	},
}

var deleteTopicCmd = &cobra.Command{
	Use:               "delete TOPIC",
	Short:             "Delete a topic",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: validTopicArgs,
	Run: func(cmd *cobra.Command, args []string) {
		admin := getClusterAdmin()

		topicName := args[0]
		err := admin.DeleteTopic(topicName)
		if err != nil {
			errorExit("Could not delete topic %v: %v\n", topicName, err.Error())
		} else {
			fmt.Fprintf(outWriter, "\xE2\x9C\x85 Deleted topic %v!\n", topicName)
		}
	},
}

var lagCmd = &cobra.Command{
	Use:   "lag",
	Short: "Display the total lags for each consumer group",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		topic := args[0]
		admin := getClusterAdmin()
		defer admin.Close()

		// Describe the topic
		topicDetails, err := admin.DescribeTopics([]string{topic})
		if err != nil || len(topicDetails) == 0 {
			errorExit("Unable to describe topics: %v\n", err)
		}

		// Get the list of partitions for the topic
		partitions := make([]int32, 0, len(topicDetails[0].Partitions))
		for _, partition := range topicDetails[0].Partitions {
			partitions = append(partitions, partition.ID)
		}
		highWatermarks := getHighWatermarks(admin, topic, partitions)

		// List all consumer groups
		consumerGroups, err := admin.ListConsumerGroups()
		if err != nil {
			errorExit("Unable to list consumer groups: %v\n", err)
		}

		var groups []string
		for group := range consumerGroups {
			groups = append(groups, group)
		}

		// Describe all consumer groups
		groupsInfo, err := admin.DescribeConsumerGroups(groups)
		if err != nil {
			errorExit("Unable to describe consumer groups: %v\n", err)
		}

		// Calculate lag for each group
		lagInfo := make(map[string]int64)
		groupStates := make(map[string]string) // To store the state of each group
		for _, group := range groupsInfo {
			var sum int64
			show := false
			for _, member := range group.Members {
				assignment, err := member.GetMemberAssignment()
				if err != nil || assignment == nil {
					continue
				}

				metadata, err := member.GetMemberMetadata()
				if err != nil || metadata == nil {
					continue
				}

				if topicPartitions, exist := assignment.Topics[topic]; exist {
					show = true
					resp, err := admin.ListConsumerGroupOffsets(group.GroupId, map[string][]int32{topic: topicPartitions})
					if err != nil {
						fmt.Fprintf(os.Stderr, "Error fetching offsets for group %s: %v\n", group.GroupId, err)
						continue
					}

					if blocks, ok := resp.Blocks[topic]; ok {
						for pid, block := range blocks {
							if hwm, ok := highWatermarks[pid]; ok {
								if block.Offset > hwm {
									fmt.Fprintf(os.Stderr, "Warning: Consumer offset (%d) is greater than high watermark (%d) for partition %d in group %s\n", block.Offset, hwm, pid, group.GroupId)
								} else if block.Offset < 0 {
									// Skip partitions with negative offsets
								} else {
									sum += hwm - block.Offset
								}
							}
						}
					}
				}
			}

			if show && sum >= 0 {
				lagInfo[group.GroupId] = sum
				groupStates[group.GroupId] = group.State // Store the state of the group
			}
		}

		// Print the lag information along with group state
		w := tabwriter.NewWriter(outWriter, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
		if !noHeaderFlag {
			fmt.Fprintf(w, "GROUP ID\tSTATE\tLAG\n")
		}
		for group, lag := range lagInfo {
			fmt.Fprintf(w, "%v\t%v\t%v\n", group, groupStates[group], lag)
		}
		w.Flush()
	},
}
