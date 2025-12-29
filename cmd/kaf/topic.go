package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/twmb/franz-go/pkg/kadm"
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
		defer admin.Close()

		topic := args[0]

		splt := strings.Split(args[1], ",")
		configs := make(map[string]*string)

		for _, kv := range splt {
			s := strings.Split(kv, "=")

			if len(s) != 2 {
				continue
			}

			key := s[0]
			value := s[1]
			configs[key] = &value
		}

		if len(configs) < 1 {
			errorExit("No valid configs found")
		}

		ctx := context.Background()
		// Use the simplified AlterTopicConfigs API - convert to the required format
		alterConfigs := make([]kadm.AlterConfig, 0, len(configs))
		for key, value := range configs {
			alterConfigs = append(alterConfigs, kadm.AlterConfig{
				Name:  key,
				Value: value,
			})
		}
		_, err := admin.AlterTopicConfigs(ctx, alterConfigs, topic)
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
		defer admin.Close()

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
			ctx := context.Background()
			_, err := admin.CreatePartitions(ctx, int(partitionsFlag), args[0])
			if err != nil {
				errorExit("Failed to create partitions: %v", err)
			}
		} else {
			// Needs at least Kafka version 2.4.0.
			ctx := context.Background()
			// Create partition assignments map
			partitionAssignments := make(map[string]map[int32][]int32)
			topicAssignments := make(map[int32][]int32)
			for i, brokers := range assignments {
				topicAssignments[int32(i)] = brokers
			}
			partitionAssignments[args[0]] = topicAssignments
			_, err := admin.AlterPartitionAssignments(ctx, partitionAssignments)
			if err != nil {
				errorExit("Failed to reassign the partition assignments: %v", err)
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
		defer admin.Close()

		ctx := context.Background()
		topicsResp, err := admin.ListTopics(ctx)
		if err != nil {
			errorExit("Unable to list topics: %v\n", err)
		}

		sortedTopics := make(
			[]struct {
				name       string
				partitions int32
				replicas   int16
			}, len(topicsResp))

		i := 0
		for name, topic := range topicsResp {
			sortedTopics[i].name = name
			sortedTopics[i].partitions = int32(len(topic.Partitions))
			// Calculate replication factor from first partition (if exists)
			if len(topic.Partitions) > 0 {
				sortedTopics[i].replicas = int16(len(topic.Partitions[0].Replicas))
			} else {
				sortedTopics[i].replicas = 0
			}
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
			fmt.Fprintf(w, "%v\t%v\t%v\t\n", topic.name, topic.partitions, topic.replicas)
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
		defer admin.Close()

		ctx := context.Background()
		topicDetails, err := admin.ListTopics(ctx, args[0])
		if err != nil {
			errorExit("Unable to describe topics: %v\n", err)
		}

		detail, exists := topicDetails[args[0]]
		if !exists {
			fmt.Printf("Topic %v not found.\n", args[0])
			return
		}

		cfgResp, err := admin.DescribeTopicConfigs(ctx, args[0])
		if err != nil {
			errorExit("Unable to describe config: %v\n", err)
		}

		var compacted bool
		for _, resourceConfig := range cfgResp {
			for _, config := range resourceConfig.Configs {
				if config.Key == "cleanup.policy" {
					if config.Value != nil {
						for _, setting := range strings.Split(*config.Value, ",") {
							if setting == "compact" {
								compacted = true
							}
						}
					}
				}
			}
		}

		// detail is already assigned above
		// Convert map to slice for sorting
		partitionSlice := make([]kadm.PartitionDetail, 0, len(detail.Partitions))
		for _, partition := range detail.Partitions {
			partitionSlice = append(partitionSlice, partition)
		}
		sort.Slice(partitionSlice, func(i, j int) bool { return partitionSlice[i].Partition < partitionSlice[j].Partition })

		w := tabwriter.NewWriter(outWriter, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
		fmt.Fprintf(w, "Name:\t%v\t\n", args[0])
		fmt.Fprintf(w, "Internal:\t%v\t\n", detail.IsInternal)
		fmt.Fprintf(w, "Compacted:\t%v\t\n", compacted)
		fmt.Fprintf(w, "Partitions:\n")

		w.Flush()
		w.Init(outWriter, tabwriterMinWidthNested, 4, 2, tabwriterPadChar, tabwriterFlags)

		fmt.Fprintf(w, "\tPartition\tHigh Watermark\tLeader\tReplicas\tISR\t\n")
		fmt.Fprintf(w, "\t---------\t--------------\t------\t--------\t---\t\n")

		partitions := make([]int32, 0, len(partitionSlice))
		for _, partition := range partitionSlice {
			partitions = append(partitions, partition.Partition)
		}
		highWatermarks := getKgoHighWatermarks(args[0], partitions)
		highWatermarksSum := 0

		for _, partition := range partitionSlice {
			sortedReplicas := partition.Replicas
			sort.Slice(sortedReplicas, func(i, j int) bool { return sortedReplicas[i] < sortedReplicas[j] })

			sortedISR := partition.ISR
			sort.Slice(sortedISR, func(i, j int) bool { return sortedISR[i] < sortedISR[j] })

			highWatermarksSum += int(highWatermarks[partition.Partition])

			fmt.Fprintf(w, "\t%v\t%v\t%v\t%v\t%v\t\n", partition.Partition, highWatermarks[partition.Partition], partition.Leader, sortedReplicas, sortedISR)
		}

		w.Flush()

		fmt.Fprintf(w, "Summed HighWatermark:\t%d\n", highWatermarksSum)
		w.Flush()

		fmt.Fprintf(w, "Config:\n")
		fmt.Fprintf(w, "\tName\tValue\t\n")
		fmt.Fprintf(w, "\t----\t-----\t\n")

		for _, resourceConfig := range cfgResp {
			for _, config := range resourceConfig.Configs {
				// Show all configs for now
				fmt.Fprintf(w, "\t%v\t%v\t\n", config.Key, *config.Value)
			}
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
		defer admin.Close()

		topicName := args[0]
		compact := "delete"
		if compactFlag {
			compact = "compact"
		}
		ctx := context.Background()
		_, err := admin.CreateTopics(ctx, partitionsFlag, replicasFlag, map[string]*string{
			"cleanup.policy": &compact,
		}, topicName)
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
		defer admin.Close()

		topic := args[0]
		key := args[1]
		value := args[2]

		ctx := context.Background()
		_, err := admin.AlterTopicConfigs(ctx, []kadm.AlterConfig{{
			Name:  key,
			Value: &value,
		}}, topic)
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
		defer admin.Close()

		topic := args[0]
		attrsToRemove := strings.Split(args[1], ",")

		updatedTopicConfigs := make(map[string]*string)

		ctx := context.Background()
		allTopicConfigs, err := admin.DescribeTopicConfigs(ctx, topic)
		if err != nil {
			errorExit("failed to describe topic config: %v", err)
		}

		if len(allTopicConfigs) > 0 {
			for _, config := range allTopicConfigs[0].Configs {
				if !slices.Contains(attrsToRemove, config.Key) {
					updatedTopicConfigs[config.Key] = config.Value
				}
			}
		}

		// Convert to the expected format
		alterConfigs := make([]kadm.AlterConfig, 0, len(updatedTopicConfigs))
		for key, value := range updatedTopicConfigs {
			alterConfigs = append(alterConfigs, kadm.AlterConfig{
				Name:  key,
				Value: value,
			})
		}
		_, err = admin.AlterTopicConfigs(ctx, alterConfigs, topic)
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
		defer admin.Close()

		topicName := args[0]
		ctx := context.Background()
		_, err := admin.DeleteTopics(ctx, topicName)
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

		ctx := context.Background()
		// Describe the topic
		topicDetails, err := admin.ListTopics(ctx, topic)
		if err != nil {
			errorExit("Unable to describe topics: %v\n", err)
		}
		
		topicDetail, exists := topicDetails[topic]
		if !exists {
			errorExit("Topic %v not found.\n", topic)
		}

		// Get the list of partitions for the topic
		partitions := make([]int32, 0, len(topicDetail.Partitions))
		for _, partition := range topicDetail.Partitions {
			partitions = append(partitions, partition.Partition)
		}
		highWatermarks := getKgoHighWatermarks(topic, partitions)

		// List all consumer groups
		consumerGroups, err := admin.ListGroups(ctx)
		if err != nil {
			errorExit("Unable to list consumer groups: %v\n", err)
		}

		groups := consumerGroups.Groups()

		// Describe all consumer groups
		groupsInfo, err := admin.DescribeGroups(ctx, groups...)
		if err != nil {
			errorExit("Unable to describe consumer groups: %v\n", err)
		}

		// Calculate lag for each group
		lagInfo := make(map[string]int64)
		groupStates := make(map[string]string) // To store the state of each group
		for _, group := range groupsInfo {
			var sum int64
			show := false
			// Try to get offsets for this group and topic
			resp, err := admin.FetchOffsets(ctx, group.Group)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error fetching offsets for group %s: %v\n", group.Group, err)
				continue
			}

			if offsets, ok := resp[topic]; ok {
				show = true
				for pid, offset := range offsets {
					if hwm, ok := highWatermarks[pid]; ok {
						if offset.At > hwm {
							fmt.Fprintf(os.Stderr, "Warning: Consumer offset (%d) is greater than high watermark (%d) for partition %d in group %s\n", offset.At, hwm, pid, group.Group)
						} else if offset.At < 0 {
							// Skip partitions with negative offsets
						} else {
							sum += hwm - offset.At
						}
					}
				}
			}

			if show && sum >= 0 {
				lagInfo[group.Group] = sum
				groupStates[group.Group] = group.State // Store the state of the group
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
