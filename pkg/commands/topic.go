package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
)

func (c *Commands) GetTopicCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "topic",
		Short: "Create and describe topics.",
	}

	cmd.AddCommand(c.createCreateTopicCmd())
	cmd.AddCommand(c.createDeleteTopicCmd())
	cmd.AddCommand(c.createLsTopicsCmd())
	cmd.AddCommand(c.createDescribeTopicCmd())
	cmd.AddCommand(c.createAddConfigCmd())
	cmd.AddCommand(c.createRemoveConfigCmd())
	cmd.AddCommand(c.createTopicSetConfigCmd())
	cmd.AddCommand(c.createUpdateTopicCmd())
	cmd.AddCommand(c.createLagCmd())

	return cmd
}

func (c *Commands) GetTopicsCmd() *cobra.Command {
	lsCmd := c.createLsTopicsCmd()

	cmd := &cobra.Command{
		Use:   "topics",
		Short: "List topics",
		Run:   lsCmd.Run,
	}

	// Copy flags from ls command
	var noHeaderFlag bool
	cmd.Flags().BoolVar(&noHeaderFlag, "no-headers", false, "Hide table headers")

	return cmd
}

func (c *Commands) createTopicSetConfigCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "set-config",
		Short:   "set topic config. requires Kafka >=2.3.0 on broker side and kaf cluster config.",
		Example: "kaf topic set-config topic.name \"cleanup.policy=delete\"",
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
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
				return fmt.Errorf("no valid configs found")
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
			_, err := c.Admin.AlterTopicConfigs(ctx, alterConfigs, topic)
			if err != nil {
				return fmt.Errorf("unable to alter topic config: %w", err)
			}
			fmt.Fprintf(c.OutWriter, "\xE2\x9C\x85 Updated config.")
			return nil
		},
	}
}

func (c *Commands) createUpdateTopicCmd() *cobra.Command {
	var partitionsFlag int32
	var partitionAssignmentsFlag string

	cmd := &cobra.Command{
		Use:     "update",
		Short:   "Update topic",
		Example: "kaf topic update -p 5 --partition-assignments '[[1,2,3],[1,2,3]]'",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if partitionsFlag == -1 && partitionAssignmentsFlag == "" {
				return fmt.Errorf("number of partitions and/or partition assigments must be given")
			}

			var assignments [][]int32
			if partitionAssignmentsFlag != "" {
				if err := json.Unmarshal([]byte(partitionAssignmentsFlag), &assignments); err != nil {
					return fmt.Errorf("invalid partition assignments: %w", err)
				}
			}

			if partitionsFlag != int32(-1) {
				ctx := context.Background()
				_, err := c.Admin.CreatePartitions(ctx, int(partitionsFlag), args[0])
				if err != nil {
					return fmt.Errorf("failed to create partitions: %w", err)
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
				_, err := c.Admin.AlterPartitionAssignments(ctx, partitionAssignments)
				if err != nil {
					return fmt.Errorf("failed to reassign the partition assignments: %w", err)
				}
			}
			fmt.Fprintf(c.OutWriter, "\xE2\x9C\x85 Updated topic!\n")
			return nil
		},
	}

	cmd.Flags().Int32VarP(&partitionsFlag, "partitions", "p", int32(-1), "Number of partitions")
	cmd.Flags().StringVar(&partitionAssignmentsFlag, "partition-assignments", "", "Partition Assignments. Optional. If set in combination with -p, an assignment must be provided for each new partition. Example: '[[1,2,3],[1,2,3]]' (JSON Array syntax) assigns two new partitions to brokers 1,2,3. If used by itself, a reassignment must be provided for all partitions.")

	return cmd
}

func (c *Commands) createLsTopicsCmd() *cobra.Command {
	var noHeaderFlag bool

	cmd := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List topics",
		Args:    cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			topicsResp, err := c.Admin.ListTopics(ctx)
			if err != nil {
				return fmt.Errorf("unable to list topics: %w", err)
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

			w := tabwriter.NewWriter(c.OutWriter, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)

			if !noHeaderFlag {
				fmt.Fprintf(w, "NAME\tPARTITIONS\tREPLICAS\t\n")
			}

			for _, topic := range sortedTopics {
				fmt.Fprintf(w, "%v\t%v\t%v\t\n", topic.name, topic.partitions, topic.replicas)
			}
			w.Flush()
			return nil
		},
	}

	cmd.Flags().BoolVar(&noHeaderFlag, "no-headers", false, "Hide table headers")

	return cmd
}

func (c *Commands) createDescribeTopicCmd() *cobra.Command {
	return &cobra.Command{
		Use:               "describe",
		Short:             "Describe topic",
		Long:              "Describe a topic. Default values of the configuration are omitted.",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: c.validTopicArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			topicDetails, err := c.Admin.ListTopics(ctx, args[0])
			if err != nil {
				return fmt.Errorf("unable to describe topics: %w", err)
			}

			detail, exists := topicDetails[args[0]]
			if !exists {
				fmt.Fprintf(c.OutWriter, "Topic %v not found.\n", args[0])
				return nil
			}

			cfgResp, err := c.Admin.DescribeTopicConfigs(ctx, args[0])
			if err != nil {
				return fmt.Errorf("unable to describe config: %w", err)
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

			// Convert map to slice for sorting
			partitionSlice := make([]kadm.PartitionDetail, 0, len(detail.Partitions))
			for _, partition := range detail.Partitions {
				partitionSlice = append(partitionSlice, partition)
			}
			sort.Slice(partitionSlice, func(i, j int) bool { return partitionSlice[i].Partition < partitionSlice[j].Partition })

			w := tabwriter.NewWriter(c.OutWriter, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
			fmt.Fprintf(w, "Name:\t%v\t\n", args[0])
			fmt.Fprintf(w, "Internal:\t%v\t\n", detail.IsInternal)
			fmt.Fprintf(w, "Compacted:\t%v\t\n", compacted)
			fmt.Fprintf(w, "Partitions:\n")

			w.Flush()
			w.Init(c.OutWriter, tabwriterMinWidthNested, 4, 2, tabwriterPadChar, tabwriterFlags)

			fmt.Fprintf(w, "\tPartition\tHigh Watermark\tLeader\tReplicas\tISR\t\n")
			fmt.Fprintf(w, "\t---------\t--------------\t------\t--------\t---\t\n")

			partitions := make([]int32, 0, len(partitionSlice))
			for _, partition := range partitionSlice {
				partitions = append(partitions, partition.Partition)
			}
			highWatermarks := c.getKgoHighWatermarks(args[0], partitions)
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
			return nil
		},
	}
}

func (c *Commands) createCreateTopicCmd() *cobra.Command {
	var partitionsFlag int32
	var replicasFlag int16
	var compactFlag bool

	cmd := &cobra.Command{
		Use:   "create TOPIC",
		Short: "Create a topic",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			topicName := args[0]
			compact := "delete"
			if compactFlag {
				compact = "compact"
			}
			ctx := context.Background()
			_, err := c.Admin.CreateTopics(ctx, partitionsFlag, replicasFlag, map[string]*string{
				"cleanup.policy": &compact,
			}, topicName)
			if err != nil {
				return fmt.Errorf("could not create topic %v: %w", topicName, err)
			}

			w := tabwriter.NewWriter(c.OutWriter, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
			fmt.Fprintf(w, "\xE2\x9C\x85 Created topic!\n")
			fmt.Fprintln(w, "\tTopic Name:\t", topicName)
			fmt.Fprintln(w, "\tPartitions:\t", partitionsFlag)
			fmt.Fprintln(w, "\tReplication Factor:\t", replicasFlag)
			fmt.Fprintln(w, "\tCleanup Policy:\t", compact)
			w.Flush()
			return nil
		},
	}

	cmd.Flags().Int32VarP(&partitionsFlag, "partitions", "p", int32(1), "Number of partitions")
	cmd.Flags().Int16VarP(&replicasFlag, "replicas", "r", int16(1), "Number of replicas")
	cmd.Flags().BoolVar(&compactFlag, "compact", false, "Enable topic compaction")

	return cmd
}

func (c *Commands) createAddConfigCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "add-config TOPIC KEY VALUE",
		Short: "Add config key/value pair to topic",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			topic := args[0]
			key := args[1]
			value := args[2]

			ctx := context.Background()
			_, err := c.Admin.AlterTopicConfigs(ctx, []kadm.AlterConfig{{
				Name:  key,
				Value: &value,
			}}, topic)
			if err != nil {
				return fmt.Errorf("failed to update topic config: %w", err)
			}
			fmt.Fprintf(c.OutWriter, "Added config %v=%v to topic %v.\n", key, value, topic)
			return nil
		},
	}
}

func (c *Commands) createRemoveConfigCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "rm-config TOPIC ATTR1,ATTR2...",
		Short: "Remove attributes from topic",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			topic := args[0]
			attrsToRemove := strings.Split(args[1], ",")

			updatedTopicConfigs := make(map[string]*string)

			ctx := context.Background()
			allTopicConfigs, err := c.Admin.DescribeTopicConfigs(ctx, topic)
			if err != nil {
				return fmt.Errorf("failed to describe topic config: %w", err)
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
			_, err = c.Admin.AlterTopicConfigs(ctx, alterConfigs, topic)
			if err != nil {
				return fmt.Errorf("failed to remove attributes from topic config: %w", err)
			}
			fmt.Fprintf(c.OutWriter, "Removed attributes %v from topic %v.\n", attrsToRemove, topic)
			return nil
		},
	}
}

func (c *Commands) createDeleteTopicCmd() *cobra.Command {
	return &cobra.Command{
		Use:               "delete TOPIC",
		Short:             "Delete a topic",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: c.validTopicArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			topicName := args[0]
			ctx := context.Background()
			_, err := c.Admin.DeleteTopics(ctx, topicName)
			if err != nil {
				return fmt.Errorf("could not delete topic %v: %w", topicName, err)
			}
			fmt.Fprintf(c.OutWriter, "\xE2\x9C\x85 Deleted topic %v!\n", topicName)
			return nil
		},
	}
}

func (c *Commands) createLagCmd() *cobra.Command {
	var noHeaderFlag bool

	cmd := &cobra.Command{
		Use:   "lag",
		Short: "Display the total lags for each consumer group",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			topic := args[0]

			ctx := context.Background()
			// Describe the topic
			topicDetails, err := c.Admin.ListTopics(ctx, topic)
			if err != nil {
				return fmt.Errorf("unable to describe topics: %w", err)
			}

			topicDetail, exists := topicDetails[topic]
			if !exists {
				return fmt.Errorf("topic %v not found", topic)
			}

			// Get the list of partitions for the topic
			partitions := make([]int32, 0, len(topicDetail.Partitions))
			for _, partition := range topicDetail.Partitions {
				partitions = append(partitions, partition.Partition)
			}
			highWatermarks := c.getKgoHighWatermarks(topic, partitions)

			// List all consumer groups
			consumerGroups, err := c.Admin.ListGroups(ctx)
			if err != nil {
				return fmt.Errorf("unable to list consumer groups: %w", err)
			}

			groups := consumerGroups.Groups()

			// Describe all consumer groups
			groupsInfo, err := c.Admin.DescribeGroups(ctx, groups...)
			if err != nil {
				return fmt.Errorf("unable to describe consumer groups: %w", err)
			}

			// Calculate lag for each group
			lagInfo := make(map[string]int64)
			groupStates := make(map[string]string) // To store the state of each group
			for _, group := range groupsInfo {
				var sum int64
				show := false
				// Try to get offsets for this group and topic
				resp, err := c.Admin.FetchOffsets(ctx, group.Group)
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
			w := tabwriter.NewWriter(c.OutWriter, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
			if !noHeaderFlag {
				fmt.Fprintf(w, "GROUP ID\tSTATE\tLAG\n")
			}
			for group, lag := range lagInfo {
				fmt.Fprintf(w, "%v\t%v\t%v\n", group, groupStates[group], lag)
			}
			w.Flush()
			return nil
		},
	}

	return cmd
}

// getKgoHighWatermarks gets high watermarks for the given topic and partitions
func (c *Commands) getKgoHighWatermarks(topic string, partitions []int32) map[int32]int64 {
	ctx := context.Background()

	offsets, err := c.Admin.ListEndOffsets(ctx, topic)
	if err != nil {
		fmt.Fprintf(c.ErrWriter, "Unable to get available offsets: %v\n", err)
		return nil
	}

	watermarks := make(map[int32]int64)
	if topicOffsets, exists := offsets[topic]; exists {
		for partition, offset := range topicOffsets {
			watermarks[partition] = offset.Offset
		}
	}
	return watermarks
}
