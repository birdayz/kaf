package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"sync"
	"text/tabwriter"
	"time"
	"unicode"

	"github.com/birdayz/kaf/pkg/streams"
	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
)

var (
	flagPeekPartitions []int32
	flagPeekBefore     int64
	flagPeekAfter      int64
	flagPeekTopics     []string

	flagNoMembers      bool
	flagDescribeTopics []string
)

func init() {
	rootCmd.AddCommand(groupCmd)
	rootCmd.AddCommand(groupsCmd)
	groupCmd.AddCommand(groupDescribeCmd)
	groupCmd.AddCommand(groupLsCmd)
	groupCmd.AddCommand(groupDeleteCmd)
	groupCmd.AddCommand(groupPeekCmd)
	groupCmd.AddCommand(createGroupCommitOffsetCmd())

	groupLsCmd.Flags().BoolVar(&noHeaderFlag, "no-headers", false, "Hide table headers")
	groupsCmd.Flags().BoolVar(&noHeaderFlag, "no-headers", false, "Hide table headers")

	groupPeekCmd.Flags().StringSliceVarP(&flagPeekTopics, "topics", "t", []string{}, "Topics to peek from")
	groupPeekCmd.Flags().Int32SliceVarP(&flagPeekPartitions, "partitions", "p", []int32{}, "Partitions to peek from")
	groupPeekCmd.Flags().Int64VarP(&flagPeekBefore, "before", "B", 0, "Number of messages to peek before current offset")
	groupPeekCmd.Flags().Int64VarP(&flagPeekAfter, "after", "A", 0, "Number of messages to peek after current offset")

	groupDescribeCmd.Flags().BoolVar(&flagNoMembers, "no-members", false, "Hide members section of the output")
	groupDescribeCmd.Flags().StringSliceVarP(&flagDescribeTopics, "topic", "t", []string{}, "topics to display for the group. defaults to all topics.")
}

const (
	tabwriterMinWidth       = 6
	tabwriterMinWidthNested = 2
	tabwriterWidth          = 4
	tabwriterPadding        = 3
	tabwriterPadChar        = ' '
	tabwriterFlags          = 0
)

var groupCmd = &cobra.Command{
	Use:   "group",
	Short: "Display information about consumer groups.",
}

var groupsCmd = &cobra.Command{
	Use:   "groups",
	Short: "List groups",
	Run:   groupLsCmd.Run,
}

var groupDeleteCmd = &cobra.Command{
	Use:               "delete",
	Short:             "Delete group",
	Args:              cobra.MaximumNArgs(1),
	ValidArgsFunction: validGroupArgs,
	Run: func(cmd *cobra.Command, args []string) {
		admin := getClusterAdmin()
		defer admin.Close()
		var group string
		if len(args) == 1 {
			group = args[0]
		}
		ctx := context.Background()
		_, err := admin.DeleteGroups(ctx, group)
		if err != nil {
			errorExit("Could not delete consumer group %v: %v\n", group, err.Error())
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "Deleted consumer group %v.\n", group)
		}

	},
}


func createGroupCommitOffsetCmd() *cobra.Command {
	var topic string
	var offset string
	var partitionFlag int32
	var allPartitions bool
	var offsetMap string
	var noconfirm bool
	res := &cobra.Command{
		Use:   "commit GROUP",
		Short: "Set offset for given consumer group",
		Long:  "Set offset for a given consumer group, creates one if it does not exist. Offsets cannot be set on a consumer group with active consumers.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// client := getClient() // Not needed for kadm-based approach

			group := args[0]
			partitionOffsets := make(map[int32]int64)

			// Verify topic exists for all cases
			admin := getClusterAdmin()
		defer admin.Close()
			ctx := context.Background()
			topics, err := admin.ListTopics(ctx, topic)
			if err != nil {
				return fmt.Errorf("unable to list topics: %w", err)
			}

			detail, exists := topics[topic]
			if !exists {
				return fmt.Errorf("topic not found: %v", topic)
			}

			if offsetMap != "" {
				if err := json.Unmarshal([]byte(offsetMap), &partitionOffsets); err != nil {
					return fmt.Errorf("wrong --offset-map format. Use JSON with keys as partition numbers and values as offsets.\nExample: --offset-map '{\"0\":123, \"1\":135, \"2\":120}'")
				}
				// Verify all partitions in the map exist in the topic
				for partition := range partitionOffsets {
					partitionExists := false
					for _, p := range detail.Partitions {
						if p.Partition == partition {
							partitionExists = true
							break
						}
					}
					if !partitionExists {
						return fmt.Errorf("partition %d not found in topic %s", partition, topic)
					}
				}
			} else {
				var partitions []int32
				if allPartitions {
					// Use all partitions from the topic
					for i := int32(0); i < int32(len(detail.Partitions)); i++ {
						partitions = append(partitions, i)
					}
				} else if partitionFlag != -1 {
					// Verify partition exists in topic
					partitionExists := false
					for _, p := range detail.Partitions {
						if p.Partition == partitionFlag {
							partitionExists = true
							break
						}
					}
					if !partitionExists {
						return fmt.Errorf("partition %d not found in topic %s", partitionFlag, topic)
					}
					partitions = []int32{partitionFlag}
				} else {
					return fmt.Errorf("either --partition, --all-partitions or --offset-map flag must be provided")
				}

				sort.Slice(partitions, func(i int, j int) bool { return partitions[i] < partitions[j] })

				type Assignment struct {
					partition int32
					offset    int64
				}
				assignments := make(chan Assignment, len(partitions))

				// TODO offset must be calced per partition
				var wg sync.WaitGroup
				for _, partition := range partitions {
					wg.Add(1)
					go func(partition int32) {
						defer wg.Done()
						i, err := strconv.ParseInt(offset, 10, 64)
						if err != nil {
							// Try oldest/newest/..
							if offset == "oldest" || offset == "earliest" {
								i = -2 // Kafka OffsetOldest
							} else if offset == "newest" || offset == "latest" {
								i = -1 // Kafka OffsetNewest
							} else {
								// Try timestamp
								t, err := time.Parse(time.RFC3339, offset)
								if err != nil {
									fmt.Fprintf(cmd.OutOrStderr(), "Error: offset is neither offset nor timestamp\n")
									return
								}
								i = t.UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
							}

							// Use kadm to get offset for timestamp
							admin := getClusterAdmin()
		defer admin.Close()
							ctx := context.Background()
							timestampOffsets, err := admin.ListOffsetsAfterMilli(ctx, i, topic)
							if err != nil {
								fmt.Fprintf(cmd.OutOrStderr(), "Failed to determine offset for timestamp: %v\n", err)
								return
							}
							var o int64 = -1
							if topicOffsets, exists := timestampOffsets[topic]; exists {
								if partitionOffset, exists := topicOffsets[partition]; exists {
									o = partitionOffset.Offset
								}
							}

							if o == -1 {
								fmt.Printf("Partition %v: could not determine offset from timestamp. Skipping.\n", partition)
								return
								//errorExit("Determined offset -1 from timestamp. Skipping.", o)
							}

							assignments <- Assignment{partition: partition, offset: o}

							fmt.Printf("Partition %v: determined offset %v from timestamp.\n", partition, o)
						} else {
							assignments <- Assignment{partition: partition, offset: i}
						}
					}(partition)
				}
				wg.Wait()
				close(assignments)

				for assign := range assignments {
					partitionOffsets[assign.partition] = assign.offset
				}
			}

			// Verify the Consumer Group is Empty
			// Add timeout for DescribeGroups to prevent hanging
			groupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			groupDescs, err := admin.DescribeGroups(groupCtx, args[0])
			if err != nil {
				// If group doesn't exist, that's fine - we can create it
				fmt.Fprintf(cmd.OutOrStdout(), "Group may not exist yet (will be created): %v\n", err)
			} else {
				for _, detail := range groupDescs {
					state := detail.State
					if !slices.Contains([]string{"Empty", "Dead"}, state) {
						return fmt.Errorf("consumer group %s has active consumers in it, cannot set offset", group)
					}
				}
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Resetting offsets to: %v\n", partitionOffsets)

			if !noconfirm {
				prompt := promptui.Prompt{
					Label:     "Reset offsets as described",
					IsConfirm: true,
				}

				_, err := prompt.Run()
				if err != nil {
					return fmt.Errorf("aborted")
				}
			}

			// Use kadm to commit offsets directly
			offsetsToCommit := make(map[string]map[int32]kadm.Offset)
			topicOffsets := make(map[int32]kadm.Offset)
			for partition, offset := range partitionOffsets {
				topicOffsets[partition] = kadm.Offset{
					At: offset,
				}
			}
			offsetsToCommit[topic] = topicOffsets

			err = admin.CommitAllOffsets(ctx, group, offsetsToCommit)
			if err != nil {
				return fmt.Errorf("failed to commit offset: %w", err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Successfully committed offsets to %v.\n", partitionOffsets)
			return nil
		},
	}
	res.Flags().StringVarP(&topic, "topic", "t", "", "topic")
	res.Flags().StringVarP(&offset, "offset", "o", "", "offset to commit")
	res.Flags().Int32VarP(&partitionFlag, "partition", "p", -1, "partition")
	res.Flags().BoolVar(&allPartitions, "all-partitions", false, "apply to all partitions")
	res.Flags().StringVar(&offsetMap, "offset-map", "", "set different offsets per different partitions in JSON format, e.g. {\"0\": 123, \"1\": 42}")
	res.Flags().BoolVar(&noconfirm, "noconfirm", false, "Do not prompt for confirmation")
	return res
}

var groupLsCmd = &cobra.Command{
	Use:   "ls",
	Short: "List groups",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		admin := getClusterAdmin()
		defer admin.Close()

		ctx := cmd.Context()
		if ctx == nil {
			ctx = context.Background()
		}
		groups, err := admin.ListGroups(ctx)
		if err != nil {
			errorExit("Unable to list consumer groups: %v\n", err)
		}

		groupList := make([]string, 0, len(groups.Groups()))
		for _, grp := range groups.Groups() {
			groupList = append(groupList, grp)
		}

		sort.Slice(groupList, func(i int, j int) bool {
			return groupList[i] < groupList[j]
		})

		w := tabwriter.NewWriter(outWriter, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)

		groupDescs, err := admin.DescribeGroups(ctx, groupList...)
		if err != nil {
			// if we can retrieve list of consumer group, but unable to describe consumer groups
			// fallback to only list group name without state
			if !noHeaderFlag {
				fmt.Fprintf(w, "NAME\n")
			}
	
			for _, group := range groupList {
				fmt.Fprintf(w, "%v\n", group)
			}
		} else {
			// return consumer group information with state
			if !noHeaderFlag {
				fmt.Fprintf(w, "NAME\tSTATE\tCONSUMERS\t\n")
			}
	
			for _, detail := range groupDescs {
				state := detail.State
				consumers := len(detail.Members)
				fmt.Fprintf(w, "%v\t%v\t%v\t\n", detail.Group, state, consumers)
			}
		}
		w.Flush()
	},
}

var groupPeekCmd = &cobra.Command{
	Use:               "peek",
	Short:             "Peek messages from consumer group offset",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: validGroupArgs,
	Run: func(cmd *cobra.Command, args []string) {
		admin := getClusterAdmin()
		defer admin.Close()

		ctx := context.Background()
		groups, err := admin.DescribeGroups(ctx, args[0])
		if err != nil {
			errorExit("Unable to describe consumer groups: %v\n", err)
		}

		if len(groups) == 0 {
			errorExit("Did not receive expected describe consumergroup result\n")
		}
		var group kadm.DescribedGroup
		for _, g := range groups {
			group = g
			break // Take the first (and should be only) group
		}

		if group.State == "Dead" {
			fmt.Printf("Group %v not found.\n", args[0])
			return
		}

		peekPartitions := make(map[int32]struct{})
		for _, partition := range flagPeekPartitions {
			peekPartitions[partition] = struct{}{}
		}

		var topicPartitions map[string][]int32
		if len(flagPeekTopics) > 0 {
			topicPartitions = make(map[string][]int32, len(flagPeekTopics))
		}
		for _, topic := range flagPeekTopics {
			topicDetails, err := admin.ListTopics(ctx, topic)
			if err != nil {
				errorExit("Unable to describe topics: %v\n", err)
			}

			detail, exists := topicDetails[topic]
			if !exists {
				fmt.Printf("Topic %v not found.\n", topic)
				return
			}

			if len(flagPeekPartitions) > 0 {
				topicPartitions[topic] = flagPeekPartitions
			} else {
				partitions := make([]int32, 0, len(detail.Partitions))
				for _, partition := range detail.Partitions {
					partitions = append(partitions, partition.Partition)
				}
				topicPartitions[topic] = partitions
			}
		}

		// TODO: Implement peek functionality with kgo instead of sarama
		fmt.Printf("Peek functionality temporarily disabled during migration to kadm\n")
	},
}

var groupDescribeCmd = &cobra.Command{
	Use:               "describe",
	Short:             "Describe consumer group",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: validGroupArgs,
	Run: func(cmd *cobra.Command, args []string) {
		// TODO List: This API can be used to find the current groups managed by a broker. To get a list of all groups in the cluster, you must send ListGroup to all brokers.
		// same goes probably for topics
		admin := getClusterAdmin()
		defer admin.Close()

		ctx := context.Background()
		groups, err := admin.DescribeGroups(ctx, args[0])
		if err != nil {
			errorExit("Unable to describe consumer groups: %v\n", err)
		}

		if len(groups) == 0 {
			errorExit("Did not receive expected describe consumergroup result\n")
		}
		var group kadm.DescribedGroup
		for _, g := range groups {
			group = g
			break // Take the first (and should be only) group
		}

		if group.State == "Dead" {
			fmt.Printf("Group %v not found.\n", args[0])
			return
		}

		w := tabwriter.NewWriter(outWriter, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
		fmt.Fprintf(w, "Group ID:\t%v\n", group.Group)
		fmt.Fprintf(w, "State:\t%v\n", group.State)
		fmt.Fprintf(w, "Protocol:\t%v\n", group.Protocol)
		fmt.Fprintf(w, "Protocol Type:\t%v\n", group.ProtocolType)

		fmt.Fprintf(w, "Offsets:\t\n")

		w.Flush()
		w.Init(outWriter, tabwriterMinWidthNested, 4, 2, tabwriterPadChar, tabwriterFlags)

		offsetAndMetadata, err := admin.FetchOffsets(ctx, args[0])
		if err != nil {
			errorExit("Failed to fetch group offsets: %v\n", err)
		}

		topics := make([]string, 0, len(offsetAndMetadata))
		for k := range offsetAndMetadata {
			topics = append(topics, k)
		}
		sort.Strings(topics)

		for _, topic := range topics {
			partitions := offsetAndMetadata[topic]
			if len(flagDescribeTopics) > 0 {
				var found bool
				for _, topicToShow := range flagDescribeTopics {
					if topic == topicToShow {
						found = true
					}
				}

				if !found {
					continue
				}
			}
			fmt.Fprintf(w, "\t%v:\n", topic)
			fmt.Fprintf(w, "\t\tPartition\tGroup Offset\tHigh Watermark\tLag\tMetadata\t\n")
			fmt.Fprintf(w, "\t\t---------\t------------\t--------------\t---\t--------\n")

			var p []int32

			for partition := range partitions {
				p = append(p, partition)
			}

			sort.Slice(p, func(i, j int) bool {
				return p[i] < p[j]
			})

			wms := getHighWatermarks(topic, p)

			lagSum := 0
			offsetSum := 0
			for _, partition := range p {
				lag := (wms[partition] - partitions[partition].At)
				lagSum += int(lag)
				offset := partitions[partition].At
				offsetSum += int(offset)
				fmt.Fprintf(w, "\t\t%v\t%v\t%v\t%v\t%v\n", partition, partitions[partition].At, wms[partition], (wms[partition] - partitions[partition].At), partitions[partition].Metadata)
			}

			fmt.Fprintf(w, "\t\tTotal\t%d\t\t%d\t\n", offsetSum, lagSum)
		}

		if !flagNoMembers {

			fmt.Fprintf(w, "Members:\t")

			w.Flush()
			w.Init(outWriter, tabwriterMinWidthNested, 4, 2, tabwriterPadChar, tabwriterFlags)

			fmt.Fprintln(w)
			for _, member := range group.Members {
				fmt.Fprintf(w, "\t%v:\n", member.ClientID)
				fmt.Fprintf(w, "\t\tHost:\t%v\n", member.ClientHost)

				// TODO: Member assignment and metadata parsing needs to be reimplemented for kadm
				// The kadm library uses a different structure for member information
				fmt.Fprintf(w, "\t\tAssignments:\t[Not available - requires kadm reimplementation]\n")
				fmt.Fprintf(w, "\t\tMetadata:\t[Not available - requires kadm reimplementation]\n")

				fmt.Fprintf(w, "\n")

			}
		}

		w.Flush()

	},
}

func getHighWatermarks(topic string, partitions []int32) (watermarks map[int32]int64) {
	// Use the existing getKgoHighWatermarks function that uses kadm
	return getKgoHighWatermarks(topic, partitions)
}

// IsASCIIPrintable returns true if the string is ASCII printable.
func IsASCIIPrintable(s string) bool {
	for _, r := range s {
		if r > unicode.MaxASCII || !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

func tryDecodeUserData(protocol string, raw []byte) (data interface{}, err error) {
	// Interpret userdata here
	decoder := streams.NewDecoder(raw)

	switch protocol {
	case "stream":
		subscriptionInfo := streams.SubscriptionInfo{}
		err = subscriptionInfo.Decode(decoder)
		if err != nil {
			return nil, err
		}
		return subscriptionInfo, nil
	default:
		return nil, errors.New("unknown protocol")
	}
}
