package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
)

const (
	tabwriterMinWidth       = 6
	tabwriterMinWidthNested = 2
	tabwriterWidth          = 4
	tabwriterPadding        = 3
	tabwriterPadChar        = ' '
	tabwriterFlags          = 0
)

func (c *Commands) createGroupCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "group",
		Short: "Display information about consumer groups.",
	}

	cmd.AddCommand(c.createGroupDescribeCmd())
	cmd.AddCommand(c.createGroupLsCmd())
	cmd.AddCommand(c.createGroupDeleteCmd())
	cmd.AddCommand(c.createGroupPeekCmd())
	cmd.AddCommand(c.createGroupCommitCmd())

	return cmd
}

func (c *Commands) createGroupsCmd() *cobra.Command {
	// Groups command is an alias for "group ls"
	var noHeaders bool

	cmd := &cobra.Command{
		Use:   "groups",
		Short: "List groups",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			groups, err := c.Admin.ListGroups(ctx)
			if err != nil {
				return fmt.Errorf("unable to list consumer groups: %w", err)
			}

			groupList := make([]string, 0, len(groups.Groups()))
			for _, grp := range groups.Groups() {
				groupList = append(groupList, grp)
			}

			sort.Slice(groupList, func(i int, j int) bool {
				return groupList[i] < groupList[j]
			})

			w := tabwriter.NewWriter(cmd.OutOrStdout(), tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)

			groupDescs, err := c.Admin.DescribeGroups(ctx, groupList...)
			if err != nil {
				// Fallback to just list group names
				if !noHeaders {
					fmt.Fprintf(w, "NAME\n")
				}
				for _, group := range groupList {
					fmt.Fprintf(w, "%v\n", group)
				}
			} else {
				if !noHeaders {
					fmt.Fprintf(w, "NAME\tSTATE\tCONSUMERS\t\n")
				}
				for _, detail := range groupDescs {
					state := detail.State
					consumers := len(detail.Members)
					fmt.Fprintf(w, "%v\t%v\t%v\t\n", detail.Group, state, consumers)
				}
			}
			w.Flush()
			return nil
		},
	}

	cmd.Flags().BoolVar(&noHeaders, "no-headers", false, "Hide table headers")
	return cmd
}

func (c *Commands) createGroupDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete GROUP",
		Short: "Delete group",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			group := args[0]
			ctx := context.Background()
			_, err := c.Admin.DeleteGroups(ctx, group)
			if err != nil {
				return fmt.Errorf("could not delete consumer group %v: %w", group, err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Deleted consumer group %v.\n", group)
			return nil
		},
	}
}

func (c *Commands) createGroupCommitCmd() *cobra.Command {
	var topic string
	var offset string
	var partitionFlag int32
	var allPartitions bool
	var offsetMap string
	var noconfirm bool

	cmd := &cobra.Command{
		Use:   "commit GROUP",
		Short: "Set offset for given consumer group",
		Long:  "Set offset for a given consumer group, creates one if it does not exist. Offsets cannot be set on a consumer group with active consumers.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			group := args[0]
			partitionOffsets := make(map[int32]int64)

			ctx := context.Background()

			// Verify topic exists
			topics, err := c.Admin.ListTopics(ctx, topic)
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
				// Verify all partitions exist
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
					for i := int32(0); i < int32(len(detail.Partitions)); i++ {
						partitions = append(partitions, i)
					}
				} else if partitionFlag != -1 {
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

				sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })

				type Assignment struct {
					partition int32
					offset    int64
				}
				assignments := make(chan Assignment, len(partitions))

				var wg sync.WaitGroup
				for _, partition := range partitions {
					wg.Add(1)
					go func(partition int32) {
						defer wg.Done()
						i, err := strconv.ParseInt(offset, 10, 64)
						if err != nil {
							// Try oldest/newest
							if offset == "oldest" || offset == "earliest" {
								i = -2
							} else if offset == "newest" || offset == "latest" {
								i = -1
							} else {
								// Try timestamp
								t, err := time.Parse(time.RFC3339, offset)
								if err != nil {
									fmt.Fprintf(cmd.OutOrStderr(), "Error: offset is neither offset nor timestamp\n")
									return
								}
								i = t.UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
							}

							timestampOffsets, err := c.Admin.ListOffsetsAfterMilli(ctx, i, topic)
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
			groupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			groupDescs, err := c.Admin.DescribeGroups(groupCtx, group)
			if err != nil {
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

			// Commit offsets
			offsetsToCommit := make(map[string]map[int32]kadm.Offset)
			topicOffsets := make(map[int32]kadm.Offset)
			for partition, offset := range partitionOffsets {
				topicOffsets[partition] = kadm.Offset{
					At: offset,
				}
			}
			offsetsToCommit[topic] = topicOffsets

			err = c.Admin.CommitAllOffsets(ctx, group, offsetsToCommit)
			if err != nil {
				return fmt.Errorf("failed to commit offset: %w", err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Successfully committed offsets to %v.\n", partitionOffsets)
			return nil
		},
	}

	cmd.Flags().StringVarP(&topic, "topic", "t", "", "topic")
	cmd.Flags().StringVarP(&offset, "offset", "o", "", "offset to commit")
	cmd.Flags().Int32VarP(&partitionFlag, "partition", "p", -1, "partition")
	cmd.Flags().BoolVar(&allPartitions, "all-partitions", false, "apply to all partitions")
	cmd.Flags().StringVar(&offsetMap, "offset-map", "", "set different offsets per different partitions in JSON format, e.g. {\"0\": 123, \"1\": 42}")
	cmd.Flags().BoolVar(&noconfirm, "noconfirm", false, "Do not prompt for confirmation")

	return cmd
}

func (c *Commands) createGroupLsCmd() *cobra.Command {
	var noHeaders bool

	cmd := &cobra.Command{
		Use:   "ls",
		Short: "List groups",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			groups, err := c.Admin.ListGroups(ctx)
			if err != nil {
				return fmt.Errorf("unable to list consumer groups: %w", err)
			}

			groupList := make([]string, 0, len(groups.Groups()))
			for _, grp := range groups.Groups() {
				groupList = append(groupList, grp)
			}

			sort.Slice(groupList, func(i int, j int) bool {
				return groupList[i] < groupList[j]
			})

			w := tabwriter.NewWriter(cmd.OutOrStdout(), tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)

			groupDescs, err := c.Admin.DescribeGroups(ctx, groupList...)
			if err != nil {
				// Fallback to just list group names
				if !noHeaders {
					fmt.Fprintf(w, "NAME\n")
				}
				for _, group := range groupList {
					fmt.Fprintf(w, "%v\n", group)
				}
			} else {
				if !noHeaders {
					fmt.Fprintf(w, "NAME\tSTATE\tCONSUMERS\t\n")
				}
				for _, detail := range groupDescs {
					state := detail.State
					consumers := len(detail.Members)
					fmt.Fprintf(w, "%v\t%v\t%v\t\n", detail.Group, state, consumers)
				}
			}
			w.Flush()
			return nil
		},
	}

	cmd.Flags().BoolVar(&noHeaders, "no-headers", false, "Hide table headers")
	return cmd
}

func (c *Commands) createGroupPeekCmd() *cobra.Command {
	var flagPeekTopics []string
	var flagPeekPartitions []int32
	var flagPeekBefore int64
	var flagPeekAfter int64

	cmd := &cobra.Command{
		Use:   "peek GROUP",
		Short: "Peek messages from consumer group offset",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// TODO: Implement peek functionality with kgo
			fmt.Printf("Peek functionality temporarily disabled during migration to kadm\n")
			return nil
		},
	}

	cmd.Flags().StringSliceVarP(&flagPeekTopics, "topics", "t", []string{}, "Topics to peek from")
	cmd.Flags().Int32SliceVarP(&flagPeekPartitions, "partitions", "p", []int32{}, "Partitions to peek from")
	cmd.Flags().Int64VarP(&flagPeekBefore, "before", "B", 0, "Number of messages to peek before current offset")
	cmd.Flags().Int64VarP(&flagPeekAfter, "after", "A", 0, "Number of messages to peek after current offset")

	return cmd
}

func (c *Commands) createGroupDescribeCmd() *cobra.Command {
	var flagNoMembers bool
	var flagDescribeTopics []string

	cmd := &cobra.Command{
		Use:   "describe GROUP",
		Short: "Describe consumer group",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			groups, err := c.Admin.DescribeGroups(ctx, args[0])
			if err != nil {
				return fmt.Errorf("unable to describe consumer groups: %w", err)
			}

			if len(groups) == 0 {
				return fmt.Errorf("did not receive expected describe consumergroup result")
			}

			var group kadm.DescribedGroup
			for _, g := range groups {
				group = g
				break
			}

			if group.State == "Dead" {
				fmt.Printf("Group %v not found.\n", args[0])
				return nil
			}

			w := tabwriter.NewWriter(cmd.OutOrStdout(), tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
			fmt.Fprintf(w, "Group ID:\t%v\n", group.Group)
			fmt.Fprintf(w, "State:\t%v\n", group.State)
			fmt.Fprintf(w, "Protocol:\t%v\n", group.Protocol)
			fmt.Fprintf(w, "Protocol Type:\t%v\n", group.ProtocolType)

			fmt.Fprintf(w, "Offsets:\t\n")

			w.Flush()
			w.Init(cmd.OutOrStdout(), tabwriterMinWidthNested, 4, 2, tabwriterPadChar, tabwriterFlags)

			offsetAndMetadata, err := c.Admin.FetchOffsets(ctx, args[0])
			if err != nil {
				return fmt.Errorf("failed to fetch group offsets: %w", err)
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

				wms := c.getHighWatermarks(topic, p)

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
				w.Init(cmd.OutOrStdout(), tabwriterMinWidthNested, 4, 2, tabwriterPadChar, tabwriterFlags)

				fmt.Fprintln(w)
				for _, member := range group.Members {
					fmt.Fprintf(w, "\t%v:\n", member.ClientID)
					fmt.Fprintf(w, "\t\tHost:\t%v\n", member.ClientHost)
					fmt.Fprintf(w, "\t\tAssignments:\t[Not available - requires kadm reimplementation]\n")
					fmt.Fprintf(w, "\t\tMetadata:\t[Not available - requires kadm reimplementation]\n")
					fmt.Fprintf(w, "\n")
				}
			}

			w.Flush()
			return nil
		},
	}

	cmd.Flags().BoolVar(&flagNoMembers, "no-members", false, "Hide members section of the output")
	cmd.Flags().StringSliceVarP(&flagDescribeTopics, "topic", "t", []string{}, "topics to display for the group. defaults to all topics.")

	return cmd
}

func (c *Commands) getHighWatermarks(topic string, partitions []int32) map[int32]int64 {
	ctx := context.Background()
	watermarks := make(map[int32]int64)

	endOffsets, err := c.Admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return watermarks
	}

	topicEndOffsets, exists := endOffsets[topic]
	if !exists {
		return watermarks
	}

	for _, partition := range partitions {
		if partitionOffset, exists := topicEndOffsets[partition]; exists {
			watermarks[partition] = partitionOffset.Offset
		}
	}

	return watermarks
}
