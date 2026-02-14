package group

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	"github.com/birdayz/kaf/pkg/app"
	pkggroup "github.com/birdayz/kaf/pkg/group"
	pkgtopic "github.com/birdayz/kaf/pkg/topic"
)

// NewCommand returns the "kaf group" parent command.
func NewCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "group",
		Short: "Display information about consumer groups.",
	}
	cmd.AddCommand(
		newListCommand(a),
		newDescribeCommand(a),
		newDeleteCommand(a),
		newPeekCommand(a),
		newCommitCommand(a),
	)
	return cmd
}

// NewGroupsAlias returns the "kaf groups" alias for "kaf group ls".
func NewGroupsAlias(a *app.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "groups",
		Short: "List groups",
		RunE:  listGroupsRunE(a),
	}
	a.AddNoHeadersFlag(cmd)
	return cmd
}

func listGroupsRunE(a *app.App) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		cl, err := a.NewKafClient()
		if err != nil {
			return err
		}
		defer cl.Close()

		groups, err := pkggroup.List(cmd.Context(), cl.Admin)
		if err != nil {
			return fmt.Errorf("unable to list consumer groups: %w", err)
		}

		w := app.NewTabWriter(a.OutWriter)
		if !a.NoHeaderFlag {
			fmt.Fprintf(w, "NAME\tSTATE\tCONSUMERS\t\n")
		}
		for _, g := range groups {
			fmt.Fprintf(w, "%v\t%v\t%v\t\n", g.Name, g.State, g.Consumers)
		}
		w.Flush()
		return nil
	}
}

func newListCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List groups",
		Args:    cobra.NoArgs,
		RunE:    listGroupsRunE(a),
	}
	a.AddNoHeadersFlag(cmd)
	return cmd
}

func newDeleteCommand(a *app.App) *cobra.Command {
	return &cobra.Command{
		Use:               "delete GROUP",
		Short:             "Delete group",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: a.ValidGroupArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, err := a.NewKafClient()
			if err != nil {
				return err
			}
			defer cl.Close()

			grp := args[0]
			err = pkggroup.Delete(cmd.Context(), cl.Admin, grp)
			if err != nil {
				return fmt.Errorf("could not delete consumer group %v: %w", grp, err)
			}
			fmt.Fprintf(a.OutWriter, "Deleted consumer group %v.\n", grp)
			return nil
		},
	}
}

func newDescribeCommand(a *app.App) *cobra.Command {
	var (
		flagNoMembers      bool
		flagDescribeTopics []string
	)
	cmd := &cobra.Command{
		Use:               "describe GROUP",
		Short:             "Describe consumer group",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: a.ValidGroupArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, err := a.NewKafClient()
			if err != nil {
				return err
			}
			defer cl.Close()

			desc, err := pkggroup.Describe(cmd.Context(), cl.Admin, args[0], flagDescribeTopics)
			if err != nil {
				return fmt.Errorf("unable to describe consumer group: %w", err)
			}

			w := app.NewTabWriter(a.OutWriter)
			fmt.Fprintf(w, "Group ID:\t%v\n", desc.Group)
			fmt.Fprintf(w, "State:\t%v\n", desc.State)
			fmt.Fprintf(w, "Protocol:\t%v\n", desc.Protocol)
			fmt.Fprintf(w, "Protocol Type:\t%v\n", desc.ProtocolType)
			fmt.Fprintf(w, "Offsets:\t\n")

			w.Flush()
			w.Init(a.OutWriter, app.TabwriterMinWidthNested, 4, 2, app.TabwriterPadChar, app.TabwriterFlags)

			for _, to := range desc.Topics {
				fmt.Fprintf(w, "\t%v:\n", to.Topic)
				fmt.Fprintf(w, "\t\tPartition\tGroup Offset\tHigh Watermark\tLag\tMetadata\t\n")
				fmt.Fprintf(w, "\t\t---------\t------------\t--------------\t---\t--------\n")

				for _, p := range to.Partitions {
					fmt.Fprintf(w, "\t\t%v\t%v\t%v\t%v\t%v\n", p.Partition, p.GroupOffset, p.HighWatermark, p.Lag, p.Metadata)
				}
				fmt.Fprintf(w, "\t\tTotal\t%d\t\t%d\t\n", to.TotalOffset, to.TotalLag)
			}

			if !flagNoMembers {
				fmt.Fprintf(w, "Members:\t")

				w.Flush()
				w.Init(a.OutWriter, app.TabwriterMinWidthNested, 4, 2, app.TabwriterPadChar, app.TabwriterFlags)

				fmt.Fprintln(w)
				for _, member := range desc.Members {
					fmt.Fprintf(w, "\t%v:\n", member.ClientID)
					fmt.Fprintf(w, "\t\tHost:\t%v\n", member.ClientHost)

					if len(member.TopicPartitions) > 0 {
						fmt.Fprintf(w, "\t\tAssignments:\n")
						fmt.Fprintf(w, "\t\t  Topic\tPartitions\t\n")
						fmt.Fprintf(w, "\t\t  -----\t----------\t")

						for topic, partitions := range member.TopicPartitions {
							fmt.Fprintf(w, "\n\t\t  %v\t%v\t", topic, partitions)
						}
					}
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

func newPeekCommand(a *app.App) *cobra.Command {
	var (
		flagPeekPartitions []int32
		flagPeekBefore     int64
		flagPeekAfter      int64
		flagPeekTopics     []string
	)
	cmd := &cobra.Command{
		Use:               "peek GROUP",
		Short:             "Peek messages from consumer group offset",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: a.ValidGroupArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			cl, err := a.NewKafClient()
			if err != nil {
				return err
			}
			defer cl.Close()

			grpName := args[0]

			described, err := cl.Admin.DescribeGroups(ctx, grpName)
			if err != nil {
				return fmt.Errorf("unable to describe consumer groups: %w", err)
			}
			g, ok := described[grpName]
			if !ok || g.State == "Dead" {
				fmt.Fprintf(a.OutWriter, "Group %v not found.\n", grpName)
				return nil
			}

			peekPartitions := make(map[int32]struct{})
			for _, p := range flagPeekPartitions {
				peekPartitions[p] = struct{}{}
			}

			var topicFilter []string
			if len(flagPeekTopics) > 0 {
				topicFilter = flagPeekTopics
				for _, t := range flagPeekTopics {
					_, err := pkgtopic.Describe(ctx, cl.Admin, t)
					if err != nil {
						return fmt.Errorf("topic %v not found: %w", t, err)
					}
				}
			}

			offsets, err := cl.Admin.FetchOffsets(ctx, grpName)
			if err != nil {
				return fmt.Errorf("failed to fetch group offsets: %w", err)
			}

			partMap := make(map[string]map[int32]kgo.Offset)

			type peekTarget struct {
				topic     string
				partition int32
				endOffset int64
			}
			var targets []peekTarget

			offsets.Each(func(o kadm.OffsetResponse) {
				t := o.Topic
				p := o.Partition
				committed := o.At

				if len(topicFilter) > 0 && !slices.Contains(topicFilter, t) {
					return
				}
				if len(peekPartitions) > 0 {
					if _, ok := peekPartitions[p]; !ok {
						return
					}
				}

				start := committed - flagPeekBefore
				if start < 0 {
					start = 0
				}

				if partMap[t] == nil {
					partMap[t] = make(map[int32]kgo.Offset)
				}
				partMap[t][p] = kgo.NewOffset().At(start)

				targets = append(targets, peekTarget{
					topic:     t,
					partition: p,
					endOffset: committed + flagPeekAfter,
				})
			})

			if len(targets) == 0 {
				fmt.Fprintf(a.OutWriter, "No committed offsets found for group %v.\n", grpName)
				return nil
			}

			endLookup := make(map[string]map[int32]int64)
			for _, tgt := range targets {
				if endLookup[tgt.topic] == nil {
					endLookup[tgt.topic] = make(map[int32]int64)
				}
				endLookup[tgt.topic][tgt.partition] = tgt.endOffset
			}

			peekCl, err := a.NewKafClient(kgo.ConsumePartitions(partMap))
			if err != nil {
				return err
			}
			defer peekCl.Close()

			mu := &sync.Mutex{}

			for {
				fetches := peekCl.KGO.PollFetches(ctx)
				if fetches.IsClientClosed() || ctx.Err() != nil {
					return nil
				}

				allDone := true
				fetches.EachRecord(func(rec *kgo.Record) {
					ends, ok := endLookup[rec.Topic]
					if !ok {
						return
					}
					endOff, ok := ends[rec.Partition]
					if !ok {
						return
					}
					if rec.Offset < endOff {
						a.HandleMessage(rec, mu, app.OutputFormatDefault, nil)
					}
					if rec.Offset+1 < endOff {
						allDone = false
					}
				})

				if allDone {
					return nil
				}
			}
		},
	}
	cmd.Flags().StringSliceVarP(&flagPeekTopics, "topics", "t", []string{}, "Topics to peek from")
	cmd.Flags().Int32SliceVarP(&flagPeekPartitions, "partitions", "p", []int32{}, "Partitions to peek from")
	cmd.Flags().Int64VarP(&flagPeekBefore, "before", "B", 0, "Number of messages to peek before current offset")
	cmd.Flags().Int64VarP(&flagPeekAfter, "after", "A", 0, "Number of messages to peek after current offset")
	return cmd
}

func newCommitCommand(a *app.App) *cobra.Command {
	var (
		topic         string
		offset        string
		partitionFlag int32
		allPartitions bool
		offsetMap     string
		noconfirm     bool
	)
	cmd := &cobra.Command{
		Use:   "commit GROUP",
		Short: "Set offset for given consumer group",
		Long:  "Set offset for a given consumer group, creates one if it does not exist. Offsets cannot be set on a consumer group with active consumers.",
		Example: `  kaf group commit my-group -t my-topic -o oldest --all-partitions
  kaf group commit my-group -t my-topic -o newest -p 0
  kaf group commit my-group -t my-topic -o 42 -p 0
  kaf group commit my-group -t my-topic -o 2024-01-15T10:00:00Z --all-partitions
  kaf group commit my-group --offset-map '{"0":123,"1":456}' -t my-topic`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, err := a.NewKafClient()
			if err != nil {
				return err
			}
			defer cl.Close()

			ctx := cmd.Context()
			grp := args[0]
			partitionOffsets := make(map[int32]int64)

			if offsetMap == "" {
				if topic == "" {
					return fmt.Errorf("--topic is required when not using --offset-map")
				}
				if offset == "" {
					return fmt.Errorf("--offset is required when not using --offset-map")
				}
			}

			if offsetMap != "" {
				if err := json.Unmarshal([]byte(offsetMap), &partitionOffsets); err != nil {
					return fmt.Errorf("wrong --offset-map format. Use JSON with keys as partition numbers and values as offsets.\nExample: --offset-map '{\"0\":123, \"1\":135, \"2\":120}'")
				}
			} else {
				var partitions []int32
				if allPartitions {
					topics, err := cl.Admin.ListTopics(ctx, topic)
					if err != nil {
						return fmt.Errorf("unable to list topic: %w", err)
					}
					td, ok := topics[topic]
					if !ok {
						return fmt.Errorf("topic %s not found", topic)
					}
					for _, p := range td.Partitions.Sorted() {
						partitions = append(partitions, p.Partition)
					}
				} else if partitionFlag != -1 {
					partitions = []int32{partitionFlag}
				} else {
					return fmt.Errorf("either --partition, --all-partitions or --offset-map flag must be provided")
				}

				sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })

				type assignment struct {
					partition int32
					offset    int64
				}

				g, gctx := errgroup.WithContext(ctx)
				var mu sync.Mutex
				var results []assignment

				for _, partition := range partitions {
					g.Go(func() error {
						i, err := strconv.ParseInt(offset, 10, 64)
						if err != nil {
							if offset == "oldest" || offset == "earliest" {
								listed, err := cl.Admin.ListStartOffsets(gctx, topic)
								if err != nil {
									return fmt.Errorf("failed to get start offsets: %w", err)
								}
								if lo, ok := listed.Lookup(topic, partition); ok {
									i = lo.Offset
								} else {
									fmt.Fprintf(a.OutWriter, "Partition %v: could not determine start offset. Skipping.\n", partition)
									return nil
								}
							} else if offset == "newest" || offset == "latest" {
								listed, err := cl.Admin.ListEndOffsets(gctx, topic)
								if err != nil {
									return fmt.Errorf("failed to get end offsets: %w", err)
								}
								if lo, ok := listed.Lookup(topic, partition); ok {
									i = lo.Offset
								} else {
									fmt.Fprintf(a.OutWriter, "Partition %v: could not determine end offset. Skipping.\n", partition)
									return nil
								}
							} else {
								t, err := time.Parse(time.RFC3339, offset)
								if err != nil {
									return fmt.Errorf("offset is neither offset nor timestamp")
								}
								millis := t.UnixMilli()
								listed, err := cl.Admin.ListOffsetsAfterMilli(gctx, millis, topic)
								if err != nil {
									return fmt.Errorf("failed to determine offset for timestamp: %w", err)
								}
								lo, ok := listed.Lookup(topic, partition)
								if !ok || lo.Offset == -1 {
									fmt.Fprintf(a.OutWriter, "Partition %v: could not determine offset from timestamp. Skipping.\n", partition)
									return nil
								}
								i = lo.Offset
								fmt.Fprintf(a.OutWriter, "Partition %v: determined offset %v from timestamp.\n", partition, i)
							}
						}
						mu.Lock()
						results = append(results, assignment{partition: partition, offset: i})
						mu.Unlock()
						return nil
					})
				}
				if err := g.Wait(); err != nil {
					return err
				}

				for _, assign := range results {
					partitionOffsets[assign.partition] = assign.offset
				}
			}

			described, err := cl.Admin.DescribeGroups(ctx, grp)
			if err != nil {
				return fmt.Errorf("unable to describe consumer groups: %w", err)
			}
			if g, ok := described[grp]; ok {
				if !slices.Contains([]string{"Empty", "Dead"}, g.State) {
					return fmt.Errorf("consumer group %s has active consumers in it, cannot set offset", grp)
				}
			}

			fmt.Fprintf(a.OutWriter, "Resetting offsets to: %v\n", partitionOffsets)

			if !noconfirm {
				prompt := promptui.Prompt{
					Label:     "Reset offsets as described",
					IsConfirm: true,
				}
				_, err := prompt.Run()
				if err != nil {
					return fmt.Errorf("aborted, exiting")
				}
			}

			topicOffsets := map[string]map[int32]int64{
				topic: partitionOffsets,
			}
			err = pkggroup.CommitOffsets(ctx, cl.Admin, grp, topicOffsets)
			if err != nil {
				return fmt.Errorf("failed to commit offset: %w", err)
			}

			fmt.Fprintf(a.OutWriter, "Successfully committed offsets to %v.\n", partitionOffsets)
			return nil
		},
	}
	cmd.Flags().StringVarP(&topic, "topic", "t", "", "topic")
	cmd.Flags().StringVarP(&offset, "offset", "o", "", "offset to commit (integer, oldest/earliest, newest/latest, or RFC3339 timestamp)")
	cmd.Flags().Int32VarP(&partitionFlag, "partition", "p", -1, "partition")
	cmd.Flags().BoolVar(&allPartitions, "all-partitions", false, "apply to all partitions")
	cmd.Flags().StringVar(&offsetMap, "offset-map", "", "set different offsets per different partitions in JSON format, e.g. {\"0\": 123, \"1\": 42}")
	cmd.Flags().BoolVar(&noconfirm, "noconfirm", false, "Do not prompt for confirmation")
	return cmd
}
