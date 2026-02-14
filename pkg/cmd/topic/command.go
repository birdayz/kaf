package topic

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/birdayz/kaf/pkg/app"
	pkgtopic "github.com/birdayz/kaf/pkg/topic"
)

// NewCommand returns the "kaf topic" parent command.
func NewCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "topic",
		Short: "Create and describe topics.",
	}
	cmd.AddCommand(
		newCreateCommand(a),
		newDeleteCommand(a),
		newListCommand(a),
		newDescribeCommand(a),
		newAddConfigCommand(a),
		newRemoveConfigCommand(a),
		newSetConfigCommand(a),
		newUpdateCommand(a),
		newLagCommand(a),
	)
	return cmd
}

// NewTopicsAlias returns the "kaf topics" alias for "kaf topic ls".
func NewTopicsAlias(a *app.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "topics",
		Short: "List topics",
		Args:  cobra.ExactArgs(0),
		RunE:  listTopicsRunE(a),
	}
	a.AddNoHeadersFlag(cmd)
	return cmd
}

func listTopicsRunE(a *app.App) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		cl, err := a.NewKafClient()
		if err != nil {
			return err
		}
		defer cl.Close()

		topics, err := pkgtopic.List(cmd.Context(), cl.Admin)
		if err != nil {
			return fmt.Errorf("unable to list topics: %w", err)
		}

		w := app.NewTabWriter(a.OutWriter)
		if !a.NoHeaderFlag {
			fmt.Fprintf(w, "NAME\tPARTITIONS\tREPLICAS\t\n")
		}
		for _, t := range topics {
			fmt.Fprintf(w, "%v\t%v\t%v\t\n", t.Name, t.Partitions, t.ReplicationFactor)
		}
		w.Flush()
		return nil
	}
}

func newListCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List topics",
		Args:    cobra.ExactArgs(0),
		RunE:    listTopicsRunE(a),
	}
	a.AddNoHeadersFlag(cmd)
	return cmd
}

func newDescribeCommand(a *app.App) *cobra.Command {
	return &cobra.Command{
		Use:               "describe TOPIC",
		Short:             "Describe topic",
		Long:              "Describe a topic. Default values of the configuration are omitted.",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: a.ValidTopicArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, err := a.NewKafClient()
			if err != nil {
				return err
			}
			defer cl.Close()

			desc, err := pkgtopic.Describe(cmd.Context(), cl.Admin, args[0])
			if err != nil {
				return fmt.Errorf("unable to describe topic: %w", err)
			}

			w := app.NewTabWriter(a.OutWriter)
			fmt.Fprintf(w, "Name:\t%v\t\n", desc.Name)
			fmt.Fprintf(w, "Internal:\t%v\t\n", desc.IsInternal)
			fmt.Fprintf(w, "Compacted:\t%v\t\n", desc.Compacted)
			fmt.Fprintf(w, "Partitions:\n")

			w.Flush()
			w.Init(a.OutWriter, app.TabwriterMinWidthNested, 4, 2, app.TabwriterPadChar, app.TabwriterFlags)

			fmt.Fprintf(w, "\tPartition\tHigh Watermark\tLeader\tReplicas\tISR\t\n")
			fmt.Fprintf(w, "\t---------\t--------------\t------\t--------\t---\t\n")

			highWatermarksSum := int64(0)
			for _, p := range desc.Partitions {
				highWatermarksSum += p.HighWatermark
				fmt.Fprintf(w, "\t%v\t%v\t%v\t%v\t%v\t\n", p.ID, p.HighWatermark, p.Leader, p.Replicas, p.ISR)
			}

			w.Flush()
			fmt.Fprintf(w, "Summed HighWatermark:\t%d\n", highWatermarksSum)
			w.Flush()

			fmt.Fprintf(w, "Config:\n")
			fmt.Fprintf(w, "\tName\tValue\tSensitive\t\n")
			fmt.Fprintf(w, "\t----\t-----\t---------\t\n")

			for _, entry := range desc.Configs {
				if entry.IsDefault {
					continue
				}
				fmt.Fprintf(w, "\t%v\t%v\t%v\t\n", entry.Name, entry.Value, entry.Sensitive)
			}

			w.Flush()
			return nil
		},
	}
}

func newCreateCommand(a *app.App) *cobra.Command {
	var (
		partitionsFlag int32
		replicasFlag   int16
		compactFlag    bool
	)
	cmd := &cobra.Command{
		Use:   "create TOPIC",
		Short: "Create a topic",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, err := a.NewKafClient()
			if err != nil {
				return err
			}
			defer cl.Close()

			topicName := args[0]
			configs := make(map[string]*string)
			if compactFlag {
				compact := "compact"
				configs["cleanup.policy"] = &compact
			}

			err = pkgtopic.Create(cmd.Context(), cl.Admin, topicName, partitionsFlag, replicasFlag, configs)
			if err != nil {
				return fmt.Errorf("could not create topic %v: %w", topicName, err)
			}

			w := app.NewTabWriter(a.OutWriter)
			fmt.Fprintf(w, "Created topic.\n")
			fmt.Fprintln(w, "\tTopic Name:\t", topicName)
			fmt.Fprintln(w, "\tPartitions:\t", partitionsFlag)
			fmt.Fprintln(w, "\tReplication Factor:\t", replicasFlag)
			if compactFlag {
				fmt.Fprintln(w, "\tCleanup Policy:\tcompact")
			}
			w.Flush()
			return nil
		},
	}
	cmd.Flags().Int32VarP(&partitionsFlag, "partitions", "p", int32(1), "Number of partitions")
	cmd.Flags().Int16VarP(&replicasFlag, "replicas", "r", int16(1), "Number of replicas")
	cmd.Flags().BoolVar(&compactFlag, "compact", false, "Enable topic compaction")
	return cmd
}

func newDeleteCommand(a *app.App) *cobra.Command {
	var noconfirm bool
	cmd := &cobra.Command{
		Use:               "delete TOPIC",
		Short:             "Delete a topic",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: a.ValidTopicArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			topicName := args[0]

			if !noconfirm {
				prompt := promptui.Prompt{
					Label:     fmt.Sprintf("Delete topic %q", topicName),
					IsConfirm: true,
				}
				if _, err := prompt.Run(); err != nil {
					return fmt.Errorf("aborted, exiting")
				}
			}

			cl, err := a.NewKafClient()
			if err != nil {
				return err
			}
			defer cl.Close()

			err = pkgtopic.Delete(cmd.Context(), cl.Admin, topicName)
			if err != nil {
				return fmt.Errorf("could not delete topic %v: %w", topicName, err)
			}
			fmt.Fprintf(a.OutWriter, "Deleted topic %v.\n", topicName)
			return nil
		},
	}
	cmd.Flags().BoolVar(&noconfirm, "noconfirm", false, "Do not prompt for confirmation")
	return cmd
}

func newAddConfigCommand(a *app.App) *cobra.Command {
	return &cobra.Command{
		Use:   "add-config TOPIC KEY VALUE",
		Short: "Add config key/value pair to topic",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, err := a.NewKafClient()
			if err != nil {
				return err
			}
			defer cl.Close()

			topicName := args[0]
			key := args[1]
			value := args[2]

			resps, err := cl.Admin.AlterTopicConfigs(cmd.Context(), []kadm.AlterConfig{
				{Name: key, Value: &value},
			}, topicName)
			if err != nil {
				return fmt.Errorf("failed to update topic config: %w", err)
			}
			for _, r := range resps {
				if r.Err != nil {
					return fmt.Errorf("failed to update topic config: %w", r.Err)
				}
			}
			fmt.Fprintf(a.OutWriter, "Added config %v=%v to topic %v.\n", key, value, topicName)
			return nil
		},
	}
}

func newRemoveConfigCommand(a *app.App) *cobra.Command {
	return &cobra.Command{
		Use:   "rm-config TOPIC ATTR1,ATTR2...",
		Short: "Remove attributes from topic",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, err := a.NewKafClient()
			if err != nil {
				return err
			}
			defer cl.Close()

			topicName := args[0]
			attrsToRemove := strings.Split(args[1], ",")

			var deleteConfigs []kadm.AlterConfig
			for _, attr := range attrsToRemove {
				deleteConfigs = append(deleteConfigs, kadm.AlterConfig{
					Op:   kadm.DeleteConfig,
					Name: attr,
				})
			}

			resps, err := cl.Admin.AlterTopicConfigs(cmd.Context(), deleteConfigs, topicName)
			if err != nil {
				return fmt.Errorf("failed to remove attributes from topic config: %w", err)
			}
			for _, r := range resps {
				if r.Err != nil {
					return fmt.Errorf("failed to remove attributes from topic config: %w", r.Err)
				}
			}
			fmt.Fprintf(a.OutWriter, "Removed attributes %v from topic %v.\n", attrsToRemove, topicName)
			return nil
		},
	}
}

func newSetConfigCommand(a *app.App) *cobra.Command {
	return &cobra.Command{
		Use:     "set-config",
		Short:   "set topic config. requires Kafka >=2.3.0 on broker side and kaf cluster config.",
		Example: "kaf topic set-config topic.name \"cleanup.policy=delete\"",
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, err := a.NewKafClient()
			if err != nil {
				return err
			}
			defer cl.Close()

			topicName := args[0]
			splt := strings.Split(args[1], ",")
			configs := make(map[string]string)

			for _, kv := range splt {
				s := strings.Split(kv, "=")
				if len(s) != 2 {
					continue
				}
				configs[s[0]] = s[1]
			}

			if len(configs) < 1 {
				return fmt.Errorf("no valid configs found")
			}

			err = pkgtopic.SetConfig(cmd.Context(), cl.Admin, topicName, configs)
			if err != nil {
				return fmt.Errorf("unable to alter topic config: %w", err)
			}
			fmt.Fprintf(a.OutWriter, "Updated config.\n")
			return nil
		},
	}
}

func newUpdateCommand(a *app.App) *cobra.Command {
	var (
		partitionsFlag           int32
		partitionAssignmentsFlag string
	)
	cmd := &cobra.Command{
		Use:     "update",
		Short:   "Update topic",
		Example: "kaf topic update -p 5 --partition-assignments '[[1,2,3],[1,2,3]]'",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, err := a.NewKafClient()
			if err != nil {
				return err
			}
			defer cl.Close()

			if partitionsFlag == -1 && partitionAssignmentsFlag == "" {
				return fmt.Errorf("number of partitions and/or partition assignments must be given")
			}

			if partitionsFlag != int32(-1) {
				err := pkgtopic.UpdatePartitions(cmd.Context(), cl.Admin, args[0], int(partitionsFlag))
				if err != nil {
					return fmt.Errorf("failed to update partitions: %w", err)
				}
			} else {
				var assignments [][]int32
				if err := json.Unmarshal([]byte(partitionAssignmentsFlag), &assignments); err != nil {
					return fmt.Errorf("invalid partition assignments: %w", err)
				}

				req := kmsg.NewAlterPartitionAssignmentsRequest()
				reqTopic := kmsg.NewAlterPartitionAssignmentsRequestTopic()
				reqTopic.Topic = args[0]
				for i, brokers := range assignments {
					p := kmsg.NewAlterPartitionAssignmentsRequestTopicPartition()
					p.Partition = int32(i)
					p.Replicas = brokers
					reqTopic.Partitions = append(reqTopic.Partitions, p)
				}
				req.Topics = append(req.Topics, reqTopic)

				resp, err := req.RequestWith(cmd.Context(), cl.KGO)
				if err != nil {
					return fmt.Errorf("failed to reassign the partition assignments: %w", err)
				}
				for _, t := range resp.Topics {
					for _, p := range t.Partitions {
						if p.ErrorCode != 0 {
							return fmt.Errorf("failed to reassign partition %v: %v", p.Partition, p.ErrorMessage)
						}
					}
				}
			}
			fmt.Fprintf(a.OutWriter, "Updated topic.\n")
			return nil
		},
	}
	cmd.Flags().Int32VarP(&partitionsFlag, "partitions", "p", int32(-1), "Number of partitions")
	cmd.Flags().StringVar(&partitionAssignmentsFlag, "partition-assignments", "", "Partition Assignments. Optional. If set in combination with -p, an assignment must be provided for each new partition. Example: '[[1,2,3],[1,2,3]]' (JSON Array syntax) assigns two new partitions to brokers 1,2,3. If used by itself, a reassignment must be provided for all partitions.")
	return cmd
}

func newLagCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "lag TOPIC",
		Short: "Display the total lags for each consumer group",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			topicName := args[0]
			cl, err := a.NewKafClient()
			if err != nil {
				return err
			}
			defer cl.Close()

			ctx := cmd.Context()

			endOffsets, err := cl.Admin.ListEndOffsets(ctx, topicName)
			if err != nil {
				return fmt.Errorf("unable to list end offsets: %w", err)
			}

			groups, err := cl.Admin.ListGroups(ctx)
			if err != nil {
				return fmt.Errorf("unable to list consumer groups: %w", err)
			}

			groupNames := groups.Groups()
			sort.Strings(groupNames)

			described, err := cl.Admin.DescribeGroups(ctx, groupNames...)
			if err != nil {
				return fmt.Errorf("unable to describe consumer groups: %w", err)
			}

			type lagEntry struct {
				group string
				state string
				lag   int64
			}

			var lagEntries []lagEntry
			for _, g := range described.Sorted() {
				var hasTopic bool
				for _, m := range g.Members {
					if ca, ok := m.Assigned.AsConsumer(); ok {
						for _, t := range ca.Topics {
							if t.Topic == topicName {
								hasTopic = true
								break
							}
						}
					}
					if hasTopic {
						break
					}
				}

				if !hasTopic {
					continue
				}

				offsets, err := cl.Admin.FetchOffsetsForTopics(ctx, g.Group, topicName)
				if err != nil {
					continue
				}

				var totalLag int64
				offsets.Each(func(o kadm.OffsetResponse) {
					if o.Topic != topicName {
						return
					}
					if endO, ok := endOffsets.Lookup(topicName, o.Partition); ok {
						if o.Offset.At >= 0 && endO.Offset > o.Offset.At {
							totalLag += endO.Offset - o.Offset.At
						}
					}
				})

				lagEntries = append(lagEntries, lagEntry{
					group: g.Group,
					state: g.State,
					lag:   totalLag,
				})
			}

			w := app.NewTabWriter(a.OutWriter)
			if !a.NoHeaderFlag {
				fmt.Fprintf(w, "GROUP ID\tSTATE\tLAG\n")
			}
			for _, e := range lagEntries {
				fmt.Fprintf(w, "%v\t%v\t%v\n", e.group, e.state, e.lag)
			}
			w.Flush()
			return nil
		},
	}
	a.AddNoHeadersFlag(cmd)
	return cmd
}
