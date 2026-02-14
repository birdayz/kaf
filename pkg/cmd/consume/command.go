package consume

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/birdayz/kaf/pkg/app"
)

// NewCommand returns the "kaf consume" command.
func NewCommand(a *app.App) *cobra.Command {
	var (
		offsetFlag        string
		groupFlag         string
		groupCommitFlag   bool
		outputFormat      = app.OutputFormatDefault
		raw               bool
		follow            bool
		tail              int32
		flagPartitions    []int32
		limitMessagesFlag int64
		headerFilterFlag  []string
	)

	cmd := &cobra.Command{
		Use:   "consume TOPIC",
		Short: "Consume messages",
		Long:  "Consume messages from a Kafka topic. Supports consumer groups, partition selection, offset control, tail mode, and multiple output formats.",
		Example: `  kaf consume my-topic
  kaf consume my-topic -f
  kaf consume my-topic --offset newest -f
  kaf consume my-topic -n 10
  kaf consume my-topic -g my-group --commit
  kaf consume my-topic --output json
  kaf consume my-topic --header "env:prod"`,
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: a.ValidTopicArgs,
		PreRunE:           a.SetupProtoDescriptorRegistry,
		RunE: func(cmd *cobra.Command, args []string) error {
			topic := args[0]
			ctx := cmd.Context()

			if outputFormat == app.OutputFormatDefault && raw {
				outputFormat = app.OutputFormatRaw
			}

			headerFilter := make(map[string]string)
			for _, f := range headerFilterFlag {
				parts := strings.SplitN(f, ":", 2)
				if len(parts) != 2 {
					return fmt.Errorf("invalid header filter format: %s, expected format: key:value", f)
				}
				headerFilter[parts[0]] = parts[1]
			}

			var scErr error
			a.SchemaCache, scErr = a.NewSchemaCache()
			if scErr != nil {
				return scErr
			}

			var opts []kgo.Opt
			opts = append(opts, kgo.ConsumeTopics(topic))

			if groupFlag != "" {
				opts = append(opts, kgo.ConsumerGroup(groupFlag))
				if !groupCommitFlag {
					opts = append(opts, kgo.DisableAutoCommit())
				}
			} else {
				switch offsetFlag {
				case "oldest":
					opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
				case "newest":
					opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
				default:
					o, err := strconv.ParseInt(offsetFlag, 10, 64)
					if err != nil {
						return fmt.Errorf("could not parse '%s' to int64: %v", offsetFlag, err)
					}
					opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().At(o)))
				}

				if len(flagPartitions) > 0 {
					partMap := make(map[string]map[int32]kgo.Offset)
					offsets := make(map[int32]kgo.Offset, len(flagPartitions))
					for _, p := range flagPartitions {
						switch offsetFlag {
						case "oldest":
							offsets[p] = kgo.NewOffset().AtStart()
						case "newest":
							offsets[p] = kgo.NewOffset().AtEnd()
						default:
							o, _ := strconv.ParseInt(offsetFlag, 10, 64)
							offsets[p] = kgo.NewOffset().At(o)
						}
					}
					partMap[topic] = offsets
					opts = append(opts, kgo.ConsumePartitions(partMap))
				}
			}

			var endOffsetMap map[int32]int64
			needsAdmin := tail > 0 || (!follow && groupFlag == "" && tail == 0)
			if needsAdmin {
				admCl, err := a.NewKafClient()
				if err != nil {
					return err
				}
				defer admCl.Close()

				if tail > 0 {
					endOffsets, err := admCl.Admin.ListEndOffsets(ctx, topic)
					if err != nil {
						return fmt.Errorf("failed to get end offsets: %v", err)
					}
					startOffsets, err := admCl.Admin.ListStartOffsets(ctx, topic)
					if err != nil {
						return fmt.Errorf("failed to get start offsets: %v", err)
					}

					partMap := make(map[string]map[int32]kgo.Offset)
					offsets := make(map[int32]kgo.Offset)
					endOffsets.Each(func(lo kadm.ListedOffset) {
						if lo.Topic != topic {
							return
						}
						start := lo.Offset - int64(tail)
						if so, ok := startOffsets.Lookup(topic, lo.Partition); ok && start < so.Offset {
							start = so.Offset
						}
						offsets[lo.Partition] = kgo.NewOffset().At(start)
					})
					partMap[topic] = offsets
					opts = append(opts, kgo.ConsumePartitions(partMap))
				}

				if !follow && groupFlag == "" && tail == 0 {
					endOffs, err := admCl.Admin.ListEndOffsets(ctx, topic)
					if err != nil {
						return fmt.Errorf("failed to get end offsets: %v", err)
					}
					endOffsetMap = app.EndOffsetMapForTopic(endOffs, topic)
				}
			}

			kafCl, err := a.NewKafClient(opts...)
			if err != nil {
				return err
			}
			defer kafCl.Close()

			var mu sync.Mutex
			partitionCounts := make(map[int32]int64)

			for {
				fetches := kafCl.KGO.PollFetches(ctx)
				if fetches.IsClientClosed() || ctx.Err() != nil {
					return nil
				}

				fetches.EachRecord(func(rec *kgo.Record) {
					a.HandleMessage(rec, &mu, outputFormat, headerFilter)
					if limitMessagesFlag > 0 {
						partitionCounts[rec.Partition]++
					}
				})

				if errs := fetches.Errors(); len(errs) > 0 {
					for _, e := range errs {
						fmt.Fprintf(a.ErrWriter, "fetch error topic %s partition %d: %v\n", e.Topic, e.Partition, e.Err)
					}
				}

				if groupCommitFlag && groupFlag != "" {
					if err := kafCl.KGO.CommitUncommittedOffsets(ctx); err != nil {
						fmt.Fprintf(a.ErrWriter, "commit error: %v\n", err)
					}
				}

				if !follow && groupFlag == "" {
					if endOffsetMap != nil {
						done := true
						for p, endOff := range endOffsetMap {
							if endOff == 0 {
								continue
							}
							if _, ok := partitionCounts[p]; !ok && endOff > 0 {
								done = false
								break
							}
						}
						fetches.EachRecord(func(rec *kgo.Record) {
							if endOff, ok := endOffsetMap[rec.Partition]; ok {
								if rec.Offset+1 < endOff {
									done = false
								}
							}
						})
						if done {
							return nil
						}
					}

					if limitMessagesFlag > 0 {
						allDone := true
						for _, count := range partitionCounts {
							if count < limitMessagesFlag {
								allDone = false
								break
							}
						}
						if allDone {
							return nil
						}
					}
				}
			}
		},
	}

	cmd.Flags().StringVar(&offsetFlag, "offset", "oldest", "Offset to start consuming. Possible values: oldest, newest, or integer.")
	cmd.Flags().BoolVar(&raw, "raw", false, "Print raw output of messages, without key or prettified JSON")
	cmd.Flags().Var(&outputFormat, "output", "Set output format messages: default, raw (without key or prettified JSON), hex (without key or prettified JSON), json, json-each-row")
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "Continue to consume messages until program execution is interrupted/terminated")
	cmd.Flags().Int32VarP(&tail, "tail", "n", 0, "Print last n messages per partition")
	a.AddProtoFlags(cmd)
	cmd.Flags().BoolVar(&a.DecodeMsgPack, "decode-msgpack", false, "Enable deserializing msgpack")
	cmd.Flags().Int32SliceVarP(&flagPartitions, "partitions", "p", []int32{}, "Partitions to consume from")
	cmd.Flags().Int64VarP(&limitMessagesFlag, "limit-messages", "l", 0, "Limit messages per partition")
	cmd.Flags().StringVarP(&groupFlag, "group", "g", "", "Consumer Group to use for consume")
	cmd.Flags().BoolVar(&groupCommitFlag, "commit", false, "Commit Group offset after receiving messages. Works only if consuming as Consumer Group")
	cmd.Flags().StringSliceVar(&headerFilterFlag, "header", []string{}, "Filter messages by header. Format: key:value. Multiple filters can be specified")

	if err := cmd.RegisterFlagCompletionFunc("output", app.CompleteOutputFormat); err != nil {
		panic(fmt.Sprintf("Failed to register flag completion: %v", err))
	}
	if err := cmd.Flags().MarkDeprecated("raw", "use --output raw instead"); err != nil {
		panic(fmt.Sprintf("Failed to mark flag as deprecated: %v", err))
	}

	return cmd
}
