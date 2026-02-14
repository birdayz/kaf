package query

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/birdayz/kaf/pkg/app"
)

// NewCommand returns the "kaf query" command.
func NewCommand(a *app.App) *cobra.Command {
	var (
		keyFlag   string
		grepValue string
	)

	cmd := &cobra.Command{
		Use:   "query TOPIC",
		Short: "Query topic by key",
		Long:  "Query a topic by scanning all partitions from the start and filtering by key. Optionally grep the value. Stops when all partitions reach the high watermark.",
		Example: `  kaf query my-topic -k my-key
  kaf query my-topic -k my-key --grep "error"`,
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: a.ValidTopicArgs,
		PreRunE:           a.SetupProtoDescriptorRegistry,
		RunE: func(cmd *cobra.Command, args []string) error {
			topic := args[0]
			ctx := cmd.Context()

			// Get end offsets so we know when to stop.
			cl, err := a.NewKafClient()
			if err != nil {
				return err
			}
			endOffsets, err := cl.Admin.ListEndOffsets(ctx, topic)
			if err != nil {
				cl.Close()
				return fmt.Errorf("failed to get end offsets: %w", err)
			}
			cl.Close()

			endOffsetMap := app.EndOffsetMapForTopic(endOffsets, topic)

			// Check if topic is empty.
			empty := true
			for _, off := range endOffsetMap {
				if off > 0 {
					empty = false
					break
				}
			}
			if empty {
				return nil
			}

			var scErr error
			a.SchemaCache, scErr = a.NewSchemaCache()
			if scErr != nil {
				return scErr
			}

			// Consume all partitions from the start.
			kafCl, err := a.NewKafClient(
				kgo.ConsumeTopics(topic),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			)
			if err != nil {
				return err
			}
			defer kafCl.Close()

			for {
				fetches := kafCl.KGO.PollFetches(ctx)
				if fetches.IsClientClosed() || ctx.Err() != nil {
					return nil
				}

				fetches.EachRecord(func(rec *kgo.Record) {
					if string(rec.Key) != keyFlag {
						return
					}

					var keyTextRaw string
					var valueTextRaw string
					if a.ProtoType != "" {
						d, err := app.ProtoDecode(a.Reg, rec.Value, a.ProtoType)
						if err != nil {
							fmt.Fprintln(a.ErrWriter, "Failed proto decode")
						}
						valueTextRaw = string(d)
					} else {
						valueTextRaw = string(rec.Value)
					}

					if a.KeyProtoType != "" {
						d, err := app.ProtoDecode(a.Reg, rec.Key, a.KeyProtoType)
						if err != nil {
							fmt.Fprintln(a.ErrWriter, "Failed proto decode")
						}
						keyTextRaw = string(d)
					} else {
						keyTextRaw = string(rec.Key)
					}

					if grepValue != "" && !strings.Contains(valueTextRaw, grepValue) {
						return
					}

					fmt.Fprintf(a.OutWriter, "Key: %v\n", keyTextRaw)
					fmt.Fprintf(a.OutWriter, "Value: %v\n", valueTextRaw)
				})

				if errs := fetches.Errors(); len(errs) > 0 {
					for _, e := range errs {
						fmt.Fprintf(a.ErrWriter, "fetch error topic %s partition %d: %v\n", e.Topic, e.Partition, e.Err)
					}
				}

				// Check if we've reached end offsets for all partitions.
				done := true
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
		},
	}

	cmd.Flags().StringVarP(&keyFlag, "key", "k", "", "Key to search for")
	a.AddProtoFlags(cmd)
	cmd.Flags().StringVar(&grepValue, "grep", "", "Grep for value")

	return cmd
}
