package main

import (
	"context"
	"fmt"
	"sync"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/spf13/cobra"
)

var (
	grepValue string
	queryKeyFlag string
)

func init() {
	rootCmd.AddCommand(queryCmd)

	queryCmd.Flags().StringVarP(&queryKeyFlag, "key", "k", "", "Key to search for")
	queryCmd.Flags().StringSliceVar(&protoFiles, "proto-include", []string{}, "Path to proto files")
	queryCmd.Flags().StringSliceVar(&protoExclude, "proto-exclude", []string{}, "Proto exclusions (path prefixes)")
	queryCmd.Flags().StringVar(&protoType, "proto-type", "", "Fully qualified name of the proto message type. Example: com.test.SampleMessage")
	queryCmd.Flags().StringVar(&keyProtoType, "key-proto-type", "", "Fully qualified name of the proto key type. Example: com.test.SampleMessage")

	queryCmd.Flags().StringVar(&grepValue, "grep", "", "Grep for value")

}

var queryCmd = &cobra.Command{
	Use:               "query TOPIC",
	Short:             "Query topic by key",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: validTopicArgs,
	PreRun:            setupProtoDescriptorRegistry,
	RunE: func(cmd *cobra.Command, args []string) error {
		topic := args[0]
		ctx := context.Background()

		// Get topic partitions info
		admin := getClusterAdmin()
		topics, err := admin.ListTopics(ctx)
		if err != nil {
			return fmt.Errorf("Unable to list topics: %v", err)
		}

		topicDetail, exists := topics[topic]
		if !exists {
			return fmt.Errorf("Topic %v not found", topic)
		}

		schemaCache = getSchemaCache()
		wg := sync.WaitGroup{}

		// Create a cancellable context so we can stop all goroutines when a match is found
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		for i := int32(0); i < int32(len(topicDetail.Partitions)); i++ {
			wg.Add(1)
			go func(partition int32) {
				defer wg.Done()

				// Create consumer for this partition
				opts := getKgoOpts()
				partToOffsets := make(map[string]map[int32]kgo.Offset)
				partToOffsets[topic] = make(map[int32]kgo.Offset)
				partToOffsets[topic][partition] = kgo.NewOffset().AtStart()
				opts = append(opts, kgo.ConsumePartitions(partToOffsets))

				cl, err := kgo.NewClient(opts...)
				if err != nil {
					fmt.Fprintf(cmd.ErrOrStderr(), "Unable to create consumer: %v\n", err)
					return
				}
				defer cl.Close()

				for {
					// Check if context is cancelled (another goroutine found a match)
					select {
					case <-ctx.Done():
						return
					default:
					}

					fetches := cl.PollFetches(ctx)
					if errs := fetches.Errors(); len(errs) > 0 {
						for _, err := range errs {
							fmt.Fprintf(cmd.ErrOrStderr(), "Error polling: %v\n", err)
						}
						return
					}

					// Check if we've reached the end of the partition
					reachedEnd := false
					fetches.EachPartition(func(p kgo.FetchTopicPartition) {
						if len(p.Records) > 0 {
							lastOffset := p.Records[len(p.Records)-1].Offset
							// If the last record's offset + 1 >= high watermark, we've read all available records
							if lastOffset+1 >= p.HighWatermark {
								reachedEnd = true
							}
						} else if p.HighWatermark == 0 {
							// Empty partition (no records ever written)
							reachedEnd = true
						}
					})

					iter := fetches.RecordIter()
					for !iter.Done() {
						record := iter.Next()

						// Check if key matches (if key filter is specified)
						keyMatches := queryKeyFlag == "" || string(record.Key) == queryKeyFlag
						if !keyMatches {
							continue
						}

						var keyTextRaw string
						var valueTextRaw string

						if protoType != "" {
							d, err := protoDecode(reg, record.Value, protoType)
							if err != nil {
								fmt.Fprintln(cmd.ErrOrStderr(), "Failed proto decode")
							}
							valueTextRaw = string(d)
						} else {
							valueTextRaw = string(record.Value)
						}

						if keyProtoType != "" {
							d, err := protoDecode(reg, record.Key, keyProtoType)
							if err != nil {
								fmt.Fprintln(cmd.ErrOrStderr(), "Failed proto decode")
							}
							keyTextRaw = string(d)
						} else {
							keyTextRaw = string(record.Key)
						}

						// Check if value matches grep filter (if grep is specified)
						match := true
						if grepValue != "" {
							if !strings.Contains(valueTextRaw, grepValue) {
								match = false
							}
						}

						if match {
							fmt.Fprintf(cmd.OutOrStdout(), "Key: %v\n", keyTextRaw)
							fmt.Fprintf(cmd.OutOrStdout(), "Value: %v\n", valueTextRaw)
							cancel() // Signal other goroutines to stop
							return
						}
					}

					if fetches.IsClientClosed() {
						return
					}

					// Exit when we've reached the end of the partition
					if reachedEnd {
						return
					}

					// Also exit when no more records to consume (empty fetch after first poll)
					if len(fetches.Records()) == 0 {
						return
					}
				}
			}(i)
		}

		wg.Wait()
		return nil
	},
}
