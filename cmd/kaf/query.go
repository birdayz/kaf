package main

import (
	"context"
	"fmt"
	"sync"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/spf13/cobra"
)

var grepValue string

func init() {
	rootCmd.AddCommand(queryCmd)

	queryCmd.Flags().StringVarP(&keyFlag, "key", "k", "", "Key to search for")
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
	Run: func(cmd *cobra.Command, args []string) {
		topic := args[0]
		ctx := context.Background()
		
		// Get topic partitions info
		admin := getClusterAdmin()
		topics, err := admin.ListTopics(ctx)
		if err != nil {
			errorExit("Unable to list topics: %v\n", err)
		}
		
		topicDetail, exists := topics[topic]
		if !exists {
			errorExit("Topic %v not found.\n", topic)
		}

		schemaCache = getSchemaCache()
		wg := sync.WaitGroup{}

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
					errorExit("Unable to create consumer: %v\n", err)
				}
				defer cl.Close()

				for {
					fetches := cl.PollFetches(ctx)
					if errs := fetches.Errors(); len(errs) > 0 {
						for _, err := range errs {
							fmt.Printf("Error polling: %v\n", err)
						}
						return
					}

					iter := fetches.RecordIter()
					for !iter.Done() {
						record := iter.Next()
						if string(record.Key) == keyFlag {
							var keyTextRaw string
							var valueTextRaw string
							
							if protoType != "" {
								d, err := protoDecode(reg, record.Value, protoType)
								if err != nil {
									fmt.Println("Failed proto decode")
								}
								valueTextRaw = string(d)
							} else {
								valueTextRaw = string(record.Value)
							}

							if keyProtoType != "" {
								d, err := protoDecode(reg, record.Key, keyProtoType)
								if err != nil {
									fmt.Println("Failed proto decode")
								}
								keyTextRaw = string(d)
							} else {
								keyTextRaw = string(record.Key)
							}

							match := true
							if grepValue != "" {
								if !strings.Contains(valueTextRaw, grepValue) {
									match = false
								}
							}

							if match {
								fmt.Printf("Key: %v\n", keyTextRaw)
								fmt.Printf("Value: %v\n", valueTextRaw)
								return
							}
						}
					}
					
					if fetches.IsClientClosed() {
						return
					}
				}
			}(i)
		}

		wg.Wait()
	},
}
