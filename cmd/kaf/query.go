package main

import (
	"fmt"
	"sync"

	"strings"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
)

var grepValue string

func init() {
	rootCmd.AddCommand(queryCmd)

	queryCmd.Flags().StringVarP(&keyFlag, "key", "k", "", "Key to search for")
	queryCmd.Flags().StringSliceVar(&protoFiles, "proto-include", []string{}, "Path to proto files")
	queryCmd.Flags().StringSliceVar(&protoExclude, "proto-exclude", []string{}, "Proto exclusions (path prefixes)")
	queryCmd.Flags().BoolVar(&confluentHeader, "confluent-header", false, "Force deserialization of messages with confluent headers (use if header detection fails)")
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
		client := getClient()

		consumer, err := sarama.NewConsumerFromClient(client)
		if err != nil {
			errorExit("Unable to create consumer from client: %v\n", err)
		}

		partitions, err := consumer.Partitions(topic)
		if err != nil {
			errorExit("Unable to get partitions: %v\n", err)
		}

		schemaCache = getSchemaCache()

		wg := sync.WaitGroup{}

		for _, partition := range partitions {
			wg.Add(1)
			go func(partition int32) {
				defer wg.Done()
				highWatermark, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
				if err != nil {
					errorExit("Failed to get high watermark: %w", err)
				}

				if highWatermark == 0 {
					return
				}

				pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
				if err != nil {
					errorExit("Unable to consume partition: %v\n", err)
				}

				for msg := range pc.Messages() {
					if string(msg.Key) == keyFlag {
						var keyTextRaw string
						var valueTextRaw string
						if protoType != "" {
							d, err := protoDecode(reg, msg.Value, protoType)
							if err != nil {
								fmt.Println("Failed proto decode")
							}
							valueTextRaw = string(d)
						} else {
							valueTextRaw = string(msg.Value)
						}

						if keyProtoType != "" {
							d, err := protoDecode(reg, msg.Key, keyProtoType)
							if err != nil {
								fmt.Println("Failed proto decode")
							}
							keyTextRaw = string(d)
						} else {
							keyTextRaw = string(msg.Key)
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
						}

						if msg.Offset == pc.HighWaterMarkOffset()-1 {
							break
						}
					}
				}
			}(partition)
		}

		wg.Wait()
	},
}
