package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"time"

	"github.com/Shopify/sarama"
	"github.com/burdiyan/kafkautil"
	pb "github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
)

var keyFlag string
var numFlag int
var partitionerFlag string
var timestampFlag string

func init() {
	rootCmd.AddCommand(produceCmd)

	produceCmd.Flags().StringVarP(&keyFlag, "key", "k", "", "Key for the record. Currently only strings are supported.")
	produceCmd.Flags().IntVarP(&numFlag, "num", "n", 1, "Number of records to send.")

	produceCmd.Flags().StringSliceVar(&protoFiles, "proto-include", []string{}, "Path to proto files")
	produceCmd.Flags().StringSliceVar(&protoExclude, "proto-exclude", []string{}, "Proto exclusions (path prefixes)")
	produceCmd.Flags().StringVar(&protoType, "proto-type", "", "Fully qualified name of the proto message type. Example: com.test.SampleMessage")

	produceCmd.Flags().StringVar(&keyProtoType, "key-proto-type", "", "Fully qualified name of the proto key type. Example: com.test.SampleMessage")
	produceCmd.Flags().StringVar(&partitionerFlag, "partitioner", "", "Select partitioner: Default or jvm")
	produceCmd.Flags().StringVar(&timestampFlag, "timestamp", "", "Select timestamp for record")

}

var produceCmd = &cobra.Command{
	Use:    "produce TOPIC",
	Short:  "Produce record. Reads data from stdin.",
	Args:   cobra.ExactArgs(1),
	PreRun: setupProtoDescriptorRegistry,
	Run: func(cmd *cobra.Command, args []string) {
		cfg := getConfig()
		if partitionerFlag != "" {
			cfg.Producer.Partitioner = kafkautil.NewJVMCompatiblePartitioner
		}
		producer, err := sarama.NewSyncProducer(currentCluster.Brokers, cfg)
		if err != nil {
			errorExit("Unable to create new sync producer: %v\n", err)
		}

		data, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			errorExit("Unable to read data\n")
		}

		var key sarama.Encoder
		key = sarama.StringEncoder(keyFlag)

		if protoType != "" {
			if dynamicMessage := reg.MessageForType(protoType); dynamicMessage != nil {
				err = dynamicMessage.UnmarshalJSON(data)
				if err != nil {
					errorExit("Failed to parse input JSON as proto type %v: %v", protoType, err)
				}

				pb, err := pb.Marshal(dynamicMessage)
				if err != nil {
					errorExit("Failed to marshal proto: %v", err)
				}

				data = pb
			}
		}

		if keyProtoType != "" {
			if dynamicMessage := reg.MessageForType(keyProtoType); dynamicMessage != nil {
				err = dynamicMessage.UnmarshalJSON([]byte(keyFlag))
				if err != nil {
					errorExit("Failed to parse input JSON as proto type %v: %v", protoType, err)
				}

				pb, err := pb.Marshal(dynamicMessage)
				if err != nil {
					errorExit("Failed to marshal proto: %v", err)
				}

				key = sarama.ByteEncoder(pb)
			}
		}

		var ts time.Time
		t, err := time.Parse(time.RFC3339, timestampFlag)
		if err != nil {
			ts = time.Now()
		} else {
			ts = t
		}

		for i := 0; i < numFlag; i++ {
			partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
				Topic:     args[0],
				Key:       key,
				Timestamp: ts,
				Value:     sarama.ByteEncoder(data),
			})
			if err != nil {
				fmt.Printf("Failed to send record: %v.", err)
				os.Exit(1)
			}

			fmt.Printf("Sent record to partition %v at offset %v.\n", partition, offset)

		}

	},
}
