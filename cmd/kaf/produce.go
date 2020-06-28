package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"time"

	"github.com/Shopify/sarama"
	"github.com/burdiyan/kafkautil"
	pb "github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
)

var (
	keyFlag         string
	headerFlag      []string
	numFlag         int
	partitionerFlag string
	timestampFlag   string
	partitionFlag   int32
)

func init() {
	rootCmd.AddCommand(produceCmd)

	produceCmd.Flags().StringVarP(&keyFlag, "key", "k", "", "Key for the record. Currently only strings are supported.")
	produceCmd.Flags().StringArrayVarP(&headerFlag, "header", "H", []string{}, "Header in format <key>:<value>. May be used multiple times to add more headers.")
	produceCmd.Flags().IntVarP(&numFlag, "num", "n", 1, "Number of records to send.")

	produceCmd.Flags().StringSliceVar(&protoFiles, "proto-include", []string{}, "Path to proto files")
	produceCmd.Flags().StringSliceVar(&protoExclude, "proto-exclude", []string{}, "Proto exclusions (path prefixes)")
	produceCmd.Flags().StringVar(&protoType, "proto-type", "", "Fully qualified name of the proto message type. Example: com.test.SampleMessage")

	produceCmd.Flags().StringVar(&keyProtoType, "key-proto-type", "", "Fully qualified name of the proto key type. Example: com.test.SampleMessage")
	produceCmd.Flags().StringVar(&partitionerFlag, "partitioner", "", "Select partitioner: Default or jvm")
	produceCmd.Flags().StringVar(&timestampFlag, "timestamp", "", "Select timestamp for record")
	produceCmd.Flags().Int32VarP(&partitionFlag, "partition", "p", -1, "Partition to produce to")

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

		if partitionFlag != int32(-1) {
			cfg.Producer.Partitioner = sarama.NewManualPartitioner
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
			} else {
				errorExit("Failed to load payload proto type")
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
			} else {
				errorExit("Failed to load key proto type")
			}

		}

		var ts time.Time
		t, err := time.Parse(time.RFC3339, timestampFlag)
		if err != nil {
			ts = time.Now()
		} else {
			ts = t
		}

		var headers []sarama.RecordHeader
		for _, h := range headerFlag {
			v := strings.SplitN(h, ":", 2)
			if len(v) == 2 {
				headers = append(headers, sarama.RecordHeader{
					Key:   []byte(v[0]),
					Value: []byte(v[1]),
				})
			}
		}

		for i := 0; i < numFlag; i++ {
			msg := &sarama.ProducerMessage{
				Topic:     args[0],
				Key:       key,
				Headers:   headers,
				Timestamp: ts,
				Value:     sarama.ByteEncoder(data),
			}
			if partitionFlag != -1 {
				msg.Partition = partitionFlag
			}
			partition, offset, err := producer.SendMessage(msg)
			if err != nil {
				fmt.Printf("Failed to send record: %v.", err)
				os.Exit(1)
			}

			fmt.Printf("Sent record to partition %v at offset %v.\n", partition, offset)
		}

	},
}
