package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"time"

	"github.com/Shopify/sarama"
	"github.com/birdayz/kaf/pkg/partitioner"
	pb "github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
)

var (
	keyFlag         string
	headerFlag      []string
	repeatFlag      int
	partitionerFlag string
	timestampFlag   string
	partitionFlag   int32
	bufferSizeFlag  int
	inputModeFlag   string
)

func init() {
	rootCmd.AddCommand(produceCmd)

	produceCmd.Flags().StringVarP(&keyFlag, "key", "k", "", "Key for the record. Currently only strings are supported.")
	produceCmd.Flags().StringArrayVarP(&headerFlag, "header", "H", []string{}, "Header in format <key>:<value>. May be used multiple times to add more headers.")
	produceCmd.Flags().IntVarP(&repeatFlag, "repeat", "n", 1, "Repeat records to send.")

	produceCmd.Flags().StringSliceVar(&protoFiles, "proto-include", []string{}, "Path to proto files")
	produceCmd.Flags().StringSliceVar(&protoExclude, "proto-exclude", []string{}, "Proto exclusions (path prefixes)")
	produceCmd.Flags().StringVar(&protoType, "proto-type", "", "Fully qualified name of the proto message type. Example: com.test.SampleMessage")

	produceCmd.Flags().StringVar(&keyProtoType, "key-proto-type", "", "Fully qualified name of the proto key type. Example: com.test.SampleMessage")
	produceCmd.Flags().StringVar(&partitionerFlag, "partitioner", "", "Select partitioner: [jvm|rand|rr|hash]")
	produceCmd.Flags().StringVar(&timestampFlag, "timestamp", "", "Select timestamp for record")
	produceCmd.Flags().Int32VarP(&partitionFlag, "partition", "p", -1, "Partition to produce to")

	produceCmd.Flags().StringVarP(&inputModeFlag, "input-mode", "", "line", "Scanning input mode: [line|full]")
	produceCmd.Flags().IntVarP(&bufferSizeFlag, "line-length-limit", "", 0, "line length limit in line input mode")
}

func readLines(reader io.Reader, out chan []byte) {
	scanner := bufio.NewScanner(reader)
	if bufferSizeFlag > 0 {
		scanner.Buffer(make([]byte, bufferSizeFlag), bufferSizeFlag)
	}

	for scanner.Scan() {
		out <- scanner.Bytes()
	}
	close(out)

	if err := scanner.Err(); err != nil {
		errorExit("scanning input failed: %v\n", err)
	}
}

func readFull(reader io.Reader, out chan []byte) {
	data, err := ioutil.ReadAll(inReader)
	if err != nil {
		errorExit("Unable to read data\n")
	}
	out <- data
	close(out)
}

var produceCmd = &cobra.Command{
	Use:               "produce TOPIC",
	Short:             "Produce record. Reads data from stdin.",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: validTopicArgs,
	PreRun:            setupProtoDescriptorRegistry,
	Run: func(cmd *cobra.Command, args []string) {
		cfg := getConfig()
		switch partitionerFlag {
		case "jvm":
			cfg.Producer.Partitioner = partitioner.NewJVMCompatiblePartitioner
		case "rand":
			cfg.Producer.Partitioner = sarama.NewRandomPartitioner
		case "rr":
			cfg.Producer.Partitioner = sarama.NewRoundRobinPartitioner
		}

		if partitionFlag != int32(-1) {
			cfg.Producer.Partitioner = sarama.NewManualPartitioner
		}

		producer, err := sarama.NewSyncProducer(currentCluster.Brokers, cfg)
		if err != nil {
			errorExit("Unable to create new sync producer: %v\n", err)
		}

		out := make(chan []byte, 1)
		switch inputModeFlag {
		case "full":
			go readFull(inReader, out)
		default:
			go readLines(inReader, out)
		}

		var key sarama.Encoder
		key = sarama.StringEncoder(keyFlag)
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

		for data := range out {
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

			var ts time.Time
			t, err := time.Parse(time.RFC3339, timestampFlag)
			if err != nil {
				ts = time.Now()
			} else {
				ts = t
			}

			for i := 0; i < repeatFlag; i++ {
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
					fmt.Fprintf(outWriter, "Failed to send record: %v.", err)
					os.Exit(1)
				}

				fmt.Fprintf(outWriter, "Sent record to partition %v at offset %v.\n", partition, offset)
			}
		}
	},
}
