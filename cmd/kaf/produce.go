package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"text/template"

	"time"

	"github.com/Masterminds/sprig"
	"github.com/Shopify/sarama"
	"github.com/birdayz/kaf/pkg/avro"
	"github.com/birdayz/kaf/pkg/partitioner"
	"github.com/birdayz/kaf/pkg/proto"
	"github.com/birdayz/kaf/pkg/codec"
	"github.com/spf13/cobra"
)

var (
	keyFlag         string
	rawKeyFlag      bool
	headerFlag      []string
	repeatFlag      int
	partitionerFlag string
	timestampFlag   string
	partitionFlag   int32
	bufferSizeFlag  int
	inputModeFlag   string
	avroSchemaID    int
	avroKeySchemaID int
	templateFlag    bool
)

func init() {
	rootCmd.AddCommand(produceCmd)

	produceCmd.Flags().StringVarP(&keyFlag, "key", "k", "", "Key for the record. Currently only strings are supported.")
	produceCmd.Flags().BoolVar(&rawKeyFlag, "raw-key", false, "Treat value of --key as base64 and use its decoded raw value as key")
	produceCmd.Flags().StringArrayVarP(&headerFlag, "header", "H", []string{}, "Header in format <key>:<value>. May be used multiple times to add more headers.")
	produceCmd.Flags().IntVarP(&repeatFlag, "repeat", "n", 1, "Repeat records to send.")

	produceCmd.Flags().StringSliceVar(&protoFiles, "proto-include", []string{}, "Path to proto files")
	produceCmd.Flags().StringSliceVar(&protoExclude, "proto-exclude", []string{}, "Proto exclusions (path prefixes)")
	produceCmd.Flags().StringVar(&protoType, "proto-type", "", "Fully qualified name of the proto message type. Example: com.test.SampleMessage")

	produceCmd.Flags().StringVar(&keyProtoType, "key-proto-type", "", "Fully qualified name of the proto key type. Example: com.test.SampleMessage")
	produceCmd.Flags().StringVar(&partitionerFlag, "partitioner", "", "Select partitioner: [jvm|rand|rr|hash]")
	produceCmd.Flags().StringVar(&timestampFlag, "timestamp", "", "Select timestamp for record")
	produceCmd.Flags().Int32VarP(&partitionFlag, "partition", "p", -1, "Partition to produce to")

	produceCmd.Flags().IntVarP(&avroSchemaID, "avro-schema-id", "", -1, "Value schema id for avro messsage encoding")
	produceCmd.Flags().IntVarP(&avroKeySchemaID, "avro-key-schema-id", "", -1, "Key schema id for avro messsage encoding")

	produceCmd.Flags().StringVarP(&inputModeFlag, "input-mode", "", "line", "Scanning input mode: [line|full]")
	produceCmd.Flags().IntVarP(&bufferSizeFlag, "line-length-limit", "", 0, "line length limit in line input mode")

	produceCmd.Flags().BoolVar(&templateFlag, "template", false, "run data through go template engine")

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

func valueEncoder() codec.Encoder {
	if protoType != "" {
		return proto.NewProtoCodec(protoType, reg)
	} else if avroSchemaID != -1 {
		return avro.NewAvroCodec(avroSchemaID, schemaCache)
	} else {
		return &codec.BypassCodec{}
	}
}

func keyEncoder() codec.Encoder {
	if keyProtoType != "" {
		return proto.NewProtoCodec(keyProtoType, reg)
	} else if avroKeySchemaID != -1 {
		return avro.NewAvroCodec(avroKeySchemaID, schemaCache)
	} else {
		return &codec.BypassCodec{}
	}
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

		if avroSchemaID != -1 || avroKeySchemaID != -1 {
			schemaCache = getSchemaCache()
			if schemaCache == nil {
				errorExit("Could not connect to schema registry")
			}
		}

		out := make(chan []byte, 1)
		switch inputModeFlag {
		case "full":
			go readFull(inReader, out)
		default:
			go readLines(inReader, out)
		}

		valueEncoder := valueEncoder()
		keyEncoder := keyEncoder()

		var key sarama.Encoder
		if rawKeyFlag {
			keyBytes, err := base64.RawStdEncoding.DecodeString(keyFlag)
			if err != nil {
				errorExit("--raw-key is given, but value of --key is not base64")
			}
			key = sarama.ByteEncoder(keyBytes)
		} else {
			encodedKey, err := keyEncoder.Encode([]byte(keyFlag))
			if err != nil {
				errorExit("%v", err)
			}
			key = sarama.ByteEncoder(encodedKey)
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
			data, err = valueEncoder.Encode(data)
			if err != nil {
				errorExit("%v", err)
			}

			var ts time.Time
			t, err := time.Parse(time.RFC3339, timestampFlag)
			if err != nil {
				ts = time.Now()
			} else {
				ts = t
			}

			for i := 0; i < repeatFlag; i++ {

				input := data

				if templateFlag {
					vars := map[string]interface{}{}
					vars["i"] = i
					tpl := template.New("kaf").Funcs(sprig.TxtFuncMap())

					tpl, err = tpl.Parse(string(data))
					if err != nil {
						errorExit("failed to parse go template: %v", err)
					}

					buf := bytes.NewBuffer(nil)

					if err := tpl.Execute(buf, vars); err != nil {
						errorExit("failed to execute go template: %v", err)
					}

					input = buf.Bytes()
				}

				msg := &sarama.ProducerMessage{
					Topic:     args[0],
					Key:       key,
					Headers:   headers,
					Timestamp: ts,
					Value:     sarama.ByteEncoder(input),
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
