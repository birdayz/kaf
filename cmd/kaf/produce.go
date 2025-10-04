package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"text/template"

	"time"

	"github.com/IBM/sarama"
	"github.com/Masterminds/sprig"
	"github.com/birdayz/kaf/pkg/partitioner"
	pb "github.com/golang/protobuf/proto"
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
	inputFormatFlag = InputFormatDefault
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
	produceCmd.Flags().Var(&inputFormatFlag, "input", "Set input format messages: default, json-each-row (json-each-row is compatible with output of kaf consume --output json-each-row)")
	produceCmd.Flags().IntVarP(&bufferSizeFlag, "line-length-limit", "", 0, "line length limit in line input mode")

	produceCmd.Flags().BoolVar(&templateFlag, "template", false, "run data through go template engine")

	if err := produceCmd.RegisterFlagCompletionFunc("input", completeInputFormat); err != nil {
		errorExit("Failed to register flag completion: %v", err)
	}
}

func readLines(reader io.Reader, out chan []byte) {
	scanner := bufio.NewScanner(reader)
	if bufferSizeFlag > 0 {
		scanner.Buffer(make([]byte, bufferSizeFlag), bufferSizeFlag)
	}

	for scanner.Scan() {
		out <- bytes.Clone(scanner.Bytes())
	}
	close(out)

	if err := scanner.Err(); err != nil {
		errorExit("scanning input failed: %v\n", err)
	}
}

func readFull(reader io.Reader, out chan []byte) {
	data, err := io.ReadAll(inReader)
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
	PreRun:            producePreRun,
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

		var key sarama.Encoder
		if rawKeyFlag {
			keyBytes, err := base64.RawStdEncoding.DecodeString(keyFlag)
			if err != nil {
				errorExit("--raw-key is given, but value of --key is not base64")
			}
			key = sarama.ByteEncoder(keyBytes)
		} else {
			key = sarama.StringEncoder(keyFlag)
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

		} else if avroKeySchemaID != -1 {
			avroKey, err := schemaCache.EncodeMessage(avroKeySchemaID, []byte(keyFlag))
			if err != nil {
				errorExit("Failed to encode avro key", err)
			}
			key = sarama.ByteEncoder(avroKey)
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

				// Encode to..something

				var marshaledInput []byte

				if protoType != "" {
					if dynamicMessage := reg.MessageForType(protoType); dynamicMessage != nil {
						err = dynamicMessage.UnmarshalJSON(input)
						if err != nil {
							errorExit("Failed to parse input JSON as proto type %v: %v", protoType, err)
						}

						pb, err := pb.Marshal(dynamicMessage)
						if err != nil {
							errorExit("Failed to marshal proto: %v", err)
						}

						marshaledInput = pb
					} else {
						errorExit("Failed to load payload proto type")
					}
				} else if avroSchemaID != -1 {
					avro, err := schemaCache.EncodeMessage(avroSchemaID, data)
					if err != nil {
						errorExit("Failed to encode avro value", err)
					}
					marshaledInput = avro
				} else {
					marshaledInput = input
				}

				var ts time.Time
				t, err := time.Parse(time.RFC3339, timestampFlag)
				if err != nil {
					ts = time.Now()
				} else {
					ts = t
				}

				msg := &sarama.ProducerMessage{
					Topic:     args[0],
					Timestamp: ts,
				}

				if inputFormatFlag == InputFormatJSONEachRow {
					jsonEachRowMsg := JSONEachRowMessage{}
					if err = json.Unmarshal(marshaledInput, &jsonEachRowMsg); err == nil {
						if keyFlag == "" {
							// if key flag not set, use the key from stdin
							key = sarama.StringEncoder(jsonEachRowMsg.Key)
						}

						for _, h := range jsonEachRowMsg.Headers {
							msg.Headers = append(msg.Headers, sarama.RecordHeader{
								Key:   []byte(h.Key),
								Value: []byte(h.Value),
							})
						}

						msg.Partition = jsonEachRowMsg.Partition
						marshaledInput = []byte(jsonEachRowMsg.Payload)
					}
				}

				msg.Key = key
				msg.Value = sarama.ByteEncoder(marshaledInput)

				if len(headers) > 0 {
					// override headers if they were set
					msg.Headers = headers
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

type InputFormat string

const (
	InputFormatDefault     InputFormat = "default"
	InputFormatJSONEachRow InputFormat = "json-each-row"
)

func (e *InputFormat) String() string {
	return string(*e)
}

func (e *InputFormat) Set(v string) error {
	switch v {
	case "default", "json-each-row":
		*e = InputFormat(v)
		return nil
	default:
		return fmt.Errorf("must be one of: default, raw, json, json-each-row")
	}
}

func (e *InputFormat) Type() string {
	return "InputFormat"
}

func completeInputFormat(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	return []string{"default", "json-each-row"}, cobra.ShellCompDirectiveNoFileComp
}

func producePreRun(cmd *cobra.Command, args []string) {
	loadProtoPathsFromConfig(cmd, args)
	setupProtoDescriptorRegistry(cmd, args)
}
