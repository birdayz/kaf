package produce

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/template"
	"time"

	"github.com/Masterminds/sprig/v3"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/birdayz/kaf/pkg/app"
)

// NewCommand returns the "kaf produce" command.
func NewCommand(a *app.App) *cobra.Command {
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
		inputFormatFlag = app.InputFormatDefault
	)

	cmd := &cobra.Command{
		Use:   "produce TOPIC",
		Short: "Produce record. Reads data from stdin.",
		Long:  "Produce records to a Kafka topic. Reads data from stdin, one record per line by default. Supports key specification, headers, partitioner selection, protobuf/avro encoding, and go templates.",
		Example: `  echo '{"hello":"world"}' | kaf produce my-topic
  echo 'value' | kaf produce my-topic -k my-key
  echo 'value' | kaf produce my-topic -H "env:prod" -H "version:1"
  echo 'value' | kaf produce my-topic -n 10
  cat data.json | kaf produce my-topic --input-mode full`,
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: a.ValidTopicArgs,
		PreRunE:           a.SetupProtoDescriptorRegistry,
		RunE: func(cmd *cobra.Command, args []string) error {
			var opts []kgo.Opt
			switch partitionerFlag {
			case "jvm":
				opts = append(opts, kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)))
			case "rand":
				opts = append(opts, kgo.RecordPartitioner(kgo.StickyPartitioner()))
			case "rr":
				opts = append(opts, kgo.RecordPartitioner(kgo.RoundRobinPartitioner()))
			}

			if partitionFlag != int32(-1) {
				opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))
			}

			cl, err := a.NewKafClient(opts...)
			if err != nil {
				return err
			}
			defer cl.Close()

			if avroSchemaID != -1 || avroKeySchemaID != -1 {
				var scErr error
				a.SchemaCache, scErr = a.NewSchemaCache()
				if scErr != nil {
					return scErr
				}
				if a.SchemaCache == nil {
					return fmt.Errorf("could not connect to schema registry")
				}
			}

			var key []byte
			if rawKeyFlag {
				keyBytes, err := base64.StdEncoding.DecodeString(keyFlag)
				if err != nil {
					return fmt.Errorf("--raw-key is given, but value of --key is not base64")
				}
				key = keyBytes
			} else if keyFlag != "" {
				key = []byte(keyFlag)
			}
			if a.KeyProtoType != "" {
				var err error
				key, err = a.ProtoEncode([]byte(keyFlag), a.KeyProtoType)
				if err != nil {
					return fmt.Errorf("failed to encode proto key: %v", err)
				}
			} else if avroKeySchemaID != -1 {
				avroKey, err := a.SchemaCache.EncodeMessage(avroKeySchemaID, []byte(keyFlag))
				if err != nil {
					return fmt.Errorf("failed to encode avro key: %v", err)
				}
				key = avroKey
			}

			var headers []kgo.RecordHeader
			for _, h := range headerFlag {
				v := strings.SplitN(h, ":", 2)
				if len(v) == 2 {
					headers = append(headers, kgo.RecordHeader{
						Key:   v[0],
						Value: []byte(v[1]),
					})
				}
			}

			ctx := cmd.Context()

			out := make(chan []byte, 1)
			errCh := make(chan error, 1)
			switch inputModeFlag {
			case "full":
				go readFull(a.InReader, out, errCh)
			default:
				go readLines(a.InReader, out, errCh, bufferSizeFlag)
			}

			for data := range out {
				for i := 0; i < repeatFlag; i++ {
					input := data

					if templateFlag {
						vars := map[string]any{"i": i}
						tpl := template.New("kaf").Funcs(sprig.HermeticTxtFuncMap())

						tpl, err := tpl.Parse(string(data))
						if err != nil {
							return fmt.Errorf("failed to parse go template: %v", err)
						}

						buf := bytes.NewBuffer(nil)
						if err := tpl.Execute(buf, vars); err != nil {
							return fmt.Errorf("failed to execute go template: %v", err)
						}
						input = buf.Bytes()
					}

					var marshaledInput []byte
					if a.ProtoType != "" {
						var err error
						marshaledInput, err = a.ProtoEncode(input, a.ProtoType)
						if err != nil {
							return fmt.Errorf("failed to encode proto value: %v", err)
						}
					} else if avroSchemaID != -1 {
						avro, err := a.SchemaCache.EncodeMessage(avroSchemaID, input)
						if err != nil {
							return fmt.Errorf("failed to encode avro value: %v", err)
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

					rec := &kgo.Record{
						Topic:     args[0],
						Key:       key,
						Value:     marshaledInput,
						Timestamp: ts,
					}

					if inputFormatFlag == app.InputFormatJSONEachRow {
						jsonEachRowMsg := app.JSONEachRowMessage{}
						if err = json.Unmarshal(marshaledInput, &jsonEachRowMsg); err == nil {
							if keyFlag == "" {
								rec.Key = []byte(jsonEachRowMsg.Key)
							}
							for _, h := range jsonEachRowMsg.Headers {
								rec.Headers = append(rec.Headers, kgo.RecordHeader{
									Key:   h.Key,
									Value: []byte(h.Value),
								})
							}
							rec.Partition = jsonEachRowMsg.Partition
							rec.Value = []byte(jsonEachRowMsg.Payload)
						}
					} else if inputFormatFlag == app.InputFormatHex {
						dst := make([]byte, hex.DecodedLen(len(marshaledInput)))
						if _, err := hex.Decode(dst, marshaledInput); err != nil {
							fmt.Fprintf(a.ErrWriter, "Failed to decode hex input: %v.\n", err)
						} else {
							rec.Value = dst
						}
					}

					if len(headers) > 0 {
						rec.Headers = headers
					}
					if partitionFlag != -1 {
						rec.Partition = partitionFlag
					}

					results := cl.KGO.ProduceSync(ctx, rec)
					if err := results.FirstErr(); err != nil {
						return fmt.Errorf("failed to send record: %w", err)
					}
					r := results[0].Record
					fmt.Fprintf(a.OutWriter, "Sent record to partition %v at offset %v.\n", r.Partition, r.Offset)
				}
			}

			select {
			case err := <-errCh:
				return err
			default:
				return nil
			}
		},
	}

	cmd.Flags().StringVarP(&keyFlag, "key", "k", "", "Key for the record. Currently only strings are supported.")
	cmd.Flags().BoolVar(&rawKeyFlag, "raw-key", false, "Treat value of --key as base64 and use its decoded raw value as key")
	cmd.Flags().StringArrayVarP(&headerFlag, "header", "H", []string{}, "Header in format <key>:<value>. May be used multiple times to add more headers.")
	cmd.Flags().IntVarP(&repeatFlag, "repeat", "n", 1, "Repeat records to send.")
	a.AddProtoFlags(cmd)
	cmd.Flags().StringVar(&partitionerFlag, "partitioner", "", "Select partitioner: [jvm|rand|rr]")
	cmd.Flags().StringVar(&timestampFlag, "timestamp", "", "Select timestamp for record")
	cmd.Flags().Int32VarP(&partitionFlag, "partition", "p", -1, "Partition to produce to")
	cmd.Flags().IntVarP(&avroSchemaID, "avro-schema-id", "", -1, "Value schema id for avro messsage encoding")
	cmd.Flags().IntVarP(&avroKeySchemaID, "avro-key-schema-id", "", -1, "Key schema id for avro messsage encoding")
	cmd.Flags().StringVarP(&inputModeFlag, "input-mode", "", "line", "Scanning input mode: [line|full]")
	cmd.Flags().Var(&inputFormatFlag, "input", "Set input format messages: default, hex, json-each-row (json-each-row is compatible with output of kaf consume --output json-each-row)")
	cmd.Flags().IntVarP(&bufferSizeFlag, "line-length-limit", "", 0, "line length limit in line input mode")
	cmd.Flags().BoolVar(&templateFlag, "template", false, "run data through go template engine")

	if err := cmd.RegisterFlagCompletionFunc("input", app.CompleteInputFormat); err != nil {
		panic(fmt.Sprintf("Failed to register flag completion: %v", err))
	}

	return cmd
}

func readLines(reader io.Reader, out chan []byte, errCh chan<- error, bufferSize int) {
	scanner := bufio.NewScanner(reader)
	if bufferSize > 0 {
		scanner.Buffer(make([]byte, bufferSize), bufferSize)
	}
	for scanner.Scan() {
		out <- bytes.Clone(scanner.Bytes())
	}
	close(out)
	if err := scanner.Err(); err != nil {
		errCh <- fmt.Errorf("scanning input failed: %w", err)
	}
}

func readFull(reader io.Reader, out chan []byte, errCh chan<- error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		close(out)
		errCh <- fmt.Errorf("unable to read data: %w", err)
		return
	}
	out <- data
	close(out)
}
