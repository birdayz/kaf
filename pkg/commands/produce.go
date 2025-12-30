package commands

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/template"
	"time"

	"github.com/Masterminds/sprig"
	"github.com/birdayz/kaf/pkg/avro"
	"github.com/birdayz/kaf/pkg/proto"
	pb "github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ProduceOptions contains all options for producing messages
type ProduceOptions struct {
	Topic           string
	Key             string
	RawKey          bool
	Headers         []string
	Repeat          int
	Partitioner     string
	Timestamp       string
	Partition       int32
	BufferSize      int
	InputMode       string
	AvroSchemaID    int
	AvroKeySchemaID int
	Template        bool
	InputFormat     InputFormat

	// Proto options
	ProtoFiles   []string
	ProtoExclude []string
	ProtoType    string
	KeyProtoType string

	// Dependencies
	SchemaCache *avro.SchemaCache
	ProtoReg    *proto.DescriptorRegistry
	InReader    io.Reader
	OutWriter   io.Writer
	ErrWriter   io.Writer
}

// InputFormat represents the input format type
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

// GetProduceCmd returns the produce command
func (c *Commands) GetProduceCmd() *cobra.Command {
	var opts ProduceOptions
	opts.InputFormat = InputFormatDefault

	cmd := &cobra.Command{
		Use:               "produce TOPIC",
		Short:             "Produce record. Reads data from stdin.",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: c.validTopicArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			// Setup proto registry if proto type is specified
			if opts.ProtoType != "" || opts.KeyProtoType != "" {
				r, err := proto.NewDescriptorRegistry(opts.ProtoFiles, opts.ProtoExclude)
				if err != nil {
					return fmt.Errorf("failed to load protobuf files: %w", err)
				}
				opts.ProtoReg = r
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.Topic = args[0]
			opts.SchemaCache = c.SchemaCache
			opts.InReader = c.InReader
			opts.OutWriter = c.OutWriter
			opts.ErrWriter = c.ErrWriter
			return c.ProduceMessages(cmd.Context(), opts)
		},
	}

	cmd.Flags().StringVarP(&opts.Key, "key", "k", "", "Key for the record. Currently only strings are supported.")
	cmd.Flags().BoolVar(&opts.RawKey, "raw-key", false, "Treat value of --key as base64 and use its decoded raw value as key")
	cmd.Flags().StringArrayVarP(&opts.Headers, "header", "H", []string{}, "Header in format <key>:<value>. May be used multiple times to add more headers.")
	cmd.Flags().IntVarP(&opts.Repeat, "repeat", "n", 1, "Repeat records to send.")

	cmd.Flags().StringSliceVar(&opts.ProtoFiles, "proto-include", []string{}, "Path to proto files")
	cmd.Flags().StringSliceVar(&opts.ProtoExclude, "proto-exclude", []string{}, "Proto exclusions (path prefixes)")
	cmd.Flags().StringVar(&opts.ProtoType, "proto-type", "", "Fully qualified name of the proto message type. Example: com.test.SampleMessage")
	cmd.Flags().StringVar(&opts.KeyProtoType, "key-proto-type", "", "Fully qualified name of the proto key type. Example: com.test.SampleMessage")

	cmd.Flags().StringVar(&opts.Partitioner, "partitioner", "", "Select partitioner: [jvm|rand|rr|hash]")
	cmd.Flags().StringVar(&opts.Timestamp, "timestamp", "", "Select timestamp for record")
	cmd.Flags().Int32VarP(&opts.Partition, "partition", "p", -1, "Partition to produce to")

	cmd.Flags().IntVarP(&opts.AvroSchemaID, "avro-schema-id", "", -1, "Value schema id for avro messsage encoding")
	cmd.Flags().IntVarP(&opts.AvroKeySchemaID, "avro-key-schema-id", "", -1, "Key schema id for avro messsage encoding")

	cmd.Flags().StringVarP(&opts.InputMode, "input-mode", "", "line", "Scanning input mode: [line|full]")
	cmd.Flags().Var(&opts.InputFormat, "input", "Set input format messages: default, json-each-row (json-each-row is compatible with output of kaf consume --output json-each-row)")
	cmd.Flags().IntVarP(&opts.BufferSize, "line-length-limit", "", 0, "line length limit in line input mode")

	cmd.Flags().BoolVar(&opts.Template, "template", false, "run data through go template engine")

	cmd.RegisterFlagCompletionFunc("input", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{"default", "json-each-row"}, cobra.ShellCompDirectiveNoFileComp
	})

	return cmd
}

// ProduceMessages produces messages to Kafka
func (c *Commands) ProduceMessages(ctx context.Context, opts ProduceOptions) error {
	kgoOpts := c.getKgoOpts()

	// Configure partitioner
	// If user specified a specific partition, use ManualPartitioner to honor it
	if opts.Partition != -1 {
		kgoOpts = append(kgoOpts, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	} else {
		switch opts.Partitioner {
		case "jvm":
			// Franz-go uses murmur2 hashing by default, which is JVM-compatible
			// No need to set anything - default behavior is already JVM-compatible
		case "rand":
			kgoOpts = append(kgoOpts, kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)))
		case "rr":
			kgoOpts = append(kgoOpts, kgo.RecordPartitioner(kgo.RoundRobinPartitioner()))
		}
	}

	cl, err := kgo.NewClient(kgoOpts...)
	if err != nil {
		return fmt.Errorf("unable to create new kafka client: %w", err)
	}
	defer cl.Close()

	if opts.AvroSchemaID != -1 || opts.AvroKeySchemaID != -1 {
		if opts.SchemaCache == nil {
			return fmt.Errorf("schema cache not available")
		}
	}

	out := make(chan []byte, 1)
	switch opts.InputMode {
	case "full":
		go c.readFull(opts.InReader, out, opts.ErrWriter)
	default:
		go c.readLines(opts.InReader, out, opts.BufferSize, opts.ErrWriter)
	}

	// Prepare key
	key, err := c.prepareKey(opts)
	if err != nil {
		return err
	}

	// Prepare headers
	var headers []kgo.RecordHeader
	for _, h := range opts.Headers {
		v := strings.SplitN(h, ":", 2)
		if len(v) == 2 {
			headers = append(headers, kgo.RecordHeader{
				Key:   v[0],
				Value: []byte(v[1]),
			})
		}
	}

	// Process messages from input
	for data := range out {
		for i := 0; i < opts.Repeat; i++ {
			input := data

			// Apply template if enabled
			if opts.Template {
				vars := map[string]interface{}{}
				vars["i"] = i
				tpl := template.New("kaf").Funcs(sprig.TxtFuncMap())

				tpl, err = tpl.Parse(string(data))
				if err != nil {
					return fmt.Errorf("failed to parse go template: %w", err)
				}

				buf := bytes.NewBuffer(nil)
				if err := tpl.Execute(buf, vars); err != nil {
					return fmt.Errorf("failed to execute go template: %w", err)
				}

				input = buf.Bytes()
			}

			// Marshal the input according to format
			marshaledInput, err := c.marshalInput(input, data, opts)
			if err != nil {
				return err
			}

			// Parse timestamp
			var ts time.Time
			t, err := time.Parse(time.RFC3339, opts.Timestamp)
			if err != nil {
				ts = time.Now()
			} else {
				ts = t
			}

			// Create record
			rec := &kgo.Record{
				Topic:     opts.Topic,
				Timestamp: ts,
			}

			// Handle json-each-row format
			if opts.InputFormat == InputFormatJSONEachRow {
				jsonEachRowMsg := JSONEachRowMessage{}
				if err = json.Unmarshal(marshaledInput, &jsonEachRowMsg); err == nil {
					if opts.Key == "" {
						// if key flag not set, use the key from stdin
						key = []byte(jsonEachRowMsg.Key)
					}

					for _, h := range jsonEachRowMsg.Headers {
						rec.Headers = append(rec.Headers, kgo.RecordHeader{
							Key:   h.Key,
							Value: []byte(h.Value),
						})
					}

					rec.Partition = jsonEachRowMsg.Partition
					marshaledInput = []byte(jsonEachRowMsg.Payload)
				}
			}

			rec.Key = key
			rec.Value = marshaledInput

			if len(headers) > 0 {
				// override headers if they were set
				rec.Headers = headers
			}
			if opts.Partition != -1 {
				rec.Partition = opts.Partition
			}

			// Produce the record
			results := cl.ProduceSync(ctx, rec)
			if err := results.FirstErr(); err != nil {
				fmt.Fprintf(opts.OutWriter, "Failed to send record: %v\n", err)
				continue
			}

			// Get the record details from the result
			for _, result := range results {
				fmt.Fprintf(opts.OutWriter, "Sent record to partition %v at offset %v.\n", result.Record.Partition, result.Record.Offset)
			}
		}
	}

	return nil
}

// readLines reads input line by line
func (c *Commands) readLines(reader io.Reader, out chan []byte, bufferSize int, errWriter io.Writer) {
	scanner := bufio.NewScanner(reader)
	if bufferSize > 0 {
		scanner.Buffer(make([]byte, bufferSize), bufferSize)
	}

	for scanner.Scan() {
		out <- bytes.Clone(scanner.Bytes())
	}
	close(out)

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(errWriter, "scanning input failed: %v\n", err)
	}
}

// readFull reads all input at once
func (c *Commands) readFull(reader io.Reader, out chan []byte, errWriter io.Writer) {
	data, err := io.ReadAll(reader)
	if err != nil {
		fmt.Fprintf(errWriter, "Unable to read data\n")
	}
	out <- data
	close(out)
}

// prepareKey prepares the key from options
func (c *Commands) prepareKey(opts ProduceOptions) ([]byte, error) {
	var key []byte

	if opts.RawKey {
		keyBytes, err := base64.RawStdEncoding.DecodeString(opts.Key)
		if err != nil {
			return nil, fmt.Errorf("--raw-key is given, but value of --key is not base64")
		}
		key = keyBytes
	} else {
		key = []byte(opts.Key)
	}

	if opts.KeyProtoType != "" {
		if opts.ProtoReg == nil {
			return nil, fmt.Errorf("proto registry not initialized")
		}
		if dynamicMessage := opts.ProtoReg.MessageForType(opts.KeyProtoType); dynamicMessage != nil {
			err := dynamicMessage.UnmarshalJSON([]byte(opts.Key))
			if err != nil {
				return nil, fmt.Errorf("failed to parse input JSON as proto type %v: %w", opts.ProtoType, err)
			}

			pb, err := pb.Marshal(dynamicMessage)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal proto: %w", err)
			}

			key = pb
		} else {
			return nil, fmt.Errorf("failed to load key proto type")
		}
	} else if opts.AvroKeySchemaID != -1 {
		if opts.SchemaCache == nil {
			return nil, fmt.Errorf("schema cache not available")
		}
		avroKey, err := opts.SchemaCache.EncodeMessage(opts.AvroKeySchemaID, []byte(opts.Key))
		if err != nil {
			return nil, fmt.Errorf("failed to encode avro key: %w", err)
		}
		key = avroKey
	}

	return key, nil
}

// marshalInput marshals the input according to the specified format
func (c *Commands) marshalInput(input []byte, originalData []byte, opts ProduceOptions) ([]byte, error) {
	var marshaledInput []byte

	if opts.ProtoType != "" {
		if opts.ProtoReg == nil {
			return nil, fmt.Errorf("proto registry not initialized")
		}
		if dynamicMessage := opts.ProtoReg.MessageForType(opts.ProtoType); dynamicMessage != nil {
			err := dynamicMessage.UnmarshalJSON(input)
			if err != nil {
				return nil, fmt.Errorf("failed to parse input JSON as proto type %v: %w", opts.ProtoType, err)
			}

			pb, err := pb.Marshal(dynamicMessage)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal proto: %w", err)
			}

			marshaledInput = pb
		} else {
			return nil, fmt.Errorf("failed to load payload proto type")
		}
	} else if opts.AvroSchemaID != -1 {
		if opts.SchemaCache == nil {
			return nil, fmt.Errorf("schema cache not available")
		}
		avro, err := opts.SchemaCache.EncodeMessage(opts.AvroSchemaID, originalData)
		if err != nil {
			return nil, fmt.Errorf("failed to encode avro value: %w", err)
		}
		marshaledInput = avro
	} else {
		marshaledInput = input
	}

	return marshaledInput, nil
}
