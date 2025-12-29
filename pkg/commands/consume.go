package commands

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/birdayz/kaf/pkg/avro"
	"github.com/birdayz/kaf/pkg/proto"
	"github.com/golang/protobuf/jsonpb"
	prettyjson "github.com/hokaccha/go-prettyjson"
	"github.com/spf13/cobra"
	"github.com/vmihailenco/msgpack/v5"
)

var (
	keyfmt = &prettyjson.Formatter{
		Newline: " ", // Replace newline with space to avoid condensed output.
		Indent:  0,
	}
)

type offsets struct {
	newest int64
	oldest int64
}

func getOffsets(client *kgo.Client, topic string, partition int32) (*offsets, error) {
	// For Franz-go, we'll use a simplified approach
	// The actual offset ranges will be handled by Franz-go internally
	return &offsets{
		newest: -1, // Franz-go will handle this
		oldest: 0,  // Franz-go will handle this
	}, nil
}

// ConsumeOptions contains all the parameters for the consume operation
type ConsumeOptions struct {
	Topic           string
	OffsetFlag      string
	GroupFlag       string
	GroupCommitFlag bool
	OutputFormat    OutputFormat
	Raw             bool
	Follow          bool
	Tail            int32
	ProtoType       string
	KeyProtoType    string
	Partitions      []int32
	LimitMessages   int64
	DecodeMsgPack   bool
	ProtoFiles      []string
	ProtoExclude    []string
	SchemaCache     *avro.SchemaCache
	OutWriter       interface{ Write([]byte) (int, error) }
	ErrWriter       interface{ Write([]byte) (int, error) }
}

func (c *Commands) createConsumeCmd() *cobra.Command {
	var (
		offsetFlag        string
		groupFlag         string
		groupCommitFlag   bool
		outputFormat      = OutputFormatDefault
		raw               bool
		follow            bool
		tail              int32
		protoType         string
		keyProtoType      string
		flagPartitions    []int32
		limitMessagesFlag int64
		headerFilterFlag  []string
		protoFiles        []string
		protoExclude      []string
		decodeMsgPack     bool
	)

	cmd := &cobra.Command{
		Use:               "consume TOPIC",
		Short:             "Consume messages",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: c.validTopicArgs,
		PreRun: func(cmd *cobra.Command, args []string) {
			if protoType != "" {
				r, err := proto.NewDescriptorRegistry(protoFiles, protoExclude)
				if err != nil {
					fmt.Fprintf(c.ErrWriter, "Failed to load protobuf files: %v\n", err)
					return
				}
				reg = r
			}
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			opts := ConsumeOptions{
				Topic:           args[0],
				OffsetFlag:      offsetFlag,
				GroupFlag:       groupFlag,
				GroupCommitFlag: groupCommitFlag,
				OutputFormat:    outputFormat,
				Raw:             raw,
				Follow:          follow,
				Tail:            tail,
				ProtoType:       protoType,
				KeyProtoType:    keyProtoType,
				Partitions:      flagPartitions,
				LimitMessages:   limitMessagesFlag,
				DecodeMsgPack:   decodeMsgPack,
				ProtoFiles:      protoFiles,
				ProtoExclude:    protoExclude,
				SchemaCache:     c.SchemaCache,
				OutWriter:       c.OutWriter,
				ErrWriter:       c.ErrWriter,
			}

			// Parse header filters
			headerFilter := make(map[string]string)
			for _, f := range headerFilterFlag {
				parts := strings.SplitN(f, ":", 2)
				if len(parts) != 2 {
					return fmt.Errorf("invalid header filter format: %s. Expected format: key:value", f)
				}
				headerFilter[parts[0]] = parts[1]
			}

			return c.ConsumeMessages(cmd.Context(), opts, headerFilter)
		},
	}

	cmd.Flags().StringVar(&offsetFlag, "offset", "oldest", "Offset to start consuming. Possible values: oldest, newest, or integer.")
	cmd.Flags().BoolVar(&raw, "raw", false, "Print raw output of messages, without key or prettified JSON")
	cmd.Flags().Var(&outputFormat, "output", "Set output format messages: default, raw (without key or prettified JSON), json, json-each-row")
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "Continue to consume messages until program execution is interrupted/terminated")
	cmd.Flags().Int32VarP(&tail, "tail", "n", 0, "Print last n messages per partition")
	cmd.Flags().StringSliceVar(&protoFiles, "proto-include", []string{}, "Path to proto files")
	cmd.Flags().StringSliceVar(&protoExclude, "proto-exclude", []string{}, "Proto exclusions (path prefixes)")
	cmd.Flags().BoolVar(&decodeMsgPack, "decode-msgpack", false, "Enable deserializing msgpack")
	cmd.Flags().StringVar(&protoType, "proto-type", "", "Fully qualified name of the proto message type. Example: com.test.SampleMessage")
	cmd.Flags().StringVar(&keyProtoType, "key-proto-type", "", "Fully qualified name of the proto key type. Example: com.test.SampleMessage")
	cmd.Flags().Int32SliceVarP(&flagPartitions, "partitions", "p", []int32{}, "Partitions to consume from")
	cmd.Flags().Int64VarP(&limitMessagesFlag, "limit-messages", "l", 0, "Limit messages per partition")
	cmd.Flags().StringVarP(&groupFlag, "group", "g", "", "Consumer Group to use for consume")
	cmd.Flags().BoolVar(&groupCommitFlag, "commit", false, "Commit Group offset after receiving messages. Works only if consuming as Consumer Group")
	cmd.Flags().StringSliceVar(&headerFilterFlag, "header", []string{}, "Filter messages by header. Format: key:value. Multiple filters can be specified")

	cmd.RegisterFlagCompletionFunc("output", completeOutputFormat)
	cmd.Flags().MarkDeprecated("raw", "use --output raw instead")

	return cmd
}

var reg *proto.DescriptorRegistry

// ConsumeMessages performs the actual consume operation with the given options
func (c *Commands) ConsumeMessages(ctx context.Context, opts ConsumeOptions, headerFilter map[string]string) error {
	var offset int64

	// Allow deprecated flag to override when outputFormat is not specified, or default.
	outputFormat := opts.OutputFormat
	if outputFormat == OutputFormatDefault && opts.Raw {
		outputFormat = OutputFormatRaw
	}

	switch opts.OffsetFlag {
	case "oldest":
		offset = -2 // kgo uses -2 for earliest
	case "newest":
		offset = -1 // kgo uses -1 for latest
	default:
		o, err := strconv.ParseInt(opts.OffsetFlag, 10, 64)
		if err != nil {
			return fmt.Errorf("could not parse '%s' to int64: %w", opts.OffsetFlag, err)
		}
		offset = o
	}

	// Setup proto descriptor registry if needed
	if opts.ProtoType != "" {
		r, err := proto.NewDescriptorRegistry(opts.ProtoFiles, opts.ProtoExclude)
		if err != nil {
			return fmt.Errorf("failed to load protobuf files: %v", err)
		}
		reg = r
	}

	if opts.GroupFlag != "" {
		kgoOpts := c.getKgoOpts()
		kgoOpts = append(kgoOpts,
			kgo.ConsumerGroup(opts.GroupFlag),
			kgo.ConsumeTopics(opts.Topic))
		client, err := kgo.NewClient(kgoOpts...)
		if err != nil {
			return fmt.Errorf("unable to create kafka client: %w", err)
		}
		defer client.Close()
		return c.consumeWithConsumerGroup(ctx, client, opts, headerFilter, outputFormat)
	} else {
		// Configure offset for the topic
		var kgoOffset kgo.Offset
		if offset == -2 {
			kgoOffset = kgo.NewOffset().AtStart()
		} else if offset == -1 {
			kgoOffset = kgo.NewOffset().AtEnd()
		} else {
			kgoOffset = kgo.NewOffset().At(offset)
		}

		kgoOpts := c.getKgoOpts()
		kgoOpts = append(kgoOpts, kgo.ConsumeResetOffset(kgoOffset))
		if len(opts.Partitions) > 0 {
			// If specific partitions are requested, use them
			partitionOffsets := make(map[string]map[int32]kgo.Offset)
			partitionOffsets[opts.Topic] = make(map[int32]kgo.Offset)
			for _, partition := range opts.Partitions {
				partitionOffsets[opts.Topic][partition] = kgoOffset
			}
			kgoOpts = append(kgoOpts, kgo.ConsumePartitions(partitionOffsets))
		} else {
			// Consume from all partitions
			kgoOpts = append(kgoOpts, kgo.ConsumeTopics(opts.Topic))
		}
		client, err := kgo.NewClient(kgoOpts...)
		if err != nil {
			return fmt.Errorf("unable to create kafka client: %w", err)
		}
		defer client.Close()
		return c.consumeWithoutConsumerGroup(ctx, client, opts, headerFilter, offset, outputFormat)
	}
}

func (c *Commands) consumeWithConsumerGroup(ctx context.Context, client *kgo.Client, opts ConsumeOptions, headerFilter map[string]string, outputFormat OutputFormat) error {
	mu := sync.Mutex{} // Synchronizes stderr and stdout.
	var count int64 = 0

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			fetches := client.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				for _, err := range errs {
					return fmt.Errorf("fetch error: %v", err)
				}
			}

			var shouldReturn bool
			fetches.EachPartition(func(p kgo.FetchTopicPartition) {
				for _, record := range p.Records {
					c.handleMessage(record, &mu, opts, headerFilter, outputFormat)
					count++
					if opts.LimitMessages > 0 && count >= opts.LimitMessages {
						shouldReturn = true
						return
					}
					if opts.GroupCommitFlag {
						client.MarkCommitRecords(record)
					}
				}
			})

			if shouldReturn {
				if opts.GroupCommitFlag {
					client.CommitUncommittedOffsets(ctx)
				}
				return nil
			}

			if opts.GroupCommitFlag {
				client.CommitUncommittedOffsets(ctx)
			}
		}
	}
}

func (c *Commands) consumeWithoutConsumerGroup(ctx context.Context, client *kgo.Client, opts ConsumeOptions, headerFilter map[string]string, offset int64, outputFormat OutputFormat) error {
	mu := sync.Mutex{} // Synchronizes stderr and stdout.
	var count int64 = 0

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			fetches := client.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				for _, err := range errs {
					return fmt.Errorf("fetch error: %v", err)
				}
			}

			var shouldReturn bool
			fetches.EachPartition(func(p kgo.FetchTopicPartition) {
				for _, record := range p.Records {
					c.handleMessage(record, &mu, opts, headerFilter, outputFormat)
					count++
					if opts.LimitMessages > 0 && count >= opts.LimitMessages {
						shouldReturn = true
						return
					}
				}
			})

			if shouldReturn {
				return nil
			}

			if !opts.Follow && len(fetches.Records()) == 0 {
				return nil
			}
		}
	}
}

func (c *Commands) handleMessage(msg *kgo.Record, mu *sync.Mutex, opts ConsumeOptions, headerFilter map[string]string, outputFormat OutputFormat) {
	if !CheckHeaders(msg.Headers, headerFilter) {
		return
	}

	var stderr bytes.Buffer

	var dataToDisplay []byte
	var keyToDisplay []byte
	var err error

	if opts.ProtoType != "" {
		dataToDisplay, err = protoDecode(reg, msg.Value, opts.ProtoType)
		if err != nil {
			fmt.Fprintf(&stderr, "failed to decode proto. falling back to binary outputla. Error: %v\n", err)
		}
	} else {
		dataToDisplay, err = avroDecode(msg.Value, opts.SchemaCache)
		if err != nil {
			fmt.Fprintf(&stderr, "could not decode Avro data: %v\n", err)
		}
	}

	if opts.KeyProtoType != "" {
		keyToDisplay, err = protoDecode(reg, msg.Key, opts.KeyProtoType)
		if err != nil {
			fmt.Fprintf(&stderr, "failed to decode proto key. falling back to binary outputla. Error: %v\n", err)
		}
	} else {
		keyToDisplay, err = avroDecode(msg.Key, opts.SchemaCache)
		if err != nil {
			fmt.Fprintf(&stderr, "could not decode Avro data: %v\n", err)
		}
	}

	if opts.DecodeMsgPack {
		var obj interface{}
		err = msgpack.Unmarshal(msg.Value, &obj)
		if err != nil {
			fmt.Fprintf(&stderr, "could not decode msgpack data: %v\n", err)
		}

		dataToDisplay, err = json.Marshal(obj)
		if err != nil {
			fmt.Fprintf(&stderr, "could not decode msgpack data: %v\n", err)
		}
	}

	dataToDisplay = formatKgoMessage(msg, dataToDisplay, keyToDisplay, &stderr, outputFormat)

	mu.Lock()
	stderr.WriteTo(opts.ErrWriter)
	if colorableOut, ok := opts.OutWriter.(interface{ Write([]byte) (int, error) }); ok {
		colorableOut.Write(dataToDisplay)
	}
	fmt.Fprintln(opts.OutWriter)
	mu.Unlock()
}

// CheckHeaders checks if record headers match the given filter
func CheckHeaders(headers []kgo.RecordHeader, filter map[string]string) bool {
	if len(filter) == 0 {
		return true
	}

	matchCount := 0
	for _, h := range headers {
		hdrStr := parseHeader(h.Value)
		if val, ok := filter[h.Key]; ok && hdrStr == val {
			matchCount++
		}
	}

	return len(headers) > 0 && matchCount >= len(filter)
}

func parseHeader(hdrBytes []byte) (hdrStr string) {
	// Try to detect azure eventhub-specific encoding
	if len(hdrBytes) > 0 {
		switch hdrBytes[0] {
		case 161:
			hdrStr = string(hdrBytes[2 : 2+hdrBytes[1]])
		case 131:
			hdrStr = strconv.FormatUint(binary.BigEndian.Uint64(hdrBytes[1:9]), 10)
		default:
			hdrStr = string(hdrBytes)
		}
	}

	return hdrStr
}

func formatKgoMessage(msg *kgo.Record, rawMessage []byte, keyToDisplay []byte, stderr *bytes.Buffer, outputFormat OutputFormat) []byte {
	switch outputFormat {
	case OutputFormatRaw:
		return rawMessage
	case OutputFormatJSON:
		jsonMessage := make(map[string]interface{})

		jsonMessage["partition"] = msg.Partition
		jsonMessage["offset"] = msg.Offset
		jsonMessage["timestamp"] = msg.Timestamp.UnixMilli()

		if len(msg.Headers) > 0 {
			headers := make(map[string]string)
			for _, hdr := range msg.Headers {
				headers[hdr.Key] = string(hdr.Value)
			}
			jsonMessage["headers"] = headers
		}

		jsonMessage["key"] = formatJSON(keyToDisplay)
		jsonMessage["payload"] = formatJSON(rawMessage)

		jsonToDisplay, err := json.Marshal(jsonMessage)
		if err != nil {
			fmt.Fprintf(stderr, "could not decode JSON data: %v", err)
		}

		return jsonToDisplay
	case OutputFormatJSONEachRow:
		jsonMessage := JSONEachRowMessage{}
		jsonMessage.Topic = msg.Topic
		jsonMessage.Partition = msg.Partition
		jsonMessage.Offset = msg.Offset
		jsonMessage.Timestamp = msg.Timestamp.UnixMilli()
		jsonMessage.Headers = make([]MessageHeader, len(msg.Headers))
		for i, hdr := range msg.Headers {
			hdrStr := parseHeader(hdr.Value)
			jsonMessage.Headers[i] = MessageHeader{
				Key:   hdr.Key,
				Value: hdrStr,
			}
		}
		jsonMessage.Key = string(keyToDisplay)
		jsonMessage.Payload = string(rawMessage)

		jsonToDisplay, err := json.Marshal(jsonMessage)
		if err != nil {
			fmt.Fprintf(stderr, "could not decode JSON data: %v", err)
		}

		return jsonToDisplay
	case OutputFormatDefault:
		fallthrough
	default:
		if isJSON(rawMessage) {
			rawMessage = formatValue(rawMessage)
		}

		if isJSON(keyToDisplay) {
			keyToDisplay = formatKey(keyToDisplay)
		}

		w := tabwriter.NewWriter(stderr, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)

		if len(msg.Headers) > 0 {
			fmt.Fprintf(w, "Headers:\n")
		}

		for _, hdr := range msg.Headers {
			hdrStr := parseHeader(hdr.Value)
			fmt.Fprintf(w, "\tKey: %v\tValue: %v\n", hdr.Key, hdrStr)
		}

		if len(msg.Key) > 0 {
			fmt.Fprintf(w, "Key:\t%v\n", string(keyToDisplay))
		}
		fmt.Fprintf(w, "Partition:\t%v\nOffset:\t%v\nTimestamp:\t%v\n", msg.Partition, msg.Offset, msg.Timestamp)
		w.Flush()

		return rawMessage
	}
}

// proto to JSON
func protoDecode(reg *proto.DescriptorRegistry, b []byte, _type string) ([]byte, error) {
	dynamicMessage := reg.MessageForType(_type)
	if dynamicMessage == nil {
		return b, nil
	}

	err := dynamicMessage.Unmarshal(b)
	if err != nil {
		return nil, err
	}

	var m jsonpb.Marshaler
	var w bytes.Buffer

	err = m.Marshal(&w, dynamicMessage)
	if err != nil {
		return nil, err
	}
	return w.Bytes(), nil

}

func avroDecode(b []byte, schemaCache *avro.SchemaCache) ([]byte, error) {
	if schemaCache != nil {
		return schemaCache.DecodeMessage(b)
	}
	return b, nil
}

func formatKey(key []byte) []byte {
	if b, err := keyfmt.Format(key); err == nil {
		return b
	}
	return key

}

func formatValue(key []byte) []byte {
	if b, err := prettyjson.Format(key); err == nil {
		return b
	}
	return key
}

func formatJSON(data []byte) interface{} {
	var i interface{}
	if err := json.Unmarshal(data, &i); err != nil {
		return string(data)
	}

	return i
}

func isJSON(data []byte) bool {
	var i interface{}
	if err := json.Unmarshal(data, &i); err == nil {
		return true
	}
	return false
}

type OutputFormat string

const (
	OutputFormatDefault     OutputFormat = "default"
	OutputFormatRaw         OutputFormat = "raw"
	OutputFormatJSON        OutputFormat = "json"
	OutputFormatJSONEachRow OutputFormat = "json-each-row"
)

func (e *OutputFormat) String() string {
	return string(*e)
}

func (e *OutputFormat) Set(v string) error {
	switch v {
	case "default", "raw", "json", "json-each-row":
		*e = OutputFormat(v)
		return nil
	default:
		return fmt.Errorf("must be one of: default, raw, json, json-each-row")
	}
}

func (e *OutputFormat) Type() string {
	return "OutputFormat"
}

func completeOutputFormat(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	return []string{"default", "raw", "json", "json-each-row"}, cobra.ShellCompDirectiveNoFileComp
}

type JSONEachRowMessage struct {
	Topic     string          `json:"topic"`
	Partition int32           `json:"partition"`
	Offset    int64           `json:"offset"`
	Timestamp int64           `json:"timestamp"`
	Headers   []MessageHeader `json:"headers"`
	Key       string          `json:"key"`
	Payload   string          `json:"payload"`
}

type MessageHeader struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
