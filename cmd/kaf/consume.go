package main

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
	offsetFlag      string
	groupFlag       string
	groupCommitFlag bool
	outputFormat    = OutputFormatDefault
	// Deprecated: Use outputFormat instead.
	raw         bool
	follow      bool
	tail        int32
	schemaCache *avro.SchemaCache
	keyfmt      *prettyjson.Formatter

	protoType    string
	keyProtoType string

	flagPartitions []int32

	limitMessagesFlag int64

	reg *proto.DescriptorRegistry

	headerFilterFlag []string
	headerFilter     = make(map[string]string)
)

func init() {
	rootCmd.AddCommand(consumeCmd)
	consumeCmd.Flags().StringVar(&offsetFlag, "offset", "oldest", "Offset to start consuming. Possible values: oldest, newest, or integer.")
	consumeCmd.Flags().BoolVar(&raw, "raw", false, "Print raw output of messages, without key or prettified JSON")
	consumeCmd.Flags().Var(&outputFormat, "output", "Set output format messages: default, raw (without key or prettified JSON), json, json-each-row")
	consumeCmd.Flags().BoolVarP(&follow, "follow", "f", false, "Continue to consume messages until program execution is interrupted/terminated")
	consumeCmd.Flags().Int32VarP(&tail, "tail", "n", 0, "Print last n messages per partition")
	consumeCmd.Flags().StringSliceVar(&protoFiles, "proto-include", []string{}, "Path to proto files")
	consumeCmd.Flags().StringSliceVar(&protoExclude, "proto-exclude", []string{}, "Proto exclusions (path prefixes)")
	consumeCmd.Flags().BoolVar(&decodeMsgPack, "decode-msgpack", false, "Enable deserializing msgpack")
	consumeCmd.Flags().StringVar(&protoType, "proto-type", "", "Fully qualified name of the proto message type. Example: com.test.SampleMessage")
	consumeCmd.Flags().StringVar(&keyProtoType, "key-proto-type", "", "Fully qualified name of the proto key type. Example: com.test.SampleMessage")
	consumeCmd.Flags().Int32SliceVarP(&flagPartitions, "partitions", "p", []int32{}, "Partitions to consume from")
	consumeCmd.Flags().Int64VarP(&limitMessagesFlag, "limit-messages", "l", 0, "Limit messages per partition")
	consumeCmd.Flags().StringVarP(&groupFlag, "group", "g", "", "Consumer Group to use for consume")
	consumeCmd.Flags().BoolVar(&groupCommitFlag, "commit", false, "Commit Group offset after receiving messages. Works only if consuming as Consumer Group")
	consumeCmd.Flags().StringSliceVar(&headerFilterFlag, "header", []string{}, "Filter messages by header. Format: key:value. Multiple filters can be specified")

	if err := consumeCmd.RegisterFlagCompletionFunc("output", completeOutputFormat); err != nil {
		errorExit("Failed to register flag completion: %v", err)
	}

	if err := consumeCmd.Flags().MarkDeprecated("raw", "use --output raw instead"); err != nil {
		errorExit("Failed to mark flag as deprecated: %v", err)
	}

	keyfmt = prettyjson.NewFormatter()
	keyfmt.Newline = " " // Replace newline with space to avoid condensed output.
	keyfmt.Indent = 0
}

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
}

// ConsumeMessages performs the actual consume operation with the given options
func ConsumeMessages(ctx context.Context, opts ConsumeOptions) error {
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
		kgoOpts := append(getKgoOpts(), 
			kgo.ConsumerGroup(opts.GroupFlag),
			kgo.ConsumeTopics(opts.Topic))
		client := getClientFromOpts(kgoOpts)
		defer client.Close()
		return consumeWithConsumerGroup(ctx, client, opts.Topic, opts.GroupFlag, opts.LimitMessages, opts.GroupCommitFlag, outputFormat)
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
		
		kgoOpts := append(getKgoOpts(), kgo.ConsumeResetOffset(kgoOffset))
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
		client := getClientFromOpts(kgoOpts)
		defer client.Close()
		return consumeWithoutConsumerGroup(ctx, client, opts.Topic, offset, opts.LimitMessages, opts.Follow, outputFormat)
	}
}

var consumeCmd = &cobra.Command{
	Use:               "consume TOPIC",
	Short:             "Consume messages",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: validTopicArgs,
	PreRun:            setupProtoDescriptorRegistry,
	Run: func(cmd *cobra.Command, args []string) {
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
		}

		// Parse header filters
		for _, f := range headerFilterFlag {
			parts := strings.SplitN(f, ":", 2)
			if len(parts) != 2 {
				errorExit("Invalid header filter format: %s. Expected format: key:value", f)
			}
			headerFilter[parts[0]] = parts[1]
		}

		err := ConsumeMessages(cmd.Context(), opts)
		if err != nil {
			errorExit("Consume failed: %v", err)
		}
	},
}


func consumeWithConsumerGroup(ctx context.Context, client *kgo.Client, topic, group string, limitMessages int64, commit bool, outputFormat OutputFormat) error {
	schemaCache = getSchemaCache()
	
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
					handleMessage(record, &mu, outputFormat)
					count++
					if limitMessages > 0 && count >= limitMessages {
						shouldReturn = true
						return
					}
					if commit {
						client.MarkCommitRecords(record)
					}
				}
			})
			
			if shouldReturn {
				if commit {
					client.CommitUncommittedOffsets(ctx)
				}
				return nil
			}
			
			if commit {
				client.CommitUncommittedOffsets(ctx)
			}
		}
	}
}


func consumeWithoutConsumerGroup(ctx context.Context, client *kgo.Client, topic string, offset int64, limitMessages int64, follow bool, outputFormat OutputFormat) error {
	schemaCache = getSchemaCache()
	
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
					handleMessage(record, &mu, outputFormat)
					count++
					if limitMessages > 0 && count >= limitMessages {
						shouldReturn = true
						return
					}
				}
			})
			
			if shouldReturn {
				return nil
			}
			
			if !follow && len(fetches.Records()) == 0 {
				return nil
			}
		}
	}
}

func handleMessage(msg *kgo.Record, mu *sync.Mutex, outputFormat OutputFormat) {
	if !checkHeaders(msg.Headers, headerFilter) {
		return
	}

	var stderr bytes.Buffer

	var dataToDisplay []byte
	var keyToDisplay []byte
	var err error

	if protoType != "" {
		dataToDisplay, err = protoDecode(reg, msg.Value, protoType)
		if err != nil {
			fmt.Fprintf(&stderr, "failed to decode proto. falling back to binary outputla. Error: %v\n", err)
		}
	} else {
		dataToDisplay, err = avroDecode(msg.Value)
		if err != nil {
			fmt.Fprintf(&stderr, "could not decode Avro data: %v\n", err)
		}
	}

	if keyProtoType != "" {
		keyToDisplay, err = protoDecode(reg, msg.Key, keyProtoType)
		if err != nil {
			fmt.Fprintf(&stderr, "failed to decode proto key. falling back to binary outputla. Error: %v\n", err)
		}
	} else {
		keyToDisplay, err = avroDecode(msg.Key)
		if err != nil {
			fmt.Fprintf(&stderr, "could not decode Avro data: %v\n", err)
		}
	}

	if decodeMsgPack {
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
	stderr.WriteTo(errWriter)
	_, _ = colorableOut.Write(dataToDisplay)
	fmt.Fprintln(outWriter)
	mu.Unlock()
}

func checkHeaders(headers []kgo.RecordHeader, filter map[string]string) bool {
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
		jsonMessage["timestamp"] = msg.Timestamp

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
		jsonMessage.Timestamp = msg.Timestamp
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

func avroDecode(b []byte) ([]byte, error) {
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
