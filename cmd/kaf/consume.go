package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/Shopify/sarama"
	"github.com/birdayz/kaf/pkg/avro"
	"github.com/birdayz/kaf/pkg/proto"
	"github.com/golang/protobuf/jsonpb"
	prettyjson "github.com/hokaccha/go-prettyjson"
	"github.com/spf13/cobra"
)

var (
	offsetFlag      string
	groupFlag       string
	groupCommitFlag bool
	raw             bool
	follow          bool
	schemaCache     *avro.SchemaCache
	keyfmt          *prettyjson.Formatter

	protoType    string
	keyProtoType string

	flagPartitions []int32

	reg *proto.DescriptorRegistry
)

func init() {
	rootCmd.AddCommand(consumeCmd)
	consumeCmd.Flags().StringVar(&offsetFlag, "offset", "oldest", "Offset to start consuming. Possible values: oldest, newest.")
	consumeCmd.Flags().BoolVar(&raw, "raw", false, "Print raw output of messages, without key or prettified JSON")
	consumeCmd.Flags().BoolVarP(&follow, "follow", "f", false, "Shorthand to start consuming with offset HEAD-1 on each partition. Overrides --offset flag")
	consumeCmd.Flags().StringSliceVar(&protoFiles, "proto-include", []string{}, "Path to proto files")
	consumeCmd.Flags().StringSliceVar(&protoExclude, "proto-exclude", []string{}, "Proto exclusions (path prefixes)")
	consumeCmd.Flags().StringVar(&protoType, "proto-type", "", "Fully qualified name of the proto message type. Example: com.test.SampleMessage")
	consumeCmd.Flags().StringVar(&keyProtoType, "key-proto-type", "", "Fully qualified name of the proto key type. Example: com.test.SampleMessage")
	consumeCmd.Flags().Int32SliceVarP(&flagPartitions, "partitions", "p", []int32{}, "Partitions to consume from")
	consumeCmd.Flags().StringVarP(&groupFlag, "group", "g", "", "Consumer Group to use for consume")
	consumeCmd.Flags().BoolVar(&groupCommitFlag, "commit", false, "Commit Group offset after receiving messages. Works only if consuming as Consumer Group")

	keyfmt = prettyjson.NewFormatter()
	keyfmt.Newline = " " // Replace newline with space to avoid condensed output.
	keyfmt.Indent = 0
}

func getAvailableOffsetsRetry(
	ldr *sarama.Broker, req *sarama.OffsetRequest, d time.Duration,
) (*sarama.OffsetResponse, error) {
	var (
		err     error
		offsets *sarama.OffsetResponse
	)

	for {
		select {
		case <-time.After(d):
			return nil, err
		default:
			offsets, err = ldr.GetAvailableOffsets(req)
			if err == nil {
				return offsets, err
			}
		}
	}
}

const (
	offsetsRetry = 500 * time.Millisecond
)

var consumeCmd = &cobra.Command{
	Use:               "consume TOPIC",
	Short:             "Consume messages",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: validTopicArgs,
	PreRun:            setupProtoDescriptorRegistry,
	Run: func(cmd *cobra.Command, args []string) {
		var offset int64
		switch offsetFlag {
		case "oldest":
			offset = sarama.OffsetOldest
		case "newest":
			offset = sarama.OffsetNewest
		default:
			// TODO: normally we would parse this to int64 but it's
			// difficult as we can have multiple partitions. need to
			// find a way to give offsets from CLI with a good
			// syntax.
			offset = sarama.OffsetNewest
		}
		cfg := getConfig()
		cfg.Consumer.Offsets.Initial = offset
		topic := args[0]
		client := getClientFromConfig(cfg)

		if groupFlag != "" {
			withConsumerGroup(cmd.Context(), client, topic, groupFlag)
		} else {
			withoutConsumerGroup(cmd.Context(), client, topic, offset)
		}

	},
}

type g struct{}

func (g *g) Setup(s sarama.ConsumerGroupSession) error {
	return nil
}

func (g *g) Cleanup(s sarama.ConsumerGroupSession) error {
	return nil
}

func (g *g) ConsumeClaim(s sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	mu := sync.Mutex{} // Synchronizes stderr and stdout.
	for msg := range claim.Messages() {
		handleMessage(msg, &mu)
		if groupCommitFlag {
			s.MarkMessage(msg, "")
		}
	}
	return nil
}

func withConsumerGroup(ctx context.Context, client sarama.Client, topic, group string) {
	cg, err := sarama.NewConsumerGroupFromClient(group, client)
	if err != nil {
		errorExit("Failed to create consumer group: %v", err)
	}

	err = cg.Consume(ctx, []string{topic}, &g{})
	if err != nil {
		errorExit("Error on consume: %v", err)
	}
}

func withoutConsumerGroup(ctx context.Context, client sarama.Client, topic string, offset int64) {
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		errorExit("Unable to create consumer from client: %v\n", err)
	}

	var partitions []int32
	if len(flagPartitions) == 0 {
		partitions, err = consumer.Partitions(topic)
		if err != nil {
			errorExit("Unable to get partitions: %v\n", err)
		}
	} else {
		partitions = flagPartitions
	}

	schemaCache = getSchemaCache()

	wg := sync.WaitGroup{}
	mu := sync.Mutex{} // Synchronizes stderr and stdout.
	for _, partition := range partitions {

		wg.Add(1)

		go func(partition int32, offset int64) {
			req := &sarama.OffsetRequest{
				Version: int16(1),
			}
			req.AddBlock(topic, partition, int64(-1), int32(0))
			ldr, err := client.Leader(topic, partition)
			if err != nil {
				errorExit("Unable to get leader: %v\n", err)
			}

			offsets, err := getAvailableOffsetsRetry(ldr, req, offsetsRetry)
			if err != nil {
				errorExit("Unable to get available offsets: %v\n", err)
			}
			followOffset := offsets.GetBlock(topic, partition).Offset - 1

			if follow && followOffset > 0 {
				offset = followOffset
				fmt.Fprintf(errWriter, "Starting on partition %v with offset %v\n", partition, offset)
			}

			pc, err := consumer.ConsumePartition(topic, partition, offset)
			if err != nil {
				errorExit("Unable to consume partition: %v %v %v %v\n", topic, partition, offset, err)
			}

			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-pc.Messages():
					handleMessage(msg, &mu)
				}
			}
		}(partition, offset)
	}
	wg.Wait()
}

func handleMessage(msg *sarama.ConsumerMessage, mu *sync.Mutex) {
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

	if !raw {
		if isJSON(dataToDisplay) {
			dataToDisplay = formatValue(dataToDisplay)
		}

		if isJSON(keyToDisplay) {
			keyToDisplay = formatKey(keyToDisplay)
		}

		w := tabwriter.NewWriter(&stderr, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)

		if len(msg.Headers) > 0 {
			fmt.Fprintf(w, "Headers:\n")
		}

		for _, hdr := range msg.Headers {
			var hdrValue string
			// Try to detect azure eventhub-specific encoding
			if len(hdr.Value) > 0 {
				switch hdr.Value[0] {
				case 161:
					hdrValue = string(hdr.Value[2 : 2+hdr.Value[1]])
				case 131:
					hdrValue = strconv.FormatUint(binary.BigEndian.Uint64(hdr.Value[1:9]), 10)
				default:
					hdrValue = string(hdr.Value)
				}
			}

			fmt.Fprintf(w, "\tKey: %v\tValue: %v\n", string(hdr.Key), hdrValue)

		}

		if msg.Key != nil && len(msg.Key) > 0 {
			fmt.Fprintf(w, "Key:\t%v\n", string(keyToDisplay))
		}
		fmt.Fprintf(w, "Partition:\t%v\nOffset:\t%v\nTimestamp:\t%v\n", msg.Partition, msg.Offset, msg.Timestamp)
		w.Flush()
	}

	mu.Lock()
	stderr.WriteTo(errWriter)
	_, _ = colorableOut.Write(dataToDisplay)
	fmt.Fprintln(outWriter)
	mu.Unlock()

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

func isJSON(data []byte) bool {
	var i interface{}
	if err := json.Unmarshal(data, &i); err == nil {
		return true
	}
	return false
}
