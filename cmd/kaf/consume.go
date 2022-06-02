package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"text/tabwriter"

	"strconv"

	"github.com/Shopify/sarama"
	"github.com/birdayz/kaf/pkg/avro"
	"github.com/birdayz/kaf/pkg/codec"
	prettyjson "github.com/hokaccha/go-prettyjson"
	"github.com/spf13/cobra"
	"github.com/vmihailenco/msgpack/v5"
)

var (
	offsetFlag      string
	groupFlag       string
	groupCommitFlag bool
	raw             bool
	follow          bool
	tail            int32
	schemaCache     *avro.SchemaCache
	keyfmt          *prettyjson.Formatter

	protoType    string
	keyProtoType string

	flagPartitions []int32

	limitMessagesFlag int64

	reg *codec.DescriptorRegistry

	protoDecoder    *codec.ProtoCodec
	protoKeyDecoder *codec.ProtoCodec
	avroDecoder     *codec.AvroCodec
)

func init() {
	rootCmd.AddCommand(consumeCmd)
	consumeCmd.Flags().StringVar(&offsetFlag, "offset", "oldest", "Offset to start consuming. Possible values: oldest, newest, or integer.")
	consumeCmd.Flags().BoolVar(&raw, "raw", false, "Print raw output of messages, without key or prettified JSON")
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

	keyfmt = prettyjson.NewFormatter()
	keyfmt.Newline = " " // Replace newline with space to avoid condensed output.
	keyfmt.Indent = 0
}

type offsets struct {
	newest int64
	oldest int64
}

func getOffsets(client sarama.Client, topic string, partition int32) (*offsets, error) {
	newest, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		return nil, err
	}

	oldest, err := client.GetOffset(topic, partition, sarama.OffsetOldest)
	if err != nil {
		return nil, err
	}

	return &offsets{
		newest: newest,
		oldest: oldest,
	}, nil
}

var consumeCmd = &cobra.Command{
	Use:               "consume TOPIC",
	Short:             "Consume messages",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: validTopicArgs,
	PreRun:            setupProtoDescriptorRegistry,
	Run: func(cmd *cobra.Command, args []string) {
		var offset int64
		cfg := getConfig()
		topic := args[0]
		client := getClientFromConfig(cfg)

		schemaCache = getSchemaCache()
		avroDecoder = codec.NewAvroCodec(-1, false, schemaCache)
		protoDecoder = codec.NewProtoCodec(protoType, reg)
		protoKeyDecoder = codec.NewProtoCodec(keyProtoType, reg)

		switch offsetFlag {
		case "oldest":
			offset = sarama.OffsetOldest
			cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
		case "newest":
			offset = sarama.OffsetNewest
			cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
		default:
			o, err := strconv.ParseInt(offsetFlag, 10, 64)
			if err != nil {
				errorExit("Could not parse '%s' to int64: %w", offsetFlag, err)
			}
			offset = o
		}

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

	wg := sync.WaitGroup{}
	mu := sync.Mutex{} // Synchronizes stderr and stdout.
	for _, partition := range partitions {
		wg.Add(1)

		go func(partition int32, offset int64) {
			defer wg.Done()

			offsets, err := getOffsets(client, topic, partition)
			if err != nil {
				errorExit("Failed to get %s offsets for partition %d: %w", topic, partition, err)
			}

			if tail != 0 {
				offset = offsets.newest - int64(tail)
				if offset < offsets.oldest {
					offset = offsets.oldest
				}
			}

			// Already at end of partition, return early
			if !follow && offsets.newest == offsets.oldest {
				return
			}

			pc, err := consumer.ConsumePartition(topic, partition, offset)
			if err != nil {
				errorExit("Unable to consume partition: %v %v %v %v\n", topic, partition, offset, err)
			}

			var count int64 = 0
			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-pc.Messages():
					handleMessage(msg, &mu)
					count++
					if limitMessagesFlag > 0 && count >= limitMessagesFlag {
						return
					}
					if !follow && msg.Offset+1 >= pc.HighWaterMarkOffset() {
						return
					}
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
		dataToDisplay, err = protoDecoder.Decode(msg.Value)
		if err != nil {
			fmt.Fprintf(&stderr, "failed to decode proto. falling back to binary output. Error: %v\n", err)
		}
	} else {
		dataToDisplay, err = avroDecoder.Decode(msg.Value)
		if err != nil {
			fmt.Fprintf(&stderr, "could not decode Avro data: %v\n", err)
		}
	}

	if keyProtoType != "" {
		keyToDisplay, err = protoKeyDecoder.Decode(msg.Key)
		if err != nil {
			fmt.Fprintf(&stderr, "failed to decode proto key. falling back to binary output. Error: %v\n", err)
		}
	} else {
		keyToDisplay, err = avroDecoder.Decode(msg.Key)
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
