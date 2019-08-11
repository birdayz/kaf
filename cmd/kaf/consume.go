package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/Shopify/sarama"
	"github.com/birdayz/kaf/avro"
	"github.com/birdayz/kaf/pkg/proto"
	"github.com/golang/protobuf/jsonpb"
	"github.com/hokaccha/go-prettyjson"
	"github.com/mattn/go-colorable"
	"github.com/spf13/cobra"
)

var (
	partitionsWhitelistFlag []int
	offsetFlags             []string
	countFlags              []int
	endFlag                 bool
	raw                     bool
	followFlag              bool
	tailFlag                int
	schemaCache             *avro.SchemaCache
	keyfmt                  *prettyjson.Formatter

	protoType string

	reg *proto.DescriptorRegistry
)

func init() {
	rootCmd.AddCommand(consumeCmd)
	consumeCmd.Flags().StringSliceVarP(&offsetFlags, "offset", "o", []string{},
		`Offset to start consuming. Pass a single value to apply to all partitions; pass a comma-delimited list to apply to specific partitions.
Possible values: oldest, newest, -X, +X, X. (X is a number)
oldest: Start reading at the oldest offset. This is the default.
newest: Start reading at the newest offset.
X: Start reading at offset X.
-X: Start reading X before newest offset.
+X: Start reading X after oldest offset.`)
	consumeCmd.Flags().IntSliceVarP(&countFlags, "count", "c", []int{}, "Number of messages to consume before exiting. Specify a single value for global limit, a comma-delimited list to limit specific partitions.")
	consumeCmd.Flags().IntSliceVarP(&partitionsWhitelistFlag, "partitions", "p", []int{}, "Partitions to read from as a comma-delimited list; if unset, all partitions will be read.")
	consumeCmd.Flags().BoolVar(&raw, "raw", false, "Print raw output of messages, without key or prettified JSON.")
	consumeCmd.Flags().BoolVarP(&followFlag, "follow", "f", false, "Shorthand to start consuming with offset HEAD-1 on each partition. Overrides --offset flag.")
	consumeCmd.Flags().IntVarP(&tailFlag, "tail", "t", 0, "Reads the last X messages by distributing X over all partitions. Overrides --offset flag.")
	consumeCmd.Flags().BoolVarP(&endFlag, "end", "e", false, "Stop reading after reaching the end of the partition.")
	consumeCmd.Flags().StringSliceVar(&protoFiles, "proto-include", []string{}, "Path to proto files.")
	consumeCmd.Flags().StringSliceVar(&protoExclude, "proto-exclude", []string{}, "Proto exclusions (path prefixes).")
	consumeCmd.Flags().StringVar(&protoType, "proto-type", "", "Fully qualified name of the proto message type. Example: com.test.SampleMessage.")

	keyfmt = prettyjson.NewFormatter()
	keyfmt.Newline = " " // Replace newline with space to avoid condensed output.
	keyfmt.Indent = 0

}

func resolveOffset(
	client sarama.Client, topic string, partition int, flag string,
) (int64, error) {

	switch flag {
	case "oldest", "":
		return sarama.OffsetOldest, nil
	case "newest":
		return sarama.OffsetNewest, nil
	default:

		var err error
		var offset, min, max int64

		isSkip := flag[0] == '+'
		isTail := flag[0] == '-'
		numStr := flag
		if isSkip || isTail {
			numStr = flag[1:]
		}
		offset, err = strconv.ParseInt(numStr, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid offset flag %q: %s", flag, err)
		}
		min, err = client.GetOffset(topic, int32(partition), sarama.OffsetOldest)
		if err != nil {
			return 0, err
		}
		max, err = client.GetOffset(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			return 0, err
		}

		if isSkip {
			offset = min + offset
		}
		if isTail {
			offset = max - offset
		}
		if offset > max {
			fmt.Fprintf(os.Stderr, "Applying offset flag %q to partition %d gives %d, which is after the max offset of %d. Using the max offset instead.\n", flag, partition, offset, max)
			offset = max
		}
		if offset < min {
			fmt.Fprintf(os.Stderr, "Applying offset flag %q to partition %d gives %d, which is before the min offset of %d. Using the min offset instead.\n", flag, partition, offset, min)
			offset = min
		}

		return offset, nil
	}
}

const (
	offsetsRetry       = 500 * time.Millisecond
	configProtobufType = "protobuf.type"
)

var consumeCmd = &cobra.Command{
	Use:    "consume",
	Short:  "Consume messages",
	Args:   cobra.ExactArgs(1),
	PreRun: setupProtoDescriptorRegistry,
	Run: func(cmd *cobra.Command, args []string) {
		topic := args[0]
		// prevent confusing "default" string in flag help
		if len(offsetFlags) == 0 {
			offsetFlags = []string{"oldest"}
		}

		if followFlag {
			offsetFlags = []string{"-1"}
		}

		client := getClient()

		consumer, err := sarama.NewConsumerFromClient(client)
		if err != nil {
			errorExit("Unable to create consumer from client: %v\n", err)
		}

		foundPartitions, err := consumer.Partitions(topic)
		if err != nil {
			errorExit("Unable to get partitions: %v\n", err)
		}

		var partitions []int

		if len(partitionsWhitelistFlag) > 0 {
			// filter the partitions but keep the order the user specified
			foundPartitionMap := make(map[int32]bool)
			for _, partition := range foundPartitions {
				foundPartitionMap[partition] = true
			}
			for _, partition := range partitionsWhitelistFlag {
				if !foundPartitionMap[int32(partition)] {
					errorExit("Requested partition %d was not found.", partition)
				}
				partitions = append(partitions, partition)
			}
		} else {
			for _, partition := range foundPartitions {
				partitions = append(partitions, int(partition))
			}
			// make sure partitions are sorted in a reasonable order if the user provided tails
			sort.Ints(partitions)
		}

		if tailFlag > 0 {
			offsetFlags = []string{}
			commonOffsetFlag := int(tailFlag / len(partitions))
			if commonOffsetFlag == 0 {
				i := 0
				for ; i < tailFlag; i++ {
					offsetFlags = append(offsetFlags, "-1")
				}
				for ; i < len(partitions); i++ {
					offsetFlags = append(offsetFlags, "newest")
				}
			} else {
				fmt.Fprintf(os.Stderr, "Setting each of %d partitions to start at HEAD-%d because of --tail %d.", len(partitions), commonOffsetFlag, tailFlag)
				offsetFlags = []string{fmt.Sprintf("-%d", commonOffsetFlag)}
			}
		}

		// offsetFlags will always contain at least one value
		// build a map containing the flags for each partition
		offsetFlagsMap := make(map[int]string, len(partitions))
		switch len(offsetFlags) {
		case 1:
			commonOffsetFlag := offsetFlags[0]
			for _, partition := range partitions {
				offsetFlagsMap[partition] = commonOffsetFlag
			}
		case len(partitions):
			for i, partition := range partitions {
				offsetFlagsMap[partition] = offsetFlags[i]
			}
		default:
			errorExit("%d offsets specified but found %d partitions.\n", len(offsetFlags), len(partitions))
		}

		countFlagsMap := make(map[int]int, len(partitions))
		var globalLimit int64 = math.MaxInt64
		hasGlobalLimit := false

		switch len(countFlags) {
		case 0:
			// no limits
		case 1:
			globalLimit = int64(countFlags[0])
			hasGlobalLimit = true
		case len(partitions):
			for i, partition := range partitions {
				countFlagsMap[partition] = countFlags[i]
			}
		default:
			errorExit("%d counts specified but found %d partitions.", len(countFlags), len(partitions))
		}

		// counter used for writing progress to stderr (needed because of buffered tabwriter)
		var globalProgress int64 = 0
		// counter used to track how many messages have been written to std out
		var globalWrittenCount int64 = 0

		wg := sync.WaitGroup{}
		mu := sync.Mutex{} // Synchronizes stderr and stdout.
		for _, partition := range partitions {

			wg.Add(1)

			go func(partition int) {

				offsetFlag := offsetFlagsMap[int(partition)]
				partitionLimit, hasPartitionLimit := countFlagsMap[int(partition)]
				offset, err := resolveOffset(client, topic, partition, offsetFlag)
				if err != nil {
					errorExit("Unable to get offsets for partition %d: %v.", partition, err)
				}

				formattedCount := "∞"
				if hasGlobalLimit {
					formattedCount = fmt.Sprintf("at most %d", globalLimit)
				}
				if hasPartitionLimit {
					formattedCount = fmt.Sprintf("%d", partitionLimit)
				}


				var  formattedOffset string
				switch offset {
				case sarama.OffsetNewest:
					formattedOffset = "newest"
				case sarama.OffsetOldest:
					formattedOffset = "oldest"
				default:
					formattedOffset = fmt.Sprintf("%v", offset)
				}

				fmt.Fprintf(os.Stderr, "Starting to read %s messages on partition %v at offset %v\n", formattedCount, partition, formattedOffset)

				pc, err := consumer.ConsumePartition(topic, int32(partition), offset)
				if err != nil {
					if strings.Contains(err.Error(), "outside the range of offsets maintained") {
						fmt.Fprintf(os.Stderr, "Unable to consume partition %d starting at offset %d; will use newest offset; error was: %v\n", partition, offset, err)
						pc, err = consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
					}
					if err != nil {
						errorExit("Unable to consume partition %d starting at offset %d: %v\n", partition, offset, err)
					}
				}

				partitionCount := 0

				for msg := range pc.Messages() {

					var stderr bytes.Buffer

					// TODO make this nicer
					var dataToDisplay []byte
					if protoType != "" {
						dataToDisplay, err = protoDecode(reg, msg.Value, protoType)
						if err != nil {
							fmt.Fprintf(&stderr, "failed to decode proto. falling back to binary output. Error: %v", err)
						}
					} else {
						dataToDisplay, err = avroDecode(msg.Value)
						if err != nil {
							fmt.Fprintf(&stderr, "could not decode Avro data: %v\n", err)
						}
					}

					if !raw {
						formatted, err := prettyjson.Format(dataToDisplay)
						if err == nil {
							dataToDisplay = formatted
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

							key, err := avroDecode(msg.Key)
							if err != nil {
								fmt.Fprintf(&stderr, "could not decode Avro data: %v\n", err)
							}
							fmt.Fprintf(w, "Key:\t%v\n", formatKey(key))
						}

						fmt.Fprintf(w, "Partition:\t%v\nOffset:\t%v\nTimestamp:\t%v\n", msg.Partition, msg.Offset, msg.Timestamp)
						partitionCount++
						if hasPartitionLimit {
							fmt.Fprintf(w, "Progress:\t%d/%d\n", partitionCount, partitionLimit)
						}
						if hasGlobalLimit {
							count := atomic.AddInt64(&globalProgress, 1)
							fmt.Fprintf(w, "Progress: \t%d/%d\n", count, globalLimit)
						}
						w.Flush()
					}

					partitionDone := func() bool {
						mu.Lock()
						defer mu.Unlock()
						stderr.WriteTo(os.Stderr)
						colorable.NewColorableStdout().Write(dataToDisplay)
						fmt.Print("\n")
						if hasGlobalLimit {
							// normal increment because we're under lock
							globalWrittenCount++
							if globalWrittenCount >= globalLimit {
								fmt.Fprintf(os.Stderr, "Reached requested message count of %d.\n", globalLimit)
								os.Exit(0)
							}
						}
						if hasPartitionLimit && partitionCount >= partitionLimit {
							fmt.Fprintf(os.Stderr, "Reached requested message count of %d on partition %d.\n", partitionLimit, partition)
							return true
						}
						if endFlag && msg.Offset+1 == pc.HighWaterMarkOffset() {
							fmt.Fprintf(os.Stderr, "Reached end of partition %d (read %d messages).\n", partition, partitionCount)
							return true
						}
						return false
					}()
					if partitionDone {
						pc.Close()
						break
					}
				}
				wg.Done()
			}(partition)
		}
		wg.Wait()

	},
}

// proto to JSON
func protoDecode(reg *proto.DescriptorRegistry, b []byte, _type string) ([]byte, error) {
	dynamicMessage := reg.MessageForType(_type)
	if dynamicMessage == nil {
		return b, nil
	}

	dynamicMessage.Unmarshal(b)

	var m jsonpb.Marshaler
	var w bytes.Buffer

	err := m.Marshal(&w, dynamicMessage)
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

func formatKey(key []byte) string {
	b, err := keyfmt.Format(key)
	if err != nil {
		return string(key)
	}
	return string(b)
}
