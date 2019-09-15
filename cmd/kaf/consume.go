package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/birdayz/kaf/avro"
	"github.com/birdayz/kaf/pkg/proto"
	"github.com/golang/protobuf/jsonpb"
	"github.com/hokaccha/go-prettyjson"
	"github.com/mattn/go-colorable"
	"github.com/spf13/cobra"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
)

var (
	partitionsWhitelistFlag []int
	offsetFlags             []string
	countFlag               int
	countsFlags             []int
	endFlag                 bool
	raw                     bool
	followFlag              bool
	tailFlag                int
	skipFlag                int
	tailsFlags              []int
	skipsFlags              []int
	schemaCache             *avro.SchemaCache
	keyfmt                  *prettyjson.Formatter

	protoType string

	reg *proto.DescriptorRegistry
)

func init() {
	rootCmd.AddCommand(consumeCmd)
	consumeCmd.Flags().StringSliceVarP(&offsetFlags, "offset", "o", []string{},
		`Offset to start consuming. Takes a single or partitioned value.
Possible values: oldest, newest, -X, +X, X. (X is a number)
oldest: Start reading at the oldest offset. This is the default if skip/take are undefined.
newest: Start reading at the newest offset.
X: Start reading at offset X.`)
	consumeCmd.Flags().IntVarP(&tailFlag, "tail", "t", 0, "Reads the last X messages across all partitions. Overrides --offset flag.")
	consumeCmd.Flags().IntSliceVarP(&tailsFlags, "tails", "T", []int{}, "Reads the last X messages on each partition. Takes a single or partitioned value.")
	consumeCmd.Flags().IntVarP(&skipFlag, "skip", "s", 0, "Skips the first X messages across all partitions. Overrides --offset flag.")
	consumeCmd.Flags().IntSliceVarP(&skipsFlags, "skips", "S", []int{}, "Skips the first X messages on each partition. Takes a single or partitioned value. ")

	consumeCmd.Flags().IntVarP(&countFlag, "count", "n", 0, "Number of messages to consume before exiting.")
	consumeCmd.Flags().IntSliceVarP(&countsFlags, "counts", "N", []int{}, "Number of messages to consume per partition before stopping. Takes a single or partitioned value.")

	consumeCmd.Flags().IntSliceVarP(&partitionsWhitelistFlag, "partitions", "p", []int{}, "Partitions to read from as a comma-delimited list; if unset, all partitions will be read.")
	consumeCmd.Flags().BoolVar(&raw, "raw", false, "Print raw output of messages, without key or prettified JSON.")
	consumeCmd.Flags().BoolVarP(&followFlag, "follow", "f", false, "Shorthand to start consuming with offset HEAD-1 on each partition.")
	consumeCmd.Flags().BoolVarP(&endFlag, "end", "e", false, "Stop reading after reaching the end of the partition(s).")
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

		logFmt := flag

		if isSkip {
			offset = min + offset
			logFmt = fmt.Sprintf("skip first %s", numStr)
		} else if isTail {
			offset = max - offset
			logFmt = fmt.Sprintf("take last %s", numStr)
		} else {
			logFmt = fmt.Sprintf("use offset %s", numStr)
		}
		if offset > max {
			fmt.Fprintf(os.Stderr, "Attempting to %s on partition %d (%d...%d) gives %d, which is after the max offset. Using the max offset instead.\n", logFmt, partition, min, max, offset)
			offset = max
		}
		if offset < min {
			fmt.Fprintf(os.Stderr, "Attempting to %s on partition %d (%d...%d) gives %d, which is before the min offset. Using the min offset instead.\n", logFmt, partition, min, max, offset)
			offset = min
		}

		return offset, nil
	}
}

var consumeCmd = &cobra.Command{
	Use:   "consume",
	Short: "Consume messages",
	Args:  cobra.ExactArgs(1),
	Long: `Flags which are documented with "Takes a single or partitioned value" can be passed a single 
value or a comma-delimited list of value. If a single value is provided, that value will be used for each 
partition. If a comma-delimited list is provided, the number of values must be the same as the number of 
partitions being consumed (based on the --partitions flag, defaults to all partitions).`,
	PreRun: setupProtoDescriptorRegistry,
	Run: func(cmd *cobra.Command, args []string) {

		topic := args[0]

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
			// make sure partitions are sorted in a reasonable order if the user provided partitioned arguments
			sort.Ints(partitions)
		}

		opts := createOptions(partitions)

		offsetFlagsMap := opts.offsetFlagsMap
		countFlagsMap := opts.countsMap
		globalLimit := int64(opts.globalCount)
		hasGlobalLimit := globalLimit > 0


		// counter used for writing progress to stderr (needed because of buffered tabwriter)
		var globalProgress int64 = 0 // int64 because it's updated by atomic.AddInt64
		// counter used to track how many messages have been written to std out
		var globalWrittenCount int64 = 0

		wg := sync.WaitGroup{}
		mu := sync.Mutex{} // Synchronizes stderr and stdout.
		for _, partition := range partitions {

			wg.Add(1)

			go func(partition int) {

				offsetFlag := offsetFlagsMap[partition]
				partitionLimit, hasPartitionLimit := countFlagsMap[partition]
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

				var formattedOffset string
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

type consumeOptions struct {
	offsetFlagsMap map[int]string
	countsMap      map[int]int
	globalCount    int
}

// createOptions validates the flags and normalizes them into
// a single offset flag map the command can use
func createOptions(partitions []int) consumeOptions {

	opts := consumeOptions{
		offsetFlagsMap: map[int]string{},
		globalCount:    countFlag,
		countsMap:      map[int]int{},
	}

	numPartitions := len(partitions)

	// validate mutually exclusive flags
	var exclusive []string
	if tailFlag > 0 {
		exclusive = append(exclusive, "tail")
	}
	if len(tailsFlags) > 0 {
		exclusive = append(exclusive, "tails")
	}
	if skipFlag > 0 {
		exclusive = append(exclusive, "skip")
	}
	if len(skipsFlags) > 0 {
		exclusive = append(exclusive, "skips")
	}
	if followFlag {
		exclusive = append(exclusive, "follow")
	}
	if len(offsetFlags) > 0 {
		exclusive = append(exclusive, "offset")
	}
	if len(exclusive) > 1 {
		errorExit("multiple mutually exclusive offset-related flags provided, please only set one (flags set: %v)\n", exclusive)
	}

	if countFlag > 0 && len(countsFlags) > 0 {
		errorExit("--count and --counts are mutually exclusive, please only set one\n")
	}
	switch len(countsFlags) {
	case 0:
	case 1:
		commonCount := countsFlags[0]
		for _, partition := range partitions {
			opts.countsMap[partition] = commonCount
		}
	case numPartitions:
		for i, partition := range partitions {
			opts.countsMap[partition] = countsFlags[i]
		}
	default:
		errorExit("--counts takes 1 value or a number of values equal to the number of partitions (have %d partitions, got %d values)", numPartitions, len(tailsFlags))
	}

	if followFlag {
		for _, partition := range partitions {
			opts.offsetFlagsMap[partition] = "-1"
		}
	}

	if tailFlag > 0 {
		tailsFlags = make([]int, numPartitions)
		countdown := tailFlag
		for countdown > 0 {
			for i := 0; i < len(partitions) && countdown > 0; i++ {
				tailsFlags[i]++
				countdown--
			}
		}
	}

	switch len(tailsFlags) {
	case 0:
	case 1:
		for _, partition := range partitions {
			opts.offsetFlagsMap[partition] = fmt.Sprintf("-%d", tailsFlags[0])
		}
	case numPartitions:
		for i, partition := range partitions {
			opts.offsetFlagsMap[partition] = fmt.Sprintf("-%d", tailsFlags[i])
		}
	default:
		errorExit("--tails takes 1 value or a number of values equal to the number of partitions (have %d partitions, got %d values)", numPartitions, len(tailsFlags))
	}

	if skipFlag > 0 {
		skipsFlags = make([]int, numPartitions)
		countdown := skipFlag
		for countdown > 0 {
			for i := 0; i < len(partitions) && countdown > 0; i++ {
				skipsFlags[i]++
				countdown--
			}
		}
	}
	switch len(skipsFlags) {
	case 0:
	case 1:
		for _, partition := range partitions {
			opts.offsetFlagsMap[partition] = fmt.Sprintf("+%d", skipsFlags[0])
		}
	case numPartitions:
		for i, partition := range partitions {
			opts.offsetFlagsMap[partition] = fmt.Sprintf("+%d", skipsFlags[i])
		}
	default:
		errorExit("--skips takes 1 value or a number of values equal to the number of partitions (have %d partitions, got %d values)", numPartitions, len(skipsFlags))
	}

	if len(opts.offsetFlagsMap) == 0 {
		// only use offsetFlags if skip and tail unset
		switch len(offsetFlags) {
		case 0:
			// no other offsetting flags set, default to "oldest"
			for _, partition := range partitions {
				opts.offsetFlagsMap[partition] = "oldest"
			}
		case 1:
			for _, partition := range partitions {
				opts.offsetFlagsMap[partition] = offsetFlags[0]
			}
		case numPartitions:
			for i, partition := range partitions {
				opts.offsetFlagsMap[partition] = offsetFlags[i]
			}
		default:
			errorExit("--offsets takes 1 value or a number of values equal to the number of partitions (have %d partitions, got %d values)", numPartitions, len(offsetFlags))

		}
	}

	return opts
}
