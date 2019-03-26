package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/Shopify/sarama"
	prettyjson "github.com/hokaccha/go-prettyjson"
	"github.com/spf13/cobra"

	"github.com/infinimesh/kaf/avro"
)

var (
	offsetFlag  string
	raw         bool
	follow      bool
	schemaCache *avro.SchemaCache
	keyfmt      *prettyjson.Formatter
)

func init() {
	rootCmd.AddCommand(consumeCmd)
	consumeCmd.Flags().StringVar(&offsetFlag, "offset", "oldest", "Offset to start consuming. Possible values: oldest, newest. Default: newest")
	consumeCmd.Flags().BoolVar(&raw, "raw", false, "Print raw output of messages, without key or prettified JSON")
	consumeCmd.Flags().BoolVarP(&follow, "follow", "f", false, "Shorthand to start consuming with offset HEAD-1 on each partition. Overrides --offset flag")

	keyfmt = prettyjson.NewFormatter()
	keyfmt.Newline = " " // Replace newline with space to avoid condensed output.
	keyfmt.Indent = 0
}

var consumeCmd = &cobra.Command{
	Use:   "consume",
	Short: "Consume messages",
	Args:  cobra.ExactArgs(1),
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

		topic := args[0]
		client, err := getClient()
		if err != nil {
			panic(err)
		}

		consumer, err := sarama.NewConsumerFromClient(client)
		if err != nil {
			panic(err)
		}

		partitions, err := consumer.Partitions(topic)
		if err != nil {
			panic(err)
		}

		schemaCache, err = getSchemaCache()
		if err != nil {
			panic(err)
		}

		// outputChan contains messages that will be written to the console
		outputChan := make(chan outputMessage)

		wg := sync.WaitGroup{}
		for _, partition := range partitions {

			wg.Add(1)

			go func(partition int32, outputChan chan outputMessage) {
				req := &sarama.OffsetRequest{
					Version: int16(1),
				}
				req.AddBlock(topic, partition, int64(-1), int32(0))
				ldr, err := client.Leader(topic, partition)
				if err != nil {
					panic(err)
				}

				// There is a race where the metadata may not have been received
				// yet when we ask for offsets.  Retry and backoff until it
				// (hopefully) connects.
				offsets, err := ldr.GetAvailableOffsets(req)
				for retries := 1; err != nil && retries <= 20; {
					if err == sarama.ErrNotConnected {
						time.Sleep(time.Duration(50*retries) * time.Millisecond)
						offsets, err = ldr.GetAvailableOffsets(req)
						retries++
					} else {
						break
					}
				}
				if err != nil {
					panic(err)
				}

				followOffset := offsets.GetBlock(topic, partition).Offset - 1

				if follow && followOffset > 0 {
					offset = followOffset
					outputChan <- outputMessage{error: fmt.Errorf("starting on partition %v with offset %v", partition, offset)}
				}

				pc, err := consumer.ConsumePartition(topic, partition, offset)
				if err != nil {
					panic(err)
				}

				for msg := range pc.Messages() {

					dataToDisplay, err := avroDecode(msg.Value)
					if err != nil {
						outputChan <- outputMessage{error: fmt.Errorf("could not decode Avro data: %v", err)}
					}

					om := outputMessage{}

					if raw {
						om.body = string(dataToDisplay)
					} else {
						formatted, err := prettyjson.Format(dataToDisplay)
						if err == nil {
							om.body = string(formatted)
						}

						headers := map[string]string{}

						for _, hdr := range msg.Headers {
							var hdrValue string
							// Try to detect azure eventhub-specific encoding
							switch hdr.Value[0] {
							case 161:
								hdrValue = string(hdr.Value[2 : 2+hdr.Value[1]])
							case 131:
								hdrValue = strconv.FormatUint(binary.BigEndian.Uint64(hdr.Value[1:9]), 10)
							default:
								hdrValue = string(hdr.Value)
							}

							headers[string(hdr.Key)] = hdrValue

						}
						om.headers = headers
					}

					// if msg.Key != nil && len(msg.Key) > 0 && !raw {
					key, err := avroDecode(msg.Key)
					if err != nil {
						outputChan <- outputMessage{error: fmt.Errorf("could not decode Avro data: %v", err)}
					}

					om.key = formatKey(key)
					om.partition = msg.Partition
					om.offset = msg.Offset
					om.timestamp = msg.Timestamp

					outputChan <- om
				}
				wg.Done()
			}(partition, outputChan)
		}

		go consumeOutput(outputChan)
		wg.Wait()
		close(outputChan)
		consumer.Close()
	},
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

func consumeOutput(outputChannel chan outputMessage) {
	w := tabwriter.NewWriter(os.Stdout, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
	for message := range outputChannel {
		if message.error != nil {
			fmt.Fprintf(os.Stderr, "Error: %s\n", message.error)
		}
		if len(message.headers) > 0 {
			fmt.Fprintf(w, "Headers:\n")

			for k, v := range message.headers {
				fmt.Fprintf(w, "\tKey: %v\tValue: %v\n", k, v)
			}
		}
		if len(message.key) > 0 && !raw {
			fmt.Fprintf(w, "Key:\t%v\nPartition:\t%v\nOffset:\t%v\nTimestamp:\t%v\n", message.key, message.partition, message.offset, message.timestamp)
		}
		fmt.Fprintf(w, "%v\n", message.body)
		w.Flush()
	}
}

type outputMessage struct {
	error     error
	body      string
	headers   map[string]string
	key       string
	partition int32
	offset    int64
	timestamp time.Time
}
