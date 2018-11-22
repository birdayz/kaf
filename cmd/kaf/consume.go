package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	"strconv"

	"text/tabwriter"

	"github.com/birdayz/sarama"
	"github.com/hokaccha/go-prettyjson"
	"github.com/spf13/cobra"
)

var (
	offsetFlag string
	raw        bool
	follow     bool
)

func init() {
	rootCmd.AddCommand(consumeCmd)
	consumeCmd.Flags().StringVar(&offsetFlag, "offset", "oldest", "Offset to start consuming. Possible values: oldest, newest. Default: newest")
	consumeCmd.Flags().BoolVar(&raw, "raw", false, "Print raw output of messages, without key or prettified JSON")
	consumeCmd.Flags().BoolVarP(&follow, "follow", "f", false, "Shorthand to start consuming with offset HEAD-1 on each partition. Overrides --offset flag")
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
		wg := sync.WaitGroup{}
		for _, partition := range partitions {

			wg.Add(1)

			go func(partition int32) {
				req := &sarama.OffsetRequest{
					Version: int16(1),
				}
				req.AddBlock(topic, partition, int64(-1), int32(0))
				ldr, err := client.Leader(topic, partition)
				if err != nil {
					panic(err)
				}
				offsets, err := ldr.GetAvailableOffsets(req)
				if err != nil {
					panic(err)
				}
				followOffset := offsets.GetBlock(topic, partition).Offset - 1

				if follow && followOffset > 0 {
					offset = followOffset
					fmt.Fprintf(os.Stderr, "Starting on partition %v with offet %v\n", partition, offset)
				}

				pc, err := consumer.ConsumePartition(topic, partition, offset)
				if err != nil {
					panic(err)
				}

				for msg := range pc.Messages() {

					dataToDisplay := msg.Value

					if !raw {
						formatted, err := prettyjson.Format(msg.Value)
						if err == nil {
							dataToDisplay = formatted
						}

						w := tabwriter.NewWriter(os.Stderr, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)

						if len(msg.Headers) > 0 {
							fmt.Fprintf(w, "Headers:\n")
						}

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

							fmt.Fprintf(w, "\tKey: %v\tValue: %v\n", string(hdr.Key), hdrValue)

						}
						w.Flush()

					}

					if msg.Key != nil && len(msg.Key) > 0 && !raw {
						fmt.Printf("%v %v\n", string(msg.Key), string(dataToDisplay))
					} else {
						fmt.Printf("%v\n", string(dataToDisplay))
					}
				}
				wg.Done()
			}(partition)
		}
		wg.Wait()

	},
}
