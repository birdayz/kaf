package main

import (
	"fmt"
	"sync"

	"github.com/birdayz/sarama"
	"github.com/spf13/cobra"
)

var offsetFlag string

func init() {
	rootCmd.AddCommand(consumeCmd)
	consumeCmd.Flags().StringVar(&offsetFlag, "offset", "oldest", "Offset to start consuming. Possible values: oldest, newest. Default: newest")
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
				pc, err := consumer.ConsumePartition(topic, partition, offset)
				if err != nil {
					panic(err)
				}

				for msg := range pc.Messages() {
					// io.Copy(os.Stdout, bytes.NewReader(msg.Value))
					fmt.Println(string(msg.Value))
					fmt.Println()
				}
				wg.Done()
			}(partition)
		}
		wg.Wait()

	},
}
