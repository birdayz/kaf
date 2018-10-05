package main

import (
	"bytes"
	"fmt"
	"os"
	"sync"

	"io"

	"github.com/birdayz/sarama"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(consumeCmd)
}

var consumeCmd = &cobra.Command{
	Use:   "consume",
	Short: "Consume messages",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
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
			pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
			if err != nil {
				panic(err)
			}

			wg.Add(1)

			go func() {
				for msg := range pc.Messages() {
					io.Copy(os.Stdout, bytes.NewReader(msg.Value))
					fmt.Println()
				}
				wg.Done()
			}()
		}
		wg.Wait()

	},
}
