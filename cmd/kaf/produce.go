package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
)

var keyFlag string
var numFlag int

func init() {
	rootCmd.AddCommand(produceCmd)

	produceCmd.Flags().StringVarP(&keyFlag, "key", "k", "", "Key for the record. Currently only strings are supported.")
	produceCmd.Flags().IntVarP(&numFlag, "num", "n", 1, "Number of records to send.")
}

var produceCmd = &cobra.Command{
	Use:   "produce TOPIC",
	Short: "Produce record. Reads data from stdin.",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		producer, err := sarama.NewSyncProducer(currentCluster.Brokers, getConfig())
		if err != nil {
			panic(err)
		}

		data, err := ioutil.ReadAll(os.Stdin)

		for i := 0; i < numFlag; i++ {
			partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
				Topic: args[0],
				Key:   sarama.StringEncoder(keyFlag),
				Value: sarama.ByteEncoder(data),
			})
			if err != nil {
				fmt.Printf("Failed to send record: %v.", err)
				os.Exit(1)
			}

			fmt.Printf("Sent record to partition %v at offset %v.\n", partition, offset)

		}

	},
}
