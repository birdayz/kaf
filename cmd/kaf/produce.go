package main

import (
	"fmt"
	"io/ioutil"
	"os"

	sarama "github.com/birdayz/sarama"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func init() {
	rootCmd.AddCommand(produceCmd)

	produceCmd.Flags().StringP("key", "k", "", "Key for the record. Currently only strings are supported.")
	viper.BindPFlag("key", produceCmd.Flags().Lookup("key"))

	produceCmd.Flags().IntP("num", "n", 1, "Number of records to send.")
	viper.BindPFlag("num", produceCmd.Flags().Lookup("num"))

}

var produceCmd = &cobra.Command{
	Use:   "produce TOPIC",
	Short: "Produce record. Reads data from stdin.",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		config := getConfig()

		producer, err := sarama.NewSyncProducer(brokers, config)
		if err != nil {
			panic(err)
		}

		data, err := ioutil.ReadAll(os.Stdin)

		for i := 0; i < viper.GetInt("num"); i++ {
			partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
				Topic: args[0],
				Key:   sarama.StringEncoder(viper.GetString("key")),
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
