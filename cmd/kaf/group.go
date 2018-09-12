package main

import (
	"errors"
	"fmt"
	"os"
	"unicode"

	"text/tabwriter"

	"encoding/base64"

	"encoding/hex"

	sarama "github.com/birdayz/sarama"
	"github.com/infinimesh/kaf"
	"github.com/spf13/cobra"
)

func init() {

}

const (
	tabwriterMinWidth       = 6
	tabwriterMinWidthNested = 2
	tabwriterWidth          = 4
	tabwriterPadding        = 3
	tabwriterPadChar        = ' '
	tabwriterFlags          = 0
)

var groupCmd = &cobra.Command{
	Use:   "group",
	Short: "Display information about consumer groups.",
}

var groupDescribeCmd = &cobra.Command{
	Use:   "describe",
	Short: "Describe consumer group",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		config := sarama.NewConfig()
		config.Version = sarama.V1_0_0_0
		config.Consumer.Return.Errors = false
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
		config.Producer.Return.Successes = true
		config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
		config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message

		admin, err := sarama.NewClusterAdmin([]string{"localhost:9092"}, config)
		if err != nil {
			panic(err)
		}

		group, err := admin.DescribeConsumerGroup(args[0])
		if err != nil {
			panic(err)
		}

		w := tabwriter.NewWriter(os.Stdout, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
		fmt.Fprintf(w, "Group ID:\t%v\n", group.GroupId)
		fmt.Fprintf(w, "State:\t%v\n", group.State)
		fmt.Fprintf(w, "Protocol:\t%v\n", group.Protocol)
		fmt.Fprintf(w, "Protocol Type:\t%v\n", group.ProtocolType)

		// Committed offsets
		// TODO get partitions
		topicPartitions := make(map[string][]int32)
		topicPartitions["public.delta.reported-state"] = []int32{0, 1, 2, 3} // TODO retrieve topics
		offsetAndMetadata, err := admin.ListConsumerGroupOffsets(args[0], topicPartitions)
		if err != nil {
			panic(err)
		}

		// TODO: move offsets section below topics, as a group can have multiple topics assigned
		fmt.Fprintf(w, "Offsets:\t\n")

		w.Flush()
		w.Init(os.Stdout, tabwriterMinWidthNested, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)

		fmt.Fprintf(w, "\tPartition\tOffset\n")
		fmt.Fprintf(w, "\t---------\t------")

		for partition, offset := range offsetAndMetadata.Blocks["public.delta.reported-state"] {
			fmt.Fprintf(w, "\n\t%v\t%v", partition, offset.Offset)
		}

		fmt.Fprintln(w)

		// fmt.Println("O partition 0", offsetAndMetadata.GetBlock(args[0], 0))

		fmt.Fprintf(w, "Members:\t\n")

		w.Flush()
		w.Init(os.Stdout, tabwriterMinWidthNested, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)

		for _, member := range group.Members {
			fmt.Fprintf(w, "\tID:\t%v\n", member.ClientId)
			fmt.Fprintf(w, "\tHost:\t%v\n", member.ClientHost)

			assignment, err := member.GetMemberAssignment()
			if err != nil {
				continue
			}

			fmt.Fprintf(w, "\tTopics:\t\n")
			fmt.Fprintf(w, "\t\tName\tPartitions\n")
			fmt.Fprintf(w, "\t\t----\t----------")
			for key, value := range assignment.Topics {
				fmt.Fprintf(w, "\n\t\t%v\t%v", key, value)
			}

			metadata, err := member.GetMemberMetadata()
			if err != nil {
				continue
			}

			// for _, topic := range metadata.Topics {
			// 	fmt.Fprintf(w, "\n\t\t%v", topic)
			// }
			fmt.Fprintln(w)

			decodedUserData, err := tryDecodeUserData(group.Protocol, metadata.UserData)
			if err != nil {
				if IsAsciiPrintable(string(metadata.UserData)) {
					fmt.Fprintf(w, "\tMetadata:\t%v\n", string(metadata.UserData))
				} else {

					fmt.Fprintf(w, "\tMetadata:\t%v\n", base64.StdEncoding.EncodeToString(metadata.UserData))
				}
			}

			switch d := decodedUserData.(type) {
			case kaf.SubscriptionInfo:

				fmt.Fprintf(w, "\tMetadata:\t\n")
				fmt.Fprintf(w, "\t\tUUID:\t%v\n", hex.EncodeToString(d.UUID))
				fmt.Fprintf(w, "\t\tUserEndpoint:\t%v\n", d.UserEndpoint)
			}
		}

		w.Flush()

	},
}

func IsAsciiPrintable(s string) bool {
	for _, r := range s {
		if r > unicode.MaxASCII || !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

func tryDecodeUserData(protocol string, raw []byte) (data interface{}, err error) {
	// Interpret userdata here
	decoder := kaf.NewDecoder(raw)

	switch protocol {
	case "stream":
		subscriptionInfo := kaf.SubscriptionInfo{}
		err = subscriptionInfo.Decode(decoder)
		if err != nil {
			return nil, err
		}
		return subscriptionInfo, nil
	default:
		return nil, errors.New("unknown protocol")
	}
}

func init() {
	rootCmd.AddCommand(groupCmd)
	groupCmd.AddCommand(groupDescribeCmd)
}
