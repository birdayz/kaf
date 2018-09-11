package main

import (
	"errors"
	"fmt"
	"os"
	"unicode"

	"text/tabwriter"

	"encoding/base64"

	"encoding/hex"

	sarama "github.com/bsm/sarama-1"
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

		client, err := sarama.NewClient([]string{"localhost:9092"}, config)
		if err != nil {
			panic(err)
		}

		client.Brokers()[0].Open(config) // Ensure that we're connected

		response, err := client.Brokers()[0].DescribeGroups(&sarama.DescribeGroupsRequest{
			Groups: []string{args[0]},
		})

		if len(response.Groups) < 1 {
			fmt.Println("Could't find data for group")
			os.Exit(1)
		}
		group := response.Groups[0]

		w := tabwriter.NewWriter(os.Stdout, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
		fmt.Fprintf(w, "Group ID:\t%v\n", group.GroupId)
		fmt.Fprintf(w, "State:\t%v\n", group.State)
		fmt.Fprintf(w, "Protocol:\t%v\n", group.Protocol)
		fmt.Fprintf(w, "Protocol Type:\t%v\n", group.ProtocolType)
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
			fmt.Fprintln(w, "\t------------------")
		}

		// Todo get offsets/assignments

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
