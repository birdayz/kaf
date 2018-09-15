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
	rootCmd.AddCommand(groupCmd)
	groupCmd.AddCommand(groupDescribeCmd)
	groupCmd.AddCommand(groupGetCmd)
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

var groupGetCmd = &cobra.Command{
	Use:   "get",
	Short: "List topics",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		admin, err := getClusterAdmin()
		if err != nil {
			panic(err)
		}

		grps, err := admin.ListConsumerGroups()
		if err != nil {
			panic(err)
		}

		w := tabwriter.NewWriter(os.Stdout, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
		fmt.Fprintf(w, "NAME\tSTATE\tCONSUMERS\t\n")

		found := false
		for group, _ := range grps {
			if len(args) > 0 && group != args[0] {
				continue
			}
			detail, err := admin.DescribeConsumerGroup(group)
			if err != nil {
				panic(err)
			}

			state := detail.State
			consumers := len(detail.Members)

			fmt.Fprintf(w, "%v\t%v\t%v\t\n", group, state, consumers)
			found = true
		}

		if found {
			w.Flush()
		} else {
			fmt.Printf("Group %v not found\n", args[0])
		}

		return
	},
}

func getClusterAdmin() (admin sarama.ClusterAdmin, err error) {
	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0

	return sarama.NewClusterAdmin([]string{"localhost:9092"}, config)
}

var groupDescribeCmd = &cobra.Command{
	Use:   "describe",
	Short: "Describe consumer group",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		admin, err := getClusterAdmin()
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
		// topicPartitions := make(map[string][]int32)
		// topicPartitions["public.delta.reported-state"] = []int32{0, 1, 2, 3} // TODO retrieve topics
		// offsetAndMetadata, err := admin.ListConsumerGroupOffsets(args[0], topicPartitions)
		// if err != nil {
		// 	panic(err)
		// }

		// TODO: move offsets section below topics, as a group can have multiple topics assigned
		// fmt.Fprintf(w, "Offsets:\t\n")

		// w.Flush()
		// w.Init(os.Stdout, tabwriterMinWidthNested, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)

		// fmt.Fprintf(w, "\tPartition\tOffset\n")
		// fmt.Fprintf(w, "\t---------\t------")

		// for partition, offset := range offsetAndMetadata.Blocks["public.delta.reported-state"] {
		// 	fmt.Fprintf(w, "\n\t%v\t%v", partition, offset.Offset)
		// }

		// fmt.Fprintln(w)

		fmt.Fprintf(w, "Members:\t")

		w.Flush()
		w.Init(os.Stdout, tabwriterMinWidthNested, 4, 2, tabwriterPadChar, tabwriterFlags)

		fmt.Fprintln(w)
		for _, member := range group.Members {
			fmt.Fprintf(w, "\t%v:\n", member.ClientId)
			fmt.Fprintf(w, "\t\tHost:\t%v\n", member.ClientHost)

			assignment, err := member.GetMemberAssignment()
			if err != nil {
				continue
			}

			// offsetAndMetadata, err := admin.ListConsumerGroupOffsets("shadow", assignment.Topics)
			// if err != nil {
			// 	panic(err)
			// }

			fmt.Fprintf(w, "\t\tAssignments:\n")

			// TODO offsets
			// fmt.Fprintf(w, "\t\t----\t----------")
			// for key, _ := range assignment.Topics {
			// fmt.Fprintf(w, "\tName: %v\n", key)
			// fmt.Fprintf(w, "\tOffsets:\t\n")

			// w.Flush()
			// w.Init(os.Stdout, tabwriterMinWidthNested, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)

			fmt.Fprintf(w, "\t\t  Topic\tPartitions\t\n")
			fmt.Fprintf(w, "\t\t  -----\t--------\t")

			for topic, partitions := range assignment.Topics {
				fmt.Fprintf(w, "\n\t\t  %v\t%v\t", topic, partitions)
			}

			// fmt.Fprintln(w)

			// fmt.Fprintf(w, "\tPartition\tOffset\n")
			// 	fmt.Fprintf(w, "\t---------\t------")

			// 	for partition, offset := range offsetAndMetadata.Blocks["public.delta.reported-state"] {
			// 		if offset.Offset != int64(-1) {
			// 			fmt.Fprintf(w, "\n\t%v\t%v", partition, offset.Offset)
			// 		}
			// 	}

			// }

			metadata, err := member.GetMemberMetadata()
			if err != nil {
				continue
			}

			// for _, topic := range metadata.Topics {
			// 	fmt.Fprintf(w, "\n\t\t%v", topic)
			// }
			// fmt.Fprintln(w)

			decodedUserData, err := tryDecodeUserData(group.Protocol, metadata.UserData)
			if err != nil {
				if IsASCIIPrintable(string(metadata.UserData)) {
					fmt.Fprintf(w, "\f\t\tMetadata:\t%v\n", string(metadata.UserData))
				} else {

					fmt.Fprintf(w, "\f\t\tMetadata:\t%v\n", base64.StdEncoding.EncodeToString(metadata.UserData))
				}
			}

			switch d := decodedUserData.(type) {
			case kaf.SubscriptionInfo:

				fmt.Fprintf(w, "\f\t\tMetadata:\t\n")
				fmt.Fprintf(w, "\t\t  UUID:\t0x%v\n", hex.EncodeToString(d.UUID))
				fmt.Fprintf(w, "\t\t  UserEndpoint:\t%v\n", d.UserEndpoint)
			}
		}

		w.Flush()

	},
}

func IsASCIIPrintable(s string) bool {
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
