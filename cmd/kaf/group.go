package main

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"unicode"

	"text/tabwriter"

	"encoding/base64"

	"encoding/hex"

	"sync"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"

	"github.com/infinimesh/kaf"
)

func init() {
	rootCmd.AddCommand(groupCmd)
	rootCmd.AddCommand(groupsCmd)
	groupCmd.AddCommand(groupDescribeCmd)
	groupCmd.AddCommand(groupLsCmd)

	groupLsCmd.Flags().BoolVar(&noHeaderFlag, "no-headers", false, "Hide table headers")
}

const (
	tabwriterMinWidth       = 6
	tabwriterMinWidthNested = 2
	tabwriterWidth          = 4
	tabwriterPadding        = 3
	tabwriterPadChar        = ' '
	tabwriterFlags          = 0
)

var groupCmd = &cobra.Command{Use: "group",
	Short: "Display information about consumer groups.",
}

var groupsCmd = &cobra.Command{
	Use:   "groups",
	Short: "List groups",
	Run:   groupLsCmd.Run,
}

var groupLsCmd = &cobra.Command{
	Use:   "ls",
	Short: "List topics",
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		admin, err := getClusterAdmin()
		if err != nil {
			panic(err)
		}

		groups, err := admin.ListConsumerGroups()
		if err != nil {
			panic(err)
		}

		groupList := make([]string, 0, len(groups))
		for grp := range groups {
			groupList = append(groupList, grp)
		}

		sort.Slice(groupList, func(i int, j int) bool {
			return groupList[i] < groupList[j]
		})

		w := tabwriter.NewWriter(os.Stdout, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)

		if !noHeaderFlag {
			fmt.Fprintf(w, "NAME\tSTATE\tCONSUMERS\t\n")
		}

		found := false

		groupDescs, err := admin.DescribeConsumerGroups(groupList)
		if err != nil {
			panic(err)
		}

		for _, detail := range groupDescs {
			state := detail.State
			consumers := len(detail.Members)
			fmt.Fprintf(w, "%v\t%v\t%v\t\n", detail.GroupId, state, consumers)
		}

		if found || len(args) == 0 {
			w.Flush()
		} else {
			fmt.Printf("Group %v not found\n", args[0])
		}

		return
	},
}

var groupDescribeCmd = &cobra.Command{
	Use:   "describe",
	Short: "Describe consumer group",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		// TODO List: This API can be used to find the current groups managed by a broker. To get a list of all groups in the cluster, you must send ListGroup to all brokers.
		// same goes probably for topics
		admin, err := getClusterAdmin()
		if err != nil {
			panic(err)
		}

		groups, err := admin.DescribeConsumerGroups([]string{args[0]})
		if err != nil {
			panic(err)
		}

		if len(groups) == 0 {
			panic("Did not receive expected describe consumergroup result")
		}
		group := groups[0]

		if group.State == "Dead" {
			fmt.Printf("Group %v not found.\n", args[0])
			return
		}

		topicsDedup := make(map[string]interface{}, 0)
		for _, member := range group.Members {
			assignment, err := member.GetMemberAssignment()
			if err != nil {
				continue
			}

			for topic := range assignment.Topics {
				topicsDedup[topic] = struct{}{}
			}
		}

		topics := make([]string, 0, len(topicsDedup))
		for topic := range topicsDedup {
			topics = append(topics, topic)
		}

		w := tabwriter.NewWriter(os.Stdout, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
		fmt.Fprintf(w, "Group ID:\t%v\n", group.GroupId)
		fmt.Fprintf(w, "State:\t%v\n", group.State)
		fmt.Fprintf(w, "Protocol:\t%v\n", group.Protocol)
		fmt.Fprintf(w, "Protocol Type:\t%v\n", group.ProtocolType)

		fmt.Fprintf(w, "Offsets:\t\n")

		w.Flush()
		w.Init(os.Stdout, tabwriterMinWidthNested, 4, 2, tabwriterPadChar, tabwriterFlags)

		if len(topics) > 0 {
			topicMeta, _ := admin.DescribeTopics(topics)

			topicPartitions := make(map[string][]int32)
			for _, topic := range topicMeta {
				topicPartitions[topic.Name] = make([]int32, 0, len(topic.Partitions))
				for _, partition := range topic.Partitions {
					topicPartitions[topic.Name] = append(topicPartitions[topic.Name], partition.ID)
				}
				sort.Slice(topicPartitions[topic.Name], func(i, j int) bool { return topicPartitions[topic.Name][i] < topicPartitions[topic.Name][j] })
			}

			offsetAndMetadata, _ := admin.ListConsumerGroupOffsets(args[0], topicPartitions)
			for topic, partitions := range topicPartitions {
				fmt.Fprintf(w, "\t%v:\n", topic)
				fmt.Fprintf(w, "\t\tPartition\tGroup Offset\tHigh Watermark\tLag\t\n")
				fmt.Fprintf(w, "\t\t---------\t------------\t--------------\t---\t\n")

				existingOffsets := make(map[int32]int64)
				for partition, groupOffset := range offsetAndMetadata.Blocks[topic] {
					existingOffsets[partition] = groupOffset.Offset
				}
				wms := getHighWatermarks(topic, partitions)

				for _, partition := range partitions {
					wm := wms[partition]
					var offset int64
					if blockOff := offsetAndMetadata.GetBlock(topic, partition).Offset; blockOff != -1 {
						offset = blockOff
					}
					fmt.Fprintf(w, "\t\t%v\t%v\t%v\t%v\t\n", partition, offset, wm, (wm - offset))
				}

			}

		}

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

			fmt.Fprintf(w, "\t\tAssignments:\n")

			fmt.Fprintf(w, "\t\t  Topic\tPartitions\t\n")
			fmt.Fprintf(w, "\t\t  -----\t----------\t")

			for topic, partitions := range assignment.Topics {
				fmt.Fprintf(w, "\n\t\t  %v\t%v\t", topic, partitions)
			}

			metadata, err := member.GetMemberMetadata()
			if err != nil {
				continue
			}

			decodedUserData, err := tryDecodeUserData(group.Protocol, metadata.UserData)
			if err != nil {
				if IsASCIIPrintable(string(metadata.UserData)) {
					fmt.Fprintf(w, "\f\t\tMetadata:\t%v\n", string(metadata.UserData))
				} else {

					fmt.Fprintf(w, "\f\t\tMetadata:\t%v\n", base64.StdEncoding.EncodeToString(metadata.UserData))
				}
			} else {
				switch d := decodedUserData.(type) {
				case kaf.SubscriptionInfo:
					fmt.Fprintf(w, "\f\t\tMetadata:\t\n")
					fmt.Fprintf(w, "\t\t  UUID:\t0x%v\n", hex.EncodeToString(d.UUID))
					fmt.Fprintf(w, "\t\t  UserEndpoint:\t%v\n", d.UserEndpoint)
				}
			}

		}

		w.Flush()

	},
}

func getHighWatermarks(topic string, partitions []int32) (watermarks map[int32]int64) {
	watermarks = make(map[int32]int64)
	client, err := getClient()
	if err != nil {
		panic(err)
	}
	leaders := make(map[*sarama.Broker][]int32)

	for _, partition := range partitions {
		leader, _ := client.Leader(topic, partition)
		leaders[leader] = append(leaders[leader], partition)
	}
	wg := sync.WaitGroup{}
	wg.Add(len(leaders))

	results := make(chan map[int32]int64, len(leaders))

	for leader, partitions := range leaders {
		req := &sarama.OffsetRequest{
			Version: int16(1),
		}

		for _, partition := range partitions {
			req.AddBlock(topic, partition, int64(-1), int32(0))
		}

		// Query distinct brokers in parallel
		go func(leader *sarama.Broker, req *sarama.OffsetRequest) {
			resp, err := leader.GetAvailableOffsets(req)
			if err != nil {
				panic(err)
			}

			watermarksFromLeader := make(map[int32]int64)
			for partition, block := range resp.Blocks[topic] {
				watermarksFromLeader[partition] = block.Offset
			}

			results <- watermarksFromLeader
			wg.Done()

		}(leader, req)

	}

	wg.Wait()
	close(results)

	watermarks = make(map[int32]int64)
	for resultMap := range results {
		for partition, offset := range resultMap {
			watermarks[partition] = offset
		}
	}

	return
}

// IsASCIIPrintable returns true if the string is ASCII printable.
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
