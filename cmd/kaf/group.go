package main

import (
	"context"
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

	"strconv"

	"time"

	"github.com/birdayz/kaf"
)

func init() {
	rootCmd.AddCommand(groupCmd)
	rootCmd.AddCommand(groupsCmd)
	groupCmd.AddCommand(groupDescribeCmd)
	groupCmd.AddCommand(groupLsCmd)
	groupCmd.AddCommand(groupDeleteCmd)
	groupCmd.AddCommand(createGroupCommitOffsetCmd())

	groupLsCmd.Flags().BoolVar(&noHeaderFlag, "no-headers", false, "Hide table headers")
	groupsCmd.Flags().BoolVar(&noHeaderFlag, "no-headers", false, "Hide table headers")
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

var groupsCmd = &cobra.Command{
	Use:   "groups",
	Short: "List groups",
	Run:   groupLsCmd.Run,
}

var groupDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete group",
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		admin := getClusterAdmin()
		var group string
		if len(args) == 1 {
			group = args[0]
		}
		err := admin.DeleteConsumerGroup(group)
		if err != nil {
			errorExit("Could not delete consumer group %v: %v\n", group, err.Error())
		} else {
			fmt.Printf("Deleted consumer group %v.\n", group)
		}

	},
}

type resetHandler struct {
	topic     string
	partition int32
	offset    int64
	client    sarama.Client
	group     string
}

func (r *resetHandler) Setup(s sarama.ConsumerGroupSession) error {
	req := &sarama.OffsetCommitRequest{
		Version:                 1,
		ConsumerGroup:           r.group,
		ConsumerGroupGeneration: s.GenerationID(),
		ConsumerID:              s.MemberID(),
	}
	req.AddBlock(r.topic, r.partition, r.offset, 0, "reseted by kaf")
	br, err := r.client.Coordinator(r.group)
	if err != nil {
		return err
	}
	br.Open(getConfig())
	_, err = br.CommitOffset(req)
	if err != nil {
		return err
	}
	return nil
}

func (r *resetHandler) Cleanup(s sarama.ConsumerGroupSession) error {
	return nil
}

func (r *resetHandler) ConsumeClaim(s sarama.ConsumerGroupSession, c sarama.ConsumerGroupClaim) error {
	return nil
}

func createGroupCommitOffsetCmd() *cobra.Command {
	var topic string
	var offset string
	var partition int32
	res := &cobra.Command{
		Use:  "commit",
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			client := getClient()

			group := args[0]

			var off int64

			i, err := strconv.ParseInt(offset, 10, 64)
			if err != nil {
				// No int -> try timestamp
				t, err := time.Parse(time.RFC3339, offset)
				if err != nil {
					errorExit("offset is neither offset nor timestamp", nil)
				}
				_ = t

				o, err := client.GetOffset(topic, partition, t.UnixNano()/(int64(time.Millisecond)/int64(time.Nanosecond)))
				if err != nil {
					errorExit("Failed to determine offset for timestamp: %v", err)
				}
				off = o

				fmt.Printf("Determined offset %v from timestamp.\n", off)
			} else {
				off = i
			}

			g, err := sarama.NewConsumerGroupFromClient(group, client)
			if err != nil {
				errorExit("Failed to create consumer group: %v", err)
			}

			err = g.Consume(context.Background(), []string{topic}, &resetHandler{
				topic:     topic,
				partition: partition,
				offset:    off,
				client:    client,
				group:     group,
			})
			if err != nil {
				errorExit("Failed to commit offset: %v", err)
			}

			fmt.Printf("Set offset to %v.", off)
		},
	}
	res.Flags().StringVarP(&topic, "topic", "t", "", "topic")
	res.Flags().StringVarP(&offset, "offset", "o", "", "offset to commit")
	res.Flags().Int32VarP(&partition, "partition", "p", 0, "partition")
	return res
}

var groupLsCmd = &cobra.Command{
	Use:   "ls",
	Short: "List groups",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		admin := getClusterAdmin()

		groups, err := admin.ListConsumerGroups()
		if err != nil {
			errorExit("Unable to list consumer groups: %v\n", err)
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

		groupDescs, err := admin.DescribeConsumerGroups(groupList)
		if err != nil {
			errorExit("Unable to describe consumer groups: %v\n", err)
		}

		for _, detail := range groupDescs {
			state := detail.State
			consumers := len(detail.Members)
			fmt.Fprintf(w, "%v\t%v\t%v\t\n", detail.GroupId, state, consumers)
		}

		w.Flush()
	},
}

var groupDescribeCmd = &cobra.Command{
	Use:   "describe",
	Short: "Describe consumer group",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		// TODO List: This API can be used to find the current groups managed by a broker. To get a list of all groups in the cluster, you must send ListGroup to all brokers.
		// same goes probably for topics
		admin := getClusterAdmin()

		groups, err := admin.DescribeConsumerGroups([]string{args[0]})
		if err != nil {
			errorExit("Unable to describe consumer groups: %v\n", err)
		}

		if len(groups) == 0 {
			errorExit("Did not receive expected describe consumergroup result\n")
		}
		group := groups[0]

		if group.State == "Dead" {
			fmt.Printf("Group %v not found.\n", args[0])
			return
		}

		w := tabwriter.NewWriter(os.Stdout, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
		fmt.Fprintf(w, "Group ID:\t%v\n", group.GroupId)
		fmt.Fprintf(w, "State:\t%v\n", group.State)
		fmt.Fprintf(w, "Protocol:\t%v\n", group.Protocol)
		fmt.Fprintf(w, "Protocol Type:\t%v\n", group.ProtocolType)

		fmt.Fprintf(w, "Offsets:\t\n")

		w.Flush()
		w.Init(os.Stdout, tabwriterMinWidthNested, 4, 2, tabwriterPadChar, tabwriterFlags)

		offsetAndMetadata, err := admin.ListConsumerGroupOffsets(args[0], nil)
		if err != nil {
			errorExit("Failed to fetch group offsets: %v\n", err)
		}

		for topic, partitions := range offsetAndMetadata.Blocks {
			fmt.Fprintf(w, "\t%v:\n", topic)
			fmt.Fprintf(w, "\t\tPartition\tGroup Offset\tHigh Watermark\tLag\tMetadata\t\n")
			fmt.Fprintf(w, "\t\t---------\t------------\t--------------\t---\t--------\n")

			var p []int32

			for partition := range partitions {
				p = append(p, partition)
			}

			sort.Slice(p, func(i, j int) bool {
				return p[i] < p[j]
			})

			wms := getHighWatermarks(topic, p)

			for _, partition := range p {
				fmt.Fprintf(w, "\t\t%v\t%v\t%v\t%v\t%v\n", partition, partitions[partition].Offset, wms[partition], (wms[partition] - partitions[partition].Offset), partitions[partition].Metadata)
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
	client := getClient()
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
				errorExit("Unable to get available offsets: %v\n", err)
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
