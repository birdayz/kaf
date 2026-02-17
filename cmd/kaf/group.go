package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"unicode"

	"text/tabwriter"

	"encoding/base64"
	"encoding/hex"

	"sync"

	"github.com/IBM/sarama"
	"github.com/birdayz/kaf/pkg/streams"
	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"

	"strconv"

	"time"
)

var (
	flagPeekPartitions []int32
	flagPeekBefore     int64
	flagPeekAfter      int64
	flagPeekTopics     []string

	flagNoMembers      bool
	flagDescribeTopics []string
)

func init() {
	rootCmd.AddCommand(groupCmd)
	rootCmd.AddCommand(groupsCmd)
	groupCmd.AddCommand(groupDescribeCmd)
	groupCmd.AddCommand(groupLsCmd)
	groupCmd.AddCommand(groupDeleteCmd)
	groupCmd.AddCommand(groupPeekCmd)
	groupCmd.AddCommand(createGroupCommitOffsetCmd())

	groupLsCmd.Flags().BoolVar(&noHeaderFlag, "no-headers", false, "Hide table headers")
	groupsCmd.Flags().BoolVar(&noHeaderFlag, "no-headers", false, "Hide table headers")

	groupPeekCmd.Flags().StringSliceVarP(&flagPeekTopics, "topics", "t", []string{}, "Topics to peek from")
	groupPeekCmd.Flags().Int32SliceVarP(&flagPeekPartitions, "partitions", "p", []int32{}, "Partitions to peek from")
	groupPeekCmd.Flags().Int64VarP(&flagPeekBefore, "before", "B", 0, "Number of messages to peek before current offset")
	groupPeekCmd.Flags().Int64VarP(&flagPeekAfter, "after", "A", 0, "Number of messages to peek after current offset")

	groupDescribeCmd.Flags().BoolVar(&flagNoMembers, "no-members", false, "Hide members section of the output")
	groupDescribeCmd.Flags().StringSliceVarP(&flagDescribeTopics, "topic", "t", []string{}, "topics to display for the group. defaults to all topics.")
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
	Use:               "delete",
	Short:             "Delete group",
	Args:              cobra.MaximumNArgs(1),
	ValidArgsFunction: validGroupArgs,
	Run: func(cmd *cobra.Command, args []string) {
		admin := getClusterAdmin()
		defer admin.Close()
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
	topic            string
	partitionOffsets map[int32]int64
	offset           int64
	client           sarama.Client
	group            string
}

func (r *resetHandler) Setup(s sarama.ConsumerGroupSession) error {
	req := &sarama.OffsetCommitRequest{
		Version:                 1,
		ConsumerGroup:           r.group,
		ConsumerGroupGeneration: s.GenerationID(),
		ConsumerID:              s.MemberID(),
	}

	for p, o := range r.partitionOffsets {
		req.AddBlock(r.topic, p, o, 0, "")
	}
	br, err := r.client.Coordinator(r.group)
	if err != nil {
		return err
	}
	_ = br.Open(getConfig())
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
	var partitionFlag int32
	var allPartitions bool
	var offsetMap string
	var noconfirm bool
	res := &cobra.Command{
		Use:   "commit GROUP",
		Short: "Set offset for given consumer group",
		Long:  "Set offset for a given consumer group, creates one if it does not exist. Offsets cannot be set on a consumer group with active consumers.",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			client := getClient()
			admin := getClusterAdmin()

			group := args[0]
			partitionOffsets := make(map[int32]int64)

			if offsetMap != "" {
				if err := json.Unmarshal([]byte(offsetMap), &partitionOffsets); err != nil {
					errorExit("Wrong --offset-map format. Use JSON with keys as partition numbers and values as offsets.\nExample: --offset-map '{\"0\":123, \"1\":135, \"2\":120}'\n")
				}
			} else {
				var partitions []int32
				if allPartitions {
					// Determine partitions
					topicDetails, err := admin.DescribeTopics([]string{topic})
					if err != nil {
						errorExit("Unable to determine partitions of topic: %v\n", err)
					}

					detail := topicDetails[0]

					for _, p := range detail.Partitions {
						partitions = append(partitions, p.ID)
					}
				} else if partitionFlag != -1 {
					partitions = []int32{partitionFlag}
				} else {
					errorExit("Either --partition, --all-partitions or --offset-map flag must be provided")
				}

				sort.Slice(partitions, func(i int, j int) bool { return partitions[i] < partitions[j] })

				type Assignment struct {
					partition int32
					offset    int64
				}
				assignments := make(chan Assignment, len(partitions))

				// TODO offset must be calced per partition
				var wg sync.WaitGroup
				for _, partition := range partitions {
					wg.Add(1)
					go func(partition int32) {
						defer wg.Done()
						i, err := strconv.ParseInt(offset, 10, 64)
						if err != nil {
							// Try oldest/newest/..
							if offset == "oldest" || offset == "earliest" {
								i = sarama.OffsetOldest
							} else if offset == "newest" || offset == "latest" {
								i = sarama.OffsetNewest
							} else {
								// Try timestamp
								t, err := time.Parse(time.RFC3339, offset)
								if err != nil {
									errorExit("offset is neither offset nor timestamp", nil)
								}
								i = t.UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
							}

							o, err := client.GetOffset(topic, partition, i)
							if err != nil {
								errorExit("Failed to determine offset for timestamp: %v", err)
							}

							if o == -1 {
								fmt.Printf("Partition %v: could not determine offset from timestamp. Skipping.\n", partition)
								return
								//errorExit("Determined offset -1 from timestamp. Skipping.", o)
							}

							assignments <- Assignment{partition: partition, offset: o}

							fmt.Printf("Partition %v: determined offset %v from timestamp.\n", partition, o)
						} else {
							assignments <- Assignment{partition: partition, offset: i}
						}
					}(partition)
				}
				wg.Wait()
				close(assignments)

				for assign := range assignments {
					partitionOffsets[assign.partition] = assign.offset
				}
			}

			// Verify the Consumer Group is Empty
			groupDescs, err := admin.DescribeConsumerGroups([]string{args[0]})
			if err != nil {
				errorExit("Unable to describe consumer groups: %v\n", err)
			}
			for _, detail := range groupDescs {
				state := detail.State
				if !slices.Contains([]string{"Empty", "Dead"}, state) {
					errorExit("Consumer group %s has active consumers in it, cannot set offset\n", group)
				}
			}

			fmt.Printf("Resetting offsets to: %v\n", partitionOffsets)

			if !noconfirm {
				prompt := promptui.Prompt{
					Label:     "Reset offsets as described",
					IsConfirm: true,
				}

				_, err := prompt.Run()
				if err != nil {
					errorExit("Aborted, exiting.\n")
					return
				}
			}

			g, err := sarama.NewConsumerGroupFromClient(group, client)
			if err != nil {
				errorExit("Failed to create consumer group: %v\n", err)
			}

			err = g.Consume(context.Background(), []string{topic}, &resetHandler{
				topic:            topic,
				partitionOffsets: partitionOffsets,
				client:           client,
				group:            group,
			})
			if err != nil {
				errorExit("Failed to commit offset: %v\n", err)
			}

			fmt.Printf("Successfully committed offsets to %v.\n", partitionOffsets)

			closeErr := g.Close()
			if closeErr != nil {
				fmt.Printf("Warning: Failed to close consumer group: %v\n", closeErr)
			}
		},
	}
	res.Flags().StringVarP(&topic, "topic", "t", "", "topic")
	res.Flags().StringVarP(&offset, "offset", "o", "", "offset to commit")
	res.Flags().Int32VarP(&partitionFlag, "partition", "p", 0, "partition")
	res.Flags().BoolVar(&allPartitions, "all-partitions", false, "apply to all partitions")
	res.Flags().StringVar(&offsetMap, "offset-map", "", "set different offsets per different partitions in JSON format, e.g. {\"0\": 123, \"1\": 42}")
	res.Flags().BoolVar(&noconfirm, "noconfirm", false, "Do not prompt for confirmation")
	return res
}

var groupLsCmd = &cobra.Command{
	Use:   "ls",
	Short: "List groups",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		admin := getClusterAdmin()
		defer admin.Close()
		
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

		w := tabwriter.NewWriter(outWriter, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)

		groupDescs, err := admin.DescribeConsumerGroups(groupList)
		if err != nil {
			// if we can retrieve list of consumer group, but unable to describe consumer groups
			// fallback to only list group name without state
			if !noHeaderFlag {
				fmt.Fprintf(w, "NAME\n")
			}
	
			for _, group := range groupList {
				fmt.Fprintf(w, "%v\n", group)
			}
		} else {
			// return consumer group information with state
			if !noHeaderFlag {
				fmt.Fprintf(w, "NAME\tSTATE\tCONSUMERS\t\n")
			}
	
			for _, detail := range groupDescs {
				state := detail.State
				consumers := len(detail.Members)
				fmt.Fprintf(w, "%v\t%v\t%v\t\n", detail.GroupId, state, consumers)
			}
		}
		w.Flush()
	},
}

var groupPeekCmd = &cobra.Command{
	Use:               "peek",
	Short:             "Peek messages from consumer group offset",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: validGroupArgs,
	Run: func(cmd *cobra.Command, args []string) {
		admin := getClusterAdmin()
		defer admin.Close()

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

		peekPartitions := make(map[int32]struct{})
		for _, partition := range flagPeekPartitions {
			peekPartitions[partition] = struct{}{}
		}

		var topicPartitions map[string][]int32
		if len(flagPeekTopics) > 0 {
			topicPartitions = make(map[string][]int32, len(flagPeekTopics))
		}
		for _, topic := range flagPeekTopics {
			topicDetails, err := admin.DescribeTopics([]string{topic})
			if err != nil {
				errorExit("Unable to describe topics: %v\n", err)
			}

			detail := topicDetails[0]
			if detail.Err == sarama.ErrUnknownTopicOrPartition {
				fmt.Printf("Topic %v not found.\n", topic)
				return
			}

			if len(flagPeekPartitions) > 0 {
				topicPartitions[topic] = flagPeekPartitions
			} else {
				partitions := make([]int32, 0, len(detail.Partitions))
				for _, partition := range detail.Partitions {
					partitions = append(partitions, partition.ID)
				}
				topicPartitions[topic] = partitions
			}
		}

		offsetAndMetadata, err := admin.ListConsumerGroupOffsets(args[0], topicPartitions)
		if err != nil {
			errorExit("Failed to fetch group offsets: %v\n", err)
		}

		cfg := getConfig()
		client := getClientFromConfig(cfg)
		consumer, err := sarama.NewConsumerFromClient(client)
		if err != nil {
			errorExit("Unable to create consumer from client: %v\n", err)
		}

		mu := &sync.Mutex{}
		wg := &sync.WaitGroup{}

		for topic, partitions := range offsetAndMetadata.Blocks {
			for partition, offset := range partitions {
				if len(peekPartitions) > 0 {
					_, ok := peekPartitions[partition]
					if !ok {
						continue
					}
				}

				wg.Add(1)
				go func(topic string, partition int32, offset int64) {
					defer wg.Done()
					var start int64
					if offset > flagPeekBefore {
						start = offset - flagPeekBefore
					}

					pc, err := consumer.ConsumePartition(topic, partition, start)
					if err != nil {
						errorExit("Unable to consume partition: %v %v %v %v\n", topic, partition, offset, err)
					}

					for {
						select {
						case <-cmd.Context().Done():
							return
						case msg := <-pc.Messages():
							handleMessage(msg, mu)
							if msg.Offset >= offset+flagPeekAfter {
								return
							}
						}
					}
				}(topic, partition, offset.Offset)
			}
		}
		wg.Wait()
	},
}

var groupDescribeCmd = &cobra.Command{
	Use:               "describe",
	Short:             "Describe consumer group",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: validGroupArgs,
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

		w := tabwriter.NewWriter(outWriter, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
		fmt.Fprintf(w, "Group ID:\t%v\n", group.GroupId)
		fmt.Fprintf(w, "State:\t%v\n", group.State)
		fmt.Fprintf(w, "Protocol:\t%v\n", group.Protocol)
		fmt.Fprintf(w, "Protocol Type:\t%v\n", group.ProtocolType)

		fmt.Fprintf(w, "Offsets:\t\n")

		w.Flush()
		w.Init(outWriter, tabwriterMinWidthNested, 4, 2, tabwriterPadChar, tabwriterFlags)

		offsetAndMetadata, err := admin.ListConsumerGroupOffsets(args[0], nil)
		if err != nil {
			errorExit("Failed to fetch group offsets: %v\n", err)
		}

		topics := make([]string, 0, len(offsetAndMetadata.Blocks))
		for k := range offsetAndMetadata.Blocks {
			topics = append(topics, k)
		}
		sort.Strings(topics)

		// Batch fetch all high watermarks at once - major performance optimization!
		allHighWatermarks := getBatchHighWatermarks(admin, offsetAndMetadata.Blocks, flagDescribeTopics)

		for _, topic := range topics {
			partitions := offsetAndMetadata.Blocks[topic]
			if len(flagDescribeTopics) > 0 {
				var found bool
				for _, topicToShow := range flagDescribeTopics {
					if topic == topicToShow {
						found = true
					}
				}

				if !found {
					continue
				}
			}
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

			wms := allHighWatermarks[topic]

			lagSum := 0
			offsetSum := 0
			for _, partition := range p {
				lag := (wms[partition] - partitions[partition].Offset)
				lagSum += int(lag)
				offset := partitions[partition].Offset
				offsetSum += int(offset)
				fmt.Fprintf(w, "\t\t%v\t%v\t%v\t%v\t%v\n", partition, partitions[partition].Offset, wms[partition], (wms[partition] - partitions[partition].Offset), partitions[partition].Metadata)
			}

			fmt.Fprintf(w, "\t\tTotal\t%d\t\t%d\t\n", offsetSum, lagSum)
		}

		if !flagNoMembers {

			fmt.Fprintf(w, "Members:\t")

			w.Flush()
			w.Init(outWriter, tabwriterMinWidthNested, 4, 2, tabwriterPadChar, tabwriterFlags)

			fmt.Fprintln(w)
			for _, member := range group.Members {
				fmt.Fprintf(w, "\t%v:\n", member.ClientId)
				fmt.Fprintf(w, "\t\tHost:\t%v\n", member.ClientHost)

				assignment, err := member.GetMemberAssignment()
				if err != nil || assignment == nil {
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
					fmt.Fprintf(w, "\n")
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
					case streams.SubscriptionInfo:
						fmt.Fprintf(w, "\f\t\tMetadata:\t\n")
						fmt.Fprintf(w, "\t\t  UUID:\t0x%v\n", hex.EncodeToString(d.UUID))
						fmt.Fprintf(w, "\t\t  UserEndpoint:\t%v\n", d.UserEndpoint)
					}
				}

				fmt.Fprintf(w, "\n")

			}
		}

		w.Flush()

	},
}

// getBatchHighWatermarks fetches high watermarks for all topics and partitions in a single batch operation
func getBatchHighWatermarks(admin sarama.ClusterAdmin, offsetBlocks map[string]map[int32]*sarama.OffsetFetchResponseBlock, filterTopics []string) map[string]map[int32]int64 {
	client := getClient()
	defer client.Close()

	// Organize all partition requests by broker leader
	leaderPartitions := make(map[*sarama.Broker]map[string][]int32)
	
	for topic, partitions := range offsetBlocks {
		// Skip topics not in filter if filter is specified
		if len(filterTopics) > 0 {
			found := false
			for _, filterTopic := range filterTopics {
				if topic == filterTopic {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		for partition := range partitions {
			leader, err := client.Leader(topic, partition)
			if err != nil {
				errorExit("Unable to get leader for topic %s partition %d: %v", topic, partition, err)
			}
			
			if leaderPartitions[leader] == nil {
				leaderPartitions[leader] = make(map[string][]int32)
			}
			leaderPartitions[leader][topic] = append(leaderPartitions[leader][topic], partition)
		}
	}

	// Fetch offsets from all brokers in parallel
	wg := sync.WaitGroup{}
	wg.Add(len(leaderPartitions))
	results := make(chan map[string]map[int32]int64, len(leaderPartitions))

	for leader, topicPartitions := range leaderPartitions {
		go func(leader *sarama.Broker, topicPartitions map[string][]int32) {
			defer wg.Done()
			
			req := &sarama.OffsetRequest{Version: int16(1)}
			for topic, partitions := range topicPartitions {
				for _, partition := range partitions {
					req.AddBlock(topic, partition, int64(-1), int32(0))
				}
			}

			resp, err := leader.GetAvailableOffsets(req)
			if err != nil {
				errorExit("Unable to get available offsets from broker: %v", err)
			}

			brokerResults := make(map[string]map[int32]int64)
			for topic, blocks := range resp.Blocks {
				brokerResults[topic] = make(map[int32]int64)
				for partition, block := range blocks {
					brokerResults[topic][partition] = block.Offset
				}
			}
			
			results <- brokerResults
		}(leader, topicPartitions)
	}

	wg.Wait()
	close(results)

	// Combine results from all brokers
	allWatermarks := make(map[string]map[int32]int64)
	for brokerResults := range results {
		for topic, partitionOffsets := range brokerResults {
			if allWatermarks[topic] == nil {
				allWatermarks[topic] = make(map[int32]int64)
			}
			for partition, offset := range partitionOffsets {
				allWatermarks[topic][partition] = offset
			}
		}
	}

	return allWatermarks
}

func getHighWatermarks(admin sarama.ClusterAdmin, topic string, partitions []int32) (watermarks map[int32]int64) {
	client := getClient()
	leaders := make(map[*sarama.Broker][]int32)

	for _, partition := range partitions {
		leader, err := client.Leader(topic, partition)
		if err != nil {
			errorExit("Unable to get available offsets for partition without leader. Topic %s Partition %d, Error: %s ", topic, partition, err)
		}
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
	decoder := streams.NewDecoder(raw)

	switch protocol {
	case "stream":
		subscriptionInfo := streams.SubscriptionInfo{}
		err = subscriptionInfo.Decode(decoder)
		if err != nil {
			return nil, err
		}
		return subscriptionInfo, nil
	default:
		return nil, errors.New("unknown protocol")
	}
}
