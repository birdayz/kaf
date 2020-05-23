package topic

import (
	context "context"
	"fmt"
	"sort"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/birdayz/kaf/api"
	"github.com/birdayz/kaf/pkg/connection"
	"github.com/davecgh/go-spew/spew"
	empty "github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Service struct {
	connManager *connection.ConnManager
}

func NewService(connManager *connection.ConnManager) (*Service, error) {
	return &Service{
		connManager: connManager,
	}, nil
}

func (s *Service) CreateTopic(context.Context, *api.CreateTopicRequest) (*api.Topic, error) {
	return &api.Topic{
		Name: "abc",
	}, nil
}
func (s *Service) GetTopic(context.Context, *api.GetTopicRequest) (*api.Topic, error) {

	return nil, nil
}
func (s *Service) UpdateTopic(context.Context, *api.UpdateTopicRequest) (*api.Topic, error) {

	return nil, nil
}
func (s *Service) ListTopics(ctx context.Context, req *api.ListTopicsRequest) (*api.ListTopicsResponse, error) {
	adminClient, err := s.connManager.GetAdminClient(req.Cluster)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	topics, err := adminClient.ListTopics()
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to list topics")
	}

	sortedTopics := make(
		[]struct {
			name string
			sarama.TopicDetail
		}, len(topics))

	i := 0
	for name, topic := range topics {
		sortedTopics[i].name = name
		sortedTopics[i].TopicDetail = topic
		i++
	}

	sort.Slice(sortedTopics, func(i int, j int) bool {
		return sortedTopics[i].name < sortedTopics[j].name
	})

	var resp api.ListTopicsResponse

	for _, entry := range sortedTopics {
		resp.Topics = append(resp.Topics, &api.Topic{
			Name:          entry.name,
			NumPartitions: entry.NumPartitions,
			NumReplicas:   int32(entry.ReplicationFactor),
			Partitions:    nil,
		})
	}

	var topicNames []string
	for k, _ := range topics {
		topicNames = append(topicNames, k)
	}

	meta, err := adminClient.DescribeTopics(topicNames)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	for _, m := range meta {
		for _, t := range resp.Topics {
			if t.Name == m.Name {
				for _, p := range m.Partitions {
					t.Partitions = append(t.Partitions, &api.Partition{Number: int64(p.ID)})
				}
			}
		}
	}

	client, err := s.connManager.GetClient(req.Cluster)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	for _, topic := range resp.Topics {
		spew.Dump(topic)
		var ps []int32
		for _, p := range topic.Partitions {
			ps = append(ps, int32(p.Number))
		}
		wms, _ := getHighWatermarks(client, topic.Name, ps)
		var msgs int64
		for _, v := range wms {
			msgs += v
		}
		fmt.Println("WMS!!", wms)
		topic.Messages = msgs
	}

	// Todo get num messages

	return &resp, nil
}

func getHighWatermarks(client sarama.Client, topic string, partitions []int32) (watermarks map[int32]int64, err error) {
	fmt.Println("ZZ")
	leaders := make(map[*sarama.Broker][]int32)

	for _, partition := range partitions {
		leader, _ := client.Leader(topic, partition)
		leaders[leader] = append(leaders[leader], partition)
	}
	wg := sync.WaitGroup{}
	wg.Add(len(leaders))

	results := make(chan map[int32]int64, len(leaders))

	fmt.Println(leaders, partitions)

	for leader, partitions := range leaders {
		fmt.Println("DD")
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
				fmt.Println(err)
				return
			}

			spew.Dump(resp)

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

	return watermarks, nil
}

func (s *Service) DeleteTopic(context.Context, *api.DeleteTopicRequest) (*empty.Empty, error) {
	return nil, nil
}
