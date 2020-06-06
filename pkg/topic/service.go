package topic

import (
	context "context"
	"fmt"
	"sort"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/birdayz/kaf/api"
	"github.com/birdayz/kaf/pkg/connection"
	"github.com/golang/glog"
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

func logDirBytes(client sarama.Client) (map[string]int64, error) {
	res := make(map[string]int64)
	for _, broker := range client.Brokers() {
		req := &sarama.DescribeLogDirsRequest{}
		resp, err := broker.DescribeLogDirs(req)
		if err != nil {
			return nil, err
		}

		for _, logDir := range resp.LogDirs {
			for _, topic := range logDir.Topics {
				if _, ok := res[topic.Topic]; !ok {
					res[topic.Topic] = int64(0)
				}
				for _, partition := range topic.Partitions {
					res[topic.Topic] += partition.Size
				}
			}
		}
	}

	return res, nil
}

func (s *Service) ListTopics(ctx context.Context, req *api.ListTopicsRequest) (*api.ListTopicsResponse, error) {
	adminClient, err := s.connManager.GetAdminClient(req.Cluster)
	if err != nil {
		glog.Errorf("ListTopics failed: %v", err)
		return nil, status.Error(codes.Internal, "Failed to list topics")
	}
	topics, err := adminClient.ListTopics()
	if err != nil {
		glog.Errorf("ListTopics failed: %v", err)
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
			TopicDetail:   &api.TopicDetail{},
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
					t.TopicDetail.Partitions = append(t.TopicDetail.Partitions, &api.Partition{Number: p.ID})
				}
			}
		}
	}

	client, err := s.connManager.GetClient(req.Cluster)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	logDirInfo, err := logDirBytes(client)
	if err != nil {
		glog.Error("Failed to fetch LogDir info: %v", err)
		// Ignore this, Log Dir is best-effort
	}

	fmt.Println("ZZ", len(resp.Topics))

	for _, topic := range resp.Topics {
		fmt.Println(topic.Name)
		if l, ok := logDirInfo[topic.Name]; ok {
			topic.LogDirBytes = l
		}
		var ps []int32
		for _, p := range topic.TopicDetail.Partitions {
			ps = append(ps, int32(p.Number))
		}
		fmt.Println("WM")
		wms, _ := s.getHighWatermarks(client, req.Cluster, topic.Name, ps)
		fmt.Println("END WM")
		var msgs int64
		for _, v := range wms {
			msgs += v
		}
		topic.TotalHighWatermarks = msgs
		fmt.Println("TOPICS")
	}

	// Todo get num messages

	fmt.Println("RET")

	return &resp, nil
}

func (s *Service) GetHighWatermarks(ctx context.Context, req *api.GetHighWatermarksRequest) (res *api.GetHighWatermarksResponse, err error) {
	client, err := s.connManager.GetClient(req.Cluster)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	adminClient, err := s.connManager.GetAdminClient(req.Cluster)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	meta, err := adminClient.DescribeTopics(req.Topics)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	res = &api.GetHighWatermarksResponse{
		HighWatermarks: make(map[string]*api.TopicHighWatermarks),
	}
	for _, topic := range req.Topics {
		var partitions []int32
		for _, topicMeta := range meta {
			if topicMeta.Name == topic {
				for _, partitionMeta := range topicMeta.Partitions {
					partitions = append(partitions, partitionMeta.ID)
				}
			}
		}
		wms, err := s.getHighWatermarks(client, req.Cluster, topic, partitions)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}

		t := &api.TopicHighWatermarks{
			HighWatermarks: wms,
		}
		res.HighWatermarks[topic] = t

	}

	return res, nil
}

func (s *Service) getHighWatermarks(client sarama.Client, cluster string, topic string, partitions []int32) (watermarks map[int32]int64, err error) {
	fmt.Println("CALL")
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
			defer wg.Done()
			resp, err := s.connManager.GetAvailableOffsets(leader, cluster, req)
			if err != nil {
				glog.Errorf("Failed to fetch available offset: %v", err)
				return
			}

			watermarksFromLeader := make(map[int32]int64)
			for partition, block := range resp.Blocks[topic] {
				watermarksFromLeader[partition] = block.Offset
			}

			results <- watermarksFromLeader

		}(leader, req)

	}

	fmt.Println("vor wait")
	wg.Wait()
	fmt.Println("nach wait")
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
