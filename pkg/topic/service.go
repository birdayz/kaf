package topic

import (
	context "context"
	"fmt"
	"sort"

	"github.com/Shopify/sarama"
	"github.com/birdayz/kaf/api"
	"github.com/birdayz/kaf/pkg/connection"
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
	fmt.Println("abc", req.Cluster)
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
		})
	}

	return &resp, nil
}
func (s *Service) DeleteTopic(context.Context, *api.DeleteTopicRequest) (*empty.Empty, error) {
	return nil, nil
}
