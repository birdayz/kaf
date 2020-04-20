package topic

import (
	context "context"
	"fmt"
	"sort"
	"time"

	"github.com/Shopify/sarama"
	"github.com/birdayz/kaf/api"
	empty "github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Service struct {
	config  *sarama.Config
	brokers []string

	adminClient sarama.ClusterAdmin
	client      sarama.Client
}

func NewService(brokers []string, config *sarama.Config) (*Service, error) {
	config.Metadata.Full = true
	config.Metadata.RefreshFrequency = time.Minute
	config.Metadata.Retry.Max = 99
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, err
	}

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return nil, err
	}

	return &Service{
		config:      config,
		brokers:     brokers,
		client:      client,
		adminClient: admin,
	}, nil
}

func (s *Service) CreateTopic(context.Context, *api.CreateTopicRequest) (*api.Topic, error) {
	fmt.Println("test")
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
func (s *Service) ListTopics(context.Context, *api.ListTopicsRequest) (*api.ListTopicsResponse, error) {
	s.client.RefreshController()
	s.client.RefreshMetadata()

	topics, err := s.adminClient.ListTopics()

	fmt.Println(err)
	if err != nil {
		s.adminClient.Close()
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
