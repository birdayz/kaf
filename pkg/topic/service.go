package topic

import (
	context "context"
	"fmt"

	"github.com/birdayz/kaf/api"
	empty "github.com/golang/protobuf/ptypes/empty"
)

type Service struct {
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

	return nil, nil
}
func (s *Service) DeleteTopic(context.Context, *api.DeleteTopicRequest) (*empty.Empty, error) {

	return nil, nil
}
