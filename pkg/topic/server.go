package topic

import (
	context "context"

	empty "github.com/golang/protobuf/ptypes/empty"
)

type Service struct {
}

func (s *Service) CreateTopic(context.Context, *CreateTopicRequest) (*Topic, error) {
	return nil, nil
}
func (s *Service) GetTopic(context.Context, *GetTopicRequest) (*Topic, error) {

	return nil, nil
}
func (s *Service) UpdateTopic(context.Context, *UpdateTopicRequest) (*Topic, error) {

	return nil, nil
}
func (s *Service) ListTopics(context.Context, *ListTopicsRequest) (*ListTopicsResponse, error) {

	return nil, nil
}
func (s *Service) DeleteTopic(context.Context, *DeleteTopicRequest) (*empty.Empty, error) {

	return nil, nil
}
