package cluster

import (
	"context"

	"github.com/birdayz/kaf/api"
	"github.com/birdayz/kaf/pkg/config"
	"github.com/birdayz/kaf/pkg/connection"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Service struct {
	c *connection.ConnManager
}

func NewService(c *connection.ConnManager) *Service {
	return &Service{c: c}
}

func (s *Service) ListClusters(ctx context.Context, in *api.ListClustersRequest) (*api.ListClustersResponse, error) {
	cfg, err := config.ReadConfig("")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	var resp api.ListClustersResponse

	for _, c := range cfg.Clusters {
		resp.Clusters = append(resp.Clusters, &api.Cluster{Name: c.Name})
	}

	return &resp, nil
}

func (s *Service) ConnectCluster(ctx context.Context, in *api.ConnectClusterRequest) (*api.ConnectClusterResponse, error) {
	err := s.c.Connect(in.Name)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &api.ConnectClusterResponse{}, nil
}
