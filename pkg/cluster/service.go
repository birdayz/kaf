package cluster

import (
	"context"

	"github.com/birdayz/kaf/api"
	"github.com/birdayz/kaf/pkg/config"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Service struct{}

func (s *Service) CreateCluster(ctx context.Context, in *api.CreateClusterRequest) (*api.Cluster, error) {
	return nil, nil
}
func (s *Service) GetCluster(ctx context.Context, in *api.GetClusterRequest) (*api.Cluster, error) {
	return nil, nil
}
func (s *Service) UpdateCluster(ctx context.Context, in *api.UpdateClusterRequest) (*api.Cluster, error) {
	return nil, nil
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
func (s *Service) DeleteCluster(ctx context.Context, in *api.DeleteClusterRequest) (*empty.Empty, error) {
	return nil, nil
}
