package node

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
)

// Node represents a Kafka broker.
type Node struct {
	ID           int32
	Host         string
	Port         int32
	IsController bool
}

// Addr returns the broker address as "host:port".
func (n Node) Addr() string {
	return fmt.Sprintf("%s:%d", n.Host, n.Port)
}

// List returns all brokers in the cluster.
func List(ctx context.Context, admin *kadm.Client) ([]Node, error) {
	meta, err := admin.BrokerMetadata(ctx)
	if err != nil {
		return nil, err
	}

	nodes := make([]Node, 0, len(meta.Brokers))
	for _, b := range meta.Brokers {
		nodes = append(nodes, Node{
			ID:           b.NodeID,
			Host:         b.Host,
			Port:         b.Port,
			IsController: b.NodeID == meta.Controller,
		})
	}

	return nodes, nil
}
