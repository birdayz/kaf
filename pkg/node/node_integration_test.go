package node

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestList(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()

	rpContainer, err := redpanda.Run(ctx, "redpandadata/redpanda:v24.3.1")
	require.NoError(t, err)
	t.Cleanup(func() { _ = rpContainer.Terminate(context.Background()) })

	brokers, err := rpContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	require.NoError(t, err)
	defer cl.Close()
	admin := kadm.NewClient(cl)
	defer admin.Close()

	nodes, err := List(ctx, admin)
	require.NoError(t, err)

	require.Len(t, nodes, 1, "single-node Redpanda cluster should return exactly one node")

	n := nodes[0]
	assert.NotEmpty(t, n.Host, "node host should not be empty")
	assert.NotZero(t, n.Port, "node port should not be zero")
	assert.True(t, n.IsController, "single node must be the controller")
}

func TestListNodeAddr(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()

	rpContainer, err := redpanda.Run(ctx, "redpandadata/redpanda:v24.3.1")
	require.NoError(t, err)
	t.Cleanup(func() { _ = rpContainer.Terminate(context.Background()) })

	brokers, err := rpContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	require.NoError(t, err)
	defer cl.Close()
	admin := kadm.NewClient(cl)
	defer admin.Close()

	nodes, err := List(ctx, admin)
	require.NoError(t, err)
	require.Len(t, nodes, 1)

	n := nodes[0]
	expected := fmt.Sprintf("%s:%d", n.Host, n.Port)
	assert.Equal(t, expected, n.Addr(), "Addr() should return host:port")
	assert.Contains(t, n.Addr(), ":", "Addr() must contain a colon separator")
}

func TestListMultipleCalls(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()

	rpContainer, err := redpanda.Run(ctx, "redpandadata/redpanda:v24.3.1")
	require.NoError(t, err)
	t.Cleanup(func() { _ = rpContainer.Terminate(context.Background()) })

	brokers, err := rpContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	require.NoError(t, err)
	defer cl.Close()
	admin := kadm.NewClient(cl)
	defer admin.Close()

	first, err := List(ctx, admin)
	require.NoError(t, err)
	require.Len(t, first, 1)

	for range 3 {
		got, err := List(ctx, admin)
		require.NoError(t, err)
		require.Len(t, got, 1)

		assert.Equal(t, first[0].ID, got[0].ID, "node ID should be consistent across calls")
		assert.Equal(t, first[0].Host, got[0].Host, "node host should be consistent across calls")
		assert.Equal(t, first[0].Port, got[0].Port, "node port should be consistent across calls")
		assert.Equal(t, first[0].IsController, got[0].IsController, "controller status should be consistent across calls")
		assert.Equal(t, first[0].Addr(), got[0].Addr(), "Addr() should be consistent across calls")
	}
}
