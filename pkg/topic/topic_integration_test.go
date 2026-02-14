package topic

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func setupAdmin(t *testing.T, ctx context.Context) *kadm.Client {
	t.Helper()

	rpContainer, err := redpanda.Run(ctx, "redpandadata/redpanda:v24.3.1")
	require.NoError(t, err)
	t.Cleanup(func() { _ = rpContainer.Terminate(context.Background()) })

	brokers, err := rpContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	require.NoError(t, err)
	t.Cleanup(func() { cl.Close() })

	admin := kadm.NewClient(cl)
	t.Cleanup(func() { admin.Close() })

	return admin
}

func TestCreate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	admin := setupAdmin(t, ctx)

	err := Create(ctx, admin, "test-topic", 1, 1, nil)
	require.NoError(t, err)

	topics, err := List(ctx, admin)
	require.NoError(t, err)

	var found bool
	for _, ti := range topics {
		if ti.Name == "test-topic" {
			found = true
			break
		}
	}
	require.True(t, found, "created topic should appear in list")
}

func TestCreateWithCompaction(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	admin := setupAdmin(t, ctx)

	compactVal := "compact"
	err := Create(ctx, admin, "compacted-topic", 1, 1, map[string]*string{
		"cleanup.policy": &compactVal,
	})
	require.NoError(t, err)

	desc, err := Describe(ctx, admin, "compacted-topic")
	require.NoError(t, err)

	assert.Equal(t, "compacted-topic", desc.Name)
	assert.True(t, desc.Compacted, "topic should be marked as compacted")
}

func TestList(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	admin := setupAdmin(t, ctx)

	// Create topics in non-alphabetical order.
	require.NoError(t, Create(ctx, admin, "charlie", 3, 1, nil))
	require.NoError(t, Create(ctx, admin, "alpha", 1, 1, nil))
	require.NoError(t, Create(ctx, admin, "bravo", 2, 1, nil))

	topics, err := List(ctx, admin)
	require.NoError(t, err)

	// Filter to only our test topics.
	expected := map[string]struct{}{
		"alpha":   {},
		"bravo":   {},
		"charlie": {},
	}
	var filtered []TopicInfo
	for _, ti := range topics {
		if _, ok := expected[ti.Name]; ok {
			filtered = append(filtered, ti)
		}
	}
	require.Len(t, filtered, 3)

	// Verify sorted order.
	assert.Equal(t, "alpha", filtered[0].Name)
	assert.Equal(t, "bravo", filtered[1].Name)
	assert.Equal(t, "charlie", filtered[2].Name)

	// Verify partition counts.
	assert.Equal(t, int32(1), filtered[0].Partitions)
	assert.Equal(t, int32(2), filtered[1].Partitions)
	assert.Equal(t, int32(3), filtered[2].Partitions)

	// Verify replication factor (single node = 1).
	for _, ti := range filtered {
		assert.Equal(t, int16(1), ti.ReplicationFactor)
	}
}

func TestDescribe(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	admin := setupAdmin(t, ctx)

	require.NoError(t, Create(ctx, admin, "describe-me", 3, 1, nil))

	desc, err := Describe(ctx, admin, "describe-me")
	require.NoError(t, err)

	assert.Equal(t, "describe-me", desc.Name)
	assert.False(t, desc.IsInternal)
	assert.False(t, desc.Compacted)

	// Verify partitions.
	require.Len(t, desc.Partitions, 3)
	for i, p := range desc.Partitions {
		assert.Equal(t, int32(i), p.ID, "partition IDs should be sequential")
		assert.GreaterOrEqual(t, p.Leader, int32(0), "leader should be a valid node")
		assert.NotEmpty(t, p.Replicas, "replicas should not be empty")
		assert.NotEmpty(t, p.ISR, "ISR should not be empty")
		assert.GreaterOrEqual(t, p.HighWatermark, int64(0), "high watermark should be non-negative")
	}

	// Verify configs are populated.
	assert.NotEmpty(t, desc.Configs, "configs should not be empty")
}

func TestDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	admin := setupAdmin(t, ctx)

	require.NoError(t, Create(ctx, admin, "to-delete", 1, 1, nil))

	// Confirm it exists.
	topics, err := List(ctx, admin)
	require.NoError(t, err)
	var found bool
	for _, ti := range topics {
		if ti.Name == "to-delete" {
			found = true
		}
	}
	require.True(t, found, "topic should exist before deletion")

	// Delete.
	require.NoError(t, Delete(ctx, admin, "to-delete"))

	// Confirm it's gone.
	topics, err = List(ctx, admin)
	require.NoError(t, err)
	for _, ti := range topics {
		assert.NotEqual(t, "to-delete", ti.Name, "deleted topic should not appear in list")
	}
}

func TestSetConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	admin := setupAdmin(t, ctx)

	require.NoError(t, Create(ctx, admin, "config-topic", 1, 1, nil))

	err := SetConfig(ctx, admin, "config-topic", map[string]string{
		"retention.ms": "86400000",
	})
	require.NoError(t, err)

	desc, err := Describe(ctx, admin, "config-topic")
	require.NoError(t, err)

	var retentionFound bool
	for _, c := range desc.Configs {
		if c.Name == "retention.ms" {
			retentionFound = true
			assert.Equal(t, "86400000", c.Value)
			assert.False(t, c.IsDefault, "explicitly set config should not be marked as default")
			break
		}
	}
	require.True(t, retentionFound, "retention.ms config should be present")
}

func TestUpdatePartitions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	admin := setupAdmin(t, ctx)

	require.NoError(t, Create(ctx, admin, "grow-partitions", 1, 1, nil))

	// Verify initial state.
	desc, err := Describe(ctx, admin, "grow-partitions")
	require.NoError(t, err)
	require.Len(t, desc.Partitions, 1)

	// Increase to 3 partitions.
	require.NoError(t, UpdatePartitions(ctx, admin, "grow-partitions", 3))

	desc, err = Describe(ctx, admin, "grow-partitions")
	require.NoError(t, err)
	assert.Len(t, desc.Partitions, 3)
}

func TestDescribeNonExistent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	admin := setupAdmin(t, ctx)

	_, err := Describe(ctx, admin, "does-not-exist")
	require.Error(t, err)
}

func TestCreateDuplicate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	admin := setupAdmin(t, ctx)

	require.NoError(t, Create(ctx, admin, "dup-topic", 1, 1, nil))

	err := Create(ctx, admin, "dup-topic", 1, 1, nil)
	require.Error(t, err, "creating a duplicate topic should return an error")
}
