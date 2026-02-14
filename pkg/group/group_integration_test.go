package group

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func createAdminClient(t *testing.T, brokers string) *kadm.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	require.NoError(t, err)
	t.Cleanup(func() { cl.Close() })
	admin := kadm.NewClient(cl)
	t.Cleanup(func() { admin.Close() })
	return admin
}

func createTopic(t *testing.T, ctx context.Context, admin *kadm.Client, topic string, partitions int32) {
	t.Helper()
	resp, err := admin.CreateTopics(ctx, partitions, 1, nil, topic)
	require.NoError(t, err)
	for _, r := range resp {
		require.NoError(t, r.Err)
	}
}

func produceMessages(t *testing.T, ctx context.Context, brokers string, topic string, count int) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	require.NoError(t, err)
	defer cl.Close()
	for i := range count {
		res := cl.ProduceSync(ctx, &kgo.Record{
			Topic: topic,
			Value: fmt.Appendf(nil, "msg-%d", i),
		})
		require.NoError(t, res.FirstErr())
	}
}

// consumeWithGroup creates a consumer group by consuming all available
// messages from the given topic and committing offsets.
func consumeWithGroup(t *testing.T, ctx context.Context, brokers string, groupName string, topic string) {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ConsumerGroup(groupName),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)

	pollCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// Poll until we get records or timeout.
	for {
		fetches := cl.PollFetches(pollCtx)
		if fetches.NumRecords() > 0 || pollCtx.Err() != nil {
			break
		}
	}

	err = cl.CommitUncommittedOffsets(ctx)
	require.NoError(t, err)
	cl.Close()
}

func startContainer(t *testing.T, ctx context.Context) string {
	t.Helper()
	rpContainer, err := redpanda.Run(ctx, "redpandadata/redpanda:v24.3.1")
	require.NoError(t, err)
	t.Cleanup(func() { _ = rpContainer.Terminate(context.Background()) })

	brokers, err := rpContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)
	return brokers
}

func TestListEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	brokers := startContainer(t, ctx)
	admin := createAdminClient(t, brokers)

	groups, err := List(ctx, admin)
	require.NoError(t, err)
	assert.Empty(t, groups)
}

func TestListWithGroups(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	brokers := startContainer(t, ctx)
	admin := createAdminClient(t, brokers)

	topic := "test-list-topic"
	createTopic(t, ctx, admin, topic, 1)
	produceMessages(t, ctx, brokers, topic, 5)

	consumeWithGroup(t, ctx, brokers, "group-alpha", topic)
	consumeWithGroup(t, ctx, brokers, "group-beta", topic)

	groups, err := List(ctx, admin)
	require.NoError(t, err)
	require.Len(t, groups, 2)

	names := make([]string, len(groups))
	for i, g := range groups {
		names[i] = g.Name
	}
	assert.Contains(t, names, "group-alpha")
	assert.Contains(t, names, "group-beta")
}

func TestDescribe(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	brokers := startContainer(t, ctx)
	admin := createAdminClient(t, brokers)

	topic := "test-describe-topic"
	createTopic(t, ctx, admin, topic, 1)
	produceMessages(t, ctx, brokers, topic, 10)
	consumeWithGroup(t, ctx, brokers, "describe-group", topic)

	desc, err := Describe(ctx, admin, "describe-group", nil)
	require.NoError(t, err)

	assert.Equal(t, "describe-group", desc.Group)
	// After consumer closed, state should be Empty.
	assert.Equal(t, "Empty", desc.State)

	require.Len(t, desc.Topics, 1)
	assert.Equal(t, topic, desc.Topics[0].Topic)
	require.Len(t, desc.Topics[0].Partitions, 1)

	p := desc.Topics[0].Partitions[0]
	assert.Equal(t, int32(0), p.Partition)
	assert.Equal(t, int64(10), p.GroupOffset)
	assert.Equal(t, int64(10), p.HighWatermark)
	assert.Equal(t, int64(0), p.Lag)
}

func TestDescribeWithTopicFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	brokers := startContainer(t, ctx)
	admin := createAdminClient(t, brokers)

	topicA := "filter-topic-a"
	topicB := "filter-topic-b"
	createTopic(t, ctx, admin, topicA, 1)
	createTopic(t, ctx, admin, topicB, 1)
	produceMessages(t, ctx, brokers, topicA, 5)
	produceMessages(t, ctx, brokers, topicB, 5)

	// Consume both topics in the same group.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ConsumerGroup("filter-group"),
		kgo.ConsumeTopics(topicA, topicB),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)

	pollCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	for {
		fetches := cl.PollFetches(pollCtx)
		if fetches.NumRecords() > 0 || pollCtx.Err() != nil {
			break
		}
	}
	// Poll a second time to make sure we get records from both topics.
	for {
		fetches := cl.PollFetches(pollCtx)
		if fetches.NumRecords() > 0 || pollCtx.Err() != nil {
			break
		}
	}
	err = cl.CommitUncommittedOffsets(ctx)
	require.NoError(t, err)
	cl.Close()

	// Describe with filter for topicA only.
	desc, err := Describe(ctx, admin, "filter-group", []string{topicA})
	require.NoError(t, err)

	require.Len(t, desc.Topics, 1)
	assert.Equal(t, topicA, desc.Topics[0].Topic)
}

func TestDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	brokers := startContainer(t, ctx)
	admin := createAdminClient(t, brokers)

	topic := "test-delete-topic"
	createTopic(t, ctx, admin, topic, 1)
	produceMessages(t, ctx, brokers, topic, 5)
	consumeWithGroup(t, ctx, brokers, "delete-group", topic)

	// Verify the group exists.
	groups, err := ListGroupNames(ctx, admin)
	require.NoError(t, err)
	assert.Contains(t, groups, "delete-group")

	// Delete it.
	err = Delete(ctx, admin, "delete-group")
	require.NoError(t, err)

	// Verify it is gone.
	groups, err = ListGroupNames(ctx, admin)
	require.NoError(t, err)
	assert.NotContains(t, groups, "delete-group")
}

func TestCommitOffsets(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	brokers := startContainer(t, ctx)
	admin := createAdminClient(t, brokers)

	topic := "test-commit-topic"
	createTopic(t, ctx, admin, topic, 2)
	produceMessages(t, ctx, brokers, topic, 10)

	// Create the group first so it exists (consume then close).
	consumeWithGroup(t, ctx, brokers, "commit-group", topic)

	// Now commit specific offsets.
	offsets := map[string]map[int32]int64{
		topic: {
			0: 3,
			1: 5,
		},
	}
	err := CommitOffsets(ctx, admin, "commit-group", offsets)
	require.NoError(t, err)

	// Verify via Describe.
	desc, err := Describe(ctx, admin, "commit-group", nil)
	require.NoError(t, err)

	require.Len(t, desc.Topics, 1)
	assert.Equal(t, topic, desc.Topics[0].Topic)

	partOffsets := make(map[int32]int64)
	for _, p := range desc.Topics[0].Partitions {
		partOffsets[p.Partition] = p.GroupOffset
	}
	assert.Equal(t, int64(3), partOffsets[0])
	assert.Equal(t, int64(5), partOffsets[1])
}

func TestListGroupNames(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	brokers := startContainer(t, ctx)
	admin := createAdminClient(t, brokers)

	topic := "test-names-topic"
	createTopic(t, ctx, admin, topic, 1)
	produceMessages(t, ctx, brokers, topic, 5)

	consumeWithGroup(t, ctx, brokers, "zulu-group", topic)
	consumeWithGroup(t, ctx, brokers, "alpha-group", topic)
	consumeWithGroup(t, ctx, brokers, "mike-group", topic)

	names, err := ListGroupNames(ctx, admin)
	require.NoError(t, err)
	require.Len(t, names, 3)

	// Verify sorted order.
	assert.Equal(t, "alpha-group", names[0])
	assert.Equal(t, "mike-group", names[1])
	assert.Equal(t, "zulu-group", names[2])
}

func TestDescribeNonExistent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	brokers := startContainer(t, ctx)
	admin := createAdminClient(t, brokers)

	_, err := Describe(ctx, admin, "non-existent-group", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestDescribeLag(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	brokers := startContainer(t, ctx)
	admin := createAdminClient(t, brokers)

	topic := "test-lag-topic"
	createTopic(t, ctx, admin, topic, 1)
	produceMessages(t, ctx, brokers, topic, 10)

	// Create a group and consume all messages.
	consumeWithGroup(t, ctx, brokers, "lag-group", topic)

	// Produce more messages after the group has committed.
	produceMessages(t, ctx, brokers, topic, 5)

	desc, err := Describe(ctx, admin, "lag-group", nil)
	require.NoError(t, err)

	require.Len(t, desc.Topics, 1)
	require.Len(t, desc.Topics[0].Partitions, 1)

	p := desc.Topics[0].Partitions[0]
	// Group committed at offset 10, HWM should be 15.
	assert.Equal(t, int64(10), p.GroupOffset)
	assert.Equal(t, int64(15), p.HighWatermark)
	assert.Equal(t, int64(5), p.Lag)
	assert.Equal(t, int64(5), desc.Topics[0].TotalLag)
}
