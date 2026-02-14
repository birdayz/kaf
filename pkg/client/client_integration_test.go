package client

import (
	"context"
	"testing"
	"time"

	"github.com/birdayz/kaf/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestNewClient(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()

	rpContainer, err := redpanda.Run(ctx, "redpandadata/redpanda:v24.3.1")
	require.NoError(t, err)
	t.Cleanup(func() { _ = rpContainer.Terminate(context.Background()) })

	brokers, err := rpContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	cluster := &config.Cluster{
		Name:    "test",
		Brokers: []string{brokers},
	}

	cl, err := New(cluster)
	require.NoError(t, err)
	require.NotNil(t, cl)
	defer cl.Close()

	assert.NotNil(t, cl.KGO)
	assert.NotNil(t, cl.Admin)

	// Verify the client actually works by pinging the cluster.
	err = cl.KGO.Ping(ctx)
	require.NoError(t, err)
}

func TestNewClientProduceConsume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()

	rpContainer, err := redpanda.Run(ctx, "redpandadata/redpanda:v24.3.1")
	require.NoError(t, err)
	t.Cleanup(func() { _ = rpContainer.Terminate(context.Background()) })

	brokers, err := rpContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	topic := "test-produce-consume"

	// Create a producer client.
	producer, err := New(&config.Cluster{
		Name:    "producer",
		Brokers: []string{brokers},
	})
	require.NoError(t, err)
	defer producer.Close()

	// Create the topic first.
	_, err = producer.Admin.CreateTopic(ctx, 1, 1, nil, topic)
	require.NoError(t, err)

	// Produce a message.
	key := []byte("test-key")
	value := []byte("test-value")
	record := &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: value,
	}
	err = producer.KGO.ProduceSync(ctx, record).FirstErr()
	require.NoError(t, err)

	// Create a consumer client with a consumer group so we get assigned.
	consumer, err := New(&config.Cluster{
		Name:    "consumer",
		Brokers: []string{brokers},
	},
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	defer consumer.Close()

	// Poll for the message with a timeout.
	deadline := time.After(10 * time.Second)
	var fetched *kgo.Record
	for fetched == nil {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for message")
		default:
		}
		fetches := consumer.KGO.PollFetches(ctx)
		fetches.EachRecord(func(r *kgo.Record) {
			fetched = r
		})
	}

	assert.Equal(t, key, fetched.Key)
	assert.Equal(t, value, fetched.Value)
	assert.Equal(t, topic, fetched.Topic)
}

func TestNewClientAdmin(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()

	rpContainer, err := redpanda.Run(ctx, "redpandadata/redpanda:v24.3.1")
	require.NoError(t, err)
	t.Cleanup(func() { _ = rpContainer.Terminate(context.Background()) })

	brokers, err := rpContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	cl, err := New(&config.Cluster{
		Name:    "admin-test",
		Brokers: []string{brokers},
	})
	require.NoError(t, err)
	defer cl.Close()

	topic := "admin-test-topic"

	// Create a topic.
	resp, err := cl.Admin.CreateTopic(ctx, 3, 1, nil, topic)
	require.NoError(t, err)
	require.NoError(t, resp.Err)

	// List topics and verify our topic exists.
	listed, err := cl.Admin.ListTopics(ctx)
	require.NoError(t, err)

	_, exists := listed[topic]
	assert.True(t, exists, "created topic should appear in topic listing")

	details := listed[topic]
	assert.Equal(t, 3, len(details.Partitions), "topic should have 3 partitions")
}

func TestNewClientClose(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()

	rpContainer, err := redpanda.Run(ctx, "redpandadata/redpanda:v24.3.1")
	require.NoError(t, err)
	t.Cleanup(func() { _ = rpContainer.Terminate(context.Background()) })

	brokers, err := rpContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	cl, err := New(&config.Cluster{
		Name:    "close-test",
		Brokers: []string{brokers},
	})
	require.NoError(t, err)

	// Verify it works before closing.
	err = cl.KGO.Ping(ctx)
	require.NoError(t, err)

	// Close the client.
	cl.Close()

	// After closing, producing should fail.
	err = cl.KGO.ProduceSync(ctx, &kgo.Record{
		Topic: "whatever",
		Value: []byte("should-fail"),
	}).FirstErr()
	assert.Error(t, err, "produce after close should fail")
}

func TestNewClientWithOpts(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()

	rpContainer, err := redpanda.Run(ctx, "redpandadata/redpanda:v24.3.1")
	require.NoError(t, err)
	t.Cleanup(func() { _ = rpContainer.Terminate(context.Background()) })

	brokers, err := rpContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	cl, err := New(&config.Cluster{
		Name:    "opts-test",
		Brokers: []string{brokers},
	}, kgo.ClientID("custom-client-id"))
	require.NoError(t, err)
	defer cl.Close()

	// Verify the client is functional - ping and metadata should work.
	err = cl.KGO.Ping(ctx)
	require.NoError(t, err)

	// Also verify we can do admin operations through this client.
	brokerMeta, err := cl.Admin.ListBrokers(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, brokerMeta, "should have at least one broker")
}

func TestNewClientInvalidBroker(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()

	// Use a broker address that nothing is listening on.
	cl, err := New(&config.Cluster{
		Name:    "invalid-broker",
		Brokers: []string{"127.0.0.1:19093"},
	}, kgo.DialTimeout(2*time.Second), kgo.MetadataMinAge(100*time.Millisecond))
	// franz-go doesn't return an error on NewClient with bad brokers;
	// the error surfaces when you actually try to use the connection.
	require.NoError(t, err)
	defer cl.Close()

	// Ping should fail because there's nothing at that address.
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err = cl.KGO.Ping(pingCtx)
	assert.Error(t, err, "ping to invalid broker should fail")
}
