package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/birdayz/kaf/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Helper function to create a topic with test messages for consume tests
func setupConsumeTestTopic(t *testing.T, kafkaAddr, topicName string, messages []struct {
	key     string
	value   string
	headers map[string]string
}) {
	ctx := context.Background()

	client, err := kgo.NewClient(kgo.SeedBrokers(kafkaAddr))
	require.NoError(t, err)
	defer client.Close()

	admin := kadm.NewClient(client)

	// Create test topic
	topicResp, err := admin.CreateTopics(ctx, 2, 1, nil, topicName)
	require.NoError(t, err)
	_, err = topicResp.On(topicName, nil)
	require.NoError(t, err)

	// Produce test messages
	for i, msg := range messages {
		record := &kgo.Record{
			Topic:     topicName,
			Partition: int32(i % 2),
			Key:       []byte(msg.key),
			Value:     []byte(msg.value),
		}

		for k, v := range msg.headers {
			record.Headers = append(record.Headers, kgo.RecordHeader{
				Key:   k,
				Value: []byte(v),
			})
		}

		results := client.ProduceSync(ctx, record)
		require.NoError(t, results.FirstErr())
	}

	// Wait for messages to be available
	time.Sleep(2 * time.Second)

	// Debug: verify messages were actually produced
	t.Logf("Produced %d messages to topic %s", len(messages), topicName)
}

// runConsumeDirectly calls the consume function directly, bypassing CLI global state
func runConsumeDirectly(t *testing.T, kafkaAddr string, opts ConsumeOptions) string {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	// Capture output
	oldOut := outWriter
	oldErr := errWriter
	oldColorableOut := colorableOut

	var outBuf, errBuf bytes.Buffer
	outWriter = &outBuf
	errWriter = &errBuf
	colorableOut = &outBuf // Also redirect colorableOut to our buffer

	defer func() {
		outWriter = oldOut
		errWriter = oldErr
		colorableOut = oldColorableOut
	}()

	// Set the current cluster to use the test Kafka address
	oldCluster := currentCluster
	currentCluster = &config.Cluster{
		Brokers: []string{kafkaAddr},
	}
	defer func() {
		currentCluster = oldCluster
	}()

	fmt.Println("git ghere")
	// Call the consume function directly
	err := ConsumeMessages(ctx, opts)

	// For consume commands, context cancellation is expected when using timeouts
	if err != nil && ctx.Err() == context.DeadlineExceeded {
		t.Logf("Consume command timed out as expected: %v", err)
	} else if err != nil {
		t.Logf("Consume failed: %v", err)
		panic("xxxxxxXX")
	}

	// Return the captured output (both stdout and stderr)
	return errBuf.String() + outBuf.String()
}

func TestConsumeFromBeginning(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	kafkaAddr := getSharedKafka(t)

	topicName := "test-consume-from-beginning"

	messages := []struct {
		key     string
		value   string
		headers map[string]string
	}{
		{key: "key1", value: "Simple message 1"},
		{key: "key2", value: "Simple message 2"},
	}

	setupConsumeTestTopic(t, kafkaAddr, topicName, messages)

	output := runConsumeDirectly(t, kafkaAddr, ConsumeOptions{
		Topic:         topicName,
		OffsetFlag:    "oldest",
		LimitMessages: 2,
	})

	// Should contain message content
	assert.Contains(t, output, "Simple message 1")
	assert.Contains(t, output, "Partition:")
	assert.Contains(t, output, "Offset:")
	assert.Contains(t, output, "Timestamp:")
}

func TestConsumeRawFormat(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Add delay to prevent Docker conflicts
	time.Sleep(3 * time.Second)

	kafkaAddr := getSharedKafka(t)

	topicName := "test-consume-raw-format"

	messages := []struct {
		key     string
		value   string
		headers map[string]string
	}{
		{key: "key1", value: "Simple message 1"},
	}

	setupConsumeTestTopic(t, kafkaAddr, topicName, messages)

	// Use the direct function call instead of CLI to avoid global state issues
	output := runConsumeDirectly(t, kafkaAddr, ConsumeOptions{
		Topic:         topicName,
		OffsetFlag:    "oldest",
		OutputFormat:  OutputFormatRaw,
		LimitMessages: 1,
	})

	// Debug output
	t.Logf("Raw format output: %q (length: %d)", output, len(output))

	// Raw format should only contain message content
	assert.NotContains(t, output, "Partition:")
	assert.NotContains(t, output, "Offset:")
	// Should contain actual message content
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) > 0 {
		assert.NotEmpty(t, lines[0])
	}
}

func TestConsumeJSONFormat(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	kafkaAddr := getSharedKafka(t)

	topicName := "test-consume-json-format"

	messages := []struct {
		key     string
		value   string
		headers map[string]string
	}{
		{key: "key1", value: "Simple message 1"},
	}

	setupConsumeTestTopic(t, kafkaAddr, topicName, messages)

	output := runConsumeDirectly(t, kafkaAddr, ConsumeOptions{
		Topic:         topicName,
		OffsetFlag:    "oldest",
		OutputFormat:  OutputFormatJSON,
		LimitMessages: 1,
	})

	// JSON format should contain structured data
	assert.Contains(t, output, `"partition"`)
	assert.Contains(t, output, `"offset"`)
	assert.Contains(t, output, `"timestamp"`)
	assert.Contains(t, output, `"payload"`)
}

func TestConsumeWithHeaders(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	kafkaAddr := getSharedKafka(t)

	topicName := "test-consume-with-headers"

	messages := []struct {
		key     string
		value   string
		headers map[string]string
	}{
		{
			key:   "key2",
			value: `{"id": 1, "name": "JSON Message"}`,
			headers: map[string]string{
				"Content-Type": "application/json",
				"Source":       "test",
			},
		},
	}

	setupConsumeTestTopic(t, kafkaAddr, topicName, messages)

	output := runConsumeDirectly(t, kafkaAddr, ConsumeOptions{
		Topic:         topicName,
		OffsetFlag:    "oldest",
		LimitMessages: 1,
	})

	// Should display headers for messages that have them
	assert.Contains(t, output, "Headers:")
	assert.Contains(t, output, "Content-Type")
	assert.Contains(t, output, "application/json")
}

func TestConsumeWithKeyDisplay(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	kafkaAddr := getSharedKafka(t)

	topicName := "test-consume-with-key-display"

	messages := []struct {
		key     string
		value   string
		headers map[string]string
	}{
		{key: "key1", value: "Simple message 1"},
	}

	setupConsumeTestTopic(t, kafkaAddr, topicName, messages)

	output := runConsumeDirectly(t, kafkaAddr, ConsumeOptions{
		Topic:         topicName,
		OffsetFlag:    "oldest",
		LimitMessages: 1,
	})

	// Should display keys for messages that have them
	assert.Contains(t, output, "Key:")
	assert.Contains(t, output, "key1")
}

func TestConsumeSpecificPartitions(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	kafkaAddr := getSharedKafka(t)

	topicName := "test-consume-specific-partitions"

	messages := []struct {
		key     string
		value   string
		headers map[string]string
	}{
		{key: "key1", value: "Simple message 1"},
	}

	setupConsumeTestTopic(t, kafkaAddr, topicName, messages)

	output := runConsumeDirectly(t, kafkaAddr, ConsumeOptions{
		Topic:         topicName,
		OffsetFlag:    "oldest",
		Partitions:    []int32{0},
		LimitMessages: 1,
	})

	// Should consume from partition 0 only
	assert.NotEmpty(t, strings.TrimSpace(output))
}

func TestConsumeMultiplePartitions(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	kafkaAddr := getSharedKafka(t)

	topicName := "test-consume-multiple-partitions"

	messages := []struct {
		key     string
		value   string
		headers map[string]string
	}{
		{key: "key1", value: "Simple message 1"},
		{key: "key2", value: "Simple message 2"},
	}

	setupConsumeTestTopic(t, kafkaAddr, topicName, messages)

	output := runConsumeDirectly(t, kafkaAddr, ConsumeOptions{
		Topic:         topicName,
		OffsetFlag:    "oldest",
		Partitions:    []int32{0, 1},
		LimitMessages: 2,
	})

	// Should consume from both partitions
	assert.NotEmpty(t, strings.TrimSpace(output))
}

func TestConsumeWithCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	kafkaAddr := getSharedKafka(t)

	topicName := "test-consume-with-commit"
	groupName := "test-consume-commit-group"

	messages := []struct {
		key     string
		value   string
		headers map[string]string
	}{
		{key: "key1", value: "commit test message"},
	}

	setupConsumeTestTopic(t, kafkaAddr, topicName, messages)

	output := runConsumeDirectly(t, kafkaAddr, ConsumeOptions{
		Topic:           topicName,
		OffsetFlag:      "oldest",
		GroupFlag:       groupName,
		GroupCommitFlag: true,
		LimitMessages:   1,
	})

	// Should consume and commit
	assert.NotEmpty(t, strings.TrimSpace(output))
}

func TestConsumeFromNonExistentTopic(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	kafkaAddr := getSharedKafka(t)

	output := runConsumeDirectly(t, kafkaAddr, ConsumeOptions{
		Topic:         "non-existent-topic-12345",
		OffsetFlag:    "oldest",
		LimitMessages: 1,
	})

	// Should handle gracefully (may auto-create topic or show error)
	assert.NotNil(t, output)
}

func TestConsumeWithInvalidOffset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	kafkaAddr := getSharedKafka(t)

	topicName := "test-consume-invalid-offset"

	messages := []struct {
		key     string
		value   string
		headers map[string]string
	}{
		{key: "key1", value: "Simple message 1"},
	}

	setupConsumeTestTopic(t, kafkaAddr, topicName, messages)

	// This test should expect an error when using invalid offset
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	// Set up the cluster config
	oldCluster := currentCluster
	currentCluster = &config.Cluster{
		Brokers: []string{kafkaAddr},
	}
	defer func() {
		currentCluster = oldCluster
	}()

	err := ConsumeMessages(ctx, ConsumeOptions{
		Topic:         topicName,
		OffsetFlag:    "invalid-offset",
		LimitMessages: 1,
	})

	// Should return an error for invalid offset
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not parse")
}
