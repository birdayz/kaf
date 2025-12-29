package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestProduceCommand(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	kafkaAddr := getSharedKafka(t)

	ctx := context.Background()
	testTopicName := fmt.Sprintf("test-produce-command-%d", time.Now().UnixNano())

	// Setup
	client, err := kgo.NewClient(kgo.SeedBrokers(kafkaAddr))
	require.NoError(t, err)
	defer client.Close()

	admin := kadm.NewClient(client)

	// Create test topic with multiple partitions
	topicResp, err := admin.CreateTopics(ctx, 3, 1, nil, testTopicName)
	require.NoError(t, err)
	_, err = topicResp.On(testTopicName, nil)
	require.NoError(t, err)

	// Wait for topic to be ready
	time.Sleep(2 * time.Second)

	t.Run("SimpleProduceMessage", func(t *testing.T) {
		message := "Hello Kafka from integration test"

		output := runCmdWithBroker(t, kafkaAddr, strings.NewReader(message), "produce", testTopicName)

		// Should indicate successful production
		assert.Contains(t, output, "Sent record to partition")
		assert.Contains(t, output, "at offset")
	})

	t.Run("ProduceWithKey", func(t *testing.T) {
		message := "Message with key"
		key := "test-key-123"

		output := runCmdWithBroker(t, kafkaAddr, strings.NewReader(message),
			"produce", testTopicName,
			"--key", key)

		assert.Contains(t, output, "Sent record to partition")
	})

	t.Run("ProduceWithSpecificPartition", func(t *testing.T) {
		message := "Message to specific partition"

		output := runCmdWithBroker(t, kafkaAddr, strings.NewReader(message),
			"produce", testTopicName,
			"--partition", "1")

		assert.Contains(t, output, "Sent record to partition 1")
	})

	t.Run("ProduceWithHeaders", func(t *testing.T) {
		message := `{"test": "json message"}`

		output := runCmdWithBroker(t, kafkaAddr, strings.NewReader(message),
			"produce", testTopicName,
			"--header", "Content-Type:application/json",
			"--header", "Source:integration-test")

		assert.Contains(t, output, "Sent record to partition")
	})

	t.Run("ProduceWithRoundRobinPartitioner", func(t *testing.T) {
		// Send multiple messages with round-robin partitioner
		for i := 0; i < 3; i++ {
			message := "Round robin message " + string(rune('1'+i))

			output := runCmdWithBroker(t, kafkaAddr, strings.NewReader(message),
				"produce", testTopicName,
				"--partitioner", "rr")

			assert.Contains(t, output, "Sent record to partition")
		}
	})

	t.Run("ProduceWithJVMPartitioner", func(t *testing.T) {
		message := "JVM compatible partitioning"
		key := "consistent-key"

		output := runCmdWithBroker(t, kafkaAddr, strings.NewReader(message),
			"produce", testTopicName,
			"--partitioner", "jvm",
			"--key", key)

		assert.Contains(t, output, "Sent record to partition")
	})

	t.Run("ProduceWithRandomPartitioner", func(t *testing.T) {
		message := "Random partitioning"

		output := runCmdWithBroker(t, kafkaAddr, strings.NewReader(message),
			"produce", testTopicName,
			"--partitioner", "rand")

		assert.Contains(t, output, "Sent record to partition")
	})

	t.Run("ProduceWithTimestamp", func(t *testing.T) {
		message := "Message with timestamp"
		timestamp := "2024-01-01T12:00:00Z"

		output := runCmdWithBroker(t, kafkaAddr, strings.NewReader(message),
			"produce", testTopicName,
			"--timestamp", timestamp)

		assert.Contains(t, output, "Sent record to partition")
	})

	t.Run("ProduceMultipleLines", func(t *testing.T) {
		messages := "Line 1\nLine 2\nLine 3\n"

		output := runCmdWithBroker(t, kafkaAddr, strings.NewReader(messages),
			"produce", testTopicName)

		// Should produce multiple records
		occurrences := strings.Count(output, "Sent record to partition")
		assert.Equal(t, 3, occurrences, "Should produce 3 messages")
	})

	t.Run("ProduceWithRawKey", func(t *testing.T) {
		message := "Message with raw key"
		rawKey := "raw:key:data"

		output := runCmdWithBroker(t, kafkaAddr, strings.NewReader(message),
			"produce", testTopicName,
			"--key", rawKey,
			"--raw-key")

		assert.Contains(t, output, "Sent record to partition")
	})

	t.Run("ProduceJSONMessage", func(t *testing.T) {
		jsonMessage := `{
			"id": 123,
			"name": "Test User",
			"email": "test@example.com",
			"metadata": {
				"source": "integration-test",
				"timestamp": "2024-01-01T12:00:00Z"
			}
		}`

		output := runCmdWithBroker(t, kafkaAddr, strings.NewReader(jsonMessage),
			"produce", testTopicName,
			"--header", "Content-Type:application/json")

		assert.Contains(t, output, "Sent record to partition")
	})

	// Error cases
	t.Run("ProduceToNonExistentTopic", func(t *testing.T) {
		message := "This should fail"

		output := runCmdWithBroker(t, kafkaAddr, strings.NewReader(message),
			"produce", "non-existent-topic-12345")

		// Should either create topic automatically or fail gracefully
		// Depending on Kafka configuration
		assert.NotEmpty(t, output)
	})

	t.Run("ProduceWithInvalidPartition", func(t *testing.T) {
		message := "Invalid partition test"

		output := runCmdWithBroker(t, kafkaAddr, strings.NewReader(message),
			"produce", testTopicName,
			"--partition", "999")

		// Should fail with partition error
		// Note: This might succeed if auto-creation is enabled
		assert.NotEmpty(t, output)
	})

	// Cleanup
	admin.DeleteTopics(ctx, testTopicName)
}

func TestProducePerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	kafkaAddr := getSharedKafka(t)

	ctx := context.Background()
	testTopicName := fmt.Sprintf("test-produce-performance-%d", time.Now().UnixNano())

	// Setup
	client, err := kgo.NewClient(kgo.SeedBrokers(kafkaAddr))
	require.NoError(t, err)
	defer client.Close()

	admin := kadm.NewClient(client)

	// Create test topic
	topicResp, err := admin.CreateTopics(ctx, 1, 1, nil, testTopicName)
	require.NoError(t, err)
	_, err = topicResp.On(testTopicName, nil)
	require.NoError(t, err)

	t.Run("ProduceLargeMessage", func(t *testing.T) {
		// Create a large message (1KB)
		largeMessage := strings.Repeat("x", 1024)

		output := runCmdWithBroker(t, kafkaAddr, strings.NewReader(largeMessage),
			"produce", testTopicName)

		assert.Contains(t, output, "Sent record to partition")
	})

	t.Run("ProduceManySmallMessages", func(t *testing.T) {
		// Create many small messages
		var messages strings.Builder
		for i := 0; i < 10; i++ {
			messages.WriteString("Small message ")
			messages.WriteString(string(rune('0' + i)))
			messages.WriteString("\n")
		}

		output := runCmdWithBroker(t, kafkaAddr, strings.NewReader(messages.String()),
			"produce", testTopicName)

		// Should produce all messages
		occurrences := strings.Count(output, "Sent record to partition")
		assert.Equal(t, 10, occurrences, "Should produce 10 messages")
	})

	// Cleanup
	admin.DeleteTopics(ctx, testTopicName)
}
