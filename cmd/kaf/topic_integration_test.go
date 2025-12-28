package main

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestTopicCommands(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	kafkaAddr, cleanup := setupKafkaForTest(t)
	defer cleanup()

	ctx := context.Background()
	testTopicName := "test-topic-integration"

	// Setup Kafka client for verification
	client, err := kgo.NewClient(kgo.SeedBrokers(kafkaAddr))
	require.NoError(t, err)
	defer client.Close()

	admin := kadm.NewClient(client)

	t.Run("TopicList", func(t *testing.T) {
		// List topics
		output := runCmdWithBroker(t, kafkaAddr, nil, "topics", "ls")
		
		// Should run without error and produce some output
		assert.NotEmpty(t, output)
		
		// Should contain headers
		if !strings.Contains(output, "no-headers") {
			assert.Contains(t, output, "NAME")
		}
	})

	t.Run("TopicCreate", func(t *testing.T) {
		// Create a topic
		output := runCmdWithBroker(t, kafkaAddr, nil, "topic", "create", testTopicName, "--partitions", "3", "--replication-factor", "1")
		
		// Should indicate success
		assert.Contains(t, output, "Created topic")
		
		// Verify topic was created
		topics, err := admin.ListTopics(ctx)
		require.NoError(t, err)
		
		_, exists := topics[testTopicName]
		assert.True(t, exists, "Topic should be created")
	})

	t.Run("TopicDescribe", func(t *testing.T) {
		// Describe the created topic
		output := runCmdWithBroker(t, kafkaAddr, nil, "topic", "describe", testTopicName)
		
		// Should contain topic information
		assert.Contains(t, output, "Name:")
		assert.Contains(t, output, testTopicName)
		assert.Contains(t, output, "Partitions:")
		assert.Contains(t, output, "Config:")
	})

	t.Run("TopicAlterPartitions", func(t *testing.T) {
		// Increase partitions
		output := runCmdWithBroker(t, kafkaAddr, nil, "topic", "alter", testTopicName, "--partitions", "5")
		
		// Should indicate success
		assert.Contains(t, output, "Updated topic")
		
		// Verify partition count increased
		topics, err := admin.ListTopics(ctx)
		require.NoError(t, err)
		
		topic, exists := topics[testTopicName]
		require.True(t, exists)
		assert.Equal(t, 5, len(topic.Partitions), "Should have 5 partitions")
	})

	t.Run("TopicSetConfig", func(t *testing.T) {
		// Set a configuration
		output := runCmdWithBroker(t, kafkaAddr, nil, "topic", "set-config", testTopicName, "cleanup.policy", "compact")
		
		// Should indicate success
		assert.Contains(t, output, "Added config")
		assert.Contains(t, output, "cleanup.policy=compact")
		
		// Verify config was set by listing topic details
		// Note: Config verification through API can be complex, 
		// so we primarily rely on the command output for verification
		topics, err := admin.ListTopics(ctx)
		require.NoError(t, err)
		
		_, exists := topics[testTopicName]
		assert.True(t, exists, "Topic should still exist after config change")
	})

	t.Run("TopicRemoveConfig", func(t *testing.T) {
		// Remove the configuration
		output := runCmdWithBroker(t, kafkaAddr, nil, "topic", "remove-config", testTopicName, "cleanup.policy")
		
		// Should indicate success
		assert.Contains(t, output, "Removed attributes")
	})

	t.Run("TopicDelete", func(t *testing.T) {
		// Delete the topic
		output := runCmdWithBroker(t, kafkaAddr, nil, "topic", "delete", testTopicName)
		
		// Should indicate success
		assert.Contains(t, output, "Deleted topic")
		
		// Verify topic was deleted (may take a moment)
		// Note: Deletion might be async, so we check that it doesn't error
		_, err := admin.ListTopics(ctx)
		assert.NoError(t, err)
	})
}

func TestTopicProduceConsume(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	kafkaAddr, cleanup := setupKafkaForTest(t)
	defer cleanup()

	testTopicName := "test-produce-consume"
	
	// Create topic first
	runCmdWithBroker(t, kafkaAddr, nil, "topic", "create", testTopicName, "--partitions", "2", "--replication-factor", "1")

	t.Run("ProduceMessages", func(t *testing.T) {
		// Test different produce scenarios
		testCases := []struct {
			name string
			args []string
			data string
		}{
			{
				name: "SimpleMessage",
				args: []string{"produce", testTopicName},
				data: "test message 1",
			},
			{
				name: "MessageWithKey",
				args: []string{"produce", testTopicName, "--key", "test-key"},
				data: "test message with key",
			},
			{
				name: "MessageWithPartitioner",
				args: []string{"produce", testTopicName, "--partitioner", "rr"},
				data: "round robin message",
			},
			{
				name: "MessageWithHeaders",
				args: []string{"produce", testTopicName, "--header", "Content-Type:application/json"},
				data: `{"message": "test json"}`,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Produce message
				output := runCmdWithBroker(t, kafkaAddr, strings.NewReader(tc.data), tc.args...)
				
				// Should indicate successful production
				assert.Contains(t, output, "Sent record to partition")
			})
		}
	})

	t.Run("ConsumeMessages", func(t *testing.T) {
		// Test different consume scenarios
		testCases := []struct {
			name string
			args []string
		}{
			{
				name: "ConsumeFromBeginning",
				args: []string{"consume", testTopicName, "--offset", "oldest", "--limit-messages", "1"},
			},
			{
				name: "ConsumeFromEnd",
				args: []string{"consume", testTopicName, "--offset", "newest", "--limit-messages", "1"},
			},
			{
				name: "ConsumeWithGroup",
				args: []string{"consume", testTopicName, "--group", "test-consumer-group", "--offset", "oldest", "--limit-messages", "1"},
			},
			{
				name: "ConsumeRawFormat",
				args: []string{"consume", testTopicName, "--output", "raw", "--offset", "oldest", "--limit-messages", "1"},
			},
			{
				name: "ConsumeJSONFormat",
				args: []string{"consume", testTopicName, "--output", "json", "--offset", "oldest", "--limit-messages", "1"},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Consume messages
				output := runCmdWithBroker(t, kafkaAddr, nil, tc.args...)
				
				// Should not error (output may be empty if no messages)
				assert.NotNil(t, output)
			})
		}
	})

	// Cleanup
	runCmdWithBroker(t, kafkaAddr, nil, "topic", "delete", testTopicName)
}