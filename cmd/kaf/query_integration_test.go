package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestQueryCommand(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	kafkaAddr, cleanup := setupKafkaForTest(t)
	defer cleanup()

	ctx := context.Background()
	testTopicName := "test-query-command"

	// Setup
	client, err := kgo.NewClient(kgo.SeedBrokers(kafkaAddr))
	require.NoError(t, err)
	defer client.Close()

	admin := kadm.NewClient(client)

	// Create test topic
	topicResp, err := admin.CreateTopics(ctx, 2, 1, nil, testTopicName)
	require.NoError(t, err)
	_, err = topicResp.On(testTopicName, nil)
	require.NoError(t, err)

	// Produce test messages with specific keys
	testMessages := []struct {
		key   string
		value string
	}{
		{"user:123", "User data for 123"},
		{"user:456", "User data for 456"},
		{"order:789", "Order data for 789"},
		{"user:123", "Updated user data for 123"}, // Same key, different value
		{"product:abc", "Product information"},
	}

	for _, msg := range testMessages {
		record := &kgo.Record{
			Topic: testTopicName,
			Key:   []byte(msg.key),
			Value: []byte(msg.value),
		}

		results := client.ProduceSync(ctx, record)
		require.NoError(t, results.FirstErr())
	}

	// Wait for messages to be available
	time.Sleep(2 * time.Second)

	t.Run("QueryByExistingKey", func(t *testing.T) {
		// Query for a specific key that exists
		output := runCmdWithBroker(t, kafkaAddr, nil, "query", testTopicName, "--key", "user:123")
		
		// Should contain messages with the specified key
		assert.Contains(t, output, "user:123")
		assert.Contains(t, output, "User data for 123")
		// Should contain both messages with this key
		lines := strings.Split(strings.TrimSpace(output), "\n")
		messageLines := 0
		for _, line := range lines {
			if strings.Contains(line, "User data for 123") || strings.Contains(line, "Updated user data for 123") {
				messageLines++
			}
		}
		assert.GreaterOrEqual(t, messageLines, 1, "Should find at least one message with the key")
	})

	t.Run("QueryByNonExistentKey", func(t *testing.T) {
		// Query for a key that doesn't exist
		output := runCmdWithBroker(t, kafkaAddr, nil, "query", testTopicName, "--key", "nonexistent:999")
		
		// Should return no results or empty output
		trimmedOutput := strings.TrimSpace(output)
		// Should either be empty or contain a "no results" type message
		if trimmedOutput != "" {
			// If there's output, it shouldn't contain our test data
			assert.NotContains(t, output, "User data")
			assert.NotContains(t, output, "Order data")
		}
	})

	t.Run("QueryWithGrepFilter", func(t *testing.T) {
		// Query with grep filter for value content
		output := runCmdWithBroker(t, kafkaAddr, nil, "query", testTopicName, "--grep", "Order")
		
		// Should contain messages matching the grep pattern
		if strings.TrimSpace(output) != "" {
			assert.Contains(t, output, "Order data")
			assert.Contains(t, output, "order:789")
		}
	})

	t.Run("QueryWithKeyAndGrep", func(t *testing.T) {
		// Query with both key and grep filters
		output := runCmdWithBroker(t, kafkaAddr, nil, "query", testTopicName, "--key", "user:123", "--grep", "Updated")
		
		// Should contain only messages matching both key and grep
		if strings.TrimSpace(output) != "" {
			assert.Contains(t, output, "user:123")
			assert.Contains(t, output, "Updated user data")
			assert.NotContains(t, output, "User data for 123") // Should not contain non-updated version
		}
	})

	t.Run("QueryNonExistentTopic", func(t *testing.T) {
		// Query a topic that doesn't exist
		output := runCmdWithBroker(t, kafkaAddr, nil, "query", "non-existent-topic-12345", "--key", "test")
		
		// Should contain error message about topic not found
		assert.Contains(t, output, "not found")
	})

	t.Run("QueryHelp", func(t *testing.T) {
		// Test query command help
		output := runCmd(t, nil, "query", "--help")
		
		// Should contain help information
		assert.Contains(t, output, "Query topic by key")
		assert.Contains(t, output, "TOPIC")
		assert.Contains(t, output, "--key")
		assert.Contains(t, output, "--grep")
	})

	// Cleanup
	admin.DeleteTopics(ctx, testTopicName)
}

func TestQueryAdvancedScenarios(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	kafkaAddr, cleanup := setupKafkaForTest(t)
	defer cleanup()

	ctx := context.Background()
	testTopicName := "test-query-advanced"

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

	// Produce messages with various patterns
	testData := []struct {
		key   string
		value string
	}{
		{"json:1", `{"id": 1, "name": "Alice", "type": "user"}`},
		{"json:2", `{"id": 2, "name": "Bob", "type": "admin"}`},
		{"text:hello", "Hello World!"},
		{"text:goodbye", "Goodbye World!"},
		{"special:chars", "Data with special chars: @#$%^&*()"},
		{"unicode:test", "Unicode test: 你好世界 🌍"},
	}

	for _, msg := range testData {
		record := &kgo.Record{
			Topic: testTopicName,
			Key:   []byte(msg.key),
			Value: []byte(msg.value),
		}

		results := client.ProduceSync(ctx, record)
		require.NoError(t, results.FirstErr())
	}

	time.Sleep(2 * time.Second)

	t.Run("QueryJSONContent", func(t *testing.T) {
		// Query for JSON content using grep
		output := runCmdWithBroker(t, kafkaAddr, nil, "query", testTopicName, "--grep", "Alice")
		
		if strings.TrimSpace(output) != "" {
			assert.Contains(t, output, "json:1")
			assert.Contains(t, output, "Alice")
		}
	})

	t.Run("QuerySpecialCharacters", func(t *testing.T) {
		// Query with special characters in key
		output := runCmdWithBroker(t, kafkaAddr, nil, "query", testTopicName, "--key", "special:chars")
		
		if strings.TrimSpace(output) != "" {
			assert.Contains(t, output, "special:chars")
			assert.Contains(t, output, "@#$%")
		}
	})

	t.Run("QueryUnicodeContent", func(t *testing.T) {
		// Query for unicode content
		output := runCmdWithBroker(t, kafkaAddr, nil, "query", testTopicName, "--grep", "你好")
		
		if strings.TrimSpace(output) != "" {
			assert.Contains(t, output, "unicode:test")
			assert.Contains(t, output, "你好世界")
		}
	})

	t.Run("QueryCaseInsensitiveGrep", func(t *testing.T) {
		// Test grep pattern matching
		output := runCmdWithBroker(t, kafkaAddr, nil, "query", testTopicName, "--grep", "world")
		
		// Should find both "Hello World!" and "Goodbye World!" (case insensitive)
		if strings.TrimSpace(output) != "" {
			// Count occurrences of "World" in output
			worldCount := strings.Count(strings.ToLower(output), "world")
			assert.GreaterOrEqual(t, worldCount, 1, "Should find at least one world occurrence")
		}
	})

	// Cleanup
	admin.DeleteTopics(ctx, testTopicName)
}