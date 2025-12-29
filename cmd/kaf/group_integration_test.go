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

func TestGroupCommands(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	kafkaAddr := getSharedKafka(t)

	ctx := context.Background()
	testTopicName := "test-group-commands"
	testGroupName := "test-group-commands-group"

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

	// Produce some test messages
	records := []*kgo.Record{
		{Topic: testTopicName, Partition: 0, Value: []byte("message-1")},
		{Topic: testTopicName, Partition: 0, Value: []byte("message-2")},
		{Topic: testTopicName, Partition: 1, Value: []byte("message-3")},
	}

	for _, record := range records {
		results := client.ProduceSync(ctx, record)
		require.NoError(t, results.FirstErr())
	}

	// Wait for messages to be available
	time.Sleep(2 * time.Second)

	t.Run("GroupsList", func(t *testing.T) {
		// List consumer groups
		output := runCmdWithBroker(t, kafkaAddr, nil, "groups", "ls")
		
		// Should run without error
		assert.NotNil(t, output)
		
		// May be empty initially, but should contain headers if not using --no-headers
		if !strings.Contains(output, "no-headers") && strings.TrimSpace(output) != "" {
			assert.Contains(t, output, "NAME")
		}
	})

	t.Run("GroupCommitOffsets", func(t *testing.T) {
		// Commit offsets for a group
		output := runCmdWithBroker(t, kafkaAddr, nil, "group", "commit", testGroupName, 
			"--topic", testTopicName, 
			"--partition", "0", 
			"--offset", "1", 
			"--noconfirm")
		
		// Should indicate success
		assert.Contains(t, output, "Successfully committed offsets")
		
		// Verify the offset was committed
		offsets, err := admin.FetchOffsets(ctx, testGroupName)
		require.NoError(t, err)
		
		topicOffsets, exists := offsets[testTopicName]
		require.True(t, exists)
		
		partitionOffset, exists := topicOffsets[0]
		require.True(t, exists)
		assert.Equal(t, int64(1), partitionOffset.At)
	})

	t.Run("GroupCommitAllPartitions", func(t *testing.T) {
		// Commit all partitions to latest
		output := runCmdWithBroker(t, kafkaAddr, nil, "group", "commit", testGroupName,
			"--topic", testTopicName,
			"--all-partitions",
			"--offset", "newest",
			"--noconfirm")
		
		// Should indicate success
		assert.Contains(t, output, "Successfully committed offsets")
	})

	t.Run("GroupCommitWithOffsetMap", func(t *testing.T) {
		// Use offset map
		offsetMap := `{"0": 1, "1": 1}`
		output := runCmdWithBroker(t, kafkaAddr, nil, "group", "commit", testGroupName,
			"--topic", testTopicName,
			"--offset-map", offsetMap,
			"--noconfirm")
		
		// Should indicate success
		assert.Contains(t, output, "Successfully committed offsets")
		
		// Verify offsets
		offsets, err := admin.FetchOffsets(ctx, testGroupName)
		require.NoError(t, err)
		
		topicOffsets, exists := offsets[testTopicName]
		require.True(t, exists)
		
		assert.Equal(t, int64(1), topicOffsets[0].At)
		assert.Equal(t, int64(1), topicOffsets[1].At)
	})

	t.Run("GroupDescribe", func(t *testing.T) {
		// Describe the group
		output := runCmdWithBroker(t, kafkaAddr, nil, "group", "describe", testGroupName)
		
		// Should contain group information
		assert.Contains(t, output, "Group ID:")
		assert.Contains(t, output, testGroupName)
		assert.Contains(t, output, "State:")
		assert.Contains(t, output, "Offsets:")
		assert.Contains(t, output, testTopicName)
		assert.Contains(t, output, "Lag")
	})

	t.Run("GroupDescribeWithTopicFilter", func(t *testing.T) {
		// Describe group with topic filter
		output := runCmdWithBroker(t, kafkaAddr, nil, "group", "describe", testGroupName, "--topic", testTopicName)
		
		// Should contain filtered topic information
		assert.Contains(t, output, testTopicName)
	})

	t.Run("GroupPeek", func(t *testing.T) {
		// Peek messages from group offset
		output := runCmdWithBroker(t, kafkaAddr, nil, "group", "peek", testGroupName, "--topics", testTopicName, "--after", "1")
		
		// Should run without error (may have no output if no messages to peek)
		assert.NotNil(t, output)
	})

	t.Run("GroupDelete", func(t *testing.T) {
		// Delete the group
		output := runCmdWithBroker(t, kafkaAddr, nil, "group", "delete", testGroupName)
		
		// Should indicate success
		assert.Contains(t, output, "Deleted consumer group")
	})

	// Cleanup
	admin.DeleteTopics(ctx, testTopicName)
}

func TestGroupWithActiveConsumer(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	kafkaAddr := getSharedKafka(t)

	ctx := context.Background()
	testTopicName := "test-active-consumer"
	testGroupName := "test-active-consumer-group"

	// Setup
	setupClient, err := kgo.NewClient(kgo.SeedBrokers(kafkaAddr))
	require.NoError(t, err)
	defer setupClient.Close()

	admin := kadm.NewClient(setupClient)

	// Create test topic
	topicResp, err := admin.CreateTopics(ctx, 1, 1, nil, testTopicName)
	require.NoError(t, err)
	_, err = topicResp.On(testTopicName, nil)
	require.NoError(t, err)

	// Create an active consumer
	consumerClient, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaAddr),
		kgo.ConsumerGroup(testGroupName),
		kgo.ConsumeTopics(testTopicName),
	)
	require.NoError(t, err)
	
	// Poll once to join the group
	go func() {
		for i := 0; i < 3; i++ {
			consumerClient.PollFetches(ctx)
			time.Sleep(1 * time.Second)
		}
	}()
	
	// Wait for consumer to join
	time.Sleep(3 * time.Second)

	t.Run("DescribeActiveGroup", func(t *testing.T) {
		// Describe group with active consumer
		output := runCmdWithBroker(t, kafkaAddr, nil, "group", "describe", testGroupName)
		
		// Should show group is active
		assert.Contains(t, output, testGroupName)
		assert.Contains(t, output, "State:")
		// State should be Stable or similar (not Empty or Dead)
		lines := strings.Split(output, "\n")
		for _, line := range lines {
			if strings.Contains(line, "State:") {
				assert.NotContains(t, line, "Empty")
				assert.NotContains(t, line, "Dead")
				break
			}
		}
	})

	t.Run("CommitOffsetsWithActiveConsumer", func(t *testing.T) {
		// Try to commit offsets while group has active consumers
		output := runCmdWithBroker(t, kafkaAddr, nil, "group", "commit", testGroupName,
			"--topic", testTopicName,
			"--partition", "0",
			"--offset", "0",
			"--noconfirm")
		
		// Should fail or warn about active consumers
		assert.Contains(t, output, "active consumers")
	})

	// Cleanup
	consumerClient.Close()
	time.Sleep(2 * time.Second) // Wait for group to become empty
	admin.DeleteTopics(ctx, testTopicName)
}