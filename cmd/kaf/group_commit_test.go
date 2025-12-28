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

func TestGroupCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	kafkaAddr, cleanup := setupKafkaForTest(t)
	defer cleanup()

	ctx := context.Background()
	topicName := "test-group-commit-topic"
	groupName := "test-group-commit-group"

	// Create Kafka client for setup
	client, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaAddr),
	)
	require.NoError(t, err)
	defer client.Close()

	admin := kadm.NewClient(client)

	// Create test topic with multiple partitions
	topicResp, err := admin.CreateTopics(ctx, 3, 1, nil, topicName)
	require.NoError(t, err)

	topicCreateResp, topicCreateErr := topicResp.On(topicName, nil)
	require.NoError(t, topicCreateErr)
	require.NotNil(t, topicCreateResp)

	// Wait for topic to be ready
	time.Sleep(2 * time.Second)

	// Produce some test messages to different partitions
	records := []*kgo.Record{
		{Topic: topicName, Partition: 0, Value: []byte("message-0-1")},
		{Topic: topicName, Partition: 0, Value: []byte("message-0-2")},
		{Topic: topicName, Partition: 1, Value: []byte("message-1-1")},
		{Topic: topicName, Partition: 1, Value: []byte("message-1-2")},
		{Topic: topicName, Partition: 2, Value: []byte("message-2-1")},
	}

	for _, record := range records {
		results := client.ProduceSync(ctx, record)
		require.NoError(t, results.FirstErr())
	}

	// Test 1: Commit specific partition offset
	t.Run("CommitSpecificPartitionOffset", func(t *testing.T) {
		// Use kaf group commit command to set offset for partition 0 to position 1
		output := runCmdWithBroker(t, kafkaAddr, nil, "group", "commit", groupName, "--topic", topicName, "--partition", "0", "--offset", "1", "--noconfirm")

		// Verify the command executed successfully
		assert.Contains(t, output, "Successfully committed offsets")

		// Verify the committed offset
		offsets, err := admin.FetchOffsets(ctx, groupName)
		require.NoError(t, err)

		topicOffsets, exists := offsets[topicName]
		require.True(t, exists, "Topic should have committed offsets")

		partitionOffset, exists := topicOffsets[0]
		require.True(t, exists, "Partition 0 should have committed offset")
		assert.Equal(t, int64(1), partitionOffset.At, "Offset should be committed at position 1")
	})

	// Test 2: Commit all partitions to latest
	t.Run("CommitAllPartitionsToLatest", func(t *testing.T) {
		// Use kaf group commit command to set all partitions to latest
		output := runCmdWithBroker(t, kafkaAddr, nil, "group", "commit", groupName, "--topic", topicName, "--all-partitions", "--offset", "newest", "--noconfirm")

		// Verify the command executed successfully
		assert.Contains(t, output, "Successfully committed offsets")

		// Verify all partitions have committed offsets at the end
		offsets, err := admin.FetchOffsets(ctx, groupName)
		require.NoError(t, err)

		topicOffsets, exists := offsets[topicName]
		require.True(t, exists, "Topic should have committed offsets")

		// Get high water marks to compare
		endOffsets, err := admin.ListEndOffsets(ctx, topicName)
		require.NoError(t, err)
		topicEndOffsets := endOffsets[topicName]

		// Verify all partitions are committed to their high water marks
		for partition := int32(0); partition < 3; partition++ {
			partitionOffset, exists := topicOffsets[partition]
			require.True(t, exists, "Partition %d should have committed offset", partition)

			expectedOffset := topicEndOffsets[partition].Offset
			assert.Equal(t, expectedOffset, partitionOffset.At, "Partition %d should be committed to high water mark", partition)
		}
	})

	// Test 3: Commit using offset map (JSON format)
	t.Run("CommitUsingOffsetMap", func(t *testing.T) {
		// Use kaf group commit command with offset map
		offsetMap := `{"0": 1, "1": 1, "2": 0}`
		output := runCmdWithBroker(t, kafkaAddr, nil, "group", "commit", groupName, "--topic", topicName, "--offset-map", offsetMap, "--noconfirm")

		// Verify the command executed successfully
		assert.Contains(t, output, "Successfully committed offsets")

		// Verify the committed offsets match the map
		offsets, err := admin.FetchOffsets(ctx, groupName)
		require.NoError(t, err)

		topicOffsets, exists := offsets[topicName]
		require.True(t, exists, "Topic should have committed offsets")

		expectedOffsets := map[int32]int64{0: 1, 1: 1, 2: 0}
		for partition, expectedOffset := range expectedOffsets {
			partitionOffset, exists := topicOffsets[partition]
			require.True(t, exists, "Partition %d should have committed offset", partition)
			assert.Equal(t, expectedOffset, partitionOffset.At, "Partition %d should have correct offset", partition)
		}
	})

	// Test 4: Verify group describe shows correct lag
	t.Run("VerifyGroupDescribeShowsLag", func(t *testing.T) {
		// First commit some offsets that create lag
		offsetMap := `{"0": 0, "1": 0, "2": 0}`
		runCmdWithBroker(t, kafkaAddr, nil, "group", "commit", groupName, "--topic", topicName, "--offset-map", offsetMap, "--noconfirm")

		// Describe the group to see lag information
		output := runCmdWithBroker(t, kafkaAddr, nil, "group", "describe", groupName)

		// Verify the output contains the group information
		assert.Contains(t, output, "Group ID:")
		assert.Contains(t, output, groupName)
		assert.Contains(t, output, "Offsets:")
		assert.Contains(t, output, topicName)

		// Should show lag for all partitions since we committed to offset 0 but there are messages
		lines := strings.Split(output, "\n")
		lagFound := false
		for _, line := range lines {
			if strings.Contains(line, "Lag") && !strings.Contains(line, "---") {
				lagFound = true
				break
			}
		}
		assert.True(t, lagFound, "Output should contain lag information")
	})

	// Test 5: Verify error handling for invalid group state
	t.Run("ErrorHandlingForActiveGroup", func(t *testing.T) {
		// Create a consumer that joins the group to make it "Stable"
		consumerClient, err := kgo.NewClient(
			kgo.SeedBrokers(kafkaAddr),
			kgo.ConsumerGroup(groupName),
			kgo.ConsumeTopics(topicName),
		)
		require.NoError(t, err)
		defer consumerClient.Close()

		// Poll multiple times to ensure the consumer fully joins the group
		// and triggers partition assignment
		for i := 0; i < 5; i++ {
			pollCtx, pollCancel := context.WithTimeout(context.Background(), 1*time.Second)
			consumerClient.PollFetches(pollCtx)
			pollCancel()
			time.Sleep(500 * time.Millisecond)
		}

		// Wait for the group to stabilize and verify it has active members
		var groupState string
		var memberCount int
		maxAttempts := 10
		for attempt := 0; attempt < maxAttempts; attempt++ {
			describeCtx, describeCancel := context.WithTimeout(context.Background(), 2*time.Second)
			groupDescs, describeErr := admin.DescribeGroups(describeCtx, groupName)
			describeCancel()

			if describeErr == nil && len(groupDescs) > 0 {
				for _, desc := range groupDescs {
					groupState = desc.State
					memberCount = len(desc.Members)
					t.Logf("Attempt %d: Group state=%s, members=%d", attempt+1, groupState, memberCount)

					if memberCount > 0 && (groupState == "Stable" || groupState == "PreparingRebalance" || groupState == "CompletingRebalance") {
						// Group has active consumers
						goto GroupReady
					}
				}
			}

			time.Sleep(1 * time.Second)
		}

		t.Fatalf("Consumer did not join group after %d attempts. Last state: %s, members: %d", maxAttempts, groupState, memberCount)

	GroupReady:
		t.Logf("Group is ready with state=%s and %d members", groupState, memberCount)

		// Try to commit offsets while group has active consumers
		// This should fail with an error about active consumers
		output, err := runCmdWithBrokerAllowFail(t, kafkaAddr, nil, "group", "commit", groupName, "--topic", topicName, "--partition", "0", "--offset", "1", "--noconfirm")

		t.Logf("Command output: %s", output)
		t.Logf("Command error: %v", err)

		// Should fail with error about active consumers
		require.Error(t, err, "Command should fail when group has active consumers")
		assert.Contains(t, output, "active consumers")
	})

	// Cleanup: Delete the test topic
	_, err = admin.DeleteTopics(ctx, topicName)
	require.NoError(t, err)
}

func TestGroupCommitEdgeCases(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	kafkaAddr, cleanup := setupKafkaForTest(t)
	defer cleanup()

	// Test with non-existent topic
	t.Run("NonExistentTopic", func(t *testing.T) {
		// Try to commit to a topic that doesn't exist
		// The command should fail quickly with a "Topic not found" error
		output, err := runCmdWithBrokerAllowFail(t, kafkaAddr, nil, "group", "commit", "test-group", "--topic", "definitely-does-not-exist-topic", "--partition", "0", "--offset", "1", "--noconfirm")

		// Should fail with an error
		require.Error(t, err, "Command should fail for non-existent topic")
		// Check output contains error message about topic not found
		assert.Contains(t, output, "not found")
	})

	// Test with invalid offset format
	t.Run("InvalidOffsetFormat", func(t *testing.T) {
		output, err := runCmdWithBrokerAllowFail(t, kafkaAddr, nil, "group", "commit", "test-group", "--topic", "test-topic", "--partition", "0", "--offset", "invalid-offset", "--noconfirm")

		// Should fail with an error about invalid offset
		require.Error(t, err, "Command should fail for invalid offset")
		assert.Contains(t, output, "offset")
	})

	// Test with invalid JSON in offset map
	t.Run("InvalidOffsetMapJSON", func(t *testing.T) {
		output, err := runCmdWithBrokerAllowFail(t, kafkaAddr, nil, "group", "commit", "test-group", "--topic", "test-topic", "--offset-map", "invalid-json", "--noconfirm")

		// Should fail with an error about JSON format
		require.Error(t, err, "Command should fail for invalid JSON")
		assert.Contains(t, output, "JSON")
	})
}
