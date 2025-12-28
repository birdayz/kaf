package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProduceConsume(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	kafkaAddr, cleanup := setupKafkaForTest(t)
	defer cleanup()
	
	msg := "this is a test"
	topicName := "test-produce-consume"

	// Create the topic first to ensure it exists
	messages := []struct {
		key     string
		value   string
		headers map[string]string
	}{
		{key: "", value: msg},  // We'll produce our own message, this is just to create the topic
	}
	setupConsumeTestTopic(t, kafkaAddr, topicName, messages)

	t.Run("produce a message", func(t *testing.T) {
		buf := bytes.NewBufferString(msg)

		out := runCmdWithBroker(t, kafkaAddr, buf, "produce", topicName)
		require.Contains(t, out, "Sent record")
	})

	t.Run("consume a message", func(t *testing.T) {
		// Use our improved consume approach
		output := runConsumeDirectly(t, kafkaAddr, ConsumeOptions{
			Topic:         topicName,
			OffsetFlag:    "oldest",
			LimitMessages: 2, // Get both the setup message and our produced message
		})
		require.Contains(t, output, msg)
	})
}
