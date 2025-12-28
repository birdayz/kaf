package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTopic(t *testing.T) {
	kafkaAddr, cleanup := setupKafkaForTest(t)
	defer cleanup()
	
	newTopic := fmt.Sprintf("new-topic-%d", time.Now().Unix())

	t.Run("ls before new topic", func(t *testing.T) {
		out := runCmdWithBroker(t, kafkaAddr, nil, "topic", "ls")
		require.NotContains(t, out, newTopic)
	})

	t.Run("create new topic", func(t *testing.T) {
		out := runCmdWithBroker(t, kafkaAddr, nil, "topic", "create", newTopic)
		require.Contains(t, out, "Created topic!")
		require.Contains(t, out, newTopic)
	})

	t.Run("ls", func(t *testing.T) {
		out := runCmdWithBroker(t, kafkaAddr, nil, "topic", "ls")
		require.Contains(t, out, newTopic)
	})

	t.Run("describe", func(t *testing.T) {
		out := runCmdWithBroker(t, kafkaAddr, nil, "topic", "describe", newTopic)
		require.Contains(t, out, newTopic)
	})

	t.Run("delete", func(t *testing.T) {
		out := runCmdWithBroker(t, kafkaAddr, nil, "topic", "delete", newTopic)
		require.Contains(t, out, fmt.Sprintf("Deleted topic %s!", newTopic))
	})

	t.Run("ls after deleted", func(t *testing.T) {
		out := runCmdWithBroker(t, kafkaAddr, nil, "topic", "ls")
		require.NotContains(t, out, newTopic)
	})
}
