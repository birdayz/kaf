package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTopics(t *testing.T) {
	kafkaAddr, cleanup := setupKafkaForTest(t)
	defer cleanup()
	
	out := runCmdWithBroker(t, kafkaAddr, nil, "topics")
	// Just check that it runs without error - no specific topics expected
	require.NotEmpty(t, out)
}
