package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNode(t *testing.T) {
	kafkaAddr, cleanup := setupKafkaForTest(t)
	defer cleanup()
	
	out := runCmdWithBroker(t, kafkaAddr, nil, "node", "ls")
	require.Contains(t, out, kafkaAddr)
}
