package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodes(t *testing.T) {
	kafkaAddr := getSharedKafka(t)
	
	out := runCmdWithBroker(t, kafkaAddr, nil, "nodes")
	require.Contains(t, out, kafkaAddr)
}
