package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNode(t *testing.T) {
	out := runCmdWithBroker(t, nil, "node", "ls")
	require.Contains(t, out, kafkaAddr)
}
