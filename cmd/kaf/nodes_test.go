package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodes(t *testing.T) {
	out := runCmdWithBroker(t, nil, "nodes")
	require.Contains(t, out, kafkaAddr)
}
