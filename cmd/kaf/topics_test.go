package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTopics(t *testing.T) {
	out := runCmdWithBroker(t, nil, "topics")
	require.Contains(t, out, "kaf-testing")
	require.Contains(t, out, "gnomock-kafka")
}
