package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProduceConsume(t *testing.T) {
	msg := "this is a test"

	t.Run("produce a message", func(t *testing.T) {
		buf := bytes.NewBufferString(msg)

		out := runCmdWithBroker(t, buf, "produce", "gnomock-kafka")
		require.Contains(t, out, "Sent record")
	})

	t.Run("consume a message", func(t *testing.T) {
		out := runCmdWithBroker(t, nil, "consume", "gnomock-kafka")
		require.Contains(t, out, msg)
	})
}
